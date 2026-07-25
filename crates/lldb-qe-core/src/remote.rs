//! Remote execution as a *plan node*, and the codec that lets it cross the wire.
//!
//! Up to now distribution lived outside the plan: [`crate::distributed`] builds SQL strings,
//! ships each to a worker, and reduces the results on the coordinator. That proves the idea but
//! it is not how an engine works — the coordinator is a bottleneck, and "distributed" applies to
//! one hand-written aggregation rather than to arbitrary SQL.
//!
//! [`FlightReaderExec`] closes that gap. It is a **leaf** node holding a sub-plan plus the
//! address of the worker that should run it. Executing it ships the sub-plan over Flight and
//! streams the results back, lazily.
//!
//! The payoff is composition. Because the node is serializable, a plan *containing*
//! `FlightReaderExec` leaves can itself be shipped to a worker:
//!
//! ```text
//!   coordinator                worker R (reduce)            workers M1..Mn (map)
//!   ───────────                ─────────────────            ────────────────────
//!   FlightReaderExec(R) ──────▶ FinalAggregate
//!                                 └─ FlightReaderExec(M1) ──▶ PartialAggregate(slice 1)
//!                                 └─ FlightReaderExec(Mn) ──▶ PartialAggregate(slice n)
//! ```
//!
//! The map output flows M→R directly; the coordinator only collects R's final rows. That is a
//! genuine worker-to-worker shuffle.
//!
//! **Why `do_get` and not `do_exchange`.** `do_exchange` is bidirectional streaming, which suits
//! a *push* shuffle where producers send partitions to consumers. This is a **pull** shuffle:
//! the consumer opens `do_get` against each producer. Pull is what Ballista does, it reuses the
//! transport already built, and it makes the plan tree the single source of truth about who
//! talks to whom. The tradeoff is that a producer re-runs its sub-plan per consumer that pulls
//! from it — fine while each stage has one consumer, and the reason a real engine eventually
//! materializes shuffle output. Noted, not hidden.
//!
//! Serialization: [`LldbCodec`] is a real [`PhysicalExtensionCodec`], replacing the
//! `DefaultPhysicalExtensionCodec` that could only handle built-in nodes.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{StreamExt, TryStreamExt};

use crate::flight;

/// Reads one partition of a sub-plan that executes on a remote worker.
///
/// From the local plan's point of view this is a leaf: [`children`] is empty, because the inner
/// plan does not run here. Optimizer passes therefore leave the remote stage alone, which is
/// what we want — it was already planned by whoever built it.
///
/// [`children`]: ExecutionPlan::children
#[derive(Debug, Clone)]
pub struct FlightReaderExec {
    /// Worker to pull from, e.g. `http://worker-1:50051`.
    worker_url: String,
    /// Which partition of the remote plan to request.
    remote_partition: u32,
    /// The sub-plan the worker should execute. Deliberately **not** a child.
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl FlightReaderExec {
    /// Wrap `inner` so it runs on `worker_url` instead of locally.
    pub fn new(
        worker_url: impl Into<String>,
        remote_partition: u32,
        inner: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema = inner.schema();
        // We surface exactly one partition: the single remote partition we were asked to read.
        // Batches arrive as the worker produces them, and a remote stream is finite.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            worker_url: worker_url.into(),
            remote_partition,
            inner,
            properties,
        }
    }

    pub fn worker_url(&self) -> &str {
        &self.worker_url
    }

    pub fn remote_partition(&self) -> u32 {
        self.remote_partition
    }

    /// The sub-plan that runs remotely.
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl DisplayAs for FlightReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FlightReaderExec: worker={}, partition={}",
            self.worker_url, self.remote_partition
        )
    }
}

impl ExecutionPlan for FlightReaderExec {
    fn name(&self) -> &str {
        "FlightReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf: `inner` executes on the worker, not in this plan tree.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(format!(
                "FlightReaderExec is a leaf and takes no children, got {}",
                children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec exposes 1 partition, asked for {partition}"
            )));
        }

        // Serialize eagerly so a bad plan fails here rather than inside the stream.
        let plan_bytes = serialize_plan(Arc::clone(&self.inner))?;
        let url = self.worker_url.clone();
        let remote_partition = self.remote_partition;
        let schema = self.schema();

        // Connecting is async but `execute` is not, so defer the whole round-trip into the
        // stream: nothing touches the network until the consumer polls.
        let stream = futures::stream::once(async move {
            flight::fetch_stream(url, remote_partition, plan_bytes)
                .await
                // `anyhow::Error` is not itself a `std::error::Error`, but it converts into a
                // boxed one — which keeps the underlying cause chain (bad URL, connect refused,
                // worker-side status) instead of flattening it to a string.
                .map_err(|e: anyhow::Error| DataFusionError::External(e.into()))
        })
        .map(|res| res.map(|s| s.map_err(|e| DataFusionError::External(Box::new(e)))))
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Extension codec that teaches `datafusion_proto` about [`FlightReaderExec`].
///
/// Wire format: `u32 LE url_len | url | u32 LE remote_partition | serialized inner plan`.
///
/// The output schema is *not* encoded — it is recovered by deserializing the inner plan and
/// asking it, which keeps the two from ever disagreeing. Encoding the inner plan recurses
/// through this same codec, so nested remote stages work.
#[derive(Debug, Default, Clone)]
pub struct LldbCodec;

impl PhysicalExtensionCodec for LldbCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (url, rest) = take_str(buf)?;
        let (remote_partition, plan_bytes) = take_u32(rest)?;

        let inner = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            plan_bytes, ctx, self,
        )?;
        Ok(Arc::new(FlightReaderExec::new(
            url,
            remote_partition,
            inner,
        )))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        let reader = node
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .ok_or_else(|| {
                DataFusionError::NotImplemented(format!("LldbCodec cannot encode {}", node.name()))
            })?;

        let url = reader.worker_url.as_bytes();
        buf.extend_from_slice(&(url.len() as u32).to_le_bytes());
        buf.extend_from_slice(url);
        buf.extend_from_slice(&reader.remote_partition.to_le_bytes());
        buf.extend_from_slice(&serialize_plan(Arc::clone(&reader.inner))?);
        Ok(())
    }
}

/// Serialize a plan with this codec, so nested [`FlightReaderExec`] nodes survive.
pub fn serialize_plan(plan: Arc<dyn ExecutionPlan>) -> DFResult<Vec<u8>> {
    let bytes =
        datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(plan, &LldbCodec)?;
    Ok(bytes.to_vec())
}

/// Deserialize a plan produced by [`serialize_plan`].
pub fn deserialize_plan(bytes: &[u8], ctx: &TaskContext) -> DFResult<Arc<dyn ExecutionPlan>> {
    datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(bytes, ctx, &LldbCodec)
}

fn take_u32(buf: &[u8]) -> DFResult<(u32, &[u8])> {
    if buf.len() < 4 {
        return Err(DataFusionError::Internal(format!(
            "truncated FlightReaderExec payload: wanted 4 bytes, had {}",
            buf.len()
        )));
    }
    let (head, rest) = buf.split_at(4);
    Ok((u32::from_le_bytes(head.try_into().expect("4 bytes")), rest))
}

fn take_str(buf: &[u8]) -> DFResult<(String, &[u8])> {
    let (len, rest) = take_u32(buf)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(DataFusionError::Internal(format!(
            "truncated FlightReaderExec url: wanted {len} bytes, had {}",
            rest.len()
        )));
    }
    let (s, rest) = rest.split_at(len);
    let s = String::from_utf8(s.to_vec()).map_err(|e| {
        DataFusionError::Internal(format!("FlightReaderExec url is not utf-8: {e}"))
    })?;
    Ok((s, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A tiny local plan to stand in for a remote stage.
    async fn sample_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx.sql("SELECT n FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn is_a_leaf_that_keeps_the_inner_schema() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::new("http://w:50051", 0, Arc::clone(&inner));

        assert!(
            reader.children().is_empty(),
            "the remote stage must not be a local child"
        );
        assert_eq!(reader.schema(), inner.schema());
        assert_eq!(reader.properties().partitioning.partition_count(), 1);
    }

    #[tokio::test]
    async fn round_trips_through_the_codec() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://worker-2:50051", 3, inner));

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");

        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");
        assert_eq!(back.worker_url(), "http://worker-2:50051");
        assert_eq!(back.remote_partition(), 3);
        assert_eq!(back.schema(), plan.schema());
    }

    /// The property the whole design rests on: a plan *containing* a remote read is itself
    /// serializable, so a reduce stage can be shipped to a worker with its map leaves intact.
    #[tokio::test]
    async fn nested_remote_stages_survive() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let map = Arc::new(FlightReaderExec::new("http://map:50051", 1, inner));
        let reduce: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://reduce:50051", 0, map));

        let bytes = serialize_plan(Arc::clone(&reduce)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");

        let outer = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("outer");
        assert_eq!(outer.worker_url(), "http://reduce:50051");
        let nested = outer
            .inner()
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("nested");
        assert_eq!(nested.worker_url(), "http://map:50051");
        assert_eq!(nested.remote_partition(), 1);
    }

    #[test]
    fn truncated_payloads_error_rather_than_panic() {
        assert!(take_u32(&[1, 2]).is_err());
        assert!(take_str(&[9, 0, 0, 0, b'a']).is_err());
    }

    #[tokio::test]
    async fn codec_refuses_unknown_nodes() {
        let ctx = SessionContext::new();
        let plan = sample_plan(&ctx).await;
        let mut buf = Vec::new();
        assert!(LldbCodec.try_encode(plan, &mut buf).is_err());
    }
}
