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
//! talks to whom. Pull's natural hazard — a producer would re-run its sub-plan once per consumer
//! that pulls from it — is defused on the worker: `do_get` materializes each producer stage once
//! into a [`crate::stage_cache::StageCache`] and serves every consumer (and every output
//! partition) from that one buffer. So the `R` reduce stages that all pull the same map producer
//! now share a single execution. See [`crate::flight`] for the stage-id ticket and the cache.
//!
//! Serialization: [`LldbCodec`] is a real [`PhysicalExtensionCodec`], replacing the
//! `DefaultPhysicalExtensionCodec` that could only handle built-in nodes.
//!
//! **Fault tolerance.** A leaf that names exactly one worker makes that worker a single point of
//! failure for the whole query. Since a stage is content-addressed and materialized once per worker
//! (the [`crate::stage_cache::StageCache`] property), re-running it somewhere else is idempotent and
//! yields identical output — so the leaf now carries an ordered list of **fallback** workers and
//! reassigns itself on a transport failure. See [`FlightReaderExec::execute`] for the one
//! correctness constraint that buys (a partition is collected in full before anything is emitted)
//! and [`crate::retry`] for which failures are worth reassigning at all.

use std::any::Any;
use std::collections::HashSet;
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
use futures::TryStreamExt;

use crate::flight;
use crate::retry::RetryPolicy;

/// Reads one partition of a sub-plan that executes on a remote worker.
///
/// From the local plan's point of view this is a leaf: [`children`] is empty, because the inner
/// plan does not run here. Optimizer passes therefore leave the remote stage alone, which is
/// what we want — it was already planned by whoever built it.
///
/// [`children`]: ExecutionPlan::children
#[derive(Debug, Clone)]
pub struct FlightReaderExec {
    /// Worker to pull from first, e.g. `http://worker-1:50051`.
    worker_url: String,
    /// Ordered failover targets, tried after `worker_url` when a pull fails retriably. Empty means
    /// "no reassignment possible" — a one-worker fleet, or a leaf built by [`Self::new`].
    fallbacks: Vec<String>,
    /// Which partition of the remote plan to request.
    remote_partition: u32,
    /// The sub-plan the worker should execute. Deliberately **not** a child.
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl FlightReaderExec {
    /// Wrap `inner` so it runs on `worker_url` instead of locally, with no failover targets.
    ///
    /// This is the pre-fault-tolerance shape and it still means exactly what it always did: one
    /// worker, one chance. Use [`with_fallbacks`](Self::with_fallbacks) to hand the leaf the rest of
    /// the fleet.
    pub fn new(
        worker_url: impl Into<String>,
        remote_partition: u32,
        inner: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self::with_fallbacks(worker_url, Vec::new(), remote_partition, inner)
    }

    /// Wrap `inner` so it runs on `worker_url`, falling back through `fallbacks` in order if that
    /// worker is lost.
    ///
    /// The primary is unchanged by the presence of fallbacks — placement policy stays with the
    /// staging planner, and this list only says *where else the same work is valid*, which is
    /// everywhere, because the stage is content-addressed and re-materializes identically.
    pub fn with_fallbacks(
        worker_url: impl Into<String>,
        fallbacks: Vec<String>,
        remote_partition: u32,
        inner: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema = inner.schema();
        // A remote read hands back the producer's partition batch-for-batch, in order — Flight is a
        // stream, not a set — so whatever ordering the remote plan guarantees *within* a partition
        // survives the hop. Carrying that ordering across keeps a consumer like
        // `SortPreservingMergeExec` honest about what it is merging: without it a distributed sort
        // would report its output as unordered even though every input stream is sorted. Only the
        // ordering is carried, not partitioning — see below.
        let eq = match inner.properties().output_ordering() {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(schema, [ordering.iter().cloned()])
            }
            None => EquivalenceProperties::new(schema),
        };
        // We surface exactly one partition: the single remote partition we were asked to read.
        // The remote stream is finite, and — see `execute` — this node buffers it in full before
        // emitting, so downstream sees one complete batch set rather than an incremental trickle.
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            worker_url: worker_url.into(),
            fallbacks,
            remote_partition,
            inner,
            properties,
        }
    }

    pub fn worker_url(&self) -> &str {
        &self.worker_url
    }

    /// The failover targets, in the order they will be tried after the primary.
    pub fn fallbacks(&self) -> &[String] {
        &self.fallbacks
    }

    pub fn remote_partition(&self) -> u32 {
        self.remote_partition
    }

    /// The sub-plan that runs remotely.
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }

    /// Every worker this leaf may pull from, primary first, **deduplicated** preserving order.
    ///
    /// Dedup matters: the staging planner hands each leaf the rest of the fleet, and a fleet list
    /// that happens to contain the primary would otherwise spend one of the bounded attempts
    /// re-dialing the node we already know is gone.
    fn candidates(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(1 + self.fallbacks.len());
        for url in std::iter::once(&self.worker_url).chain(self.fallbacks.iter()) {
            if seen.insert(url.as_str()) {
                out.push(url.clone());
            }
        }
        out
    }
}

impl DisplayAs for FlightReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FlightReaderExec: worker={}, partition={}",
            self.worker_url, self.remote_partition
        )?;
        if !self.fallbacks.is_empty() {
            write!(f, ", fallbacks={}", self.fallbacks.len())?;
        }
        Ok(())
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

    /// Pull this leaf's partition, reassigning to a fallback worker if the primary is lost.
    ///
    /// # Why the whole partition is buffered before anything is emitted
    ///
    /// This node used to hand batches downstream as they arrived off the wire. That is the right
    /// shape for a healthy pull — and it is unrecoverable. A worker lost *mid-stream* has already
    /// given the consumer some batches; the replacement worker re-materializes the stage from the
    /// top (precisely what makes re-running it safe), so resuming the pull elsewhere would re-emit
    /// those batches and **silently duplicate rows**. A retry that corrupts the answer is worse than
    /// the failure it replaces.
    ///
    /// So the retry unit is "fetch this partition *completely*": nothing is emitted downstream until
    /// a full `Vec<RecordBatch>` is in hand, which makes reassignment safe by construction — either
    /// a candidate delivers the whole partition or its partial work is discarded.
    ///
    /// The cost is buffering one partition of one stage on the puller. That is the same order of
    /// memory the producing worker already holds for that stage in its [`StageCache`], so it does not
    /// change the fleet's memory profile in kind, only in placement. True incremental streaming with
    /// resume is possible but needs per-batch sequencing (a producer-side cursor so a reassigned pull
    /// can say "resume after batch *k*"); that is a follow-up, not built here.
    ///
    /// [`StageCache`]: crate::stage_cache::StageCache
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

        // Serialize eagerly so a bad plan fails here rather than inside the stream. It also means
        // every candidate ships byte-identical bytes, hence the same content-addressed stage id.
        let plan_bytes = serialize_plan(Arc::clone(&self.inner))?;
        let candidates = self.candidates();
        let remote_partition = self.remote_partition;
        let schema = self.schema();

        // Connecting is async but `execute` is not, so defer the whole round-trip into the
        // stream: nothing touches the network until the consumer polls.
        let stream = futures::stream::once(async move {
            let batches = flight::fetch_partition_with_failover(
                &candidates,
                remote_partition,
                plan_bytes,
                &RetryPolicy::default(),
            )
            .await
            // `anyhow::Error` is not itself a `std::error::Error`, but it converts into a
            // boxed one — which keeps the underlying cause chain (the candidates tried, connect
            // refused, worker-side status) instead of flattening it to a string.
            .map_err(|e: anyhow::Error| DataFusionError::External(e.into()))?;
            Ok::<_, DataFusionError>(futures::stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Upper bound on the fallback list a decoder will accept.
///
/// The count is read from the buffer before the URLs are, so a corrupt or hostile buffer could
/// otherwise ask us to reserve four billion strings. No plausible fleet has a thousand workers
/// standing behind one stage; a bound this loose never binds in practice and turns a garbage length
/// into an error instead of an allocation.
const MAX_ENCODED_FALLBACKS: u32 = 1024;

/// Extension codec that teaches `datafusion_proto` about [`FlightReaderExec`].
///
/// Wire format:
///
/// ```text
/// u32 LE url_len | url | u32 LE remote_partition
///   | u32 LE fallback_count | (u32 LE len | url)*
///   | serialized inner plan
/// ```
///
/// The fallback list sits **before** the inner plan because the plan is "the rest of the buffer" —
/// it has no length prefix of its own, so anything variable-length has to precede it.
///
/// There is no backward-compatibility obligation across this change: the coordinator and every
/// worker run the identical build (`CLAUDE.md`), which the Flight boundary already requires for
/// DataFusion plan bytes to mean the same thing on both ends.
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
        let (remote_partition, rest) = take_u32(rest)?;
        let (fallback_count, mut rest) = take_u32(rest)?;
        if fallback_count > MAX_ENCODED_FALLBACKS {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec fallback count {fallback_count} exceeds the maximum of \
                 {MAX_ENCODED_FALLBACKS}; the payload is corrupt"
            )));
        }
        let mut fallbacks = Vec::with_capacity(fallback_count as usize);
        for _ in 0..fallback_count {
            let (fallback, tail) = take_str(rest)?;
            fallbacks.push(fallback);
            rest = tail;
        }

        let inner = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            rest, ctx, self,
        )?;
        Ok(Arc::new(FlightReaderExec::with_fallbacks(
            url,
            fallbacks,
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

        if reader.fallbacks.len() > MAX_ENCODED_FALLBACKS as usize {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec has {} fallbacks, more than the {MAX_ENCODED_FALLBACKS} the wire \
                 format carries",
                reader.fallbacks.len()
            )));
        }

        put_str(buf, &reader.worker_url);
        buf.extend_from_slice(&reader.remote_partition.to_le_bytes());
        buf.extend_from_slice(&(reader.fallbacks.len() as u32).to_le_bytes());
        for fallback in &reader.fallbacks {
            put_str(buf, fallback);
        }
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

/// Append a length-prefixed string.
fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
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

    /// Order survives the hop, so the reader advertises it. A distributed sort leans on this: the
    /// coordinator's `SortPreservingMergeExec` merges remote sorted runs, and a reader that claimed
    /// no ordering would make the merged plan look unordered to anything inspecting it.
    #[tokio::test]
    async fn a_remote_read_carries_the_inner_plans_ordering() {
        let ctx = SessionContext::new();
        let unsorted = sample_plan(&ctx).await;
        assert!(
            unsorted.properties().output_ordering().is_none(),
            "test setup: a bare scan has no ordering"
        );
        assert!(
            FlightReaderExec::new("http://w:50051", 0, unsorted)
                .properties()
                .output_ordering()
                .is_none(),
            "no ordering to carry, none claimed"
        );

        let sorted = ctx
            .sql("SELECT n FROM t ORDER BY n")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let expected = sorted
            .properties()
            .output_ordering()
            .expect("test setup: an ORDER BY plan is ordered")
            .clone();
        let reader = FlightReaderExec::new("http://w:50051", 0, sorted);
        assert_eq!(
            reader.properties().output_ordering(),
            Some(&expected),
            "the reader must advertise the producer's ordering"
        );
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

    /// A leaf built by `new` has no failover targets — the pre-fault-tolerance meaning, preserved.
    #[tokio::test]
    async fn a_plain_reader_has_no_fallbacks() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::new("http://w:50051", 0, inner);
        assert!(reader.fallbacks().is_empty());
        assert_eq!(reader.candidates(), vec!["http://w:50051".to_string()]);
    }

    /// The primary must not burn one of the bounded attempts twice just because the fleet list it
    /// was handed also contains it.
    #[tokio::test]
    async fn candidates_are_the_primary_then_fallbacks_deduplicated_in_order() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::with_fallbacks(
            "http://w1:50051",
            vec![
                "http://w2:50051".into(),
                "http://w1:50051".into(), // the primary again
                "http://w3:50051".into(),
                "http://w2:50051".into(), // and a repeat fallback
            ],
            0,
            inner,
        );
        assert_eq!(
            reader.candidates(),
            vec![
                "http://w1:50051".to_string(),
                "http://w2:50051".to_string(),
                "http://w3:50051".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn fallbacks_round_trip_through_the_codec() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::with_fallbacks(
            "http://worker-2:50051",
            vec![
                "http://worker-3:50051".into(),
                "http://worker-4:50051".into(),
            ],
            3,
            inner,
        ));

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");
        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");

        assert_eq!(back.worker_url(), "http://worker-2:50051");
        assert_eq!(back.remote_partition(), 3);
        assert_eq!(
            back.fallbacks(),
            ["http://worker-3:50051", "http://worker-4:50051"],
            "failover targets must survive the trip, in order"
        );
        assert_eq!(back.schema(), plan.schema());
    }

    #[tokio::test]
    async fn an_empty_fallback_list_round_trips() {
        // The one-worker-fleet shape: a zero count, then straight into the inner plan bytes.
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::with_fallbacks(
            "http://only:50051",
            Vec::new(),
            0,
            inner,
        ));

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");
        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");
        assert!(back.fallbacks().is_empty());
    }

    /// A fallback list that promises more URLs than the buffer holds must error, and a wild count
    /// must be refused before it becomes an allocation.
    #[tokio::test]
    async fn a_truncated_or_absurd_fallback_list_errors_rather_than_panics() {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        // url "u", partition 0, promises 2 fallbacks, then runs out after one.
        let mut truncated = Vec::new();
        put_str(&mut truncated, "u");
        truncated.extend_from_slice(&0u32.to_le_bytes());
        truncated.extend_from_slice(&2u32.to_le_bytes());
        put_str(&mut truncated, "http://w:50051");
        assert!(codec.try_decode(&truncated, &[], &task_ctx).is_err());

        // A count far beyond the bound is refused outright.
        let mut absurd = Vec::new();
        put_str(&mut absurd, "u");
        absurd.extend_from_slice(&0u32.to_le_bytes());
        absurd.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = codec
            .try_decode(&absurd, &[], &task_ctx)
            .expect_err("an absurd fallback count must be rejected");
        assert!(
            err.to_string().contains("exceeds the maximum"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn codec_refuses_unknown_nodes() {
        let ctx = SessionContext::new();
        let plan = sample_plan(&ctx).await;
        let mut buf = Vec::new();
        assert!(LldbCodec.try_encode(plan, &mut buf).is_err());
    }
}
