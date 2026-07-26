//! Arrow Flight transport: ship a physical plan to a worker, stream Arrow batches back.
//!
//! This is the foundation of distributed execution. The moves:
//!
//! 1. **Coordinator** serializes a physical sub-plan to bytes ([`datafusion_proto`]) and puts
//!    them in a Flight `Ticket` alongside a stage id and the partition index to run.
//! 2. **Worker** ([`WorkerFlightService`]) receives the ticket in `do_get`, materializes the
//!    plan's output **once per stage** into a [`StageCache`], and streams the requested
//!    partition's cached `RecordBatch`es back encoded as `FlightData`.
//! 3. **Coordinator** decodes the `FlightData` stream back into `RecordBatch`es.
//!
//! Arrow Flight is just gRPC that speaks Arrow natively — the batches cross the wire with no
//! row-by-row (de)serialization.
//!
//! # Materialize-once shuffle
//!
//! This is a **pull** shuffle: each consumer opens `do_get` against a producer. Earlier, `do_get`
//! deserialized the ticket's plan and called `plan.execute(partition, ..)` on *every* request, so a
//! producer pulled by `R` consumers (e.g. the `R` reduce stages of a partitioned hash join, all
//! pulling different partitions of one shared map producer) re-ran its whole scan + partial `R`
//! times — an `M×R` blowup across `M` producers. Now the worker keys on a **stage id** carried in
//! the ticket, runs the producer's plan exactly once via a [`StageCache`], buffers all its output
//! partitions, and serves every consumer straight from that buffer. The producer executes once; the
//! consumers each still get complete, correct output. See [`crate::stage_cache`] for the design
//! (single-flight, all-partitions materialization, LRU eviction, execution metric).
//!
//! Ticket wire format (little-endian): `stage_id: u64 LE` ++ `partition: u32 LE` ++
//! `serialized physical plan`. The `stage_id` is a stable content hash of the plan bytes, derived
//! at fetch time (see [`fetch_stream`]), so all consumers of one producer name the same cache
//! entry without any coordinator-side stage assignment.
//!
//! Note: coordinator and worker MUST run the identical DataFusion build — serialized plans
//! are not cross-version compatible.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, collect_partitioned};
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

use crate::stage_cache::{MaterializedStage, StageCache, stage_id_of};

/// Boxed tonic response stream.
type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Number of fixed header bytes in a ticket: `stage_id` (u64) + `partition` (u32).
const TICKET_HEADER_LEN: usize = 8 + 4;

/// Encode a Flight ticket: 8-byte LE stage id, 4-byte LE partition index, then the plan bytes.
///
/// The `stage_id` names the producer stage so the worker can serve many consumers of the same
/// producer from one materialization (see [`StageCache`]); the `partition` selects which of that
/// stage's output partitions this consumer wants.
fn encode_ticket(stage_id: u64, partition: u32, plan_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(TICKET_HEADER_LEN + plan_bytes.len());
    buf.extend_from_slice(&stage_id.to_le_bytes());
    buf.extend_from_slice(&partition.to_le_bytes());
    buf.extend_from_slice(plan_bytes);
    buf
}

/// Decode a Flight ticket into `(stage_id, partition, plan_bytes)`.
fn decode_ticket(ticket: &[u8]) -> Result<(u64, u32, &[u8])> {
    if ticket.len() < TICKET_HEADER_LEN {
        return Err(anyhow!(
            "ticket too short: {} bytes, need at least {TICKET_HEADER_LEN}",
            ticket.len()
        ));
    }
    let (head, plan_bytes) = ticket.split_at(TICKET_HEADER_LEN);
    let stage_id = u64::from_le_bytes(head[..8].try_into().expect("8 bytes"));
    let partition = u32::from_le_bytes(head[8..12].try_into().expect("4 bytes"));
    Ok((stage_id, partition, plan_bytes))
}

/// Serialize a physical plan for shipping to a worker.
///
/// Uses [`LldbCodec`], not the default codec, so plans containing our own nodes survive the
/// trip. That is what makes worker-to-worker exchange possible: a reduce stage is just a plan
/// whose leaves are [`FlightReaderExec`]s pointing at map workers, and a worker can only run
/// such a plan if it can decode those leaves.
///
/// [`LldbCodec`]: crate::remote::LldbCodec
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub fn serialize_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
    crate::remote::serialize_plan(plan).context("serializing physical plan")
}

/// Deserialize a physical plan received from a coordinator, executable in `ctx`.
pub fn deserialize_plan(bytes: &[u8], ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
    // The proto decoder resolves UDFs/runtime from a TaskContext, not the session directly.
    let task_ctx = ctx.task_ctx();
    crate::remote::deserialize_plan(bytes, task_ctx.as_ref()).context("deserializing physical plan")
}

// ---------------------------------------------------------------------------
// Worker side — a Flight server that executes sub-plans.
// ---------------------------------------------------------------------------

/// A worker that executes physical plans arriving over Flight, caching each producer stage's
/// output so it is materialized once and served to many consumers.
///
/// The cache lives as long as the service (an `Arc<StageCache>` shared across every `do_get`), so
/// the `R` reducers of a shuffle that all pull one producer share a single execution. Cloning the
/// service — tonic clones it per request — clones the `Arc`, not the cache, so all clones share the
/// one cache.
#[derive(Clone)]
pub struct WorkerFlightService {
    ctx: SessionContext,
    cache: Arc<StageCache>,
}

impl WorkerFlightService {
    /// A worker with a fresh, empty stage cache.
    pub fn new(ctx: SessionContext) -> Self {
        Self::new_with_cache(ctx, Arc::new(StageCache::new()))
    }

    /// A worker sharing the given stage cache. Tests use this to retain a handle to the cache and
    /// assert [`StageCache::execution_count`] after driving consumers.
    pub fn new_with_cache(ctx: SessionContext, cache: Arc<StageCache>) -> Self {
        Self { ctx, cache }
    }

    /// The stage cache this worker serves from.
    pub fn stage_cache(&self) -> &Arc<StageCache> {
        &self.cache
    }
}

/// Serve the worker on `listener` until the process ends, with a fresh stage cache.
pub async fn serve_worker(listener: TcpListener, ctx: SessionContext) -> Result<()> {
    serve_worker_with_cache(listener, ctx, Arc::new(StageCache::new())).await
}

/// Like [`serve_worker`] but sharing a caller-provided [`StageCache`].
///
/// The caller keeps its own `Arc` clone, so a test can start a real in-process worker and then read
/// the cache's [`execution_count`](StageCache::execution_count) to prove a producer ran exactly once
/// across `N` consumer pulls.
pub async fn serve_worker_with_cache(
    listener: TcpListener,
    ctx: SessionContext,
    cache: Arc<StageCache>,
) -> Result<()> {
    let service = FlightServiceServer::new(WorkerFlightService::new_with_cache(ctx, cache));
    Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("Flight server terminated")
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;

    /// Materialize the ticket's producer stage (once, via the cache) and stream the requested
    /// partition's batches back.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (ticket_stage_id, partition, plan_bytes) = decode_ticket(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;

        // The stage id is content-addressed, so recompute it from the plan bytes rather than trust
        // the header: a buggy or hostile client must not be able to point a plan's request at a
        // *different* stage's cached output. Reject a ticket whose header disagrees with its own
        // plan bytes, and key the cache on the recomputed id.
        let stage_id = stage_id_of(plan_bytes);
        if stage_id != ticket_stage_id {
            return Err(Status::invalid_argument(format!(
                "ticket stage id {ticket_stage_id} does not match its plan bytes (hash {stage_id})"
            )));
        }

        // Materialize the whole producer once per stage; subsequent consumers (and other
        // partitions) hit the cache. The plan is deserialized and executed only inside this
        // once-closure, i.e. only on a cache miss.
        let plan_bytes = plan_bytes.to_vec();
        let ctx = self.ctx.clone();
        let materialized = self
            .cache
            .get_or_materialize(stage_id, || async move {
                let plan = deserialize_plan(&plan_bytes, &ctx)?;
                let schema = plan.schema();
                // Drive every output partition concurrently — a RepartitionExec fans its single
                // input read into per-partition channels, so draining them one-at-a-time can
                // deadlock. See `stage_cache` for the full rationale.
                let partitions = collect_partitioned(plan, ctx.task_ctx()).await?;
                Ok(Arc::new(MaterializedStage { schema, partitions }))
            })
            .await
            .map_err(|e| Status::internal(format!("materialize stage {stage_id}: {e}")))?;

        let idx = partition as usize;
        if idx >= materialized.partition_count() {
            return Err(Status::invalid_argument(format!(
                "partition {partition} out of range: stage {stage_id} produced {} partition(s)",
                materialized.partition_count()
            )));
        }

        // Stream the cached partition straight from the shared `Arc`, cloning one batch at a time as
        // the consumer pulls — no upfront clone of the whole partition vector. The `unfold` state
        // owns the `Arc`, so the buffer outlives the stream. Set the schema explicitly so a
        // partition with zero batches still encodes a valid (schema-only) stream.
        // Meter what actually leaves this worker as it leaves — the cache counts materializations,
        // this counts transfers, and a planner that shuffles a large table instead of broadcasting a
        // small one shows up here and nowhere else. See [`StageCache::rows_served`].
        let schema = materialized.schema.clone();
        let meter = Arc::clone(&self.cache);
        let batch_stream = futures::stream::unfold(
            (materialized, idx, 0usize, meter),
            |(stage, idx, i, meter)| async move {
                let partition = &stage.partitions[idx];
                if i < partition.len() {
                    let batch = partition[i].clone();
                    meter.record_rows_served(batch.num_rows());
                    Some((Ok::<_, FlightError>(batch), (stage, idx, i + 1, meter)))
                } else {
                    None
                }
            },
        );
        let flight_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("flight encode: {e}")));

        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

// ---------------------------------------------------------------------------
// Coordinator side — fetch a partition's results from a worker.
// ---------------------------------------------------------------------------

/// Ship `plan` to the worker at `worker_url` (e.g. `http://127.0.0.1:50051`), execute its
/// `partition`, and collect the streamed result batches.
pub async fn fetch(
    worker_url: &str,
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = serialize_plan(plan)?;
    let stream = fetch_stream(worker_url.to_string(), partition, plan_bytes).await?;
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .context("decoding flight data stream")?;
    Ok(batches)
}

/// Like [`fetch`], but hands back the *stream* instead of collecting it, and takes plan bytes
/// that are already serialized.
///
/// This is what [`FlightReaderExec`] needs: an `ExecutionPlan` must produce batches lazily, so
/// buffering a whole remote partition into a `Vec` first would defeat the point — a reduce
/// stage should start work on the first batch, not the last.
///
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub async fn fetch_stream(
    worker_url: String,
    partition: u32,
    plan_bytes: Vec<u8>,
) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static> {
    // Derive the stage id from the plan bytes themselves: every consumer of one producer ships
    // byte-identical bytes (only `partition` differs), so they all name the same cache entry on the
    // worker without any coordinator-side stage assignment.
    let stage_id = stage_id_of(&plan_bytes);
    let ticket = Ticket {
        ticket: encode_ticket(stage_id, partition, &plan_bytes).into(),
    };

    let channel = Channel::from_shared(worker_url.clone())
        .with_context(|| format!("invalid worker url {worker_url}"))?
        .connect()
        .await
        .with_context(|| format!("connecting to worker {worker_url}"))?;
    let mut client = FlightServiceClient::new(channel);

    let flight_data = client
        .do_get(ticket)
        .await
        .context("do_get request")?
        .into_inner()
        .map_err(|status| FlightError::ExternalError(Box::new(status)));

    Ok(FlightRecordBatchStream::new_from_flight_data(flight_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_roundtrips() {
        let ticket = encode_ticket(0xDEAD_BEEF_1234_5678, 7, b"plan-bytes");
        let (stage_id, partition, plan) = decode_ticket(&ticket).unwrap();
        assert_eq!(stage_id, 0xDEAD_BEEF_1234_5678);
        assert_eq!(partition, 7);
        assert_eq!(plan, b"plan-bytes");
    }

    #[test]
    fn ticket_carries_an_empty_plan() {
        // A stage-only ticket (no plan bytes) still round-trips its header.
        let ticket = encode_ticket(1, 2, b"");
        let (stage_id, partition, plan) = decode_ticket(&ticket).unwrap();
        assert_eq!((stage_id, partition), (1, 2));
        assert!(plan.is_empty());
    }

    #[test]
    fn short_ticket_errors() {
        // Fewer than the 12 header bytes must error, not panic.
        assert!(decode_ticket(&[1, 2]).is_err());
        assert!(decode_ticket(&[0; TICKET_HEADER_LEN - 1]).is_err());
    }
}
