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
//! # Failover
//!
//! Because a stage is content-addressed and materialized once per worker, *who* runs it does not
//! change *what* it produces. [`fetch_partition_with_failover`] turns that property into fault
//! tolerance: a pull that fails for a transport reason is reassigned to the next candidate worker,
//! while a pull that fails for a request reason ([`Status::invalid_argument`], [`Status::internal`]
//! — the two faults `do_get` raises about itself) is surfaced immediately, because an identical
//! fleet would only reproduce it. See [`crate::retry`] for the classification contract.
//!
//! Note: coordinator and worker MUST run the identical DataFusion build — serialized plans
//! are not cross-version compatible.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
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

use crate::retry::{Retriability, RetryPolicy, classify};
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
        let schema = materialized.schema.clone();
        let batch_stream =
            futures::stream::unfold((materialized, idx, 0usize), |(stage, idx, i)| async move {
                let partition = &stage.partitions[idx];
                if i < partition.len() {
                    let batch = partition[i].clone();
                    Some((Ok::<_, FlightError>(batch), (stage, idx, i + 1)))
                } else {
                    None
                }
            });
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
    fetch_partition(worker_url, partition, plan_bytes).await
}

/// Ship `plan` to the first worker in `candidates` that can serve it, failing over to the next on a
/// [retriable](Retriability::Retriable) failure.
///
/// This is [`fetch`] with the fleet behind it. The coordinator's boundary-less path — a query with
/// no distribution boundary, shipped whole to one worker — used to name a single address, so a fleet
/// of ten with one dead task failed the query nine healthy workers short of having to. Reassignment
/// is safe for the same reason it is safe at the [`FlightReaderExec`] pull boundary: the plan is
/// self-contained, the stage id is a content hash of its bytes, and the worker materializes it once
/// into its [`StageCache`] — so whoever runs it produces identical output.
///
/// Uses the default [`RetryPolicy`]; each candidate is tried at most once, in order.
///
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub async fn fetch_with_failover(
    candidates: &[String],
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = serialize_plan(plan)?;
    fetch_partition_with_failover(candidates, partition, plan_bytes, &RetryPolicy::default()).await
}

/// Pull one partition **completely** from one worker.
///
/// The full collection is the point, not an accident: it is the unit the failover loop retries. See
/// [`fetch_partition_with_failover`] for why a half-delivered partition cannot be resumed.
async fn fetch_partition(
    worker_url: &str,
    partition: u32,
    plan_bytes: Vec<u8>,
) -> Result<Vec<RecordBatch>> {
    let stream = fetch_stream(worker_url.to_string(), partition, plan_bytes).await?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .with_context(|| format!("streaming partition {partition} from worker {worker_url}"))
}

/// Fetch one partition of an already-serialized plan, walking `candidates` until one answers.
///
/// The retry unit is **a whole partition**, deliberately. A stage pull that has already handed
/// batches downstream cannot be reassigned: the replacement worker re-materializes the stage from
/// the top (that is exactly what makes it safe to re-run), so re-pulling after a partial delivery
/// would emit those first batches twice and silently duplicate rows. A retry that corrupts the
/// answer is worse than the failure it replaces, so nothing is emitted until a complete
/// `Vec<RecordBatch>` is in hand.
///
/// Termination: each candidate is tried **once**, in order, so the retry budget is the candidate
/// list itself — no cycling, no unbounded loop. A fatal classification (see [`classify`]) returns
/// immediately and names the worker; a retriable one logs, backs off, and moves to the next
/// candidate. When the list runs out, the error names *every* candidate tried and keeps the last
/// underlying cause in its chain, because a query-level failure must still say what failed.
pub async fn fetch_partition_with_failover(
    candidates: &[String],
    partition: u32,
    plan_bytes: Vec<u8>,
    policy: &RetryPolicy,
) -> Result<Vec<RecordBatch>> {
    if candidates.is_empty() {
        bail!("no worker candidates to fetch partition {partition} from");
    }

    let mut last_error: Option<anyhow::Error> = None;
    for (attempt, worker_url) in candidates.iter().enumerate() {
        match fetch_partition(worker_url, partition, plan_bytes.clone()).await {
            Ok(batches) => {
                if attempt > 0 {
                    tracing::info!(
                        worker = %worker_url,
                        partition,
                        attempt = attempt + 1,
                        "stage reassigned to a healthy worker after a retriable failure"
                    );
                }
                return Ok(batches);
            }
            Err(err) => match classify(&err) {
                Retriability::Fatal => {
                    return Err(err.context(format!(
                        "worker {worker_url} failed serving partition {partition}; not retried \
                         (the fault is in the request, and every worker runs the identical build)"
                    )));
                }
                Retriability::Retriable => {
                    match candidates.get(attempt + 1) {
                        Some(next) => tracing::warn!(
                            failed_worker = %worker_url,
                            next_worker = %next,
                            partition,
                            attempt = attempt + 1,
                            error = %format!("{err:#}"),
                            "worker failed serving a stage; reassigning to the next candidate"
                        ),
                        None => tracing::warn!(
                            failed_worker = %worker_url,
                            partition,
                            attempt = attempt + 1,
                            error = %format!("{err:#}"),
                            "worker failed serving a stage and no healthy candidates remain"
                        ),
                    }
                    last_error = Some(err);
                    if attempt + 1 < candidates.len() {
                        tokio::time::sleep(policy.backoff(attempt as u32)).await;
                    }
                }
            },
        }
    }

    // Exhaustion. Keep the last cause in the chain and name every target, so the operator sees both
    // "which nodes did I lose" and "what did the last one actually say".
    let tried = candidates.join(", ");
    Err(last_error
        .expect("a non-empty candidate list always records an error before exhausting")
        .context(format!(
            "all {} candidate worker(s) failed serving partition {partition}: {tried}",
            candidates.len()
        )))
}

/// Like [`fetch`], but hands back the *stream* instead of collecting it, and takes plan bytes
/// that are already serialized.
///
/// The lowest layer of the pull: one connection, one `do_get`, no policy. Everything that decides
/// *which* worker to ask and *whether* to ask another one is built on top of it
/// ([`fetch_partition`], [`fetch_partition_with_failover`]).
///
/// It used to be handed straight to [`FlightReaderExec`], which streamed batches downstream as they
/// arrived. That is no longer safe once a lost stage can be reassigned — a half-delivered partition
/// cannot be resumed on another worker without duplicating rows — so the reader now collects a whole
/// partition before emitting. The streaming shape stays here because it is the honest primitive, and
/// because a future incremental-with-resume design (per-batch sequencing) would build on it.
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
        .with_context(|| format!("do_get request to worker {worker_url}"))?
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

    /// A candidate list with nothing in it is a planner bug, not a fleet outage — say so plainly
    /// instead of returning an empty result that would look like a legitimately empty partition.
    #[tokio::test]
    async fn an_empty_candidate_list_is_rejected() {
        let err = fetch_partition_with_failover(&[], 0, b"plan".to_vec(), &RetryPolicy::default())
            .await
            .expect_err("no candidates is invalid");
        assert!(
            err.to_string().contains("no worker candidates"),
            "got: {err}"
        );
    }

    /// A `127.0.0.1` address with nothing listening: bind a port, read it, drop the listener. A
    /// connection there is refused immediately — the cheapest faithful stand-in for a lost worker.
    async fn dead_worker_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        format!("http://{addr}")
    }

    /// A zero-wait policy: these tests are about the loop's shape, and the backoff *schedule* is
    /// covered in [`crate::retry`].
    fn instant_policy() -> RetryPolicy {
        RetryPolicy {
            base_backoff: std::time::Duration::ZERO,
            max_backoff: std::time::Duration::ZERO,
        }
    }

    /// Exhaustion must name every target tried, not just the first one to fail — losing a whole
    /// fleet is a different operational story from losing one node, and the error has to tell it.
    #[tokio::test]
    async fn exhausting_every_candidate_names_them_all() {
        let candidates = vec![
            dead_worker_url().await,
            dead_worker_url().await,
            dead_worker_url().await,
        ];

        let err =
            fetch_partition_with_failover(&candidates, 7, b"plan".to_vec(), &instant_policy())
                .await
                .expect_err("every candidate is unreachable");

        let message = err.to_string();
        assert!(
            message.contains("all 3 candidate worker(s) failed serving partition 7"),
            "got: {message}"
        );
        for candidate in &candidates {
            assert!(
                message.contains(candidate),
                "exhaustion must name `{candidate}`, got: {message}"
            );
        }
        // The last underlying cause survives in the chain, so the operator still learns *why*.
        assert!(
            format!("{err:#}").contains("Connection refused"),
            "the last cause must remain in the chain, got: {err:#}"
        );
    }

    /// A malformed worker URL is a coordinator-side configuration fault, not a fleet outage: it
    /// would fail identically against every candidate, so it is fatal and stops at the first one.
    /// (`discover_workers` validates scheme and port, so a well-configured fleet never gets here.)
    #[tokio::test]
    async fn a_malformed_worker_url_is_fatal_and_never_replayed() {
        let candidates = vec![
            "not a uri at all".to_string(),
            dead_worker_url().await,
            dead_worker_url().await,
        ];

        let err =
            fetch_partition_with_failover(&candidates, 0, b"plan".to_vec(), &instant_policy())
                .await
                .expect_err("a malformed url cannot be fetched from");

        let message = err.to_string();
        assert!(message.contains("not retried"), "got: {message}");
        assert!(
            !message.contains("all 3 candidate"),
            "a fatal fault must not walk the fleet, got: {message}"
        );
    }
}
