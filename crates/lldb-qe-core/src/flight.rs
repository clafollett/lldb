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
//! # Who may call a worker
//!
//! Everything above describes a port that, until issue #19, had no lock on it at all: any process
//! that could reach a worker's Flight port could ship it an arbitrary physical plan and have it
//! executed with that worker's storage credentials. [`FleetAuth`] closes that minimally — a shared
//! secret in the `authorization` metadata, constant-time compared, `UNAUTHENTICATED` without it.
//!
//! The credential is **ambient**, read once from `LLDB_FLEET_TOKEN` by [`ambient_fleet_auth`], and
//! that is a deliberate choice rather than laziness. The alternative — threading a token through
//! [`fetch`], [`fetch_with_failover`], [`fetch_partition_with_failover`] and into
//! [`FlightReaderExec`] — founders on the fact that a `FlightReaderExec` is *serialized into a
//! plan* and re-executed on a worker for worker-to-worker exchange. A per-call token would either
//! have to travel inside those plan bytes (a credential in a cached, content-hashed payload: no) or
//! be absent exactly where worker-to-worker pulls need it. A process-wide deployment secret read
//! from the process's own environment is what it actually is.
//!
//! What that secret proves is "you are part of this deployment", *not* "you are user X". The
//! per-request half is [`crate::plan_assertion`], and it is a **second** credential on the same
//! call: a short-lived, MAC'd statement naming the account, the user and the object-store locations
//! the coordinator authorized, which a worker verifies and then checks the plan against. Both
//! headers are required and both are checked — the fleet secret says which *deployment* is calling,
//! the assertion says which *request* this is and what it may read.
//!
//! It travels in the metadata beside the fleet token and **never inside the ticket**, which is not
//! hygiene but arithmetic: [`stage_id_of`] hashes the plan bytes, so a per-request value inside them
//! would give every request a distinct stage id and turn every pull into a cache miss. That is the
//! materialize-once property this module exists for, so the assertion is carried, forwarded and
//! checked entirely outside the bytes the stage id is computed from. A worker that pulls from
//! another worker forwards it through the [`TaskContext`](datafusion::execution::TaskContext) it
//! executes under; see [`crate::plan_assertion`] for why that, and not a task-local, is the channel.
//!
//! # …and over what
//!
//! That secret used to cross the wire in the clear, which made it readable and replayable by
//! anyone on the path. [`crate::tls`] is the answer: a worker serves TLS when it is given a
//! certificate, and **refuses to bind a plaintext port at all when `LLDB_FLEET_TOKEN` is set and
//! `--allow-plaintext` is not** — a checked credential on an unencrypted port has to be chosen, not
//! defaulted into. A worker with no fleet token has no secret to leak and keeps binding plaintext
//! with no configuration, which is what `cargo run -p lldb-qe-worker` depends on.
//!
//! The client half is ambient for exactly the reason the credential is, and the same argument
//! applies verbatim: a [`FlightReaderExec`] is serialized into a plan, so nothing per-call can
//! travel with it. [`crate::tls::dial`] reads this process's installed trust and encrypts iff the
//! worker URL says `https://`.
//!
//! TLS is *server* authentication only. It does not tell a worker which fleet member is calling —
//! `LLDB_FLEET_TOKEN` is still the only thing that does, and is deliberately untouched here.
//!
//! [`FlightReaderExec`]: crate::remote::FlightReaderExec
//!
//! Note: coordinator and worker MUST run the identical DataFusion build — serialized plans
//! are not cross-version compatible.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};

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
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{AUTHORIZATION_HEADER, AuthError, FleetAuth, bearer_header, bearer_token};
use crate::discovery::redact_endpoint;
use crate::plan_assertion::{
    AssertionError, PLAN_ASSERTION_HEADER, PlanAuth, SignedAssertion, VerifiedAssertion,
};
use crate::retry::{Retriability, RetryPolicy, classify};
use crate::stage_cache::{MaterializedStage, StageCache, stage_id_of};
use crate::tls::ServerTls;

/// Boxed tonic response stream.
type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// This process's fleet credential, read from the environment exactly once.
///
/// Both sides use it: a worker built with [`WorkerFlightService::new`] requires it, and every
/// outgoing [`fetch_stream`] presents it. Reading it once rather than per call is what makes the
/// coordinator's and the worker's view of the deployment provably the same within a process — and
/// `std::env::var` is not free on a hot path anyway.
pub fn ambient_fleet_auth() -> &'static FleetAuth {
    static AMBIENT: OnceLock<FleetAuth> = OnceLock::new();
    AMBIENT.get_or_init(FleetAuth::from_env)
}

/// Map a fleet-credential refusal onto the gRPC status a client retries (or does not) against.
///
/// `UNAUTHENTICATED` rather than `INVALID_ARGUMENT` matters: [`classify`] treats a request fault as
/// fatal and a transport fault as retriable, and a fleet misconfiguration is neither of those — it
/// would fail identically on every worker, so it must *not* walk the fleet pretending each node
/// might answer. See [`crate::retry`].
fn fleet_status(error: AuthError) -> Status {
    Status::unauthenticated(format!(
        "worker refused the request: {error}. Every coordinator and worker must share the same \
         {} value.",
        crate::auth::FLEET_TOKEN_ENV
    ))
}

/// Map a plan-assertion refusal onto its gRPC status.
///
/// Two codes, and the split is the same one [`crate::server`] makes between "get a credential" and
/// "get a grant": a missing, malformed, wrongly-signed or expired assertion is `UNAUTHENTICATED`,
/// while an assertion that verifies and simply does not *cover* what the plan reads is
/// `PERMISSION_DENIED`. Both are [`Retriability::Fatal`] (see [`crate::retry`]), which is what stops
/// a refusal walking the whole fleet: an identical fleet would refuse identically.
fn assertion_status(error: AssertionError) -> Status {
    match error {
        AssertionError::NotCovered { .. } | AssertionError::Traversal(_) => {
            Status::permission_denied(error.to_string())
        }
        _ => Status::unauthenticated(error.to_string()),
    }
}

/// Map a failed materialization onto a status, keeping an authorization refusal legible.
///
/// The covering check runs *inside* the once-closure (it has to: that is the last point before the
/// files are opened), and that closure hands back an [`anyhow::Error`]. Reporting a refusal as
/// `INTERNAL` would tell the operator the worker broke, and — worse — `INTERNAL` and
/// `PERMISSION_DENIED` are both fatal but only one of them names the fix. So the refusal is
/// recovered from the error rather than flattened into a string.
fn materialize_status(stage_id: u64, error: anyhow::Error) -> Status {
    match error.downcast::<AssertionError>() {
        Ok(refusal) => assertion_status(refusal),
        Err(other) => Status::internal(format!("materialize stage {stage_id}: {other}")),
    }
}

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
    /// What this worker requires of whoever connects. [`FleetAuth::Open`] is the no-configuration
    /// default and is warned about at startup; see [`crate::auth`].
    auth: FleetAuth,
    /// What this worker requires of each individual *request*: a plan assertion signed with a key
    /// derived from the same fleet secret. Derived from `auth` rather than configured separately,
    /// so the two postures cannot disagree — see [`crate::plan_assertion`].
    plan_auth: PlanAuth,
}

impl WorkerFlightService {
    /// A worker with a fresh, empty stage cache, using this process's ambient fleet credential.
    pub fn new(ctx: SessionContext) -> Self {
        Self::new_with_cache(ctx, Arc::new(StageCache::new()))
    }

    /// A worker sharing the given stage cache. Tests use this to retain a handle to the cache and
    /// assert [`StageCache::execution_count`] after driving consumers.
    pub fn new_with_cache(ctx: SessionContext, cache: Arc<StageCache>) -> Self {
        Self::new_with_auth(ctx, cache, ambient_fleet_auth().clone())
    }

    /// A worker with an explicit fleet credential. The seam a test uses to stand up a *closed*
    /// worker without mutating the process environment — `set_var` is `unsafe` in edition 2024 and
    /// would race every other test sharing the process.
    pub fn new_with_auth(ctx: SessionContext, cache: Arc<StageCache>, auth: FleetAuth) -> Self {
        let plan_auth = PlanAuth::from_fleet_auth(&auth);
        Self::new_with_postures(ctx, cache, auth, plan_auth)
    }

    /// A worker whose two postures — fleet membership and per-request authorization — are set
    /// **independently**. A test seam, and the only one of these constructors that can express a
    /// combination production never produces.
    ///
    /// In a real deployment both come from `LLDB_FLEET_TOKEN` through [`Self::new_with_auth`], which
    /// is what makes "the fleet secret is set, therefore assertions are checked" true by
    /// construction rather than by configuration. This exists because the thing that makes those two
    /// agree is also what makes them untestable together in one process: `ambient_fleet_auth` is a
    /// `OnceLock` over the environment, `set_var` is `unsafe` in edition 2024, and a *worker*
    /// dialling another worker presents the ambient token — so two closed workers in one test
    /// process can never authenticate to each other. A worker that checks the assertion and not the
    /// fleet token is how `worker_plan_assertion` observes the forwarding path in isolation.
    pub fn new_with_postures(
        ctx: SessionContext,
        cache: Arc<StageCache>,
        auth: FleetAuth,
        plan_auth: PlanAuth,
    ) -> Self {
        Self {
            ctx,
            cache,
            auth,
            plan_auth,
        }
    }

    /// The stage cache this worker serves from.
    pub fn stage_cache(&self) -> &Arc<StageCache> {
        &self.cache
    }

    /// Check an incoming request's fleet credential.
    fn check_credential<T>(&self, request: &Request<T>) -> Result<(), Status> {
        // Read only when a credential is actually required, so an open worker does no metadata
        // work at all and the no-configuration path stays exactly as fast as it was.
        if !self.auth.is_required() {
            return Ok(());
        }
        let presented = request
            .metadata()
            .get(AUTHORIZATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| bearer_token(value).ok());
        self.auth.check(presented).map_err(fleet_status)
    }

    /// Verify the request's plan assertion — the *per-request* half of the door.
    ///
    /// `Ok(None)` only for a worker with no fleet secret, which has no key and therefore nothing to
    /// verify; that is the same no-configuration path [`FleetAuth::Open`] keeps open. The metadata
    /// key is read rather than the ticket for the reason this module's header gives: the ticket is
    /// hashed into the stage id.
    fn verify_assertion<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<VerifiedAssertion>, Status> {
        if !self.plan_auth.is_required() {
            return Ok(None);
        }
        let presented = request
            .metadata()
            .get(PLAN_ASSERTION_HEADER)
            .and_then(|value| value.to_str().ok());
        self.plan_auth
            .verify(presented, std::time::SystemTime::now())
            .map_err(assertion_status)
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
    serve_worker_with_auth(listener, ctx, cache, ambient_fleet_auth().clone()).await
}

/// Like [`serve_worker_with_cache`] but with an explicit fleet credential.
///
/// The posture is logged here rather than in the worker binary so that an in-process test worker
/// and the real `lldb-qe-worker` report the identical line — the warning about an open port is the
/// only thing standing between "we never configured a fleet secret" and "we did not notice".
pub async fn serve_worker_with_auth(
    listener: TcpListener,
    ctx: SessionContext,
    cache: Arc<StageCache>,
    auth: FleetAuth,
) -> Result<()> {
    serve_worker_with(listener, ctx, cache, auth, ServerTls::plaintext()).await
}

/// Like [`serve_worker_with_auth`] but setting the per-request posture independently.
///
/// The serving half of [`WorkerFlightService::new_with_postures`] — read that for why a seam that
/// can express an inconsistent pair exists at all, and why production cannot reach it.
pub async fn serve_worker_with_postures(
    listener: TcpListener,
    ctx: SessionContext,
    cache: Arc<StageCache>,
    auth: FleetAuth,
    plan_auth: PlanAuth,
) -> Result<()> {
    auth.log_posture();
    let service = FlightServiceServer::new(WorkerFlightService::new_with_postures(
        ctx, cache, auth, plan_auth,
    ));
    Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("Flight server terminated")
}

/// Like [`serve_worker_with_auth`] but also choosing what the port is served *over*.
///
/// The full-fidelity entry point, and the one `lldb-qe-worker` calls. Both postures — the
/// credential and the transport — are logged from here rather than from the binary so that an
/// in-process test worker and the real one report the identical two lines; a warning only one of
/// them prints is a warning nobody can rely on.
///
/// Note the asymmetry, and that it is deliberate: `tls` says what this worker *serves*, while what
/// it presents when it dials **another** worker (the worker-to-worker shuffle) comes from
/// [`crate::tls::client_trust`], because that dial happens inside a plan that was serialized
/// somewhere else. See [`crate::tls`].
pub async fn serve_worker_with(
    listener: TcpListener,
    ctx: SessionContext,
    cache: Arc<StageCache>,
    auth: FleetAuth,
    tls: ServerTls,
) -> Result<()> {
    auth.log_posture();
    tls.log_posture("worker");
    let service = FlightServiceServer::new(WorkerFlightService::new_with_auth(ctx, cache, auth));
    tls.configure(Server::builder())?
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
    ///
    /// **The partition range test below is necessarily post-materialization.** It compares against
    /// the materialized stage's partition count, and on a cache hit nothing here deserializes the
    /// plan at all — so testing any earlier means deserializing on every request, which is the cost
    /// the [`StageCache`] exists to remove. A ticket naming a partition its stage does not have
    /// therefore spends a connect, a plan deserialize and a *full* materialization before it is
    /// refused, and it is refused with `InvalidArgument`, which [`crate::retry`] classifies fatal:
    /// no failover, and the query dies having paid for the whole stage.
    ///
    /// The cheap check is [`FlightReaderExec::with_fallbacks`][fr] in `remote.rs`, which range-checks
    /// `remote_partition` against the sub-plan the leaf is built around, on the coordinator, before
    /// anything is dialled. That is what keeps the path above rare, and the reading to reject is the
    /// reverse one: this test is the last line for a ticket this process did not build, not a reason
    /// the coordinator's can go.
    ///
    /// [fr]: crate::remote::FlightReaderExec::with_fallbacks
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Before the ticket is even decoded: an unauthenticated caller must not be able to make
        // this process deserialize bytes it chose. Two credentials, both required when this worker
        // is configured with a fleet secret — the deployment's, then this request's.
        self.check_credential(&request)?;
        let verified = self.verify_assertion(&request)?;

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
        // The verified assertion, hung on the `TaskContext` this stage executes under so that every
        // operator beneath it travels with it — in particular a `FlightReaderExec` leaf, which dials
        // *another* worker and must present the same assertion there. It is deliberately not in the
        // plan bytes; see this module's header and [`crate::plan_assertion`].
        let task_ctx = crate::plan_assertion::task_ctx_with(
            &ctx.task_ctx(),
            verified.as_ref().map(|v| v.signed.clone()),
        );
        let plan_auth = self.plan_auth.clone();
        let materializing = verified.clone();
        let materialized = self
            .cache
            .get_or_materialize(stage_id, || async move {
                let plan = crate::remote::deserialize_plan(&plan_bytes, task_ctx.as_ref())
                    .context("deserializing physical plan")?;
                // **Before a single byte is read.** What the plan actually touches must be inside
                // what the assertion covers, and this is the last moment that is true: one line
                // further down the files are open. Inside the once-closure so a refusal is not
                // cached and the next, authorized puller still materializes normally.
                let reads = crate::plan_assertion::plan_reads(&plan);
                plan_auth.check_cover(materializing.as_ref(), &reads)?;
                let schema = plan.schema();
                // Drive every output partition concurrently — a RepartitionExec fans its single
                // input read into per-partition channels, so draining them one-at-a-time can
                // deadlock. See `stage_cache` for the full rationale.
                let partitions = collect_partitioned(plan, task_ctx).await?;
                Ok(Arc::new(MaterializedStage {
                    schema,
                    partitions,
                    reads,
                }))
            })
            .await
            .map_err(|e| materialize_status(stage_id, e))?;

        // The other two ways into this line: a cache **hit**, and a single-flight follower whose
        // sibling did the materializing. Neither ran the check above, and both are about to be
        // handed the same rows — so the requester is checked against what the stage recorded it
        // reads. Without this, a stage materialized for one authorized caller would serve any later
        // caller whose assertion merely *verified*, which is the second-fleet-token failure this
        // whole module exists to avoid. It is cheap because the read set was computed once, when the
        // stage was deserialized.
        self.plan_auth
            .check_cover(verified.as_ref(), &materialized.reads)
            .map_err(assertion_status)?;
        if let Some(verified) = &verified {
            tracing::debug!(
                stage_id,
                partition,
                caller = %verified.assertion,
                "serving a stage under a verified plan assertion"
            );
        }

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
///
/// Carries no plan assertion, so a worker with a fleet secret refuses it. That is the right default
/// for the callers it has — tests and tools driving an open, single-node worker; the query path goes
/// through [`fetch_with_failover`], which takes one.
pub async fn fetch(
    worker_url: &str,
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = serialize_plan(plan)?;
    fetch_partition(worker_url, partition, plan_bytes, None).await
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
/// `assertion` is the per-request authorization this pull presents — the coordinator's offload path
/// passes what it minted, and `None` means "this fleet has no secret, so there is nothing to
/// present". See [`crate::plan_assertion`].
///
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub async fn fetch_with_failover(
    candidates: &[String],
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
    assertion: Option<&SignedAssertion>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = serialize_plan(plan)?;
    fetch_partition_with_failover(
        candidates,
        partition,
        plan_bytes,
        &RetryPolicy::default(),
        assertion,
    )
    .await
}

/// Pull one partition **completely** from one worker.
///
/// The full collection is the point, not an accident: it is the unit the failover loop retries. See
/// [`fetch_partition_with_failover`] for why a half-delivered partition cannot be resumed.
async fn fetch_partition(
    worker_url: &str,
    partition: u32,
    plan_bytes: Vec<u8>,
    assertion: Option<&SignedAssertion>,
) -> Result<Vec<RecordBatch>> {
    let stream = fetch_stream_with(
        worker_url.to_string(),
        partition,
        plan_bytes,
        ambient_fleet_auth(),
        assertion,
    )
    .await?;
    stream.try_collect::<Vec<_>>().await.with_context(|| {
        format!(
            "streaming partition {partition} from worker {}",
            redact_endpoint(worker_url)
        )
    })
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
    assertion: Option<&SignedAssertion>,
) -> Result<Vec<RecordBatch>> {
    if candidates.is_empty() {
        bail!("no worker candidates to fetch partition {partition} from");
    }

    let mut last_error: Option<anyhow::Error> = None;
    for (attempt, worker_url) in candidates.iter().enumerate() {
        // The same assertion goes to every candidate, which is what makes reassignment work at all:
        // it authorizes *what this plan reads*, not *which worker reads it*.
        match fetch_partition(worker_url, partition, plan_bytes.clone(), assertion).await {
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
/// (`fetch_partition`, [`fetch_partition_with_failover`]).
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
    fetch_stream_with(
        worker_url,
        partition,
        plan_bytes,
        ambient_fleet_auth(),
        None,
    )
    .await
}

/// [`fetch_stream`] with an explicit fleet credential instead of the process's ambient one.
///
/// Exists for the same reason [`WorkerFlightService::new_with_auth`] does: proving that a *closed*
/// worker serves a correctly-credentialled caller requires setting the credential without touching
/// the process environment, which is `unsafe` in edition 2024 and races every concurrent test.
/// Production always goes through [`fetch_stream`].
pub async fn fetch_stream_with_auth(
    worker_url: String,
    partition: u32,
    plan_bytes: Vec<u8>,
    auth: &FleetAuth,
) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static> {
    fetch_stream_with(worker_url, partition, plan_bytes, auth, None).await
}

/// The full-fidelity pull: both credentials, explicitly.
///
/// Both are optional in effect and both follow the same rule — a fleet with no secret presents
/// neither and a worker with no secret checks neither — but they answer different questions. `auth`
/// says which deployment is calling; `assertion` says which request this is and what it authorizes
/// reading. A closed worker requires both, so a caller that presents only the fleet token is refused
/// as `UNAUTHENTICATED`, naming the missing header.
pub async fn fetch_stream_with(
    worker_url: String,
    partition: u32,
    plan_bytes: Vec<u8>,
    auth: &FleetAuth,
    assertion: Option<&SignedAssertion>,
) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static> {
    // Derive the stage id from the plan bytes themselves: every consumer of one producer ships
    // byte-identical bytes (only `partition` differs), so they all name the same cache entry on the
    // worker without any coordinator-side stage assignment.
    let stage_id = stage_id_of(&plan_bytes);
    let ticket = Ticket {
        ticket: encode_ticket(stage_id, partition, &plan_bytes).into(),
    };

    // Encrypted iff the URL says `https://`, against this process's installed CA. There is no
    // negotiation and no fallback: a downgrade a client can be talked into is not transport
    // security. See [`crate::tls`].
    let channel = crate::tls::dial(&worker_url)
        .await
        .with_context(|| format!("dialing worker {}", redact_endpoint(&worker_url)))?;
    let mut client = FlightServiceClient::new(channel);

    // The credential rides in the request metadata, never in the ticket: a ticket is hashed into a
    // stage id, cached on the worker and logged, and a secret must be in none of those.
    let mut request = Request::new(ticket);
    if let Some(token) = auth.token() {
        let value = bearer_header(token).parse().with_context(|| {
            format!(
                "{} is not usable as an HTTP header value",
                crate::auth::FLEET_TOKEN_ENV
            )
        })?;
        request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
    }
    // Beside the fleet token, and beside it for the same reason: the ticket is hashed into a stage
    // id and cached. This one is per request, so putting it there would also make every request a
    // different stage.
    if let Some(assertion) = assertion {
        let value = assertion
            .as_header_value()
            .parse()
            .context("the plan assertion is not usable as an HTTP header value")?;
        request.metadata_mut().insert(PLAN_ASSERTION_HEADER, value);
    }

    let flight_data = client
        .do_get(request)
        .await
        .with_context(|| format!("do_get request to worker {}", redact_endpoint(&worker_url)))?
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
        let err =
            fetch_partition_with_failover(&[], 0, b"plan".to_vec(), &RetryPolicy::default(), None)
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

        let err = fetch_partition_with_failover(
            &candidates,
            7,
            b"plan".to_vec(),
            &instant_policy(),
            None,
        )
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

        let err = fetch_partition_with_failover(
            &candidates,
            0,
            b"plan".to_vec(),
            &instant_policy(),
            None,
        )
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
