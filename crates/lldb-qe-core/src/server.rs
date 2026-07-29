//! **The query server** — a long-running coordinator that accepts concurrent queries, schedules
//! them, and streams Arrow back.
//!
//! # What changed, and why it needed changing
//!
//! `lldb-qe-coordinator` runs exactly one query per process: build a plan, run it, print, exit.
//! That is a fine shape for a demo and a hopeless one for a warehouse. There is no way to submit a
//! second query while the first runs, no bound on how much work a fleet is asked to do at once,
//! and no record that a query ever happened. This module is the other shape: one process, many
//! clients, a bounded number of queries actually executing, the rest queued, and every one of them
//! written to query history.
//!
//! The one-shot binary is untouched and keeps working exactly as it did — compose and the
//! cross-container smoke test depend on it. Both now call the same
//! [execution pipeline](crate::engine), so the two cannot drift.
//!
//! # Transport: Flight, not a second stack
//!
//! Submissions arrive as an Arrow Flight `do_get`. This is deliberate and it is the *cheap*
//! choice: the repo already speaks Flight everywhere (workers serve `do_get`, the coordinator
//! already pulls stages over it), results are already Arrow `RecordBatch`es, and gRPC + tonic +
//! `arrow-flight` are already dependencies. An HTTP/JSON API would mean a second serialization
//! story for the same bytes, a second server stack, and a row-by-row encoding of columnar data —
//! all to deliver something Flight already delivers natively.
//!
//! So the server is a *sibling* of [`crate::flight`]'s worker service. The symmetry is the point:
//! a worker's ticket carries a serialized plan and streams back that plan's output; the
//! coordinator's ticket carries SQL and streams back the query's output. Same verb, one level up.
//!
//! Ticket wire format (little-endian): `account_len: u32` ++ `account` ++ `warehouse_len: u32` ++
//! `warehouse` ++ `sql`. Empty account or warehouse means "unset" — the server's defaults apply.
//! Hand-rolled for the same reason [`crate::flight`]'s is: three fields do not justify a
//! serialization dependency, and the format is readable at a glance.
//!
//! The assigned query id comes back in the gRPC response metadata as `lldb-query-id`, on success
//! **and** on failure, so a client always has the handle it needs to look the query up in history.
//!
//! # Cancellation: the same id, sent back as an action
//!
//! `do_action("cancel", <query id in decimal ASCII>)` stops a query this coordinator is running and
//! returns its admission slot to the warehouse at once, so the queue behind it advances. The body is
//! byte-for-byte what `lldb-query-id` returned, so a client copies one into the other and never
//! learns a second wire format. `list_actions` advertises it, which is how a Flight client discovers
//! the verb without reading this file.
//!
//! Three things about it are decided elsewhere and worth following the links for: the *mechanism* is
//! a signal into the query's own task rather than an abort from outside, so every exit path keeps
//! running the same bookkeeping ([`crate::cancel`]); the *scope* is this process's queries, like
//! admission ([`crate::scheduler`]); and worker-side work is **not** cancelled across the Flight
//! boundary, it drains on its own ([`crate::cancel`] again, which states exactly what that does and
//! does not stop).
//!
//! # Identity: proven, not claimed
//!
//! This used to be the hole in the design. The ticket's `account` field was believed verbatim, so
//! anyone who could reach the port could name any tenant, read whatever that tenant could read, and
//! have the query filed under their history. Issue #19 closes it:
//!
//! 1. The credential is an API key in the request **metadata** (`authorization: Bearer <token>`),
//!    never in the ticket — a ticket is logged and stored, and a secret must not be.
//! 2. The tenant is derived from the credential. A ticket that *also* names an account must name
//!    the same one, or the request is `PERMISSION_DENIED`; the ticket's field is now a redundant
//!    assertion the server can check, not an input it obeys.
//! 3. Every object the query's logical plan touches is checked against the caller's grants before
//!    the plan is staged, dispatched, or answered from the result cache. See [`crate::rbac`].
//! 4. That credential travels inside TLS. A token on an unencrypted channel is a token anyone on
//!    the path can read and replay indefinitely, so a coordinator that is *checking* credentials
//!    (i.e. one with a services database) will not bind a plaintext port unless an operator
//!    explicitly asks for one. See [`crate::tls`].
//! 5. The answer travels *with the query*. Everything above happens here, on the coordinator, and a
//!    worker used to see none of it — it received a physical plan and ran it. Every dispatch now
//!    carries a signed, short-lived [plan assertion](crate::plan_assertion) naming the account, the
//!    user and the locations this plan was authorized to read, and a worker checks the plan it is
//!    handed against it. Read that module for what the check can and cannot verify.
//!
//! **Whether authentication is enforced follows the services database**, because that is where
//! accounts, users, keys and grants live. With no `--metadata-*` there is nothing to authenticate
//! against and everything runs — the documented single-node mode (CLAUDE.md: `cargo run` must never
//! need Postgres). With one, a credential is required unless an operator explicitly sets
//! [`CoordinatorConfig::allow_anonymous`], which exists for the deployment that adds a control
//! plane before it has issued its first key, and which logs a warning on every startup for exactly
//! as long as it is set.
//!
//! # Admission is fleet-wide, with one condition
//!
//! Two servers pointed at one warehouse share its limit: a query needs a slot in this process's
//! semaphore *and* one of the warehouse's fleet-wide slots in the services database, so `K` is a
//! property of the warehouse rather than of however many coordinators are running. The condition is
//! the usual one — it needs a services database, and this process must be registered in it
//! ([`crate::liveness`]), because a claim's expiry *is* its holder's registration. With no
//! `--metadata-*` there is no control plane to share a limit through and admission is per process,
//! exactly as it always was. So is a query routed at a raw `--workers` fleet, which has no
//! warehouse row for two coordinators to agree on. The design, the costs and the degraded mode are
//! all in [`crate::scheduler`] and [`crate::fleet_admission`].
//!
//! # What this does NOT do
//!
//! - **No submit-then-poll.** `do_get` is one call: the client's stream stays open while the query
//!   queues and runs, then delivers the batches. That is how a JDBC-style client behaves anyway. A
//!   detached submit — `do_action("submit")` returning a id, then `do_get` on a ticket naming it —
//!   is expressible in Flight and would need a place to park results; it is not needed to serve
//!   concurrent queries, so it is not here.
//! - **No proof of *which* peer is calling.** Transport security is in place and is server
//!   authenticated only: this port serves TLS when it is given a certificate, and **refuses to bind
//!   a plaintext one at all** while a services database is configured unless `--allow-plaintext`
//!   says so (see [`crate::tls`] for the rule and why it keys on that). So the bearer token above
//!   is no longer readable or replayable by someone on the path — but nothing here verifies a
//!   *client* certificate. mTLS, and the question of whether it should replace the worker
//!   boundary's shared secret, is a separate decision and is not what this does; `LLDB_FLEET_TOKEN`
//!   is untouched and is still the only thing proving fleet membership.
//! - **No *proof* that the coordinator authorized a worker's plan — only that the fleet did.** This
//!   bullet used to read "no per-request identity at the worker boundary", and that part is closed:
//!   every dispatch now carries a short-lived, MAC'd assertion naming the account, the user and the
//!   object-store locations the grant check passed, and a worker refuses a plan whose file scans
//!   fall outside them ([`crate::plan_assertion`]). What is *not* closed is who can produce one. The
//!   key is HMAC-derived from `LLDB_FLEET_TOKEN`, so it is symmetric: a compromised **worker** can
//!   mint an assertion as easily as it verifies one, and the honest claim is "someone in this fleet
//!   authorized this plan", not "the coordinator did". Nor can a worker check the *whole* assertion
//!   — a physical plan has no table names, so `SELECT on table lldb.sales.orders` travels for audit
//!   while only the file locations are verifiable. Asymmetric keys are what would make the first gap
//!   go away; neither is what this does.
//! - **No cancellation of another coordinator's query, and none of the fleet's work.** A cancel is
//!   answered by the process that is running the query and refused as "not running here" by any
//!   other, and the workers executing its stages are not told (see [`crate::cancel`] for both, and
//!   for what dropping the coordinator's streams does and does not stop).
//! - **No streaming results.** Batches are collected whole before any are sent, which is what makes
//!   stage reassignment safe — see [`crate::flight::fetch_partition_with_failover`].
//!
//! # Shutdown
//!
//! On the shutdown signal the scheduler is [closed](crate::scheduler::Scheduler::close) *first*,
//! then tonic drains. Closing wakes every queued query with "coordinator is shutting down" and
//! marks its history row `failed`, rather than leaving it waiting for a slot that is never coming.
//! Queries already running finish normally, and the server exits when the last one does.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{AUTHORIZATION_HEADER, AuthError, Principal, bearer_header, bearer_token};
use crate::cancel::{
    CANCEL_ACCEPTED, CANCEL_ACTION, Cancellation, QueryRegistry, decode_cancel_body,
    encode_cancel_body,
};
use crate::engine::{
    BoxResolver, TenantSession, TenantSessions, execute_query_cached, resolve_fleet, total_rows,
};
use crate::liveness::CoordinatorIdentity;
use crate::query_log::{QueryRecord, QueryState};
use crate::rbac::{ObjectRef, Privilege, QueryAuthorization, Requirement};
use crate::result_cache::ResultCache;
use crate::scheduler::{Admission, AdmissionError, AdmissionLimits, DEFAULT_FLEET_KEY, Scheduler};
use crate::services::ServicesDb;
use crate::tls::ServerTls;
use crate::warehouse::Warehouse;

/// Boxed tonic response stream.
type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Default address the query server binds. One below the worker's 50051, so a laptop can run both
/// without a flag.
pub const DEFAULT_SERVER_BIND: &str = "127.0.0.1:50050";

/// gRPC response-metadata key carrying the assigned query id, on success and on failure alike.
pub const QUERY_ID_HEADER: &str = "lldb-query-id";

/// Number of fixed header bytes in a query ticket: two `u32` length prefixes.
const TICKET_HEADER_LEN: usize = 4 + 4;

/// What a client asks the server to run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryRequest {
    /// Tenant to run as. `None` means the server's `--account` default.
    pub account: Option<String>,
    /// Virtual warehouse to run on. `None` means the server's `--workers` fleet.
    pub warehouse: Option<String>,
    pub sql: String,
}

impl QueryRequest {
    /// A query against the server's defaults.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            ..Self::default()
        }
    }

    /// Run it on a named warehouse.
    pub fn on_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Run it as a named tenant.
    pub fn as_account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }
}

/// Encode a query ticket. See the module docs for the layout.
pub fn encode_query_ticket(request: &QueryRequest) -> Vec<u8> {
    let account = request.account.as_deref().unwrap_or_default().as_bytes();
    let warehouse = request.warehouse.as_deref().unwrap_or_default().as_bytes();
    let sql = request.sql.as_bytes();

    let mut buf =
        Vec::with_capacity(TICKET_HEADER_LEN + account.len() + warehouse.len() + sql.len());
    buf.extend_from_slice(&(account.len() as u32).to_le_bytes());
    buf.extend_from_slice(account);
    buf.extend_from_slice(&(warehouse.len() as u32).to_le_bytes());
    buf.extend_from_slice(warehouse);
    buf.extend_from_slice(sql);
    buf
}

/// Decode a query ticket, or say precisely what is malformed about it.
pub fn decode_query_ticket(ticket: &[u8]) -> Result<QueryRequest> {
    /// Read a `u32`-length-prefixed UTF-8 field, returning it and the remainder.
    fn take_field<'a>(bytes: &'a [u8], what: &str) -> Result<(&'a str, &'a [u8])> {
        if bytes.len() < 4 {
            bail!("ticket truncated before the {what} length prefix");
        }
        let (len_bytes, rest) = bytes.split_at(4);
        let len = u32::from_le_bytes(len_bytes.try_into().expect("4 bytes")) as usize;
        if rest.len() < len {
            bail!(
                "ticket claims a {len}-byte {what} but only {} bytes remain",
                rest.len()
            );
        }
        let (field, rest) = rest.split_at(len);
        let field = std::str::from_utf8(field)
            .with_context(|| format!("ticket {what} is not valid UTF-8"))?;
        Ok((field, rest))
    }

    let (account, rest) = take_field(ticket, "account")?;
    let (warehouse, sql) = take_field(rest, "warehouse")?;
    let sql = std::str::from_utf8(sql).context("ticket SQL is not valid UTF-8")?;
    if sql.trim().is_empty() {
        bail!("ticket carries no SQL");
    }

    Ok(QueryRequest {
        account: non_empty(account),
        warehouse: non_empty(warehouse),
        sql: sql.to_string(),
    })
}

/// `Some(s)` for a non-blank field, `None` otherwise — an empty length-prefixed field means
/// "unset", not "the empty account".
fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// How a query failed, in the two categories a client must be able to tell apart.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The scheduler refused it: the queue is full, or the server is shutting down. Nothing ran.
    /// Retriable by the client, unlike everything else here.
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    /// The credential was missing, malformed, revoked, expired, or simply wrong — or the control
    /// plane could not be asked. Nothing about the query was even looked at.
    #[error("{0}")]
    Unauthenticated(AuthError),
    /// Authenticated, but not entitled: a missing grant, or a ticket claiming another tenant.
    #[error("{0:#}")]
    Denied(anyhow::Error),
    /// The request itself is wrong — an unknown account or warehouse, a suspended warehouse, a
    /// malformed ticket. Retrying verbatim will fail identically.
    #[error("{0:#}")]
    Request(anyhow::Error),
    /// Planning or execution failed.
    #[error("{0:#}")]
    Execution(anyhow::Error),
    /// Somebody stopped it with `do_action("cancel", <id>)`. The string is the same reason stored
    /// on the history row, so the client and the row tell one story.
    ///
    /// Not a variant of [`QueryError::Execution`]: a cancelled query did not fail, and a client
    /// that cannot tell "your query hit an error" from "somebody stopped your query" will retry the
    /// second one forever.
    #[error("{0}")]
    Cancelled(String),
    /// A cancel named a query this coordinator is not running.
    ///
    /// Also what a caller gets for a query belonging to **another account** — deliberately
    /// indistinguishable, so query ids cannot be probed across the tenant boundary. See
    /// [`Coordinator::cancel_query`].
    #[error(
        "query {0} is not running on this coordinator: it has already finished, it was never \
         submitted here, or it belongs to another coordinator process (see queries.coordinator in \
         the services database — a cancel must be sent to the coordinator that is running it)"
    )]
    NotRunning(i64),
}

impl QueryError {
    /// The gRPC status this maps to. The codes are the contract a client retries against:
    /// `RESOURCE_EXHAUSTED` and `UNAVAILABLE` say "try again", `INVALID_ARGUMENT` says "do not".
    ///
    /// `UNAUTHENTICATED` and `PERMISSION_DENIED` are kept distinct because they call for different
    /// actions: the first means "get a credential", the second means "get a grant". Collapsing them
    /// into one code is a common and unhelpful habit — it leaks nothing, since a caller who reached
    /// `PERMISSION_DENIED` has already proven who they are.
    pub fn to_status(&self) -> Status {
        match self {
            QueryError::Admission(AdmissionError::QueueFull { .. }) => {
                Status::resource_exhausted(self.to_string())
            }
            QueryError::Admission(AdmissionError::ShuttingDown { .. }) => {
                Status::unavailable(self.to_string())
            }
            // A control plane that will not answer is our fault and is worth retrying; a bad
            // credential is the caller's and is not.
            QueryError::Unauthenticated(AuthError::Unavailable(_)) => {
                Status::unavailable(self.to_string())
            }
            QueryError::Unauthenticated(_) => Status::unauthenticated(self.to_string()),
            QueryError::Denied(_) => Status::permission_denied(self.to_string()),
            QueryError::Request(_) => Status::invalid_argument(self.to_string()),
            QueryError::Execution(_) => Status::internal(self.to_string()),
            // `CANCELLED` is gRPC's own word for it, and it is neither `INTERNAL` (nothing broke)
            // nor `ABORTED` (nothing will succeed on retry) — a client that resubmits will simply
            // run the query again, which is exactly right.
            QueryError::Cancelled(_) => Status::cancelled(self.to_string()),
            QueryError::NotRunning(_) => Status::not_found(self.to_string()),
        }
    }
}

/// What happened to one submission. The id is carried separately from the result because a client
/// needs it in *both* outcomes — a failed query is exactly the one whose history row gets read.
pub struct QueryOutcome {
    /// The id assigned in query history, or `None` when no services database is configured.
    pub query_id: Option<i64>,
    pub result: Result<Vec<RecordBatch>, QueryError>,
}

/// Everything the server needs that does not change between queries.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Tenant a query runs as when its ticket names none.
    pub default_account: String,
    /// The fleet endpoints used when a query names no warehouse — `--workers`, verbatim.
    pub workers: Vec<String>,
    /// Templates a warehouse's endpoint is rendered from; `{warehouse}` is substituted.
    pub warehouse_endpoint: Vec<String>,
    /// Concurrency limit. `None` means "size each warehouse's gate from its row", which is the
    /// intended configuration — an explicit value overrides *every* warehouse, including ones
    /// this server has never heard of.
    pub max_concurrent_queries: Option<usize>,
    /// Queue depth per warehouse.
    pub max_queued_queries: usize,
    /// How this process identifies itself on every row it writes: a stable slot (what
    /// `--coordinator-id` names, and what `queries.coordinator` has always held) plus a per-process
    /// incarnation.
    ///
    /// It is a pair rather than a string because a coordinator id is ambiguous on its own — a
    /// restart onto a *new* address looks like a different coordinator, and a restart onto the
    /// *same* one looks like the same process. Recording both means a reader of history can tell
    /// which happened. See [`crate::liveness`], which is also what turns the pair into a liveness
    /// answer when there is a services database to register with.
    pub coordinator: CoordinatorIdentity,
    /// Serve unauthenticated requests *even though* a services database is configured.
    ///
    /// The escape hatch, and it is `false` by default on purpose: security that has to be turned on
    /// is security that is off. It exists because adding a control plane and issuing the first API
    /// key are two deploys, not one, and a cluster that becomes unqueryable in between is a cluster
    /// nobody adds a control plane to. A server with this set logs a warning at startup naming the
    /// flag, every time, for as long as it is set.
    ///
    /// It has no effect without a services database — there is nothing to authenticate against
    /// there, so anonymous is already the only mode.
    pub allow_anonymous: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            default_account: "default".to_string(),
            workers: Vec::new(),
            warehouse_endpoint: vec![crate::discovery::DEFAULT_WAREHOUSE_ENDPOINT.to_string()],
            max_concurrent_queries: None,
            max_queued_queries: crate::scheduler::DEFAULT_MAX_QUEUED_QUERIES,
            coordinator: CoordinatorIdentity::new(DEFAULT_SERVER_BIND),
            allow_anonymous: false,
        }
    }
}

/// The long-running coordinator: its tenants' sessions, a scheduler, and optionally a control
/// plane.
///
/// Held behind an `Arc` and shared by every in-flight request. **Nothing in here is per-query
/// state**, which is what makes serving N queries from one of these correct rather than lucky.
///
/// That sentence used to be true because everything here was immutable after construction. It is
/// still true, but for a slightly weaker and more interesting reason, and pretending otherwise
/// would be the kind of stale invariant that gets someone into trouble: [`TenantSessions`] is a
/// *mutable memo*. It builds an account's [`SessionContext`] the first time that account submits a
/// query and keeps it. What makes that safe is that a session is a pure function of `(account,
/// storage config, catalog source)` — no part of a query, its ticket, its warehouse or its
/// credential reaches it — so the map is a cache of a deterministic computation rather than
/// accumulated request state. Per-*tenant* is not per-query. Anything genuinely per-query still
/// lives on the stack of [`Self::run_query`], and anything that starts depending on the *order*
/// queries arrived in has broken the invariant even if it compiles.
pub struct Coordinator {
    /// One [`SessionContext`] per account. See [`TenantSessions`] for why a single process-wide
    /// context is the wrong answer here: `register_catalog` is global to a context, so one context
    /// serving every tenant would make every tenant's catalog *name* visible to every other one.
    sessions: TenantSessions,
    db: Option<ServicesDb>,
    config: CoordinatorConfig,
    scheduler: Scheduler,
    /// Injected DNS. `None` is the production path (real `lookup_host`); tests supply a fake so a
    /// warehouse's name can resolve to in-process workers.
    resolver: Option<BoxResolver>,
    /// The cross-query result cache, when one is configured. `None` disables it entirely, which is
    /// also what a server with no services database gets.
    result_cache: Option<ResultCache>,
    /// Which queries this process is running, so a `do_action("cancel", id)` has something to reach
    /// into. Per-query in *content* but not per-query in *lifetime* — entries come and go with the
    /// requests that own them, which is why the invariant above still holds.
    in_flight: QueryRegistry,
}

impl Coordinator {
    /// A coordinator serving one fixed session to every caller, recording history in `db` when
    /// there is one.
    ///
    /// The **single-tenant** constructor. It is right for a process with no control plane, and for
    /// an embedding that builds its own context; it is wrong for a multi-tenant front door, which
    /// wants [`Self::multi_tenant`] so that each account gets its own catalogs rather than sharing
    /// one. Kept as `new` because a fixed session is what every caller that hand-registers tables
    /// means, and because a server *with* a services database now names the other one explicitly.
    pub fn new(ctx: SessionContext, db: Option<ServicesDb>, config: CoordinatorConfig) -> Self {
        Self::multi_tenant(
            TenantSessions::fixed(TenantSession::new(ctx, Vec::new())),
            db,
            config,
        )
    }

    /// A coordinator over `sessions`, which decides how tenants are kept apart.
    pub fn multi_tenant(
        sessions: TenantSessions,
        db: Option<ServicesDb>,
        config: CoordinatorConfig,
    ) -> Self {
        let scheduler = Scheduler::new(AdmissionLimits {
            max_concurrent: config
                .max_concurrent_queries
                .unwrap_or(crate::scheduler::DEFAULT_MAX_CONCURRENT_QUERIES),
            max_queued: config.max_queued_queries,
        });
        Self {
            sessions,
            db,
            config,
            scheduler,
            resolver: None,
            result_cache: None,
            in_flight: QueryRegistry::new(),
        }
    }

    /// Resolve worker endpoints through `resolver` instead of DNS. Test seam; see [`BoxResolver`].
    pub fn with_resolver(mut self, resolver: BoxResolver) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Share every warehouse's concurrency limit with the rest of the fleet through `fleet`.
    ///
    /// A builder called before the first query, because a warehouse's gate is created on first
    /// sight and keeps whatever bound it was born with. `lldb-qe-server` passes
    /// [`crate::fleet_admission::FleetAdmission::start_if_registered`]'s answer straight in, so a
    /// deployment with no services database — or one whose registration did not happen — is simply
    /// a coordinator that never calls this and is bounded per process, exactly as before.
    pub fn with_fleet_admission(mut self, fleet: Arc<dyn crate::scheduler::FleetGate>) -> Self {
        self.scheduler = self.scheduler.with_fleet(fleet);
        self
    }

    /// Serve repeat queries from `cache`.
    ///
    /// The lakehouses the cache versions against are **not** passed here any more, and that is the
    /// fix rather than a simplification: they now come from the same [`TenantSession`] the query
    /// plans in, so a query's key can only ever be versioned against the catalogs that query
    /// actually read. Handing them in separately was what made it possible to pair a cache with
    /// another tenant's handles — which does not fail, it just stops caching (see
    /// [`ResultCache::catalog_mismatch_count`]).
    pub fn with_result_cache(mut self, cache: ResultCache) -> Self {
        self.result_cache = Some(cache);
        self
    }

    /// The result cache in force, if any — what a test asserts hits and executions on.
    pub fn result_cache(&self) -> Option<&ResultCache> {
        self.result_cache.as_ref()
    }

    /// The admission control this server is enforcing — the handle a test asserts peak
    /// concurrency on, and what a future `/stats` endpoint would render.
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// The configuration in force.
    pub fn config(&self) -> &CoordinatorConfig {
        &self.config
    }

    /// The queries this process is running right now — what a cancel reaches into.
    pub fn in_flight(&self) -> &QueryRegistry {
        &self.in_flight
    }

    /// Whether this server requires a credential. See [`CoordinatorConfig::allow_anonymous`].
    pub fn requires_authentication(&self) -> bool {
        self.db.is_some() && !self.config.allow_anonymous
    }

    /// Say the security posture out loud, once, at startup. Called by `lldb-qe-server`; a posture
    /// nobody logged is a posture nobody chose.
    pub fn log_posture(&self) {
        if self.db.is_none() {
            tracing::warn!(
                "no services database: this coordinator is UNAUTHENTICATED and enforces no access \
                 control — every query runs as the configured default account. That is the \
                 supported single-node mode; configure --metadata-url to turn accounts, API keys \
                 and grants on."
            );
        } else if self.config.allow_anonymous {
            tracing::warn!(
                "--allow-anonymous is set: requests WITHOUT an API key are served and are not \
                 access-checked, even though a services database is configured. Issue a key with \
                 `lldb-qe-auth key create` and drop the flag."
            );
        } else {
            tracing::info!(
                "authentication is on: every query needs `authorization: Bearer <token>` and is \
                 authorized against its account's grants before dispatch"
            );
        }
    }

    /// Stop admitting new work. Queued queries wake refused; running ones finish.
    pub fn begin_shutdown(&self) {
        tracing::info!("shutting down: no new queries will be admitted");
        self.scheduler.close();
    }

    /// Schedule and run one query, start to finish.
    ///
    /// The order of operations is the design:
    ///
    /// 0. **Authenticate** — turn `credential` into a [`Principal`], and load its grants. Before
    ///    anything else, because everything else is scoped by the answer, and because an
    ///    unauthenticated caller must not be able to make this process do work.
    /// 1. **Resolve the target** — account, warehouse, endpoints. Before any row is written,
    ///    because a query whose tenant cannot be resolved has nothing to be attributed to. This is
    ///    also where `USAGE` on the named warehouse is checked, so an unauthorized warehouse never
    ///    reaches history or the admission queue.
    /// 2. **Record it `queued`** — before admission, so a query that waits ten minutes for a slot
    ///    is visible for those ten minutes rather than appearing only once it runs.
    /// 3. **Acquire a slot** — this is where queueing happens, and where a refusal is turned into
    ///    a `failed` row rather than a silent drop.
    /// 4. **Mark it `running`, execute, mark it terminal.** The slot is an RAII guard held across
    ///    all of it, so every exit path returns it. The *object*-level authorization check happens
    ///    inside execution rather than here, because it needs the logical plan — see
    ///    [`crate::rbac`] for why the logical plan is the last place it can happen.
    ///
    /// History writes after step 2 are **best effort**: a query that is already executing must not
    /// be killed because the services database hiccuped, and the terminal write will correct the
    /// row anyway. Step 2 itself is *not* best effort — if the control plane is configured and
    /// cannot record an accepted query, accepting it would be a lie.
    ///
    /// `credential` is the bearer token from the request metadata, if the client sent one. It is a
    /// separate parameter rather than a field on [`QueryRequest`] on purpose: the request is what
    /// gets encoded into a ticket, logged and stored, and a secret belongs in none of that.
    pub async fn run_query(&self, request: QueryRequest, credential: Option<&str>) -> QueryOutcome {
        let (principal, authorization) = match self.authenticate(credential).await {
            Ok(pair) => pair,
            Err(error) => {
                return QueryOutcome {
                    query_id: None,
                    result: Err(error),
                };
            }
        };

        let target = match self
            .resolve_target(&request, principal.as_ref(), authorization.as_ref())
            .await
        {
            Ok(target) => target,
            Err(error) => {
                return QueryOutcome {
                    query_id: None,
                    result: Err(error),
                };
            }
        };

        let record = match self.record_submission(&target, &request.sql).await {
            Ok(record) => record,
            Err(error) => {
                return QueryOutcome {
                    query_id: None,
                    result: Err(QueryError::Execution(error)),
                };
            }
        };
        let query_id = record.as_ref().map(|r| r.id);
        // From here on the row is active, and everything below is awaited — so everything below is
        // a point at which this whole future can be dropped, taking the terminal write with it.
        // The guard is what closes that hole; see [`ActiveQuery`].
        let mut active = self
            .db
            .clone()
            .zip(query_id)
            .map(|(db, id)| ActiveQuery::new(db, id));

        // Registered from here to the end of the call, so `do_action("cancel", id)` can find it.
        // Only a query that *has* an id is registerable, which is the same thing as saying
        // cancellation follows the services database — with no control plane there is no id to name
        // it by. See [`crate::cancel`].
        let running = query_id.zip(target.account_id).map(|(id, account_id)| {
            self.in_flight
                .register(id, account_id, target.admission_key.clone())
        });

        // Only the cancellation branch below can clear this. Every other path has already issued
        // its own terminal write by the time it returns here, and stands the guard down as it
        // always did.
        let mut cancellation_recorded = true;
        let result = match running {
            None => {
                self.run_admitted(&target, &request.sql, query_id, authorization.as_ref())
                    .await
            }
            Some(mut running) => {
                // The whole mechanism, in one statement. `select!` drops the losing branch, so a
                // cancellation drops the admit-and-execute future — and with it the `QuerySlot`
                // guard, which hands the permit straight to the next waiter. Nothing calls
                // `release()`, because there is no `release()`; see [`crate::scheduler`].
                //
                // `biased` so execution is polled first: a query that finished in the same tick as
                // its cancellation arrived is reported by its real outcome, not overwritten by a
                // cancellation that changed nothing.
                tokio::select! {
                    biased;
                    outcome = self.run_admitted(
                        &target, &request.sql, query_id, authorization.as_ref(),
                    ) => outcome,
                    cancellation = running.cancelled() => {
                        // Re-arm the guard with the cancellation *before* attempting the write.
                        // Doing it in this order is what lets the failure path need no code of its
                        // own: if the write lands, the guard is stood down below and this cost
                        // nothing; if it does not, the guard is already carrying the right terminal
                        // state rather than its default of "abandoned by its client".
                        let reason = cancellation.reason();
                        if let Some(active) = active.as_mut() {
                            active.will_record_cancellation(&reason);
                        }
                        // Written here rather than inside `run_admitted`, which no longer exists by
                        // the time this runs. Unconditional, like every other terminal write on
                        // this path: this task owns the row and is the authority on it, which is
                        // exactly the asymmetry `crate::reaper` rests on.
                        cancellation_recorded = self.mark_cancelled(query_id, &reason).await;
                        Err(QueryError::Cancelled(reason))
                    }
                }
            }
        };
        // Reached on every path that produced an outcome, and each of them — success, failure,
        // refusal, cancellation — has already written a terminal state, so the guard has nothing
        // left to do and is stood down.
        //
        // Except when a cancellation's own terminal write *failed*, which is the one case this
        // condition exists for. Standing the guard down there would leave the row in `running` with
        // nothing left in the world to resolve it: not this process, which has just declared itself
        // finished with the query, and not `crate::reaper`, whose predicate takes a row only when no
        // *live* coordinator matches both its slot and its incarnation — and this coordinator is
        // alive. The row would sit active until this process exits, counting against
        // `list_active_queries` and against every later query's `peak_concurrency`, which is exactly
        // the drift the reaper was built to remove.
        //
        // So the guard stays armed instead — and it stays armed with the cancellation, because the
        // objection to its default is still right: recording a query somebody deliberately stopped
        // as abandoned by its client would be a worse lie than either outcome. `Drop` writes
        // `cancelled`, on a second and independent round trip, which is where a blip that took the
        // first one has the best chance of already being over.
        if cancellation_recorded && let Some(active) = active {
            active.finished();
        }
        match &result {
            // A cancellation is not a failure and must not be logged as one — an operator grepping
            // for failed queries is asking a health question, and careful users answering it wrong
            // is the same mistake `QueryState::Cancelled` exists to avoid in history.
            Err(QueryError::Cancelled(reason)) => tracing::info!(
                query_id = ?query_id,
                warehouse = %target.admission_key,
                reason = %reason,
                "query cancelled; its slot is back"
            ),
            Err(error) => tracing::warn!(
                query_id = ?query_id,
                warehouse = %target.admission_key,
                user = principal.as_ref().map(|p| p.to_string()),
                error = %error,
                "query failed"
            ),
            Ok(_) => {}
        }
        QueryOutcome { query_id, result }
    }

    /// Stop a query this coordinator is running, by the id its submission returned.
    ///
    /// The order of operations is the design, and two of the three steps are about not answering
    /// questions the caller has no right to ask:
    ///
    /// 0. **Authenticate**, exactly as a submission does. An unauthenticated caller must not be able
    ///    to make this process do work, and must certainly not be able to stop somebody's query.
    /// 1. **Find it, then check the tenant.** A query belonging to another account is answered with
    ///    the *same* [`QueryError::NotRunning`] as one that is not here at all. That is deliberate:
    ///    query ids are consecutive integers from one sequence shared by every tenant, so a
    ///    distinguishable "permission denied" would let any authenticated caller walk the id space
    ///    and learn precisely which ids belong to other tenants and when they were running. The
    ///    account is taken from the credential and never from the request, like everywhere else.
    /// 2. **Check the grant.** Within the account, cancelling is
    ///    [`Privilege::Cancel`](crate::rbac::Privilege::Cancel) on the warehouse whose slot the
    ///    cancellation frees. This one *is* reported as `PERMISSION_DENIED`, because a caller who
    ///    got here has already proven they are in the tenant, so naming the missing grant leaks
    ///    nothing and saves a support ticket.
    ///
    /// Only then is anything signalled — the check and the signal are two calls, so there is no
    /// interleaving in which a refusal still stops a query.
    ///
    /// This coordinator's own queries and no others; see [`crate::cancel`] for why forwarding is not
    /// the right shape here.
    pub async fn cancel_query(
        &self,
        query_id: i64,
        credential: Option<&str>,
    ) -> Result<(), QueryError> {
        let (principal, authorization) = self.authenticate(credential).await?;

        let Some(running) = self.in_flight.describe(query_id) else {
            return Err(QueryError::NotRunning(query_id));
        };

        if let Some(principal) = &principal
            && principal.account_id != running.account_id
        {
            // Logged, because from the caller's side this is indistinguishable from a stale id and
            // an operator investigating a cross-tenant probe would otherwise have nothing to read.
            tracing::warn!(
                query_id,
                user = %principal,
                "refused a cancel for a query belonging to a different account"
            );
            return Err(QueryError::NotRunning(query_id));
        }

        if let Some(authorization) = &authorization {
            authorization
                .check(&Requirement::new(
                    Privilege::Cancel,
                    ObjectRef::warehouse(running.admission_key.clone()),
                ))
                .map_err(QueryError::Denied)?;
        }

        let cancellation = match &principal {
            Some(principal) => Cancellation::by(principal.user_name.clone()),
            None => Cancellation::anonymous(),
        };
        if !self.in_flight.cancel(query_id, cancellation) {
            // It finished between the lookup and here. Nothing went wrong; there is simply nothing
            // to stop, and saying so is more useful than a success the caller would misread.
            return Err(QueryError::NotRunning(query_id));
        }
        tracing::info!(
            query_id,
            warehouse = %running.admission_key,
            user = principal.as_ref().map(|p| p.to_string()),
            "cancellation requested"
        );
        Ok(())
    }

    /// Step 0: prove the credential and load what it may do.
    ///
    /// Returns `(None, None)` for a server with no control plane, and for one running with
    /// [`CoordinatorConfig::allow_anonymous`] against a caller that sent nothing — both mean "there
    /// is no identity here", and every downstream check is skipped rather than defaulted.
    ///
    /// A caller that *does* present a credential is always verified, even under `allow_anonymous`.
    /// Anything else would let a bad token be quietly upgraded to full access, which is the worst
    /// possible reading of a permissive flag.
    async fn authenticate(
        &self,
        credential: Option<&str>,
    ) -> Result<(Option<Principal>, Option<QueryAuthorization>), QueryError> {
        let Some(db) = &self.db else {
            return Ok((None, None));
        };
        let Some(token) = credential else {
            if self.config.allow_anonymous {
                return Ok((None, None));
            }
            return Err(QueryError::Unauthenticated(AuthError::Missing));
        };

        let principal = db
            .authenticate(token)
            .await
            .map_err(QueryError::Unauthenticated)?;
        let authorization = db
            .authorization_for(&principal)
            .await
            .map_err(|e| QueryError::Unauthenticated(AuthError::Unavailable(e)))?;
        tracing::debug!(
            user = %principal,
            api_key = %principal.api_key_name,
            grants = authorization.grants.len(),
            "authenticated"
        );
        Ok((Some(principal), Some(authorization)))
    }

    /// Steps 3 and 4: queue for a slot, then run under it.
    async fn run_admitted(
        &self,
        target: &Target,
        sql: &str,
        query_id: Option<i64>,
        authorization: Option<&QueryAuthorization>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let admission = self.admission_for(target);
        // The guard. Everything below this line runs holding one permit, and every path out of it
        // — including `?` — drops the guard and hands the permit to the next waiter.
        let slot = match admission.acquire().await {
            Ok(slot) => slot,
            Err(refusal) => {
                self.mark_failed(query_id, &refusal.to_string()).await;
                return Err(QueryError::Admission(refusal));
            }
        };
        tracing::info!(
            query_id = ?query_id,
            warehouse = %slot.warehouse(),
            running = admission.snapshot().running,
            limit = admission.limits().max_concurrent,
            // Which bound this query actually passed. `running` above is this process's count, and
            // under a fleet-wide bound it is legitimately below the limit while the warehouse is
            // saturated by another coordinator — so an operator reading these needs to know which
            // number they are looking at.
            fleet_slot = slot.fleet_lease().map(|lease| lease.slot_no),
            "query admitted"
        );

        if let (Some(db), Some(id)) = (&self.db, query_id)
            && let Err(error) = db.mark_query_running(id).await
        {
            tracing::warn!(query_id = id, error = %format!("{error:#}"), "could not mark the query running");
        }

        let outcome = self.execute(target, sql, authorization).await;
        match &outcome {
            Ok(batches) => {
                let rows = total_rows(batches);
                if let (Some(db), Some(id)) = (&self.db, query_id)
                    && let Err(error) = db.mark_query_succeeded(id, rows as i64).await
                {
                    tracing::warn!(query_id = id, error = %format!("{error:#}"), "could not record the query's success");
                }
                tracing::info!(query_id = ?query_id, rows, "query succeeded");
            }
            Err(error) => self.mark_failed(query_id, &format!("{error:#}")).await,
        }
        // A refusal is *not* an internal error, and a client has to be able to tell them apart —
        // one means "ask for a grant", the other means "file a bug". The probe is on the type, not
        // on the message; see [`crate::rbac::Denied`].
        outcome.map_err(|error| {
            if crate::rbac::is_denial(&error) {
                QueryError::Denied(error)
            } else {
                QueryError::Execution(error)
            }
        })
    }

    /// Discover the fleet and run the query on it. Separated so the slot-holding path above reads
    /// as bookkeeping and this reads as execution.
    async fn execute(
        &self,
        target: &Target,
        sql: &str,
        authorization: Option<&QueryAuthorization>,
    ) -> Result<Vec<RecordBatch>> {
        // Discovery runs per query, exactly as it does in the one-shot: a warehouse resized while
        // this server has been up is picked up by the next query, with no restart.
        let fleet = resolve_fleet(
            &target.endpoints,
            target.declared_size,
            self.resolver.as_ref(),
        )
        .await?;
        // The tenant is `target.account_id` — the one resolved from the credential, never the one
        // the ticket claimed — so one account can never be served another's cached rows, and now
        // also never planned against another's catalogs: this is where a per-account session is
        // selected, and it is selected by the same id. The object-level grant check happens inside,
        // between planning and the cache lookup.
        let session = self.sessions.for_account(target.account_id).await?;
        execute_query_cached(
            session.ctx(),
            self.result_cache.as_ref(),
            session.lakehouses(),
            target.account_id,
            authorization,
            sql,
            &fleet,
        )
        .await
    }

    /// The gate this query queues on. Keyed by warehouse so two warehouses never share a line —
    /// and, for the *fleet-wide* half of the bound, by the warehouse's row id, because a name is
    /// unique only within an account and shared state keyed by one would merge two tenants'
    /// concurrency budgets. See [`crate::scheduler`].
    fn admission_for(&self, target: &Target) -> Arc<Admission> {
        self.scheduler.admission_for(
            &target.admission_key,
            target.warehouse_id,
            target.max_concurrent,
        )
    }

    /// Turn a request into everything execution needs, refusing anything unroutable — or anything
    /// the caller is not entitled to.
    ///
    /// Mirrors the one-shot coordinator's startup exactly — same lookups, same errors, same
    /// suspended-warehouse guard — except that it runs per query rather than per process, because
    /// a long-running server must see an account created, or a warehouse resumed, without a
    /// restart.
    ///
    /// **The tenant comes from `principal`, never from the ticket.** That inversion is the whole
    /// point of the issue this method was rewritten for. The ticket's `account` field survives only
    /// as an assertion: if it names a *different* account than the credential does, the request is
    /// denied rather than quietly reinterpreted, because a client that believes it is talking to
    /// tenant B while the server serves tenant A is a bug worth surfacing loudly on both sides.
    async fn resolve_target(
        &self,
        request: &QueryRequest,
        principal: Option<&Principal>,
        authorization: Option<&QueryAuthorization>,
    ) -> Result<Target, QueryError> {
        // Precedence: the proven identity, then the ticket's claim (only legal when there is no
        // identity to contradict it), then the server's default.
        let account_name = match principal {
            Some(principal) => {
                if let Some(claimed) = &request.account
                    && claimed != &principal.account_name
                {
                    return Err(QueryError::Denied(anyhow!(
                        "this API key belongs to account `{}`, but the request asks to run as \
                         `{claimed}`. An account is derived from the credential and cannot be \
                         chosen by the caller.",
                        principal.account_name
                    )));
                }
                principal.account_name.clone()
            }
            None => request
                .account
                .clone()
                .unwrap_or_else(|| self.config.default_account.clone()),
        };

        let Some(db) = &self.db else {
            // No control plane, so nothing knows what a warehouse name means. Say that rather than
            // falling back to `--workers` and running the query on a fleet nobody chose.
            if let Some(name) = &request.warehouse {
                return Err(QueryError::Request(anyhow!(
                    "--warehouse {name} needs a services database to resolve the warehouse in: \
                     start the server with --metadata-url (LLDB_METADATA_URL), or --metadata-host \
                     (LLDB_METADATA_HOST) plus the other --metadata-* parts"
                )));
            }
            return Ok(Target {
                account_id: None,
                warehouse_id: None,
                endpoints: self.config.workers.clone(),
                declared_size: None,
                admission_key: DEFAULT_FLEET_KEY.to_string(),
                max_concurrent: self
                    .config
                    .max_concurrent_queries
                    .unwrap_or(crate::scheduler::DEFAULT_MAX_CONCURRENT_QUERIES),
            });
        };

        let account = db
            .account_by_name(&account_name)
            .await
            .map_err(QueryError::Request)?
            .with_context(|| {
                format!(
                    "account `{account_name}` does not exist in the services database; create it \
                     with `lldb-qe-migrate --seed-account {account_name}`"
                )
            })
            .map_err(QueryError::Request)?;

        let Some(name) = &request.warehouse else {
            return Ok(Target {
                account_id: Some(account.id),
                warehouse_id: None,
                endpoints: self.config.workers.clone(),
                declared_size: None,
                admission_key: DEFAULT_FLEET_KEY.to_string(),
                max_concurrent: self
                    .config
                    .max_concurrent_queries
                    .unwrap_or(crate::scheduler::DEFAULT_MAX_CONCURRENT_QUERIES),
            });
        };

        // `USAGE` on the warehouse, checked *before* it is looked up.
        //
        // Before, not after, and that ordering is deliberate: a caller with no grant on `analytics`
        // must not be able to learn whether `analytics` exists in this account by comparing "no
        // such warehouse" against "permission denied". The privilege check is a pure function of
        // the name they typed, so it costs nothing to run it first, and running it first makes the
        // two outcomes indistinguishable from outside.
        if let Some(authorization) = authorization {
            authorization
                .check(&Requirement::new(
                    Privilege::Usage,
                    ObjectRef::warehouse(name.clone()),
                ))
                .map_err(QueryError::Denied)?;
        }

        // Scoped by the account id, so another tenant's identically-named warehouse is simply not
        // visible from here.
        let warehouse: Warehouse = db
            .warehouse_by_name(account.id, name)
            .await
            .map_err(QueryError::Request)?
            .with_context(|| {
                format!(
                    "warehouse `{name}` does not exist for account `{}`; create it with \
                     `lldb-qe-warehouse create --account {} --name {name} --size 2`",
                    account.name, account.name
                )
            })
            .map_err(QueryError::Request)?;
        // `endpoint` is what refuses a suspended warehouse — the guard lives on the type, so it
        // cannot be forgotten by a second caller.
        let endpoints = self
            .config
            .warehouse_endpoint
            .iter()
            .map(|template| warehouse.endpoint(template))
            .collect::<Result<Vec<_>>>()
            .map_err(QueryError::Request)?;

        Ok(Target {
            account_id: Some(account.id),
            warehouse_id: Some(warehouse.id),
            endpoints,
            declared_size: Some(warehouse.size),
            admission_key: warehouse.name.clone(),
            // "Sized from the warehouse's row" — one running query per worker unless an operator
            // says otherwise. It is a starting point, not a law of nature: a warehouse serving
            // many small queries wants a higher number and `--max-concurrent-queries` is how to
            // say so.
            max_concurrent: self
                .config
                .max_concurrent_queries
                .unwrap_or(warehouse.size.max(1) as usize),
        })
    }

    /// Step 2: write the `queued` row, when there is a control plane to write it to.
    async fn record_submission(&self, target: &Target, sql: &str) -> Result<Option<QueryRecord>> {
        let (Some(db), Some(account_id)) = (&self.db, target.account_id) else {
            return Ok(None);
        };
        let record = db
            .submit_query(
                account_id,
                target.warehouse_id,
                sql,
                Some(&self.config.coordinator),
            )
            .await
            .context("recording the query in history")?;
        Ok(Some(record))
    }

    /// Best-effort terminal write for a cancellation, and the reason it does *not* need to be a
    /// compare-and-swap.
    ///
    /// [`crate::reaper`] introduced a second writer to a query row and settled the question with an
    /// asymmetry: the owning coordinator writes unconditionally because it is the authority on its
    /// own query, and the reaper proves the row has not moved. Cancellation adds **no third
    /// writer**. The `do_action` handler signals; the task that already owns the row is the one
    /// that writes, from the same `run_query` frame that writes every other terminal state. So this
    /// is writer number one, unchanged, and it composes with the reaper's CAS by construction:
    /// `cancelled` is terminal and therefore outside the reaper's `state IN (active)` predicate, so
    /// a sweep racing this write either sees the row still active and reaps it (the coordinator was
    /// unreachable long enough to be judged dead — the price [`crate::liveness`]'s decision 2 names)
    /// or sees `cancelled` and skips it. Both orderings end in a terminal state and neither loses a
    /// slot.
    ///
    /// Best effort like the rest: a control-plane hiccup must not become a data-plane outage, and
    /// the slot has already been returned by the time this runs.
    ///
    /// Returns **whether the row actually reached `cancelled`**, because best effort is not the same
    /// as nobody caring. `run_query` stands the [`ActiveQuery`] guard down only on `true`; on
    /// `false` it leaves the guard armed with the cancellation, so the row still reaches a terminal
    /// state from the destructor instead of sitting in `running` until this process exits.
    async fn mark_cancelled(&self, query_id: Option<i64>, reason: &str) -> bool {
        let (Some(db), Some(id)) = (&self.db, query_id) else {
            // No services database or no id means there is no row — so there is nothing to write
            // and, equally, nothing left active. Reported as recorded, which is also the only
            // honest answer for the guard: it cannot exist without both of these either.
            return true;
        };
        match db.mark_query_cancelled(id, reason).await {
            Ok(_) => true,
            Err(write_error) => {
                tracing::warn!(
                    query_id = id,
                    error = %format!("{write_error:#}"),
                    "could not record the query's cancellation; \
                     leaving it to the guard to close out"
                );
                false
            }
        }
    }

    /// Best-effort terminal write for a failure. Never propagates: the query has already failed
    /// and the client is owed *that* error, not a second one about the history table.
    async fn mark_failed(&self, query_id: Option<i64>, error: &str) {
        if let (Some(db), Some(id)) = (&self.db, query_id)
            && let Err(write_error) = db.mark_query_failed(id, error).await
        {
            tracing::warn!(
                query_id = id,
                error = %format!("{write_error:#}"),
                "could not record the query's failure"
            );
        }
    }
}

/// What a client sees in `queries.error` when it hung up before its query finished.
const ABANDONED: &str = "the client disconnected before the query finished";

/// Rows [`ActiveQuery`]'s destructor has successfully closed out, and the ones it could not.
///
/// A best-effort write issued from a destructor is the least observable thing in this module:
/// nobody awaits it, no request fails when it does not happen, and the only trace it leaves is a
/// log line. These make it countable — for an operator who wants to know whether clients are
/// hanging up, and for the test that proves the guard actually fires. Process-wide rather than
/// per-coordinator because that is the scope a destructor can reach.
///
/// Named for the case they were built for and the case that overwhelmingly dominates them. The
/// other one — a cancellation whose own terminal write failed, see [`Unrecorded`] — is closed out
/// by the same destructor and counted here too, because what these actually measure is
/// guard-issued writes, and `abandoned_unclosed`'s meaning ("history has rows stuck in
/// `queued`/`running` that only a reaper will resolve") is exactly as true of that case.
static ABANDONED_CLOSED: AtomicUsize = AtomicUsize::new(0);
static ABANDONED_UNCLOSED: AtomicUsize = AtomicUsize::new(0);

/// Abandoned queries whose history row this process managed to close out.
pub fn abandoned_closed() -> usize {
    ABANDONED_CLOSED.load(AtomicOrdering::Acquire)
}

/// Abandoned queries this process could **not** close out — no runtime left, or the write failed.
/// A non-zero value here means history has rows stuck in `queued`/`running` that only a reaper
/// will resolve.
pub fn abandoned_unclosed() -> usize {
    ABANDONED_UNCLOSED.load(AtomicOrdering::Acquire)
}

/// Closes out a query's history row if the request is dropped before it reaches a terminal state.
///
/// A client that hangs up — Ctrl-C, a timeout, a closed tab — makes tonic drop the request future
/// wherever it happens to be awaiting. Without this, the `queued` or `running` row written on the
/// way in is simply never updated: `list_active_queries` accumulates rows that will never finish,
/// and the sweep-line over history that this server's concurrency is *verified* with starts
/// counting phantom queries. The instrument would drift precisely under the conditions you most
/// want to trust it.
///
/// `Drop` cannot await and the terminal write is a database round trip, so the write is handed to
/// the runtime instead. That makes it best effort — exactly like every other history write after
/// acceptance, for the same reason: a database hiccup must not be able to take the server with it.
/// `Handle::try_current` rather than a bare `tokio::spawn` because dropping after the runtime has
/// gone (shutdown, a test) would otherwise panic in a destructor.
///
/// # It is also the backstop for a terminal write that failed
///
/// A client that vanishes is not the only way a row is left active. A **cancellation** whose own
/// `UPDATE` failed — a Postgres blip in exactly that instant — leaves one too, and that row is
/// worse off than an abandoned one: [`crate::reaper`] takes a row only when no *live* coordinator
/// matches its slot and incarnation, and the coordinator that just failed to write it is alive. So
/// nothing out of process would resolve it either, and it would sit in `running` until the process
/// exited. `run_query` therefore does not stand the guard down after a failed cancellation write,
/// and tells it — via [`Unrecorded`] — to record `cancelled` rather than its default.
///
/// Two gaps remain, and both are a reaper's job rather than oversights:
///
/// - **A coordinator that dies outright.** Nothing in-process can close out a row once the process
///   is gone.
/// - **The insert-to-guard window.** The row is created by an `await`, and that insert commits
///   before the future is resumed with the new id — so between "the row exists" and "this guard
///   exists" there is an instant in which a cancellation leaves the row active with nothing
///   watching it. It cannot be closed from here, because constructing a guard needs an id that
///   only the completed insert can supply. The window is one scheduler wake-up wide and a client
///   has to hang up inside it, so it is small — but it is real, and it was not theoretical: the
///   test for this guard originally dropped its query in exactly that window and failed about one
///   run in six until it was taught to wait for the query to reach the admission queue.
///
/// That is what the `coordinator` column is for.
struct ActiveQuery {
    db: ServicesDb,
    query_id: i64,
    finished: bool,
    unrecorded: Unrecorded,
}

/// What [`ActiveQuery`]'s destructor writes when it fires.
///
/// Two cases, and the distinction is the entire reason this is not a `bool`. A row left active
/// because its client vanished and a row left active because a *cancellation's* terminal write
/// failed are different events, and recording the first onto the second would tell an operator that
/// somebody hung up on a query they had in fact deliberately stopped — a lie in history, and in
/// `queries.error` the only place anyone would ever look for the truth.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Unrecorded {
    /// The default, and the case this guard was built for: nothing wrote a terminal state, so the
    /// only remaining explanation is that the request future was dropped out from under the query.
    AbandonedByClient,
    /// The query *was* cancelled and [`Coordinator::mark_cancelled`]'s write did not land. Carries
    /// the same reason that write carried, so the row is indistinguishable from one the ordinary
    /// path wrote.
    Cancelled(String),
}

impl Unrecorded {
    /// The row this guard would write: the state, and the prose that goes in `error`.
    ///
    /// Pulled out of `Drop` so the choice is assertable without a database, a runtime, or a
    /// destructor — which is the only part of this mechanism a test can reach directly.
    fn terminal(&self) -> (QueryState, &str) {
        match self {
            Unrecorded::AbandonedByClient => (QueryState::Failed, ABANDONED),
            Unrecorded::Cancelled(reason) => (QueryState::Cancelled, reason),
        }
    }
}

impl ActiveQuery {
    fn new(db: ServicesDb, query_id: i64) -> Self {
        Self {
            db,
            query_id,
            finished: false,
            unrecorded: Unrecorded::AbandonedByClient,
        }
    }

    /// This query was cancelled: if the guard fires, record *that* rather than an abandonment.
    ///
    /// Called before the cancellation's own terminal write is attempted, so the guard is correct
    /// whether or not that write lands. It does not arm the guard — the guard is armed from
    /// construction — it only changes what arming means.
    fn will_record_cancellation(&mut self, reason: &str) {
        self.unrecorded = Unrecorded::Cancelled(reason.to_string());
    }

    /// The query reached a terminal state under its own power; stand down.
    ///
    /// Consuming `self` is deliberate: a guard that has been stood down cannot be re-armed, so the
    /// only way to reach `Drop` with something left to write is never to have called this.
    fn finished(mut self) {
        self.finished = true;
    }
}

impl Drop for ActiveQuery {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let (db, id) = (self.db.clone(), self.query_id);
        let unrecorded = self.unrecorded.clone();
        let (state, detail) = unrecorded.terminal();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            ABANDONED_UNCLOSED.fetch_add(1, AtomicOrdering::AcqRel);
            tracing::warn!(
                query_id = id,
                state = %state,
                "no runtime left to close out an active query; the row stays active"
            );
            return;
        };
        tracing::info!(
            query_id = id,
            state = %state,
            detail = %detail,
            "closing out a query row the request left active"
        );
        handle.spawn(async move {
            // One arm per variant rather than one call parameterised by state, because
            // `mark_query_failed` and `mark_query_cancelled` are the two documented entry points
            // and a new `Unrecorded` variant should fail to compile here rather than pick one.
            let written = match &unrecorded {
                Unrecorded::AbandonedByClient => db.mark_query_failed(id, ABANDONED).await,
                Unrecorded::Cancelled(reason) => db.mark_query_cancelled(id, reason).await,
            };
            match written {
                Ok(_) => {
                    ABANDONED_CLOSED.fetch_add(1, AtomicOrdering::AcqRel);
                }
                Err(error) => {
                    ABANDONED_UNCLOSED.fetch_add(1, AtomicOrdering::AcqRel);
                    tracing::warn!(
                        query_id = id,
                        error = %format!("{error:#}"),
                        "could not close out an active query row"
                    );
                }
            }
        });
    }
}

/// Everything resolving a request yields: where to run, whose it is, and what gate it queues on.
#[derive(Debug, Clone)]
struct Target {
    account_id: Option<i64>,
    warehouse_id: Option<i64>,
    endpoints: Vec<String>,
    /// The warehouse row's size, so discovery can warn when desired and observed compute diverge.
    declared_size: Option<i32>,
    admission_key: String,
    max_concurrent: usize,
}

// ---------------------------------------------------------------------------
// Flight service — the wire face of the coordinator.
// ---------------------------------------------------------------------------

/// The Flight service a client submits to. Cloned per request by tonic; the clone is of the
/// `Arc`, not of the coordinator.
#[derive(Clone)]
pub struct CoordinatorFlightService {
    coordinator: Arc<Coordinator>,
}

impl CoordinatorFlightService {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        Self { coordinator }
    }
}

/// Read the bearer token out of a request's metadata, for any verb this service exposes.
///
/// The credential is read from the metadata and never from the ticket or the action body — both of
/// those are logged and stored, and a secret must be in neither.
///
/// A header that is *present but unparseable* is refused here rather than folded into `None`.
/// Folding looks harmless — under the default posture both end in the same `UNAUTHENTICATED` — but
/// under `--allow-anonymous` `None` means "run as nobody, with nothing checked", so a corrupted or
/// mistyped token would silently stop being a credential and its caller would be served as
/// anonymous while believing it was authenticated and scoped. Losing an identity quietly is worse
/// than being told the token is bad.
///
/// Shared by `do_get` and `do_action` rather than written twice: the second copy is exactly where
/// the strictness above would eventually be relaxed by accident.
fn credential_of(metadata: &tonic::metadata::MetadataMap) -> Result<Option<String>, Status> {
    match metadata.get(AUTHORIZATION_HEADER) {
        None => Ok(None),
        Some(value) => Ok(Some(
            value
                .to_str()
                .ok()
                .and_then(|value| bearer_token(value).ok())
                .ok_or_else(|| {
                    Status::unauthenticated(
                        "unauthenticated: the `authorization` header is not a usable \
                         `Bearer <token>` value. Send a valid API key, or send no header at \
                         all — a credential this server cannot read is never treated as one",
                    )
                })?
                .to_string(),
        )),
    }
}

/// Attach the query id to a gRPC response or status, so the client can look the query up in
/// history whichever way it went.
fn stamp_query_id<T>(response: &mut Response<T>, query_id: Option<i64>) {
    if let Some(id) = query_id
        && let Ok(value) = id.to_string().parse()
    {
        response.metadata_mut().insert(QUERY_ID_HEADER, value);
    }
}

/// Serve the query server on `listener` until `shutdown` resolves.
///
/// The signal closes the scheduler *before* tonic begins draining: queued queries wake with a
/// clear "shutting down" and their history rows are marked `failed`, rather than waiting inside a
/// server that is trying to exit. Running queries are allowed to finish.
pub async fn serve_coordinator<F>(
    listener: TcpListener,
    coordinator: Arc<Coordinator>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    serve_coordinator_with_tls(listener, coordinator, ServerTls::plaintext(), shutdown).await
}

/// [`serve_coordinator`], also choosing what the port is served *over*.
///
/// The entry point `lldb-qe-server` calls. Whether `tls` may be
/// [`ServerTls::Plaintext`](crate::tls::ServerTls::Plaintext) at all is decided before we get here,
/// by [`crate::tls::TlsArgs::resolve_server`] — a coordinator with a services database is checking
/// bearer tokens, and a plaintext port under that condition has to be asked for. The posture is
/// logged from in here, next to the credential posture, so every process that serves this port
/// reports both the same way.
pub async fn serve_coordinator_with_tls<F>(
    listener: TcpListener,
    coordinator: Arc<Coordinator>,
    tls: ServerTls,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tls.log_posture("coordinator");
    let service = FlightServiceServer::new(CoordinatorFlightService::new(Arc::clone(&coordinator)));
    tls.configure(Server::builder())?
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            coordinator.begin_shutdown();
        })
        .await
        .context("coordinator Flight server terminated")
}

#[tonic::async_trait]
impl FlightService for CoordinatorFlightService {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;

    /// Submit a query: decode the ticket, schedule it, and stream the answer.
    ///
    /// This call blocks the client's stream for as long as the query is queued *and* running,
    /// which is the honest thing for a synchronous submit to do — and it means a client that hangs
    /// up while queued removes itself from the line.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let credential = credential_of(request.metadata())?;

        let ticket = request.into_inner();
        let query = decode_query_ticket(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad query ticket: {e:#}")))?;

        let outcome = self
            .coordinator
            .run_query(query, credential.as_deref())
            .await;
        let batches = match outcome.result {
            Ok(batches) => batches,
            Err(error) => {
                // Carry the id on the failure too: a failed query is exactly the one whose history
                // row someone wants to read.
                let mut status = Response::new(());
                stamp_query_id(&mut status, outcome.query_id);
                let metadata = status.metadata().clone();
                return Err(Status::with_metadata(
                    error.to_status().code(),
                    error.to_string(),
                    metadata,
                ));
            }
        };

        // Set the schema explicitly so a zero-batch answer still encodes a valid (schema-only)
        // stream — a query that legitimately returns nothing must not look like a broken one.
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        let flight_data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(
                batches.into_iter().map(Ok::<_, FlightError>),
            ))
            .map_err(|e| Status::internal(format!("flight encode: {e}")));

        let mut response = Response::new(Box::pin(flight_data) as Self::DoGetStream);
        stamp_query_id(&mut response, outcome.query_id);
        Ok(response)
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
    /// Stop a running query: `do_action("cancel", <query id in decimal ASCII>)`.
    ///
    /// The body is byte-for-byte what the submission's `lldb-query-id` response header carried, so
    /// a client hands one straight to the other. The credential comes from the metadata, exactly as
    /// it does for a submission — cancelling is an authorized operation, not a side channel around
    /// authorization.
    ///
    /// The reply is a single [`arrow_flight::Result`] carrying [`CANCEL_ACCEPTED`], because a Flight
    /// client that gets an empty stream back cannot tell a server that did the work from one that
    /// ignored the action. "Accepted" is the honest word: the query's task has been signalled and
    /// its slot is returned as it unwinds, which is promptly but not synchronously.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let credential = credential_of(request.metadata())?;
        let action = request.into_inner();
        if action.r#type != CANCEL_ACTION {
            return Err(Status::unimplemented(format!(
                "unknown action `{}`; this coordinator serves `{CANCEL_ACTION}` (call \
                 list_actions)",
                action.r#type
            )));
        }
        let query_id = decode_cancel_body(&action.body)
            .map_err(|e| Status::invalid_argument(format!("bad cancel action: {e:#}")))?;

        self.coordinator
            .cancel_query(query_id, credential.as_deref())
            .await
            .map_err(|error| error.to_status())?;

        let accepted = arrow_flight::Result {
            body: CANCEL_ACCEPTED.as_bytes().to_vec().into(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(accepted) })) as Self::DoActionStream,
        ))
    }

    /// What this coordinator's `do_action` accepts — the one verb, described.
    ///
    /// Implemented rather than left `unimplemented`: `list_actions` is how a Flight client that has
    /// never heard of lldb discovers that cancellation exists, and a Flight service whose actions
    /// can only be learned from a source file is a Flight service in name only.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let cancel = ActionType {
            r#type: CANCEL_ACTION.to_string(),
            description: "Stop a running query. The body is the decimal query id returned in the \
                          `lldb-query-id` response header. Requires CANCEL on the warehouse the \
                          query is running on, within the caller's own account."
                .to_string(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async move { Ok(cancel) })) as Self::ListActionsStream,
        ))
    }
}

// ---------------------------------------------------------------------------
// Client side — submit a query and collect the answer.
// ---------------------------------------------------------------------------

/// What a submission came back with: the assigned query id (when the server has a control plane)
/// and the rows.
#[derive(Debug)]
pub struct SubmittedQuery {
    pub query_id: Option<i64>,
    pub batches: Vec<RecordBatch>,
}

/// Submit `request` to the query server at `server_url`, unauthenticated.
///
/// Kept as its own function rather than folded into [`submit_query_as`] with a `None` at every call
/// site, because "this call carries no credential" should be a thing a reader can see. Against a
/// server with a services database it will be refused — which is the correct outcome and exactly
/// what the acceptance test asserts.
pub async fn submit_query(server_url: &str, request: &QueryRequest) -> Result<SubmittedQuery> {
    submit_query_as(server_url, request, None).await
}

/// Submit `request` to the query server at `server_url`, presenting `token`, and collect the whole
/// answer.
///
/// The reference client, and the one the tests use. It is deliberately tiny: a ticket, a `do_get`,
/// and a decode — which is the point of choosing Flight, since any Arrow Flight client in any
/// language can do the same three things without a line of lldb-specific code. The credential is an
/// ordinary `authorization: Bearer` header, so "any language" includes languages that have never
/// heard of lldb.
pub async fn submit_query_as(
    server_url: &str,
    request: &QueryRequest,
    token: Option<&str>,
) -> Result<SubmittedQuery> {
    // TLS iff `server_url` says `https://`, verified against this process's installed CA. A client
    // that wants an encrypted submission asks for one by URL; there is no negotiation, and a
    // coordinator serving TLS refuses a plaintext client rather than obliging it.
    let channel = crate::tls::dial(server_url)
        .await
        .with_context(|| format!("dialing coordinator {server_url}"))?;
    let mut client = FlightServiceClient::new(channel);

    let mut grpc_request = Request::new(Ticket {
        ticket: encode_query_ticket(request).into(),
    });
    if let Some(token) = token {
        let value = bearer_header(token)
            .parse()
            .context("the API key is not usable as an HTTP header value")?;
        grpc_request
            .metadata_mut()
            .insert(AUTHORIZATION_HEADER, value);
    }

    let response = client.do_get(grpc_request).await.map_err(|status| {
        // Keep the id from the failure metadata in the message: it is the handle to the history
        // row that explains what went wrong.
        match query_id_from(status.metadata()) {
            Some(id) => anyhow!("query {id} failed: {}", status.message()),
            None => anyhow!("submitting the query to {server_url}: {status}"),
        }
    })?;

    let query_id = query_id_from(response.metadata());
    let flight_data = response
        .into_inner()
        .map_err(|status| FlightError::ExternalError(Box::new(status)));
    let batches = FlightRecordBatchStream::new_from_flight_data(flight_data)
        .try_collect::<Vec<_>>()
        .await
        .with_context(|| format!("streaming the result from {server_url}"))?;

    Ok(SubmittedQuery { query_id, batches })
}

/// Ask the query server at `server_url` to stop query `query_id`, presenting `token`.
///
/// The client half of `do_action("cancel", ...)`, and as small as [`submit_query_as`] for the same
/// reason: any Arrow Flight client in any language can send an action with a decimal id in its body
/// and an `authorization: Bearer` header, so nothing here is lldb-specific except the two constants.
///
/// Returns once the coordinator has accepted the cancellation, which is *before* the query's row
/// reaches `cancelled` — the terminal write happens as the query's own task unwinds. A caller that
/// needs to observe the end state polls [`ServicesDb::query_by_id`](crate::services::ServicesDb).
pub async fn cancel_query(server_url: &str, query_id: i64, token: Option<&str>) -> Result<()> {
    let channel = crate::tls::dial(server_url)
        .await
        .with_context(|| format!("dialing coordinator {server_url}"))?;
    let mut client = FlightServiceClient::new(channel);

    let mut grpc_request = Request::new(Action {
        r#type: CANCEL_ACTION.to_string(),
        body: encode_cancel_body(query_id).into(),
    });
    if let Some(token) = token {
        let value = bearer_header(token)
            .parse()
            .context("the API key is not usable as an HTTP header value")?;
        grpc_request
            .metadata_mut()
            .insert(AUTHORIZATION_HEADER, value);
    }

    let mut results = client
        .do_action(grpc_request)
        .await
        .map_err(|status| anyhow!("cancelling query {query_id} on {server_url}: {status}"))?
        .into_inner();
    // Drained rather than ignored: the body is the server's confirmation that it did the work, and
    // a client that never reads it cannot tell acceptance from an empty stream.
    let accepted = results
        .try_next()
        .await
        .with_context(|| format!("reading the cancellation reply for query {query_id}"))?
        .map(|result| String::from_utf8_lossy(&result.body).into_owned());
    match accepted.as_deref() {
        Some(CANCEL_ACCEPTED) => Ok(()),
        other => bail!("the coordinator answered the cancellation with {other:?}"),
    }
}

/// Read the `lldb-query-id` header, if the server sent one.
fn query_id_from(metadata: &tonic::metadata::MetadataMap) -> Option<i64> {
    metadata
        .get(QUERY_ID_HEADER)?
        .to_str()
        .ok()?
        .parse::<i64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ticket_round_trips_every_field() {
        let request = QueryRequest::new("SELECT 1")
            .as_account("acme")
            .on_warehouse("analytics");
        let decoded = decode_query_ticket(&encode_query_ticket(&request)).expect("round trip");
        assert_eq!(decoded, request);
    }

    #[test]
    fn unset_fields_stay_unset() {
        // An empty length-prefixed field means "use the server's default", not "the empty
        // account" — otherwise a client that omits the warehouse would ask for one named "".
        let request = QueryRequest::new("SELECT 42");
        let decoded = decode_query_ticket(&encode_query_ticket(&request)).expect("round trip");
        assert_eq!(decoded.account, None);
        assert_eq!(decoded.warehouse, None);
        assert_eq!(decoded.sql, "SELECT 42");
    }

    #[test]
    fn whitespace_only_fields_read_as_unset() {
        let request = QueryRequest::new("SELECT 1")
            .as_account("   ")
            .on_warehouse("\t");
        let decoded = decode_query_ticket(&encode_query_ticket(&request)).expect("round trip");
        assert_eq!((decoded.account, decoded.warehouse), (None, None));
    }

    #[test]
    fn sql_containing_anything_survives_the_wire() {
        // The SQL is the tail of the ticket with no escaping, so it must tolerate the bytes that
        // would break a delimiter-based format.
        let sql = "SELECT '\u{1F600} ünï\0code', x'00' FROM t WHERE s = 'a\nb'";
        let request = QueryRequest::new(sql);
        let decoded = decode_query_ticket(&encode_query_ticket(&request)).expect("round trip");
        assert_eq!(decoded.sql, sql);
    }

    #[test]
    fn a_malformed_ticket_errors_rather_than_panicking() {
        // Every one of these is reachable from a hostile or buggy client, and none may panic a
        // server thread.
        assert!(decode_query_ticket(&[]).is_err(), "empty");
        assert!(decode_query_ticket(&[1, 2, 3]).is_err(), "short header");
        // Claims a 99-byte account it does not have.
        let mut lying = 99u32.to_le_bytes().to_vec();
        lying.extend_from_slice(b"abc");
        assert!(decode_query_ticket(&lying).is_err(), "over-long field");
        // Well-formed header, no SQL.
        let mut empty_sql = 0u32.to_le_bytes().to_vec();
        empty_sql.extend_from_slice(&0u32.to_le_bytes());
        let err = decode_query_ticket(&empty_sql).expect_err("no SQL");
        assert!(err.to_string().contains("no SQL"), "{err}");
    }

    #[test]
    fn invalid_utf8_in_a_field_is_rejected() {
        let mut ticket = 2u32.to_le_bytes().to_vec();
        ticket.extend_from_slice(&[0xFF, 0xFE]);
        ticket.extend_from_slice(&0u32.to_le_bytes());
        ticket.extend_from_slice(b"SELECT 1");
        let err = decode_query_ticket(&ticket).expect_err("invalid UTF-8");
        assert!(format!("{err:#}").contains("UTF-8"), "{err:#}");
    }

    #[test]
    fn refusals_and_failures_map_to_distinguishable_grpc_codes() {
        // The retry contract: a client must be able to tell "come back later" from "never".
        let full = QueryError::Admission(AdmissionError::QueueFull {
            warehouse: "analytics".to_string(),
            max_queued: 4,
        });
        assert_eq!(full.to_status().code(), tonic::Code::ResourceExhausted);

        let closing = QueryError::Admission(AdmissionError::ShuttingDown {
            warehouse: "analytics".to_string(),
        });
        assert_eq!(closing.to_status().code(), tonic::Code::Unavailable);

        let bad = QueryError::Request(anyhow!("no such warehouse"));
        assert_eq!(bad.to_status().code(), tonic::Code::InvalidArgument);

        let boom = QueryError::Execution(anyhow!("table not found"));
        assert_eq!(boom.to_status().code(), tonic::Code::Internal);
        assert!(boom.to_status().message().contains("table not found"));
    }

    #[test]
    fn identity_failures_are_three_distinguishable_things() {
        // "Get a credential", "get a grant", and "our control plane is down" call for three
        // different actions by the client, so they get three different codes.
        let missing = QueryError::Unauthenticated(AuthError::Missing);
        assert_eq!(missing.to_status().code(), tonic::Code::Unauthenticated);
        assert!(
            missing.to_string().contains("lldb-qe-auth key create"),
            "{missing}"
        );

        let denied = QueryError::Denied(anyhow!("missing SELECT on table lldb.sales.orders"));
        assert_eq!(denied.to_status().code(), tonic::Code::PermissionDenied);
        assert!(denied.to_status().message().contains("lldb.sales.orders"));

        // A services database that will not answer is *our* fault. Reporting it as
        // UNAUTHENTICATED would tell an operator to go re-issue perfectly good API keys.
        let down = QueryError::Unauthenticated(AuthError::Unavailable(anyhow!("pool timed out")));
        assert_eq!(down.to_status().code(), tonic::Code::Unavailable);
    }

    /// With no services database there is no identity to prove, so a query runs and a credential
    /// is irrelevant. This is the documented single-node mode, and it is the one property here
    /// that a regression would break silently for every developer in a checkout.
    #[tokio::test]
    async fn without_a_control_plane_nothing_is_authenticated() {
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                ..CoordinatorConfig::default()
            },
        );
        assert!(!coordinator.requires_authentication());
        // Port 1 has no worker, so this fails — but it must fail in *execution*, having got past
        // identity entirely, rather than being refused for want of a credential.
        for credential in [None, Some("lldb_whatever")] {
            let outcome = coordinator
                .run_query(QueryRequest::new("SELECT 1"), credential)
                .await;
            let error = outcome.result.expect_err("port 1 has no worker");
            assert!(
                matches!(error, QueryError::Execution(_)),
                "credential {credential:?} produced {error:?}"
            );
        }
    }

    /// With no services database a query still runs — it just has no history. Same bargain as
    /// everywhere else in this codebase.
    #[tokio::test]
    async fn a_warehouse_needs_a_control_plane_to_be_resolvable() {
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                ..CoordinatorConfig::default()
            },
        );
        let outcome = coordinator
            .run_query(
                QueryRequest::new("SELECT 1").on_warehouse("analytics"),
                None,
            )
            .await;
        assert_eq!(outcome.query_id, None, "no control plane, no history row");
        let error = outcome.result.expect_err("no warehouse can be resolved");
        assert!(matches!(error, QueryError::Request(_)), "{error:?}");
        let message = error.to_string();
        assert!(message.contains("analytics"), "{message}");
        assert!(message.contains("services database"), "{message}");
        assert_eq!(error.to_status().code(), tonic::Code::InvalidArgument);
    }

    /// A dead fleet is an execution failure, not a request failure — and, critically, it must
    /// return its admission slot so the next query is not blocked behind it.
    #[tokio::test]
    async fn a_failing_query_releases_its_slot() {
        // Nothing is listening on port 1, so discovery+execution fail fast.
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                max_concurrent_queries: Some(1),
                ..CoordinatorConfig::default()
            },
        );
        for _ in 0..3 {
            let outcome = coordinator
                .run_query(QueryRequest::new("SELECT 1"), None)
                .await;
            assert!(outcome.result.is_err(), "port 1 has no worker");
        }
        let snapshot = coordinator.scheduler().snapshot();
        let gate = &snapshot[DEFAULT_FLEET_KEY];
        assert_eq!(
            gate.running, 0,
            "every failed query must give its slot back"
        );
        assert_eq!(gate.admitted, 3);
        assert_eq!(gate.peak_running, 1, "the limit of 1 was respected");
    }

    /// A refusal a client must be able to tell from every other one.
    #[test]
    fn cancellation_and_a_stale_id_have_their_own_status_codes() {
        // `CANCELLED` rather than `INTERNAL`: nothing broke, and a client that reads a cancellation
        // as an engine failure will report a bug that does not exist.
        let cancelled = QueryError::Cancelled("cancelled: user `dana` stopped it".to_string());
        assert_eq!(cancelled.to_status().code(), tonic::Code::Cancelled);
        assert!(cancelled.to_status().message().contains("dana"));

        // `NOT_FOUND`, and the message has to point at *which* coordinator to ask, because the
        // cancellation registry is per process — the one boundary in `crate::cancel` that
        // fleet-wide admission did not move, since stopping a query means reaching into the task
        // running it and only its own process can do that.
        let stale = QueryError::NotRunning(41);
        assert_eq!(stale.to_status().code(), tonic::Code::NotFound);
        let message = stale.to_string();
        assert!(message.contains("41"), "{message}");
        assert!(message.contains("queries.coordinator"), "{message}");
    }

    /// A cancel naming a query this process is not running is refused, not silently accepted.
    ///
    /// "Silently accepted" is the tempting shape — the query is not running, so arguably there is
    /// nothing to do — and it is wrong: a client that hits the wrong coordinator of two would be
    /// told its expensive query had been stopped while it kept running.
    #[tokio::test]
    async fn cancelling_a_query_this_coordinator_is_not_running_is_refused() {
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                ..CoordinatorConfig::default()
            },
        );
        let error = coordinator
            .cancel_query(7, None)
            .await
            .expect_err("nothing with that id is running here");
        assert!(matches!(error, QueryError::NotRunning(7)), "{error:?}");
        assert!(coordinator.in_flight().is_empty());
    }

    /// With no services database a query has no id, so there is nothing to cancel it by.
    ///
    /// Asserted rather than assumed because it is the one place cancellation is *absent* by design:
    /// the handle is the history row's id, history needs a control plane, and `cargo run` must
    /// never need Postgres. A single-node user hangs up instead.
    #[tokio::test]
    async fn without_a_control_plane_a_query_is_never_registered_for_cancellation() {
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                ..CoordinatorConfig::default()
            },
        );
        let outcome = coordinator
            .run_query(QueryRequest::new("SELECT 1"), None)
            .await;
        assert_eq!(outcome.query_id, None);
        assert!(outcome.result.is_err(), "port 1 has no worker");
        assert!(
            coordinator.in_flight().is_empty(),
            "a query with no id must not occupy the registry"
        );
    }

    /// The signal reaches the query, and carries no user when there is no identity to carry.
    ///
    /// Registered by hand rather than through `run_query`, because `run_query` only registers a
    /// query that has a history id and this coordinator deliberately has no database. What is under
    /// test is the wiring from `cancel_query` to the guard — the *policy* on top of it (tenant and
    /// grant) needs real accounts and is asserted in `tests/integration/query_cancel.rs`.
    #[tokio::test]
    async fn a_coordinator_with_nothing_to_enforce_cancels_on_request() {
        let coordinator =
            Coordinator::new(SessionContext::new(), None, CoordinatorConfig::default());
        let mut running = coordinator.in_flight().register(11, 1, "analytics");
        coordinator
            .cancel_query(11, None)
            .await
            .expect("no services database means nothing is enforced");
        let cancellation =
            tokio::time::timeout(std::time::Duration::from_secs(5), running.cancelled())
                .await
                .expect("the signal must arrive");
        assert_eq!(cancellation.requested_by, None);
        assert!(cancellation.reason().starts_with("cancelled: "));

        // Once the query is gone, the same call is refused rather than being a silent no-op.
        drop(running);
        assert!(matches!(
            coordinator.cancel_query(11, None).await,
            Err(QueryError::NotRunning(11))
        ));
    }

    /// Once shut down, submissions are refused with a code that says "not now" rather than
    /// "never".
    #[tokio::test]
    async fn a_shut_down_coordinator_refuses_new_queries() {
        let coordinator = Coordinator::new(
            SessionContext::new(),
            None,
            CoordinatorConfig {
                workers: vec!["http://127.0.0.1:1".to_string()],
                ..CoordinatorConfig::default()
            },
        );
        coordinator.begin_shutdown();
        let outcome = coordinator
            .run_query(QueryRequest::new("SELECT 1"), None)
            .await;
        let error = outcome.result.expect_err("the server is going away");
        assert_eq!(error.to_status().code(), tonic::Code::Unavailable);
        assert!(error.to_string().contains("shutting down"), "{error}");
    }

    /// The guard's default is the case it was built for: nobody wrote anything, so the client
    /// vanished.
    #[test]
    fn an_untouched_guard_records_an_abandonment() {
        let (state, detail) = Unrecorded::AbandonedByClient.terminal();
        assert_eq!(state, QueryState::Failed);
        assert_eq!(detail, ABANDONED);
        assert!(
            state.is_terminal(),
            "the whole point of the guard is that the row stops being active"
        );
    }

    /// A cancellation whose terminal write failed must reach `cancelled`, carrying the same reason
    /// the write would have carried — **not** `failed` with the abandonment prose.
    ///
    /// This is the assertion the fix exists for. Standing the guard down there would leave the row
    /// in `running` forever (this coordinator is alive, so `crate::reaper` will not take it), and
    /// leaving it armed with the *default* would record a query somebody deliberately stopped as one
    /// whose client hung up. Both are wrong, and this is the third option.
    #[test]
    fn a_guard_told_about_a_cancellation_records_the_cancellation() {
        let unrecorded = Unrecorded::Cancelled("cancelled: by alice".to_string());
        let (state, detail) = unrecorded.terminal();
        assert_eq!(
            state,
            QueryState::Cancelled,
            "recording a cancelled query as abandoned by its client is the lie this avoids"
        );
        assert_eq!(
            detail, "cancelled: by alice",
            "the reason must survive, or the row cannot say who stopped it"
        );
        assert!(state.is_terminal(), "the row must stop being active");
        assert!(
            state.may_carry_an_error(),
            "`queries_error_only_when_unsuccessful` must permit the reason this writes"
        );
    }

    /// With no services database there is no row, so "the cancellation was recorded" is vacuously
    /// true — and it must report that rather than `false`, which would ask `run_query` to leave a
    /// guard armed that cannot exist without a database either.
    #[tokio::test]
    async fn a_cancellation_with_no_row_to_write_counts_as_recorded() {
        let coordinator =
            Coordinator::new(SessionContext::new(), None, CoordinatorConfig::default());
        assert!(
            coordinator
                .mark_cancelled(None, "cancelled: by alice")
                .await,
            "no id and no database means nothing was left active"
        );
        assert!(
            coordinator
                .mark_cancelled(Some(7), "cancelled: by alice")
                .await,
            "an id without a database still names no row this process can write"
        );
    }
}
