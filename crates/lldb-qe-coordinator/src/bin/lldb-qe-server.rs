//! `lldb-qe-server` — the coordinator as a **long-running service**: many concurrent queries over
//! one Arrow Flight port, bounded by admission control, recorded in query history.
//!
//! # Why a second binary rather than a flag
//!
//! Same reasoning as `lldb-qe-migrate` and `lldb-qe-warehouse`: these are different programs with
//! different contracts. `lldb-qe-coordinator` runs one query and exits — that is what compose
//! runs, what the cross-container smoke test asserts on, and what a person types in a checkout.
//! Bolting a `--serve` mode onto it would put a server's lifecycle, signal handling and scheduler
//! behind a flag on a CLI whose every other flag describes a single query. Two binaries in one
//! image, built from one source tree, keeps both contracts legible and neither at risk from the
//! other.
//!
//! # What it does
//!
//! Builds the catalog once, connects to the services database if one is configured, then serves
//! Flight `do_get`. Each ticket carries `(account, warehouse, sql)`; the server assigns a query id,
//! records it `queued`, waits for an admission slot on that warehouse, runs the query across the
//! warehouse's fleet, streams Arrow back and marks the row terminal. The design — transport,
//! admission, lifecycle, and the things it deliberately does not do — is documented on
//! [`lldb_qe_core::server`] and [`lldb_qe_core::scheduler`]. **Admission control is fleet-wide**
//! when a services database is configured: two of these against one warehouse share its limit
//! rather than doubling it (see [`lldb_qe_core::fleet_admission`]).
//!
//! ```text
//!   lldb-qe-server --workers http://worker-1:50051,http://worker-2:50051 \
//!     --manifest /manifests/tpch.toml --metadata-host postgres --bind 0.0.0.0:50050
//! ```
//!
//! With no `--metadata-*` it still runs; it just has no accounts, no warehouses, no history, no
//! liveness registration and no shared admission limit — the same bargain every other binary here
//! strikes (see CLAUDE.md).
//!
//! # Liveness
//!
//! With a services database, this process registers itself in `coordinators` before it serves and
//! renews on an interval, so the rest of the fleet can tell a coordinator that died from one that is
//! merely busy. It deregisters on a clean exit. The design, and the four decisions behind it, are on
//! [`lldb_qe_core::liveness`].
//!
//! That registration is also what makes fleet-wide admission safe, and why the two are wired
//! together below: a query slot is held by a *process*, so a claim's expiry is its holder's lease.
//! A coordinator holding slots without a registration would have them reclaimed the instant it
//! wrote them, so [`lldb_qe_core::fleet_admission::FleetAdmission`] cannot be built without one.
//!
//! # Security posture
//!
//! This is the one binary in the repo that is meant to face people who are not operators, so it is
//! the one that authenticates. With a services database configured, every submission needs
//! `authorization: Bearer <token>` and is checked against its account's grants before it is
//! dispatched; the design is on [`lldb_qe_core::auth`] and [`lldb_qe_core::rbac`]. Without one, it
//! is wide open by construction — there are no accounts to be. Whichever it is, it says so at
//! startup, because a posture nobody logged is a posture nobody chose.
//!
//! Two things this does *not* do, and a deployment must: terminate TLS in front of this port (a
//! bearer token on a plaintext channel is replayable by anyone on the path), and set
//! `LLDB_FLEET_TOKEN` to the same value here and on every worker, so the workers this server
//! dispatches to refuse plans from anyone else.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use lldb_qe_core::engine::TenantSessions;
use lldb_qe_core::fleet_admission::FleetAdmission;
use lldb_qe_core::liveness::{
    CoordinatorIdentity, CoordinatorRegistration, DEFAULT_RENEW_INTERVAL, death_threshold,
};
use lldb_qe_core::scheduler::DEFAULT_MAX_QUEUED_QUERIES;
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, DEFAULT_SERVER_BIND, serve_coordinator,
};
use lldb_qe_core::{
    CatalogSource, DEFAULT_WAREHOUSE_ENDPOINT, ResultCacheArgs, ServicesArgs, StorageArgs,
    init_tracing, redact_url, reject_inmemory_storage,
};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-server",
    about = "Long-running lldb coordinator: concurrent queries, admission control, query history",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Address to serve Flight on. One below the worker's default so both fit on a laptop.
    #[arg(long, env = "LLDB_SERVER_BIND", default_value = DEFAULT_SERVER_BIND)]
    bind: SocketAddr,

    /// Comma-separated worker Flight endpoints, used by any query that names no warehouse. Each
    /// entry is resolved to the whole fleet behind it, on every query — so scaling changes the
    /// fan-out with no restart.
    #[arg(
        long,
        env = "LLDB_WORKERS",
        value_delimiter = ',',
        default_value = "http://127.0.0.1:50051"
    )]
    workers: Vec<String>,

    /// Optional catalog manifest (TOML). When omitted, the TPC-H listing tables are seeded.
    #[arg(long, env = "LLDB_MANIFEST")]
    manifest: Option<PathBuf>,

    /// TPC-H data subdir under the storage root, used only when no `--manifest` is given.
    #[arg(long, default_value = "sf1")]
    tpch_subdir: String,

    /// Tenant a query runs as when its ticket names none.
    #[arg(long, env = "LLDB_ACCOUNT", default_value = "default")]
    account: String,

    /// Template for a warehouse's Flight endpoint; `{warehouse}` is replaced with its name.
    /// Comma-separated for a warehouse reachable under several names.
    #[arg(
        long,
        env = "LLDB_WAREHOUSE_ENDPOINT",
        value_delimiter = ',',
        default_value = DEFAULT_WAREHOUSE_ENDPOINT
    )]
    warehouse_endpoint: Vec<String>,

    /// Queries that may execute simultaneously **per warehouse**. Unset — the default — sizes each
    /// warehouse's limit from its own row (one running query per worker), which is the point of
    /// having warehouses. Setting it overrides every warehouse, including ones this server has not
    /// seen yet.
    ///
    /// With a services database this is a limit on the **warehouse**, shared by every coordinator:
    /// two servers pointed at one warehouse admit this many between them, not each. Without one
    /// there is nothing to share it through and each process enforces it alone. Set the same value
    /// on every coordinator — they are not obliged to agree, and if they disagree the warehouse ends
    /// up bounded by the largest.
    #[arg(long, env = "LLDB_MAX_CONCURRENT_QUERIES")]
    max_concurrent_queries: Option<usize>,

    /// Queries that may wait per warehouse before submission is refused with
    /// `RESOURCE_EXHAUSTED`. The cap exists so a client that submits faster than the warehouse
    /// drains gets backpressure instead of quietly consuming this process's memory.
    #[arg(long, env = "LLDB_MAX_QUEUED_QUERIES", default_value_t = DEFAULT_MAX_QUEUED_QUERIES)]
    max_queued_queries: usize,

    /// How this process names itself in `queries.coordinator` and in the `coordinators` table.
    /// Defaults to the bound address.
    ///
    /// This is a **deployment slot**, not a process id: it is meant to survive a restart, and the
    /// per-process half of the identity is minted here and never configurable (see
    /// [`lldb_qe_core::liveness`]). Worth setting to something stable — a task id, a hostname —
    /// when several coordinators write to one services database, because two coordinators sharing a
    /// slot is a misconfiguration that costs one of them its registration, and a coordinator with
    /// no registration holds no fleet-wide admission slots.
    #[arg(long, env = "LLDB_COORDINATOR_ID")]
    coordinator_id: Option<String>,

    /// How often this coordinator renews its registration in the services database, in seconds.
    ///
    /// The liveness threshold is a fixed multiple of this and there is deliberately no separate
    /// setting for it: two knobs could be configured inconsistently, and the failure mode of that
    /// is a live coordinator's queries being treated as abandoned. Lower it to shorten how long a
    /// dead coordinator goes unnoticed; raise it to cut control-plane writes on a large fleet.
    /// Ignored entirely when no services database is configured.
    #[arg(long, env = "LLDB_COORDINATOR_RENEW_INTERVAL_SECS", default_value_t = DEFAULT_RENEW_INTERVAL.as_secs())]
    coordinator_renew_interval_secs: u64,

    /// Serve requests that carry **no API key**, even though a services database is configured.
    ///
    /// The migration hatch, and nothing more: adding a control plane and issuing the first key are
    /// two deploys, and a cluster that is unqueryable in between is a cluster nobody upgrades. A
    /// server started with this warns on every startup. A request that *does* carry a key is still
    /// verified — a permissive flag must never turn a bad credential into a good one.
    #[arg(long, env = "LLDB_ALLOW_ANONYMOUS")]
    allow_anonymous: bool,

    #[command(flatten)]
    storage: StorageArgs,

    #[command(flatten)]
    services: ServicesArgs,

    #[command(flatten)]
    result_cache: ResultCacheArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_core::BUILD_VERSION,
        "starting lldb-qe-server"
    );

    let config = cli.storage.to_config()?;
    // Same guard as the one-shot coordinator: an in-memory object store is invisible to a worker
    // process, so the combination can only produce silently empty answers.
    reject_inmemory_storage(&config)?;

    // Connect *before* binding. A services database that is configured but unreachable is a
    // startup failure, not a per-query surprise — the alternative is a server that accepts queries
    // and fails every one of them at the point where it tries to record them.
    let db = cli.services.connect().await?;
    if let Some(db) = &db {
        let url = cli
            .services
            .resolve_url()?
            .expect("a connection implies a resolved url");
        db.health_check().await.with_context(|| {
            format!("services database at {} is not answering", redact_url(&url))
        })?;
        tracing::info!(
            metadata_url = %redact_url(&url),
            "connected to the services database; query history is on"
        );
    } else {
        tracing::warn!(
            "no services database configured: queries will run, but there is no query history, \
             no accounts and no warehouse routing (set --metadata-url or --metadata-host)"
        );
    }

    let catalog = match &cli.manifest {
        Some(path) => CatalogSource::Manifest(path.clone()),
        None => CatalogSource::Tpch {
            subdir: cli.tpch_subdir.clone(),
        },
    };
    // One session per account, built the first time that account submits a query, rather than one
    // process-wide session built here. This is the *front door*: it serves many tenants, and a
    // shared session would have to hold every tenant's catalog — making every tenant's catalog name
    // visible to every other one, and putting isolation back on the grant check instead of on the
    // structure. See `lldb_qe_core::tenancy`.
    //
    // Building lazily rather than eagerly is not an optimization: the set of accounts is a table in
    // the services database that changes while this process runs, so there is no moment at startup
    // when "every tenant" is a knowable list.
    let sessions = TenantSessions::per_account(config, catalog);

    let listener = TcpListener::bind(cli.bind)
        .await
        .context("binding server")?;
    let addr = listener.local_addr()?;
    // The slot is what an operator configures and what survives a restart; the incarnation is minted
    // right here, once, and cannot be. Every query row this process writes carries both, so a
    // restart — onto this address or another — is distinguishable from the process it replaced.
    let identity = CoordinatorIdentity::new(cli.coordinator_id.unwrap_or_else(|| addr.to_string()));
    let renew_interval = Duration::from_secs(cli.coordinator_renew_interval_secs.max(1));

    // Registration follows the services database, like history and auth: with none there is nothing
    // to register with, nobody to reap and no fleet to coordinate, so this is `None` and the process
    // behaves exactly as it did before liveness existed — no row, no background task, no per-query
    // anything. See `lldb_qe_core::liveness`.
    let registration =
        CoordinatorRegistration::start_if_configured(db.clone(), identity.clone(), renew_interval)
            .await?;
    if registration.is_some() {
        // A read, never an action: this process must not conclude anything *about* its peers, and
        // certainly must not sweep after them at startup — a fleet restarting together would have
        // every member judging the others through a lease none of them had renewed yet. Logging the
        // count tells an operator how many coordinators this warehouse's limit is now being shared
        // with, at the moment that starts being true.
        if let Some(db) = &db {
            match db.live_coordinators().await {
                Ok(live) => tracing::info!(
                    live_coordinators = live.len(),
                    renew_interval_secs = renew_interval.as_secs(),
                    death_threshold_secs = death_threshold(renew_interval).as_secs(),
                    "coordinator liveness is on (this coordinator included in the count); \
                     admission slots held by any of these are honoured fleet-wide, and slots held \
                     by a coordinator that stops renewing become claimable after the threshold"
                ),
                Err(error) => tracing::warn!(
                    error = %format!("{error:#}"),
                    "could not read the coordinator registry; this is informational only"
                ),
            }
        }
    }

    // Fleet-wide admission follows the registration, which follows the services database: a claim's
    // expiry *is* its holder's lease, so a coordinator with no registration must not hold slots —
    // and `start_if_registered` is that rule as a function rather than an `if` here. `None` leaves
    // the scheduler exactly the per-process module it has always been.
    let fleet = FleetAdmission::start_if_registered(db.clone(), registration.as_ref());

    let mut coordinator = Coordinator::multi_tenant(
        sessions,
        db.clone(),
        CoordinatorConfig {
            default_account: cli.account,
            workers: cli.workers,
            warehouse_endpoint: cli.warehouse_endpoint,
            max_concurrent_queries: cli.max_concurrent_queries,
            max_queued_queries: cli.max_queued_queries,
            coordinator: identity.clone(),
            allow_anonymous: cli.allow_anonymous,
        },
    );
    if let Some(fleet) = fleet.clone() {
        coordinator = coordinator.with_fleet_admission(fleet);
    }
    // The result cache is shared by every query this process serves, keyed by the account resolved
    // from the services database rather than the one a ticket claimed — so it can never serve one
    // tenant another's rows. It needs that database, so a server without one simply has no cache.
    if let Some(db) = db.clone()
        && let Some(cache) = cli.result_cache.build(db)
    {
        tracing::info!(config = ?cache.config(), "result cache is on");
        coordinator = coordinator.with_result_cache(cache);
    }
    let coordinator = Arc::new(coordinator);
    // Before the port is served, and unconditionally: whichever posture this is, it is stated.
    coordinator.log_posture();
    tracing::info!(
        addr = %addr,
        coordinator_id = %identity.slot(),
        incarnation = %identity.incarnation(),
        max_concurrent_queries = ?coordinator.config().max_concurrent_queries,
        max_queued_queries = coordinator.config().max_queued_queries,
        fleet_wide_admission = coordinator.scheduler().is_fleet_wide(),
        "lldb-qe-server listening"
    );
    if !coordinator.scheduler().is_fleet_wide() {
        // Said out loud rather than left to be inferred from a `false` in the line above, because
        // it is the one posture in which running two of these against one warehouse doubles the
        // work it does — and the operator who is about to do that is the one reading this log.
        tracing::warn!(
            "admission control is PER PROCESS: this coordinator is not registered in a services \
             database, so it cannot share a warehouse's concurrency limit with any other. Two \
             coordinators on one warehouse will each admit their own limit. Configure \
             --metadata-url (LLDB_METADATA_URL) to bound a warehouse fleet-wide."
        );
    }

    // Ctrl-C / SIGTERM: close the scheduler so queued queries are told the truth immediately,
    // then let tonic drain the ones that are already running. See `serve_coordinator`.
    let result = serve_coordinator(listener, Arc::clone(&coordinator), async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => tracing::info!("received interrupt; draining"),
            Err(error) => tracing::error!(%error, "could not listen for an interrupt"),
        }
    })
    .await;

    tracing::info!(admission = ?coordinator.scheduler().snapshot(), "lldb-qe-server stopped");
    // Deregister *before* closing the pool, and before returning: a coordinator that stopped on
    // purpose must be observably not-live at once rather than after the liveness threshold, and the
    // write needs a connection to do it. Failing to get it in is survivable — the registration then
    // simply expires the way a killed coordinator's does.
    if let Some(registration) = registration {
        registration.shut_down().await;
    }
    if let Some(db) = db {
        db.close().await;
    }
    result
}
