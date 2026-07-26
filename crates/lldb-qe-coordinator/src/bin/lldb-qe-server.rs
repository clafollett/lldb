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
//! [`lldb_qe_core::server`] and [`lldb_qe_core::scheduler`]. Read those before deploying more than
//! one of these against a single warehouse: **admission control is per process.**
//!
//! ```text
//!   lldb-qe-server --workers http://worker-1:50051,http://worker-2:50051 \
//!     --manifest /manifests/tpch.toml --metadata-host postgres --bind 0.0.0.0:50050
//! ```
//!
//! With no `--metadata-*` it still runs; it just has no accounts, no warehouses and no history —
//! the same bargain every other binary here strikes (see CLAUDE.md).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use lldb_qe_core::scheduler::DEFAULT_MAX_QUEUED_QUERIES;
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, DEFAULT_SERVER_BIND, serve_coordinator,
};
use lldb_qe_core::{
    CatalogSource, DEFAULT_WAREHOUSE_ENDPOINT, ServicesArgs, StorageArgs, build_query_session,
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
    /// Note the scope: this is a limit **per coordinator process**, not fleet-wide. Two servers
    /// pointed at one warehouse each enforce it independently.
    #[arg(long, env = "LLDB_MAX_CONCURRENT_QUERIES")]
    max_concurrent_queries: Option<usize>,

    /// Queries that may wait per warehouse before submission is refused with
    /// `RESOURCE_EXHAUSTED`. The cap exists so a client that submits faster than the warehouse
    /// drains gets backpressure instead of quietly consuming this process's memory.
    #[arg(long, env = "LLDB_MAX_QUEUED_QUERIES", default_value_t = DEFAULT_MAX_QUEUED_QUERIES)]
    max_queued_queries: usize,

    /// How this process names itself in `queries.coordinator`. Defaults to the bound address.
    /// Worth setting to something stable (a task id, a hostname) when several coordinators write
    /// to one services database, because concurrency limits are only meaningful within one value
    /// of that column.
    #[arg(long, env = "LLDB_COORDINATOR_ID")]
    coordinator_id: Option<String>,

    #[command(flatten)]
    storage: StorageArgs,

    #[command(flatten)]
    services: ServicesArgs,
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
    let ctx = build_query_session(config, &catalog).await?;

    let listener = TcpListener::bind(cli.bind)
        .await
        .context("binding server")?;
    let addr = listener.local_addr()?;
    let coordinator_id = cli.coordinator_id.unwrap_or_else(|| addr.to_string());

    let coordinator = Arc::new(Coordinator::new(
        ctx,
        db.clone(),
        CoordinatorConfig {
            default_account: cli.account,
            workers: cli.workers,
            warehouse_endpoint: cli.warehouse_endpoint,
            max_concurrent_queries: cli.max_concurrent_queries,
            max_queued_queries: cli.max_queued_queries,
            coordinator_id: coordinator_id.clone(),
        },
    ));
    tracing::info!(
        addr = %addr,
        coordinator_id = %coordinator_id,
        max_concurrent_queries = ?coordinator.config().max_concurrent_queries,
        max_queued_queries = coordinator.config().max_queued_queries,
        "lldb-qe-server listening (admission control is per-process, not fleet-wide)"
    );

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
    if let Some(db) = db {
        db.close().await;
    }
    result
}
