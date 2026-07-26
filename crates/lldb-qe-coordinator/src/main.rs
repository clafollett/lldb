//! `lldb-qe-coordinator` — builds a query's physical plan, distributes it across the whole worker
//! fleet, and prints the result.
//!
//! Configured by flags/env so it drops cleanly into a container:
//!   `--workers` (`LLDB_WORKERS`, comma-separated endpoints), `--manifest` (`LLDB_MANIFEST`, a
//!   TOML catalog description; when omitted it seeds the TPC-H listing tables under
//!   `--tpch-subdir`), `--sql`, and the shared `--storage …` group.
//!
//! # Fleet discovery
//!
//! Each `--workers` entry is an *endpoint*, not necessarily a single worker: its host is resolved
//! to **every** IP behind it (see [`discover_workers`]). A literal IP is one worker; a single Cloud
//! Map DNS name like `http://worker.lldb.local:50051` enumerates the whole ECS service — one URL per
//! healthy task. Because that resolution runs on every invocation, scaling the fleet changes the
//! discovered worker count, and therefore the plan's fan-out, with no redeploy. The discovered fleet
//! size is logged at startup so an operator can *see* scaling take effect.
//!
//! # Distribute vs. offload
//!
//! Once the fleet is known the coordinator builds the physical plan and hands it to
//! `plan_distributed`, which cuts *every* distribution boundary it recognizes — a `GROUP BY`, a
//! partitioned or broadcast join, a sort, a partitioned window — into a DAG of stages fanned across
//! the fleet. The policy — **distribute what can be distributed and reduce locally; offload a
//! boundary-less plan whole to one worker** — and the failover behaviour of both paths live in
//! [`lldb_qe_core::engine`], because there are now two front ends running queries and they must
//! not drift: this one-shot binary and the long-running `lldb-qe-server`.
//!
//! # One query, then exit
//!
//! That is this binary's whole contract, and it is deliberately unchanged. It needs no scheduler,
//! writes no query history, and requires no services database — a checkout, a laptop and the
//! cross-container smoke test all depend on that. Concurrency, admission control and query history
//! are `lldb-qe-server`'s job; see [`lldb_qe_core::server`].
//!
//! # Tenancy
//!
//! When a services database is configured (`--metadata-url`, or the discrete `--metadata-*`
//! parts — see [`ServicesArgs`]), the coordinator resolves `--account` to a row in `accounts` at
//! startup and logs its id. That is the hook every later control-plane feature hangs off: a
//! warehouse, a query-history row and a grant all need to know *whose* they are, and resolving
//! it once at startup means the rest of the process can carry an id rather than re-deriving a
//! name. Enforcement — refusing to touch another tenant's data — lands with accounts/RBAC
//! (#19); this only establishes the identity.
//!
//! With **no** services database configured the coordinator behaves exactly as it always has.
//! A checkout, a laptop, and a single-node demo have no control plane to talk to, and requiring
//! one would mean `cargo run` needs Postgres.
//!
//! # Warehouse routing
//!
//! `--warehouse <NAME>` (`LLDB_WAREHOUSE`) runs the query on a named [virtual
//! warehouse](lldb_qe_core::warehouse) instead of on whatever `--workers` points at. The
//! coordinator resolves `(account, name)` in the services database, refuses a **suspended**
//! warehouse with an error naming the resume command, and renders that warehouse's own endpoint
//! from `--warehouse-endpoint` (default `http://{warehouse}.lldb.local:50051`) — one fan-out
//! point per warehouse, discovered exactly like any other. Routing to the wrong pool is therefore
//! impossible without a DNS answer that lies.
//!
//! The flag is **opt-in and unset by default**, which is the whole compatibility story: without
//! it — and in particular with no services database at all — `--workers` is used verbatim and
//! nothing here needs Postgres. Passing `--warehouse` without a services database is an error
//! rather than a fallback, because there is nowhere to look the warehouse up and silently
//! querying *some* fleet would be worse than stopping.
//!
//! Start a worker first: `cargo run -p lldb-qe-worker`.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use lldb_qe_core::{
    CatalogSource, DEFAULT_WAREHOUSE_ENDPOINT, ServicesArgs, StorageArgs, build_query_session,
    execute_query, init_tracing, reject_inmemory_storage, resolve_fleet,
};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-coordinator",
    about = "SQL entry point for lldb",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Comma-separated worker Flight endpoints (`scheme://host:port`). Each entry is resolved to the
    /// whole fleet behind it: a literal IP is one worker, while a single DNS name (e.g. a Cloud Map
    /// service name) enumerates every task registered under it. Distributable stages then fan across
    /// all discovered workers.
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

    /// SQL to plan and execute.
    #[arg(long, default_value = "SELECT n_name FROM nation ORDER BY n_name")]
    sql: String,

    /// Tenant this invocation runs as. Only consulted when a services database is configured;
    /// it must already exist (`lldb-qe-migrate --seed-account <NAME>` creates it).
    #[arg(long, env = "LLDB_ACCOUNT", default_value = "default")]
    account: String,

    /// Virtual warehouse to run on. When set, it is resolved in the services database (under
    /// `--account`) and *replaces* `--workers`: the query fans across that warehouse's fleet and
    /// no other. A suspended warehouse is refused. Unset — the default — means "use `--workers`",
    /// which is how every pre-warehouse deployment keeps working.
    #[arg(long, env = "LLDB_WAREHOUSE")]
    warehouse: Option<String>,

    /// Template for a warehouse's Flight endpoint; `{warehouse}` is replaced with its name.
    /// Comma-separated for a warehouse reachable under several names. Only used with
    /// `--warehouse`. Cloud Map spells this `<name>.lldb.local`; a compose network alias is a
    /// bare `<name>`, hence a template rather than a hard-coded pattern.
    #[arg(
        long,
        env = "LLDB_WAREHOUSE_ENDPOINT",
        value_delimiter = ',',
        default_value = DEFAULT_WAREHOUSE_ENDPOINT
    )]
    warehouse_endpoint: Vec<String>,

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
        "starting lldb-qe-coordinator"
    );

    let config = cli.storage.to_config()?;
    // The coordinator always ships plans to a separate worker process. The in-memory object
    // store lives in *this* process, so a remote worker can never see data written to it —
    // reject the combination up front instead of failing deep in a scan with an empty result.
    reject_inmemory_storage(&config)?;

    // Which endpoints to discover the fleet behind. `--workers` unless a warehouse says otherwise.
    let mut endpoints = cli.workers.clone();
    // The warehouse's declared size, kept so the discovered fleet can be compared against it — a
    // mismatch means the desired state in Postgres has not been applied to the compute yet, which
    // is the one failure mode this "database is desired state" design can produce.
    let mut declared_size: Option<i32> = None;

    // Resolve the tenant, if there is a control plane to resolve it against. A missing account is
    // an error naming the tool that creates one — a silent fallback would let a typo run a query
    // under nobody's identity, which is precisely the bug #19's enforcement has to rule out.
    if let Some(db) = cli.services.connect().await? {
        let account = db.account_by_name(&cli.account).await?.with_context(|| {
            format!(
                "account `{}` does not exist in the services database; create it with \
                 `lldb-qe-migrate --seed-account {}`",
                cli.account, cli.account
            )
        })?;
        tracing::info!(
            account = %account.name,
            account_id = account.id,
            "resolved tenant"
        );

        // Then the warehouse, if one was asked for. Resolution is scoped by the account id, so
        // another tenant's identically-named warehouse is simply not visible from here.
        if let Some(name) = &cli.warehouse {
            let warehouse = db
                .warehouse_by_name(account.id, name)
                .await?
                .with_context(|| {
                    format!(
                        "warehouse `{name}` does not exist for account `{}`; create it with \
                         `lldb-qe-warehouse create --account {} --name {name} --size 2`",
                        account.name, account.name
                    )
                })?;
            // `endpoint` is what refuses a suspended warehouse — the guard lives on the type, so
            // it cannot be forgotten by a second caller.
            endpoints = cli
                .warehouse_endpoint
                .iter()
                .map(|template| warehouse.endpoint(template))
                .collect::<Result<Vec<_>>>()?;
            declared_size = Some(warehouse.size);
            tracing::info!(
                warehouse = %warehouse.name,
                warehouse_id = warehouse.id,
                size = warehouse.size,
                state = %warehouse.state,
                endpoints = ?endpoints,
                "routing to warehouse"
            );
        }

        db.close().await;
    } else if let Some(name) = &cli.warehouse {
        // No control plane, so there is nothing that knows what `name` means. Say that, rather
        // than falling back to `--workers` and running the query on a fleet nobody chose.
        bail!(
            "--warehouse {name} needs a services database to resolve the warehouse in: set \
             --metadata-url (LLDB_METADATA_URL), or --metadata-host (LLDB_METADATA_HOST) plus \
             the other --metadata-* parts. Without one, use --workers directly."
        );
    }

    // Populate the catalog: an explicit manifest if provided, else the TPC-H seed.
    let catalog = match &cli.manifest {
        Some(path) => CatalogSource::Manifest(path.clone()),
        None => CatalogSource::Tpch {
            subdir: cli.tpch_subdir.clone(),
        },
    };
    let ctx = build_query_session(config, &catalog).await?;

    // Discover the concrete fleet behind the configured endpoints. Logging the size + URLs at info
    // level is the operator-visible signal that scaling the ECS service actually changed the fleet,
    // and `resolve_fleet` is also what warns when the warehouse row and the fleet disagree.
    let fleet = resolve_fleet(&endpoints, declared_size, None).await?;

    let batches = execute_query(&ctx, &cli.sql, &fleet).await?;

    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
