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
//! [`plan_distributed`], which rewrites distribution boundaries (a `GROUP BY`, a partitioned join)
//! into map/reduce stages fanned across the fleet. The policy: **distribute what can be distributed
//! and reduce locally; offload a boundary-less plan whole to one worker.** Concretely — if the
//! rewrite inserted any [`FlightReaderExec`] leaves, the plan is genuinely distributed and the
//! coordinator runs it locally with `collect` (the leaves make the remote calls). If it did not
//! (a constant query, a bare scan — nothing to shuffle), there is nothing to fan out, so the whole
//! plan is shipped to one worker over Flight. That keeps simple queries exercising a real worker,
//! which is exactly what the cross-container cluster smoke test relies on.
//!
//! Both paths tolerate losing a worker: the distributed one reassigns each stage through the
//! fallbacks its [`FlightReaderExec`] leaves carry, and the offload path walks the fleet with
//! `fetch_with_failover`. A query fails only once every healthy target has been tried.
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
//! Start a worker first: `cargo run -p lldb-qe-worker`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use lldb_qe_core::manifest::Manifest;
use lldb_qe_core::{
    FlightReaderExec, ServicesArgs, StorageArgs, StorageConfig, apply_manifest, build_session,
    discover_workers, fetch_with_failover, init_tracing, plan_distributed, register_tpch_parquet,
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
    if matches!(config, StorageConfig::InMemory) {
        bail!(
            "--storage memory can't be used with remote workers: the in-memory object store is \
             per-process, so workers can't see the coordinator's data. Use `--storage local` \
             (a shared filesystem) or `--storage s3`."
        );
    }

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
        db.close().await;
    }

    let (ctx, storage) = build_session(config).await?;

    // Populate the catalog: an explicit manifest if provided, else the TPC-H seed.
    match &cli.manifest {
        Some(path) => {
            let manifest = Manifest::from_path(path)?;
            apply_manifest(&ctx, &storage, &manifest).await?;
        }
        None => register_tpch_parquet(&ctx, &storage, &cli.tpch_subdir).await?,
    }

    // Discover the concrete fleet behind the configured endpoints. Logging the size + URLs at info
    // level is the operator-visible signal that scaling the ECS service actually changed the fleet.
    let fleet = discover_workers(&cli.workers)
        .await
        .context("discovering the worker fleet")?;
    if fleet.is_empty() {
        bail!(
            "no workers discovered from --workers {:?}: every endpoint resolved to nothing",
            cli.workers
        );
    }
    tracing::info!(
        fleet_size = fleet.len(),
        workers = ?fleet,
        "discovered worker fleet"
    );

    // Build the physical plan, then rewrite its distribution boundaries across the whole fleet.
    let plan = ctx.sql(&cli.sql).await?.create_physical_plan().await?;
    let coordinated = plan_distributed(Arc::clone(&plan), &fleet)?;

    // Policy: distribute what can be distributed and reduce locally; offload a boundary-less plan
    // whole to one worker. `plan_distributed` returns the plan *unchanged* when there is no
    // distribution boundary (a constant query, a bare scan), so detect "did it actually distribute"
    // by looking for the FlightReaderExec leaves the rewrite inserts.
    let batches = if contains_flight_reader(&coordinated) {
        // Genuinely distributed: the coordinator runs the reduce side locally and its
        // FlightReaderExec leaves pull each map/reduce stage from a worker over Flight.
        collect(coordinated, ctx.task_ctx())
            .await
            .context("running the distributed query across the fleet")?
    } else {
        // Boundary-less: nothing to fan out. Collapse to a single output partition and ship the
        // whole plan to one worker — this still exercises a real worker over Flight (what the
        // cross-container cluster smoke test proves) instead of running everything locally.
        //
        // "One worker" is a placement, not a commitment: the plan is self-contained and the worker
        // materializes it once by content hash, so any member of the fleet produces the same answer.
        // `fetch_with_failover` therefore walks the fleet in order — a dead `fleet[0]` no longer
        // fails a query that N-1 healthy workers could have served.
        let plan = Arc::new(CoalescePartitionsExec::new(plan));
        tracing::info!(
            worker = %fleet[0],
            fleet_size = fleet.len(),
            "no distribution boundary; offloading the whole plan to one worker (failing over across the fleet if it is lost)"
        );
        fetch_with_failover(&fleet, 0, plan)
            .await
            .context("running the query on a single worker")?
    };

    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}

/// True if `plan` contains at least one [`FlightReaderExec`] leaf — i.e. [`plan_distributed`]
/// actually cut a distribution boundary and inserted remote reads, rather than returning the plan
/// unchanged.
fn contains_flight_reader(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    // `apply` is infallible here (the closure never errors), so the expect cannot fire.
    plan.apply(|node| {
        if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("walking the plan for FlightReaderExec does not error");
    found
}
