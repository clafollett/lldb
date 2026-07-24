//! `lldb-qe-coordinator` — builds a query's physical plan and runs it on a remote worker over
//! Arrow Flight, then prints the result.
//!
//! Configured by flags/env so it drops cleanly into a container:
//!   `--workers` (`LLDB_WORKERS`, comma-separated fleet), `--manifest` (`LLDB_MANIFEST`, a
//!   TOML catalog description; when omitted it seeds the TPC-H listing tables under
//!   `--tpch-subdir`), `--sql`, and the shared `--storage …` group.
//!
//! Start a worker first: `cargo run -p lldb-qe-worker`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use lldb_qe_core::manifest::Manifest;
use lldb_qe_core::{
    StorageArgs, StorageConfig, apply_manifest, build_session, fetch, init_tracing,
    register_tpch_parquet,
};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-coordinator",
    about = "SQL entry point for lldb",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Comma-separated worker Flight URLs. The demo ships the plan to the first one.
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

    #[command(flatten)]
    storage: StorageArgs,
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

    let (ctx, storage) = build_session(config).await?;

    // Populate the catalog: an explicit manifest if provided, else the TPC-H seed.
    match &cli.manifest {
        Some(path) => {
            let manifest = Manifest::from_path(path)?;
            apply_manifest(&ctx, &storage, &manifest).await?;
        }
        None => register_tpch_parquet(&ctx, &storage, &cli.tpch_subdir).await?,
    }

    // Collapse to a single output partition, then run partition 0 on the first worker.
    let plan = ctx.sql(&cli.sql).await?.create_physical_plan().await?;
    let plan = Arc::new(CoalescePartitionsExec::new(plan));

    let worker = cli
        .workers
        .first()
        .context("at least one worker required")?;
    tracing::info!(worker = %worker, "shipping plan to worker");
    let batches = fetch(worker, 0, plan).await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
