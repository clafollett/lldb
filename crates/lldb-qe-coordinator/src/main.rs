//! `lldb-qe-coordinator` — builds a query's physical plan and runs it on a remote worker
//! over Arrow Flight, then prints the result.
//!
//! Usage: `lldb-qe-coordinator [worker_url] [data_dir] [sql]`
//! Defaults: `http://127.0.0.1:50051`, `data`, `SELECT n_name FROM nation ORDER BY n_name`.
//!
//! Start a worker first: `cargo run -p lldb-qe-worker`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use lldb_qe_core::{StorageConfig, build_session, fetch, register_tpch_parquet};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let worker = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    let data = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));
    let sql = args
        .next()
        .unwrap_or_else(|| "SELECT n_name FROM nation ORDER BY n_name".to_string());

    let (ctx, storage) = build_session(StorageConfig::Local(data)).await?;
    register_tpch_parquet(&ctx, &storage, "sf1").await?;

    // Collapse to a single output partition, then run partition 0 on the worker.
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let plan = Arc::new(CoalescePartitionsExec::new(plan));

    println!("shipping plan to worker {worker} …");
    let batches = fetch(&worker, 0, plan).await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
