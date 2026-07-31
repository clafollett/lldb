//! Phase 3 deliverable: a physical sub-plan ships to a worker, executes there, and the Arrow
//! batches stream back over Flight — the foundation the distributed shuffle is built on.
//!
//! The worker and coordinator run in one process here (worker on a background task) so the
//! whole round-trip is a single `cargo test`. Skips if the data is absent.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::SessionContext;
use lldb_qe_core::{StorageConfig, build_session, flight, register_tpch_parquet};
use tokio::net::TcpListener;

use crate::support::Servers;
use crate::support::gates;

/// How this suite names itself in the skip report.
const SUITE: &str = "flight_transport";

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

#[tokio::test]
async fn scan_round_trips_through_a_worker() -> anyhow::Result<()> {
    if !data_dir().join("sf1/nation.parquet").exists() {
        gates::skip(SUITE, &gates::TPCH_DATA);
        return Ok(());
    }

    // Worker: a bare session executes whatever plan arrives (the plan's file paths are
    // absolute, so no table registration is needed on the worker).
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // Held rather than detached, and so stopped when this test returns — see [`Servers`].
    let mut workers = Servers::new();
    workers.spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });

    // Coordinator: build a single-partition scan plan for `nation`.
    let (ctx, storage) = build_session(StorageConfig::Local(data_dir())).await?;
    register_tpch_parquet(&ctx, &storage, "sf1").await?;
    let plan = ctx
        .sql("SELECT * FROM nation")
        .await?
        .create_physical_plan()
        .await?;
    let plan = Arc::new(CoalescePartitionsExec::new(plan));

    // Ship it to the worker, execute partition 0 there, collect the streamed batches.
    let url = format!("http://{addr}");
    let remote = flight::fetch(&url, 0, plan).await?;
    let remote_rows: usize = remote.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        remote_rows, 25,
        "nation has 25 rows, fetched over Arrow Flight"
    );

    // Correctness: identical row count to executing locally.
    let local = ctx.sql("SELECT * FROM nation").await?.collect().await?;
    let local_rows: usize = local.iter().map(|b| b.num_rows()).sum();
    assert_eq!(remote_rows, local_rows, "remote result must match local");
    Ok(())
}
