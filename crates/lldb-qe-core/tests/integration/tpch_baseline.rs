//! Phase 2 deliverable: single-node TPC-H baseline + physical-plan inspection.
//!
//! Runs Q1 and Q6 over the raw SF1 Parquet (the pure DataFusion baseline the later
//! distributed phases are measured against) and prints each physical plan so the operator
//! tree is visible. Skips if the data is absent (run `./scripts/bootstrap.sh`).

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use lldb_qe_core::{StorageConfig, build_session, register_tpch_parquet, tpch};

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

/// A session with the 8 TPC-H tables registered, or `None` if the data isn't generated yet.
async fn session_with_tpch() -> anyhow::Result<Option<SessionContext>> {
    if !data_dir().join("sf1/lineitem.parquet").exists() {
        eprintln!("SKIP: no data — run ./scripts/bootstrap.sh");
        return Ok(None);
    }
    let (ctx, storage) = build_session(StorageConfig::Local(data_dir())).await?;
    register_tpch_parquet(&ctx, &storage, "sf1").await?;
    Ok(Some(ctx))
}

#[tokio::test]
async fn q1_runs_and_returns_four_groups() -> anyhow::Result<()> {
    let Some(ctx) = session_with_tpch().await? else {
        return Ok(());
    };
    println!(
        "=== Q1 physical plan ===\n{}",
        tpch::physical_plan_string(&ctx, tpch::Q1).await?
    );
    let batches = tpch::run(&ctx, tpch::Q1).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 4,
        "TPC-H Q1 returns 4 (returnflag, linestatus) groups"
    );
    Ok(())
}

#[tokio::test]
async fn q6_runs_and_returns_one_row() -> anyhow::Result<()> {
    let Some(ctx) = session_with_tpch().await? else {
        return Ok(());
    };
    println!(
        "=== Q6 physical plan ===\n{}",
        tpch::physical_plan_string(&ctx, tpch::Q6).await?
    );
    let batches = tpch::run(&ctx, tpch::Q6).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "TPC-H Q6 returns a single revenue row");
    Ok(())
}
