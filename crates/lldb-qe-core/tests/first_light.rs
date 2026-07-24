//! Phase 0 deliverable: the four-stage pipeline, end to end, over real TPC-H data.
//!
//! Generate the data from the workspace root first:
//!   `tpchgen-cli -s 1 --format=parquet --output-dir data/sf1`
//!
//! If the data is absent the test skips (with a hint) rather than failing, so `cargo test`
//! stays green on a fresh checkout.

use std::path::PathBuf;

use lldb_qe_core::{StorageConfig, build_session, register_tpch_parquet};

/// Absolute path to the workspace `data/` dir (tests run with CWD = crate dir).
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

#[tokio::test]
async fn first_light_group_by_over_lineitem() -> anyhow::Result<()> {
    let lineitem = data_dir().join("sf1/lineitem.parquet");
    if !lineitem.exists() {
        eprintln!(
            "SKIP first_light: no data at {}.\n  Generate it with: \
             tpchgen-cli -s 1 --format=parquet --output-dir data/sf1",
            lineitem.display()
        );
        return Ok(());
    }

    // Storage root is data/ ; the TPC-H tables live under the sf1/ subdir.
    let (ctx, storage) = build_session(StorageConfig::Local(data_dir())).await?;
    register_tpch_parquet(&ctx, &storage, "sf1").await?;

    let batches = ctx
        .sql(
            "SELECT l_returnflag, COUNT(*) AS n \
             FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag",
        )
        .await?
        .collect()
        .await?;

    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)?
    );

    // TPC-H lineitem carries exactly three distinct return flags: A, N, R.
    let groups: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(groups, 3, "expected 3 return-flag groups (A, N, R)");
    Ok(())
}
