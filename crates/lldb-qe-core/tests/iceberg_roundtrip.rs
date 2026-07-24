//! Phase 1 deliverable: the same data, now served through an Iceberg table.
//!
//! Creates a MemoryCatalog over a temp warehouse, loads a TPC-H table into Iceberg via
//! DataFusion `INSERT INTO`, queries it back through the catalog, and inspects the snapshot
//! the write produced. Skips if the SF1 data is absent (run `./scripts/bootstrap.sh`).

use std::path::PathBuf;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::util::pretty::pretty_format_batches;
use lldb_qe_core::{Lakehouse, StorageConfig, build_session};

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

fn sf1(table: &str) -> PathBuf {
    data_dir().join(format!("sf1/{table}.parquet"))
}

/// Scalar `i64` out of a single-row, single-column result (e.g. `SELECT count(*)`).
fn scalar_i64(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .value(0)
}

#[tokio::test]
async fn nation_roundtrips_through_iceberg() -> anyhow::Result<()> {
    let nation = sf1("nation");
    if !nation.exists() {
        eprintln!(
            "SKIP: no data at {} — run ./scripts/bootstrap.sh",
            nation.display()
        );
        return Ok(());
    }

    let (ctx, _storage) = build_session(StorageConfig::Local(data_dir())).await?;

    // A fresh Iceberg warehouse on disk, torn down when `warehouse` drops.
    let warehouse = tempfile::tempdir()?;
    let lake = Lakehouse::open_memory(warehouse.path()).await?;
    lake.load_tpch(&ctx, &[("nation", nation.to_string_lossy().into_owned())])
        .await?;

    // Query through the Iceberg catalog — three-part name: catalog.namespace.table.
    let batches = ctx
        .sql("SELECT count(*) AS n FROM lldb.tpch.nation")
        .await?
        .collect()
        .await?;
    println!("{}", pretty_format_batches(&batches)?);
    assert_eq!(scalar_i64(&batches), 25, "TPC-H nation has 25 rows");

    // The append created a snapshot; inspect it.
    let snapshot = lake.current_snapshot_id("nation").await?;
    assert!(snapshot.is_some(), "a write must produce a snapshot");
    let summary = lake.snapshot_summary("nation").await?;
    println!("nation snapshot {snapshot:?}, summary: {summary:?}");
    assert_eq!(
        summary.get("total-records").map(String::as_str),
        Some("25"),
        "snapshot summary should record 25 total rows"
    );
    Ok(())
}

/// The Phase 0 query, now answered through Iceberg. Heavy (loads 6M rows) — run explicitly
/// with `cargo test -p lldb-qe-core --test iceberg_roundtrip -- --ignored`.
#[tokio::test]
#[ignore = "loads the 6M-row lineitem table into Iceberg"]
async fn lineitem_group_by_through_iceberg() -> anyhow::Result<()> {
    let lineitem = sf1("lineitem");
    if !lineitem.exists() {
        eprintln!("SKIP: no data — run ./scripts/bootstrap.sh");
        return Ok(());
    }

    let (ctx, _storage) = build_session(StorageConfig::Local(data_dir())).await?;
    let warehouse = tempfile::tempdir()?;
    let lake = Lakehouse::open_memory(warehouse.path()).await?;
    lake.load_tpch(
        &ctx,
        &[("lineitem", lineitem.to_string_lossy().into_owned())],
    )
    .await?;

    let batches = ctx
        .sql(
            "SELECT l_returnflag, COUNT(*) AS n \
             FROM lldb.tpch.lineitem GROUP BY l_returnflag ORDER BY l_returnflag",
        )
        .await?
        .collect()
        .await?;
    println!("{}", pretty_format_batches(&batches)?);

    // Same answer as Phase 0's first_light — but served from an Iceberg table.
    let groups: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(groups, 3, "expected 3 return-flag groups (A, N, R)");
    Ok(())
}
