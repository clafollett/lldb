//! Phase 4 deliverable: a distributed hash aggregation across two workers matches the
//! single-node answer exactly. Map is distributed over Flight; the coordinator hash-shuffles
//! partials by group key and reduces. Skips if the data is absent.

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::{
    StorageConfig, build_session, distributed_group_count, flight, register_tpch_parquet,
};
use tokio::net::TcpListener;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

/// Start a worker on a random port and return its URL.
async fn start_worker() -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

#[tokio::test]
async fn distributed_group_count_matches_single_node() -> anyhow::Result<()> {
    if !data_dir().join("sf1/orders.parquet").exists() {
        eprintln!("SKIP: no data — run ./scripts/bootstrap.sh");
        return Ok(());
    }

    let workers = vec![start_worker().await?, start_worker().await?];

    let (ctx, storage) = build_session(StorageConfig::Local(data_dir())).await?;
    register_tpch_parquet(&ctx, &storage, "sf1").await?;

    // Distributed: map across 2 workers (each reading its own byte-range slice of orders),
    // hash-shuffle by o_orderstatus, reduce.
    let distributed = distributed_group_count(&ctx, &workers, "orders", "o_orderstatus").await?;
    println!("distributed group counts: {distributed:?}");

    // Single-node oracle.
    let batches = ctx
        .sql("SELECT o_orderstatus AS g, count(*) AS cnt FROM orders GROUP BY o_orderstatus")
        .await?
        .collect()
        .await?;
    let mut expected: Vec<GroupCount> = extract_group_counts(&batches)?;
    expected.sort();

    assert_eq!(
        distributed, expected,
        "distributed hash aggregation must equal the single-node group-by"
    );
    // Sanity: the counts cover all 1.5M orders.
    let total: i64 = distributed.iter().map(|(_, c)| c).sum();
    assert_eq!(total, 1_500_000, "TPC-H SF1 has 1,500,000 orders");
    Ok(())
}
