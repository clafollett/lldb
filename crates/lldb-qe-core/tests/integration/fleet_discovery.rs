//! Fleet discovery drives real distribution: a query fans across *all* discovered workers, and a
//! fleet with no healthy worker left fails with an error that names every node it tried.
//!
//! This is issue #6's "done when", proven end-to-end without external data or DNS:
//!
//! 1. **Distributes across all N.** Start N in-process workers on distinct `127.0.0.1` ports, seed a
//!    multi-row-group parquet, and discover each worker's URL. Literal IPs pass straight through
//!    [`discover_workers`], so the discovered fleet is exactly those N URLs. `plan_distributed` then
//!    rewrites the `GROUP BY` to fan across all N, and we assert both that the distributed answer
//!    equals the single-node answer *and* that the rewritten plan references all N distinct worker
//!    URLs — the proof that fan-out follows the discovered fleet size (discover N → fan across N).
//! 2. **Vanished workers are named — after the fleet is exhausted.** Issue #15 changed what a dead
//!    worker means: a stage whose primary is gone is now reassigned to a healthy fallback (see
//!    `stage_reassignment.rs`), so a query fails only once *every* candidate has been tried. The
//!    assertion below is the strengthened form of the original: an entirely dead fleet must still
//!    fail with an error naming the nodes, so an operator learns which ones died rather than getting
//!    an opaque status.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::{FlightReaderExec, discover_workers, flight, plan_distributed};
use tokio::net::TcpListener;

/// Start an in-process worker on a random `127.0.0.1` port; returns its `http://` URL.
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

/// A `host:port` with nothing listening: bind a port, read its address, then drop the listener so
/// the port frees up. A connection there will be refused — a stand-in for a task that vanished.
async fn dead_worker() -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(format!("http://{addr}"))
}

/// Seed a parquet file with several row groups (so a byte-range split can divide it between map
/// workers) and a controllable number of distinct groups.
fn seed_parquet(
    dir: &std::path::Path,
    rows: i64,
    groups: i64,
) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let g: Vec<String> = (0..rows).map(|i| format!("g{}", i % groups)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(g)),
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
        ],
    )?;
    let path = dir.join("rows.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(128))
        .build();
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// A session configured so tiny test data still yields a real distribution boundary: multiple target
/// partitions and file scans splittable down to the byte.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

/// The distinct worker URLs any [`FlightReaderExec`] leaf in the (coordinator) plan points at.
fn referenced_worker_urls(plan: &Arc<dyn ExecutionPlan>) -> BTreeSet<String> {
    let mut urls = BTreeSet::new();
    plan.apply(|node| {
        if let Some(reader) = node.as_any().downcast_ref::<FlightReaderExec>() {
            urls.insert(reader.worker_url().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    urls
}

async fn sorted_counts(batches: &[RecordBatch]) -> anyhow::Result<Vec<GroupCount>> {
    let mut counts = extract_group_counts(batches)?;
    counts.sort();
    Ok(counts)
}

#[tokio::test]
async fn distributes_across_all_discovered_workers() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;

    // Three in-process workers on distinct ports.
    let endpoints = vec![
        start_worker().await?,
        start_worker().await?,
        start_worker().await?,
    ];

    // Discovery resolves each literal `127.0.0.1:port` to itself, so the fleet is exactly the three.
    let fleet = discover_workers(&endpoints).await?;
    assert_eq!(
        fleet.len(),
        3,
        "three literal endpoints discover three workers"
    );

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let sql = "SELECT g, count(*) AS cnt FROM rows GROUP BY g";
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let dist = plan_distributed(plan, &fleet)?;

    // The rewrite must reference *all three* discovered workers — the fan-out follows the fleet.
    let referenced = referenced_worker_urls(&dist);
    let expected: BTreeSet<String> = fleet.iter().cloned().collect();
    assert_eq!(
        referenced, expected,
        "the distributed plan must fan across every discovered worker"
    );

    // And the distributed answer must equal the single-node oracle.
    let distributed = sorted_counts(&collect(dist, ctx.task_ctx()).await?).await?;
    let expected_counts = sorted_counts(&ctx.sql(sql).await?.collect().await?).await?;
    assert_eq!(
        distributed, expected_counts,
        "distributed GROUP BY must equal the single-node answer"
    );
    let total: i64 = distributed.iter().map(|(_, c)| c).sum();
    assert_eq!(total, 2000, "every seeded row is accounted for");

    Ok(())
}

#[tokio::test]
async fn fewer_workers_fan_out_less() -> anyhow::Result<()> {
    // The companion to the above: discovering *two* workers fans the same query across exactly two,
    // so scaling the discovered fleet is what changes parallelism.
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 1500, 5)?;

    let endpoints = vec![start_worker().await?, start_worker().await?];
    let fleet = discover_workers(&endpoints).await?;
    assert_eq!(fleet.len(), 2);

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = ctx
        .sql("SELECT g, count(*) FROM rows GROUP BY g")
        .await?
        .create_physical_plan()
        .await?;
    let dist = plan_distributed(plan, &fleet)?;

    assert_eq!(
        referenced_worker_urls(&dist).len(),
        2,
        "two discovered workers → a two-way fan-out"
    );
    Ok(())
}

/// A query fails only after every healthy target is exhausted — and then it names them.
///
/// This assertion used to mean "one vanished worker fails the query immediately". Since issue #15 it
/// means something stronger: a lost worker is reassigned to a fallback (proven in
/// `stage_reassignment.rs`), so failing the query now requires losing the *whole* fleet — and the
/// error must still say which nodes were tried, or an operator is left with nothing to act on.
#[tokio::test]
async fn a_query_fails_only_after_exhausting_the_fleet_and_names_every_worker_tried()
-> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;

    // Every worker is a port nothing listens on: each stage burns its primary and both fallbacks.
    let fleet = vec![
        dead_worker().await?,
        dead_worker().await?,
        dead_worker().await?,
    ];

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = ctx
        .sql("SELECT g, count(*) FROM rows GROUP BY g")
        .await?
        .create_physical_plan()
        .await?;
    let dist = plan_distributed(plan, &fleet)?;

    let err = collect(dist, ctx.task_ctx())
        .await
        .expect_err("a fleet with no reachable worker must fail the query, not hang or drop rows");

    // Every candidate's `host:port` must appear, so the operator sees the whole set that died
    // rather than only whichever one happened to be pulled first.
    let chain = format!("{err}");
    for worker in &fleet {
        let addr = worker.strip_prefix("http://").unwrap();
        assert!(
            chain.contains(addr),
            "error must name every worker tried, missing `{addr}`, got: {chain}"
        );
    }
    Ok(())
}
