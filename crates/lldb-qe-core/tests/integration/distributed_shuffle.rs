//! The staging planner distributes an arbitrary `GROUP BY` and the result matches single-node.
//!
//! This is the aggregate half of issue #4's "done when": a `GROUP BY` distributes with **no**
//! bespoke code — `plan_distributed` rewrites the physical plan into map/reduce stages, the
//! coordinator executes the rewritten plan with `collect`, and the FlightReaderExec leaves fan the
//! map stage across a fleet of in-process workers. We assert the distributed answer equals the
//! plain single-node answer, for two group cardinalities.
//!
//! No external data: the test seeds its own multi-row-group parquet in a tempdir, so it runs in CI.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::{flight, plan_distributed};
use tokio::net::TcpListener;

use crate::support::Servers;

/// Start an in-process worker on a random port; returns its URL.
///
/// The handle goes into the caller's [`Servers`] rather than being dropped: a dropped `JoinHandle`
/// detaches the task instead of stopping it, and since #44 this is one binary, so a detached worker
/// holds its port for the rest of the run.
async fn start_worker(workers: &mut Servers) -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    workers.spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Seed a parquet file with several row groups (so the byte-range split can divide it between map
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

/// A session configured so tiny test data still yields a real distribution boundary: multiple
/// target partitions and file scans splittable down to the byte.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

/// Run `sql`'s grouped counts distributed across `workers`, then sorted for comparison.
async fn distributed_counts(
    ctx: &SessionContext,
    workers: &[String],
    sql: &str,
) -> anyhow::Result<Vec<GroupCount>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let dist: Arc<dyn ExecutionPlan> = plan_distributed(plan, workers)?;
    let batches = collect(dist, ctx.task_ctx()).await?;
    let mut counts = extract_group_counts(&batches)?;
    counts.sort();
    Ok(counts)
}

async fn single_node_counts(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<GroupCount>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut counts = extract_group_counts(&batches)?;
    counts.sort();
    Ok(counts)
}

#[tokio::test]
async fn distributed_group_by_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 5)?;

    let mut fleet = Servers::new();
    let workers = vec![
        start_worker(&mut fleet).await?,
        start_worker(&mut fleet).await?,
        start_worker(&mut fleet).await?,
    ];

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let sql = "SELECT g, count(*) AS cnt FROM rows GROUP BY g";
    let distributed = distributed_counts(&ctx, &workers, sql).await?;
    let expected = single_node_counts(&ctx, sql).await?;

    assert_eq!(
        distributed, expected,
        "distributed GROUP BY must equal the single-node answer"
    );
    // Sanity: every seeded row is accounted for.
    let total: i64 = distributed.iter().map(|(_, c)| c).sum();
    assert_eq!(total, 2000);
    Ok(())
}

/// A different cardinality and worker count, to show the rewrite is not tuned to one shape.
#[tokio::test]
async fn distributed_group_by_high_cardinality() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 3000, 50)?;

    let mut fleet = Servers::new();
    let workers = vec![
        start_worker(&mut fleet).await?,
        start_worker(&mut fleet).await?,
    ];

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let sql = "SELECT g, count(*) AS cnt FROM rows GROUP BY g";
    let distributed = distributed_counts(&ctx, &workers, sql).await?;
    let expected = single_node_counts(&ctx, sql).await?;

    assert_eq!(distributed, expected);
    assert_eq!(distributed.len(), 50, "all 50 groups present exactly once");
    Ok(())
}
