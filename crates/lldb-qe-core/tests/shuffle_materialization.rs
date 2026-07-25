//! Pull-shuffle materialization: a producer stage pulled by `N` consumers executes **once**.
//!
//! This is the "Done when" of the materialize-once shuffle. A single in-process worker holds a
//! [`StageCache`]; we hand the test a clone of that cache so it can observe execution counts. We
//! build a producer sub-plan with several output partitions, then fire `N` concurrent consumers
//! that each pull one partition over Flight. The assertions:
//!
//!   (a) every consumer gets complete, correct output — checked against a single-node
//!       `collect_partitioned` of the same producer, and
//!   (b) the cache's `execution_count()` is exactly `1`: the producer ran once and the other
//!       consumers were served from the buffer.
//!
//! A second test proves the cache is not accidentally global: two *different* producers get
//! different stage ids and both execute (`execution_count() == 2`).
//!
//! No external data: the test seeds its own parquet in a tempdir, so it runs everywhere.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect_partitioned};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::future::try_join_all;
use lldb_qe_core::flight::{self, serialize_plan};
use lldb_qe_core::{StageCache, stage_id_of};
use tokio::net::TcpListener;

/// Start an in-process worker sharing `cache`; returns its URL. The caller keeps its own `Arc`
/// clone of the cache to read execution counts afterwards.
async fn start_worker_with_cache(cache: Arc<StageCache>) -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        flight::serve_worker_with_cache(listener, SessionContext::new(), cache)
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Seed a small multi-row-group parquet file so a scan over it yields several partitions once
/// repartitioned. Returns the file path.
fn seed_parquet(dir: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let g: Vec<String> = (0..900).map(|i| format!("k{}", i % 6)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(g)),
            Arc::new(Int64Array::from((0..900).collect::<Vec<_>>())),
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

/// A session tuned to produce several output partitions from a small file (so a producer has
/// multiple partitions worth pulling in parallel).
fn multi_partition_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

/// Build a producer sub-plan with `>1` output partition: a hash repartition over the file scan.
/// This is exactly the kind of shared producer a shuffle fans into many reduce consumers, and the
/// kind whose partitions must be drained together (hence `collect_partitioned`).
async fn producer_plan(ctx: &SessionContext, path: &std::path::Path) -> Arc<dyn ExecutionPlan> {
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    // A GROUP BY forces a hash RepartitionExec; its partial-aggregate child is a well-formed
    // producer with `target_partitions` output partitions.
    let plan = ctx
        .sql("SELECT g, count(*) FROM rows GROUP BY g")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    // Descend to the RepartitionExec so the producer we ship really has multiple partitions.
    find_multi_partition_node(&plan).unwrap_or(plan)
}

/// Find the deepest node with more than one output partition — a stand-in producer that fans out.
fn find_multi_partition_node(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    for child in plan.children() {
        if let Some(found) = find_multi_partition_node(child) {
            return Some(found);
        }
    }
    if plan.properties().partitioning.partition_count() > 1 {
        Some(Arc::clone(plan))
    } else {
        None
    }
}

#[tokio::test]
async fn a_producer_pulled_by_many_consumers_executes_once() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path())?;

    let cache = Arc::new(StageCache::new());
    let worker = start_worker_with_cache(Arc::clone(&cache)).await?;

    let ctx = multi_partition_ctx();
    let producer = producer_plan(&ctx, &path).await;
    let n_parts = producer.properties().partitioning.partition_count();
    assert!(
        n_parts > 1,
        "test needs a multi-partition producer, got {n_parts}"
    );

    // Ground truth: a single-node materialization of the same producer, partition by partition.
    let expected = collect_partitioned(Arc::clone(&producer), ctx.task_ctx()).await?;

    // Fire N concurrent consumers, one per output partition — the shuffle fan-out. Each wraps the
    // *same* producer bytes in a FlightReaderExec differing only in `remote_partition`, so they all
    // resolve to one stage id and share one cache entry.
    let pulls = (0..n_parts).map(|part| {
        let worker = worker.clone();
        let producer = Arc::clone(&producer);
        async move { flight::fetch(&worker, part as u32, producer).await }
    });
    let got: Vec<Vec<RecordBatch>> = try_join_all(pulls).await?;

    // (a) Every consumer got complete, correct output for its partition.
    for (part, (got, expected)) in got.iter().zip(expected.iter()).enumerate() {
        let got_rows: usize = got.iter().map(|b| b.num_rows()).sum();
        let exp_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(got_rows, exp_rows, "partition {part} row count");
    }
    let total_got: usize = got.iter().flatten().map(|b| b.num_rows()).sum();
    let total_exp: usize = expected.iter().flatten().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_got, total_exp,
        "all rows accounted for across partitions"
    );

    // (b) The producer executed exactly once despite N concurrent pulls.
    assert_eq!(
        cache.execution_count(),
        1,
        "the shared producer stage must materialize once, not once per consumer"
    );
    // The N pulls all resolved to one content-addressed stage id, so the cache holds exactly one.
    let _stage_id = stage_id_of(&serialize_plan(Arc::clone(&producer))?);
    assert_eq!(cache.len(), 1, "one shared stage entry");
    // Re-pulling the same producer is served from cache — no new materialization.
    let _ = flight::fetch(&worker, 0, Arc::clone(&producer)).await?;
    assert_eq!(cache.execution_count(), 1, "re-pull hits the cache");

    Ok(())
}

#[tokio::test]
async fn different_producers_get_different_stage_ids() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path())?;

    let cache = Arc::new(StageCache::new());
    let worker = start_worker_with_cache(Arc::clone(&cache)).await?;

    // Two genuinely different producers over the same data.
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    let plan_a = ctx
        .sql("SELECT g, v FROM rows WHERE v < 100")
        .await?
        .create_physical_plan()
        .await?;
    let plan_b = ctx
        .sql("SELECT g, v FROM rows WHERE v >= 800")
        .await?
        .create_physical_plan()
        .await?;

    // Distinct plan bytes → distinct stage ids.
    let id_a = stage_id_of(&serialize_plan(Arc::clone(&plan_a))?);
    let id_b = stage_id_of(&serialize_plan(Arc::clone(&plan_b))?);
    assert_ne!(id_a, id_b, "different producers must not share a stage id");

    // Both pulled → both materialize; caching is per-stage, not global.
    flight::fetch(&worker, 0, plan_a).await?;
    flight::fetch(&worker, 0, plan_b).await?;
    assert_eq!(cache.execution_count(), 2);
    assert_eq!(cache.len(), 2);

    Ok(())
}
