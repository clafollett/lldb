//! Multi-boundary plans, broadcast joins and distributed sorts, each checked against single-node.
//!
//! This is the "Done when" of the richer-operators milestone. The staging planner no longer cuts one
//! boundary per plan; it recurses, so a join feeding a `GROUP BY`, two joins in one query, or a
//! `UNION ALL` of two aggregates all become a DAG of stages. Alongside that it learned two shapes it
//! had no answer for before: a broadcast join (replicate the small side rather than shuffle the
//! large one) and a distributed sort (sort on the workers, merge on the coordinator).
//!
//! Every case is asserted against the **single-node oracle** — the same SQL run by plain DataFusion
//! in the same session. That is the only assertion that matters: a distributed plan is worth nothing
//! if it does not produce exactly what one node would have. The broadcast case additionally asserts
//! on *data movement*, using the worker's [`StageCache::rows_served`] meter, because "correct" and
//! "did not ship the large table across the network" are different claims and the second one is the
//! whole reason broadcast joins exist.
//!
//! The last case is a shape the planner deliberately declines to distribute. It is here for the same
//! reason as the rest: falling back must still give the right answer.
//!
//! No external data and no Docker — the tests seed their own parquet in a tempdir and run a fleet of
//! in-process Flight workers on `127.0.0.1` random ports.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::{FlightReaderExec, StageCache, flight, plan_distributed};
use tokio::net::TcpListener;

/// One result row, rendered column-by-column as text so any schema can be compared.
type Row = Vec<String>;

/// Start an in-process worker on a random port; returns its URL.
async fn start_worker() -> anyhow::Result<String> {
    start_worker_with_cache(Arc::new(StageCache::new())).await
}

/// Start an in-process worker sharing `cache`, so the test can read its data-movement counters.
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

/// Seed a table as a directory of `files` parquet files, each with several row groups.
///
/// Multiple files give the listing scan multiple partitions (what makes the optimizer insert the
/// hash-repartition seams the planner cuts at) and multiple row groups give [`split_scan`] something
/// to divide by byte range. `keys` sets the join/group cardinality; `v` is unique across the whole
/// table so an ordering over it is total and a row is identifiable.
///
/// [`split_scan`]: lldb_qe_core::split_scan
fn seed_table(
    dir: &std::path::Path,
    name: &str,
    files: usize,
    rows: i64,
    keys: i64,
) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let tdir = dir.join(name);
    std::fs::create_dir_all(&tdir)?;
    for f in 0..files {
        let base = f as i64 * rows;
        let g: Vec<String> = (0..rows)
            .map(|i| format!("k{}", (base + i) % keys))
            .collect();
        let v: Vec<i64> = (0..rows).map(|i| base + i).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(g)),
                Arc::new(Int64Array::from(v)),
            ],
        )?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .build();
        let file = std::fs::File::create(tdir.join(format!("part{f}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(tdir)
}

/// A session that forces the partitioned (shuffle) hash-join path on tiny data.
fn shuffling_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    let opts = cfg.options_mut();
    opts.optimizer.repartition_file_min_size = 1;
    opts.optimizer.hash_join_single_partition_threshold = 0;
    opts.optimizer.hash_join_single_partition_threshold_rows = 0;
    SessionContext::new_with_config(cfg)
}

/// A session with the collect-left thresholds left alone, so a small build side is planned as a
/// broadcast join instead of a shuffle.
fn broadcasting_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

async fn register(ctx: &SessionContext, tables: &[(&str, &std::path::Path)]) -> anyhow::Result<()> {
    for (name, path) in tables {
        ctx.register_parquet(*name, path.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
    }
    Ok(())
}

/// Render result batches as rows of text — schema-agnostic, so one helper serves every query here.
fn rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<Row>> {
    let opts = FormatOptions::default();
    let mut out = Vec::new();
    for batch in batches {
        let cols = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<Vec<_>, _>>()?;
        for i in 0..batch.num_rows() {
            out.push(cols.iter().map(|c| c.value(i).to_string()).collect());
        }
    }
    Ok(out)
}

/// Remote stages visible in a coordinator plan. Counted so a test cannot pass by quietly running
/// everything locally — "matches single-node" is trivially true of a plan that *is* single-node.
fn remote_reads(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut n = 0;
    plan.apply(|node| {
        if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
            n += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("counting walk does not error");
    n
}

/// Run `sql` distributed across `workers`, returning its rows **in the order produced** and how many
/// remote stages the coordinator ended up pulling from.
async fn distributed_rows(
    ctx: &SessionContext,
    workers: &[String],
    sql: &str,
) -> anyhow::Result<(Vec<Row>, usize)> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let dist: Arc<dyn ExecutionPlan> = plan_distributed(plan, workers)?;
    let stages = remote_reads(&dist);
    Ok((rows(&collect(dist, ctx.task_ctx()).await?)?, stages))
}

/// The single-node oracle: the same SQL, same session, plain DataFusion.
async fn single_node_rows(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<Row>> {
    rows(&ctx.sql(sql).await?.collect().await?)
}

/// Assert a distributed run matches single-node as a *set* of rows (order is not defined by the
/// query, so neither side may claim one).
async fn assert_matches_single_node(
    ctx: &SessionContext,
    workers: &[String],
    sql: &str,
) -> anyhow::Result<Vec<Row>> {
    let (mut distributed, stages) = distributed_rows(ctx, workers, sql).await?;
    let mut expected = single_node_rows(ctx, sql).await?;
    assert!(!expected.is_empty(), "the query must return rows: {sql}");
    assert!(
        stages > 1,
        "the plan must actually have been distributed, but the coordinator pulls {stages} \
         stage(s) for: {sql}"
    );
    distributed.sort();
    expected.sort();
    assert_eq!(
        distributed, expected,
        "distributed result must equal the single-node answer for: {sql}"
    );
    Ok(expected)
}

// ---------------------------------------------------------------------------
// Multi-boundary plans
// ---------------------------------------------------------------------------

/// A join *feeding* a `GROUP BY`: two boundaries stacked, which the single-boundary planner
/// rejected outright. The join's hash buckets become reduce stages and the aggregate is pushed onto
/// each bucket's worker.
#[tokio::test]
async fn join_feeding_a_group_by_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = seed_table(tmp.path(), "a", 2, 300, 12)?;
    let b = seed_table(tmp.path(), "b", 2, 200, 12)?;

    let workers = vec![start_worker().await?, start_worker().await?];
    let ctx = shuffling_ctx();
    register(&ctx, &[("a", &a), ("b", &b)]).await?;

    let counts = assert_matches_single_node(
        &ctx,
        &workers,
        "SELECT a.g, count(*) AS n, sum(b.v) AS total FROM a JOIN b ON a.g = b.g GROUP BY a.g",
    )
    .await?;
    assert_eq!(counts.len(), 12, "one row per join key");
    Ok(())
}

/// Two joins in one plan. The inner join's stages end up *inside* the outer join's stages — the
/// nesting the plan codec was built for.
#[tokio::test]
async fn two_joins_in_one_plan_match_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = seed_table(tmp.path(), "a", 2, 60, 30)?;
    let b = seed_table(tmp.path(), "b", 2, 60, 30)?;
    let c = seed_table(tmp.path(), "c", 2, 60, 30)?;

    let workers = vec![start_worker().await?, start_worker().await?];
    let ctx = shuffling_ctx();
    register(&ctx, &[("a", &a), ("b", &b), ("c", &c)]).await?;

    assert_matches_single_node(
        &ctx,
        &workers,
        "SELECT a.v, b.v, c.v FROM a JOIN b ON a.g = b.g JOIN c ON a.g = c.g",
    )
    .await?;
    Ok(())
}

/// A `UNION ALL` of two aggregates: two independent boundaries side by side rather than stacked.
/// Each is cut on its own, which is exactly what recursion buys over "find the one boundary".
#[tokio::test]
async fn union_all_of_two_aggregates_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = seed_table(tmp.path(), "a", 2, 500, 7)?;
    let b = seed_table(tmp.path(), "b", 2, 400, 5)?;

    let workers = vec![start_worker().await?, start_worker().await?];
    let ctx = shuffling_ctx();
    register(&ctx, &[("a", &a), ("b", &b)]).await?;

    let rows = assert_matches_single_node(
        &ctx,
        &workers,
        "SELECT g, count(*) AS n FROM a GROUP BY g \
         UNION ALL SELECT g, count(*) AS n FROM b GROUP BY g",
    )
    .await?;
    assert_eq!(rows.len(), 12, "7 groups from a plus 5 from b");
    Ok(())
}

// ---------------------------------------------------------------------------
// Broadcast join
// ---------------------------------------------------------------------------

/// A broadcast join must be correct *and* must not move the large side.
///
/// The optimizer picks `CollectLeft` when the build side is small. The planner replicates that small
/// side to every join stage and slices the large side's scan in place, so the large table is read
/// where it lives and only join output travels. We prove that with the worker-side rows-served
/// meter: run the same query both ways over the same data and compare what crossed the wire. The
/// shuffled plan moves at least every row of the large table; the broadcast plan moves a small
/// multiple of the *small* table plus the result.
#[tokio::test]
async fn broadcast_join_matches_single_node_without_shuffling_the_large_side() -> anyhow::Result<()>
{
    let tmp = tempfile::tempdir()?;
    // 5000 rows over 100 keys on the large side; 6 rows over 3 keys on the small one. Only 3% of the
    // large table can match, so a plan that ships it is obvious in the counters.
    let big = seed_table(tmp.path(), "big", 2, 2500, 100)?;
    let small = seed_table(tmp.path(), "small", 1, 6, 3)?;
    let big_rows = 5000;
    let sql = "SELECT big.g, big.v, small.v FROM small JOIN big ON small.g = big.g";

    // --- broadcast: collect-left thresholds left at their defaults.
    let cache = Arc::new(StageCache::new());
    let workers = vec![
        start_worker_with_cache(Arc::clone(&cache)).await?,
        start_worker_with_cache(Arc::clone(&cache)).await?,
    ];
    let ctx = broadcasting_ctx();
    register(&ctx, &[("big", &big), ("small", &small)]).await?;
    let expected = assert_matches_single_node(&ctx, &workers, sql).await?;
    let broadcast_rows_moved = cache.rows_served();

    assert!(
        broadcast_rows_moved >= expected.len(),
        "the coordinator must at least have received the result: {broadcast_rows_moved} moved for \
         {} result rows",
        expected.len()
    );
    assert!(
        broadcast_rows_moved < big_rows,
        "the large side ({big_rows} rows) must never cross the network, but {broadcast_rows_moved} \
         rows were served"
    );

    // --- the same query shuffled, for contrast: now both sides move.
    let shuffle_cache = Arc::new(StageCache::new());
    let shuffle_workers = vec![
        start_worker_with_cache(Arc::clone(&shuffle_cache)).await?,
        start_worker_with_cache(Arc::clone(&shuffle_cache)).await?,
    ];
    let shuffle_ctx = shuffling_ctx();
    register(&shuffle_ctx, &[("big", &big), ("small", &small)]).await?;
    let shuffled = assert_matches_single_node(&shuffle_ctx, &shuffle_workers, sql).await?;
    let shuffled_rows_moved = shuffle_cache.rows_served();

    assert_eq!(shuffled, expected, "both plans compute the same join");
    println!(
        "rows across the wire: broadcast {broadcast_rows_moved}, shuffle {shuffled_rows_moved} \
         (large side = {big_rows} rows, result = {} rows)",
        expected.len()
    );
    assert!(
        shuffled_rows_moved >= big_rows,
        "shuffling is supposed to move the large side; only {shuffled_rows_moved} rows moved"
    );
    assert!(
        broadcast_rows_moved * 4 < shuffled_rows_moved,
        "broadcasting must move dramatically less than shuffling: {broadcast_rows_moved} vs \
         {shuffled_rows_moved}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Distributed sort and window
// ---------------------------------------------------------------------------

/// `ORDER BY` distributed as per-worker sorts under the coordinator's merge. The assertion is on the
/// **ordering**, not on set equality: the rows are compared position by position against the
/// single-node answer, and the sort key is checked to be strictly increasing. A merge that silently
/// interleaved unsorted runs would pass a set comparison and fail this one.
#[tokio::test]
async fn distributed_order_by_matches_single_node_ordering() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let t = seed_table(tmp.path(), "t", 3, 400, 9)?;

    let workers = vec![
        start_worker().await?,
        start_worker().await?,
        start_worker().await?,
    ];
    let ctx = shuffling_ctx();
    register(&ctx, &[("t", &t)]).await?;

    let sql = "SELECT g, v FROM t ORDER BY v";
    let (distributed, stages) = distributed_rows(&ctx, &workers, sql).await?;
    let expected = single_node_rows(&ctx, sql).await?;

    assert_eq!(
        stages,
        workers.len(),
        "one sorted run per worker under the coordinator's merge"
    );
    assert_eq!(expected.len(), 1200, "every seeded row comes back");
    assert_eq!(
        distributed, expected,
        "the distributed sort must match single-node row for row, in order"
    );

    // Independently of the oracle: the merged stream really is ordered.
    let keys: Vec<i64> = distributed
        .iter()
        .map(|row| row[1].parse::<i64>().expect("v is an integer"))
        .collect();
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "the merged stream must be strictly increasing in v"
    );
    Ok(())
}

/// A window function over a distributed input. `PARTITION BY` gives a safe hash key, so the window
/// is cut at that seam and evaluated per bucket on the fleet.
#[tokio::test]
async fn windowed_aggregate_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let t = seed_table(tmp.path(), "t", 2, 300, 8)?;

    let workers = vec![start_worker().await?, start_worker().await?];
    let ctx = shuffling_ctx();
    register(&ctx, &[("t", &t)]).await?;

    assert_matches_single_node(
        &ctx,
        &workers,
        "SELECT g, v, sum(v) OVER (PARTITION BY g ORDER BY v) AS running FROM t",
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Falling back
// ---------------------------------------------------------------------------

/// Shapes the planner declines to distribute must still answer correctly.
///
/// Two of them here. A `MemTable` aggregate has a boundary but nothing sliceable underneath, so the
/// whole plan is handed back untouched and runs on the coordinator. A `GROUP BY … ORDER BY`
/// distributes its aggregate but not its sort — a hash repartition sits between the sort and the
/// aggregate's remote branches, and pushing through it would be reordering rows we do not control.
/// Half a distributed plan and a right answer beats a whole one and a wrong answer.
#[tokio::test]
async fn shapes_that_fall_back_still_answer_correctly() -> anyhow::Result<()> {
    let workers = vec![start_worker().await?, start_worker().await?];

    // (a) A MemTable aggregate: no file scan to slice, so nothing is shipped at all.
    let ctx = shuffling_ctx();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b", "a"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
        ],
    )?;
    ctx.register_table(
        "mem",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
    )?;

    let sql = "SELECT g, count(*) AS n, sum(v) AS total FROM mem GROUP BY g";
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let dist = plan_distributed(Arc::clone(&plan), &workers)?;
    assert!(
        Arc::ptr_eq(&plan, &dist),
        "an unsliceable aggregate is left exactly as it was"
    );
    let mut fallback = rows(&collect(dist, ctx.task_ctx()).await?)?;
    let mut expected = single_node_rows(&ctx, sql).await?;
    fallback.sort();
    expected.sort();
    assert_eq!(fallback, expected, "the fallback must still be right");

    // (b) A sort over an aggregate: the aggregate distributes, the sort does not, and the answer is
    // still ordered correctly.
    let tmp = tempfile::tempdir()?;
    let t = seed_table(tmp.path(), "t", 2, 400, 20)?;
    let file_ctx = shuffling_ctx();
    register(&file_ctx, &[("t", &t)]).await?;

    let sql = "SELECT g, count(*) AS n FROM t GROUP BY g ORDER BY g";
    let (distributed, stages) = distributed_rows(&file_ctx, &workers, sql).await?;
    let expected = single_node_rows(&file_ctx, sql).await?;
    assert_eq!(
        stages,
        workers.len(),
        "the aggregate half still went to the fleet"
    );
    assert_eq!(expected.len(), 20);
    assert_eq!(
        distributed, expected,
        "a partly distributed plan must still match single-node, in order"
    );
    Ok(())
}
