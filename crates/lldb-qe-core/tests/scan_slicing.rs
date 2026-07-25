//! Issue #3 deliverable: scan-level slicing distributes **IO**, not just compute.
//!
//! The old map stage sliced with a SQL predicate, so every worker read the whole file — an
//! `n`-worker run did `n`× the IO. [`split_scan`] instead hands each worker a disjoint set of
//! byte ranges. This test proves the two "Done when" criteria from the issue, on a seeded
//! multi-row-group parquet file so it runs everywhere (no external data):
//!
//! 1. **Correctness** — the rows read across the slices sum to exactly the single-node row
//!    count: every row is read once, none twice, none missed.
//! 2. **IO is divided, not multiplied** — the `bytes_scanned` metric summed across the slices is
//!    ≈ a single-node scan, nowhere near `n`× it.
//!
//! A second test drives the full map→reduce that [`lldb_qe_core::distributed_group_count`] uses —
//! slice a grouped `COUNT(*)`, run each slice, sum the partials — and checks the answer matches a
//! single-node `GROUP BY` exactly, the issue's third "Done when".

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::{flight, split_scan};

/// Write `rows` rows to parquet with a small row-group size (so there are many row groups for the
/// byte-range split to spread across slices). Each row has an integer `n` and a group key `g` in
/// `{a, b, c}` cycled by `n % 3`, so a `GROUP BY g` has known counts.
fn seed_parquet(dir: &std::path::Path, rows: i64) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
    ]));
    let groups = ["a", "b", "c"];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| groups[(i % 3) as usize])
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let path = dir.join("nums.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(128))
        .build();
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// Execute a plan to completion and return `(rows produced, bytes scanned)`. `bytes_scanned` is
/// a Parquet-source metric only populated once the stream is drained, so we read it after
/// `collect`.
async fn run(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> anyhow::Result<(i64, usize)> {
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let mut bytes = 0usize;
    plan.apply(|node| {
        if let Some(metrics) = node.metrics()
            && let Some(v) = metrics.sum_by_name("bytes_scanned")
        {
            bytes += v.as_usize();
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok((rows, bytes))
}

#[tokio::test]
async fn slices_read_disjoint_bytes_and_match_the_single_node_scan() -> anyhow::Result<()> {
    const ROWS: i64 = 5_000;
    const WORKERS: usize = 4;

    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), ROWS)?;
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "nums",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Single-node baseline: one scan of the whole file.
    let whole = ctx
        .sql("SELECT n FROM nums")
        .await?
        .create_physical_plan()
        .await?;
    let (whole_rows, whole_bytes) = run(&ctx, Arc::clone(&whole)).await?;
    assert_eq!(whole_rows, ROWS, "baseline reads every row");
    assert!(whole_bytes > 0, "baseline scans some bytes");

    // Sliced: one disjoint byte-range plan per worker.
    let slices = split_scan(whole, WORKERS)?;
    assert_eq!(slices.len(), WORKERS);

    let mut total_rows = 0i64;
    let mut total_bytes = 0usize;
    let mut nonempty_slices = 0;
    for slice in slices {
        // Round-trip each slice through the plan codec exactly as shipping it to a worker does.
        // This proves the byte range survives serialization (the property that makes sliced
        // scans usable across the fleet at all), and it hands the slice a *fresh* metrics set:
        // an in-process clone shares the original scan's `bytes_scanned` counter, so measuring
        // the clone would tally every slice's reads together.
        let wire = flight::serialize_plan(slice)?;
        let slice = flight::deserialize_plan(&wire, &ctx)?;

        let (rows, bytes) = run(&ctx, slice).await?;
        total_rows += rows;
        total_bytes += bytes;
        if rows > 0 {
            nonempty_slices += 1;
            assert!(rows < ROWS, "no single slice reads the whole table");
        }
    }

    // (1) Correctness: every row read exactly once across the fleet.
    assert_eq!(
        total_rows, ROWS,
        "the slices must partition the rows — no duplication, no gaps"
    );

    // The work actually spread out (a real split, not one slice doing everything).
    assert!(
        nonempty_slices >= 2,
        "expected the scan to spread over multiple slices, got {nonempty_slices}"
    );

    // (2) IO divided, not multiplied: the fleet's bytes ≈ one scan, nowhere near WORKERS×.
    //     The old whole-file-per-worker behaviour would read ~WORKERS× `whole_bytes`.
    assert!(
        total_bytes <= whole_bytes * 3 / 2,
        "fleet scanned {total_bytes} bytes vs a single-node {whole_bytes}; \
         should be ≈1×, and must be far below the old {}× ({} bytes)",
        WORKERS,
        whole_bytes * WORKERS
    );

    Ok(())
}

/// The map→reduce that [`lldb_qe_core::distributed_group_count`] runs, exercised end to end
/// without workers or external data: slice a grouped `COUNT(*)`, run each slice, sum the partials,
/// and require the result to equal a single-node `GROUP BY`.
#[tokio::test]
async fn sliced_group_by_reduces_to_the_single_node_answer() -> anyhow::Result<()> {
    const ROWS: i64 = 5_000;
    const WORKERS: usize = 3;
    const SQL: &str = "SELECT g AS g, count(*) AS cnt FROM nums GROUP BY g";

    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), ROWS)?;
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "nums",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Single-node oracle.
    let batches = ctx.sql(SQL).await?.collect().await?;
    let mut expected = extract_group_counts(&batches)?;
    expected.sort();

    // Map: one partial-count plan per worker, each over its own byte-range slice.
    let map_plan = ctx.sql(SQL).await?.create_physical_plan().await?;
    let slices = split_scan(map_plan, WORKERS)?;

    // Reduce: sum the per-slice partials by group (count is additive, so this is exact).
    let mut totals: HashMap<String, i64> = HashMap::new();
    for slice in slices {
        let wire = flight::serialize_plan(slice)?;
        let slice = flight::deserialize_plan(&wire, &ctx)?;
        let batches = collect(slice, ctx.task_ctx()).await?;
        for (group, cnt) in extract_group_counts(&batches)? {
            *totals.entry(group).or_default() += cnt;
        }
    }
    let mut got: Vec<GroupCount> = totals.into_iter().collect();
    got.sort();

    assert_eq!(
        got, expected,
        "sliced map→reduce must equal the single-node GROUP BY"
    );
    assert_eq!(
        got.iter().map(|(_, c)| c).sum::<i64>(),
        ROWS,
        "every row must be counted exactly once"
    );
    Ok(())
}
