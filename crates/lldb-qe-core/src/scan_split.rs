//! Scan-level slicing: distribute **IO**, not just compute.
//!
//! [`crate::distributed`] used to slice the map stage with a SQL predicate
//! (`WHERE abs(col) % n = w`): every worker read the *whole* file and discarded the rows that
//! were not its slice, so an `n`-worker run did `n`× the IO of a single node. This module slices
//! the *scan itself*. Each worker is handed a disjoint set of **byte ranges** of the table's
//! files, so it reads only its own bytes and the fleet's total IO ≈ one single-node scan.
//!
//! The mechanism is DataFusion's own [`FileGroupPartitioner`], which splits a set of
//! `PartitionedFile`s into groups of roughly equal *size* — not equal file *count*, because
//! skew matters (a fleet is only balanced if every worker reads a similar number of bytes).
//! A Parquet reader honours a `PartitionedFile`'s byte range by scanning only the row groups
//! whose first page begins inside it, and the ranges are contiguous and half-open, so every row
//! group is assigned to exactly one worker. That is what makes the sliced result identical to a
//! single-node scan: no row group is read twice, and none is missed.
//!
//! [`split_scan`] rewrites a physical plan. It finds the single file-scan leaf
//! ([`DataSourceExec`] over a [`FileScanConfig`]) and returns `n` copies of the whole plan, each
//! with that leaf restricted to one slice. Because the rest of the plan (a partial aggregate,
//! say) rides along unchanged, the caller can ship each copy to a worker via
//! [`crate::remote::FlightReaderExec`] and reduce the partials — IO and compute both distributed.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;

/// Split `plan` into `partitions` copies, each reading a disjoint slice of the scan's bytes.
///
/// `plan` must contain exactly one file-scan leaf — a [`DataSourceExec`] backed by a
/// [`FileScanConfig`], which is what a registered Parquet/listing table produces. The scan's
/// files are repartitioned into `partitions` byte-range groups balanced by size, and one copy of
/// the plan is returned per group with its leaf restricted to that group.
///
/// The returned vector always has exactly `partitions` entries. If the data is too small to
/// split that many ways, the surplus copies scan an empty range (and so produce no rows), which
/// keeps a caller's one-plan-per-worker bookkeeping simple.
///
/// # Errors
/// Returns an error if `plan` has no file-scan leaf, or more than one — scan-level slicing is a
/// single-scan operation in this POC (a join over two scans would need each side sliced and
/// co-located, which is the staging planner's job, not this primitive's).
pub fn split_scan(
    plan: Arc<dyn ExecutionPlan>,
    partitions: usize,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    assert!(partitions > 0, "need at least one partition");

    let groups = find_scan_file_groups(&plan)?;
    let slices = slice_by_size(&groups, partitions);

    (0..partitions)
        .map(|i| {
            // `slice_by_size` may yield fewer groups than requested when the data is small;
            // the missing slices scan nothing.
            let group = slices
                .get(i)
                .cloned()
                .unwrap_or_else(|| FileGroup::new(Vec::new()));
            rebuild_with_file_group(Arc::clone(&plan), group)
        })
        .collect()
}

/// Collect the file groups of the plan's single scan leaf, erroring on zero or many scans.
fn find_scan_file_groups(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<FileGroup>> {
    let mut scans: Vec<Vec<FileGroup>> = Vec::new();
    plan.apply(|node| {
        if let Some(config) = file_scan_config(node) {
            scans.push(config.file_groups.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|e| anyhow!("walking physical plan for scan leaves: {e}"))?;

    match scans.len() {
        0 => bail!(
            "plan has no file-scan leaf to slice — scan-level slicing needs a registered \
             parquet/listing table, not an in-memory or computed source"
        ),
        1 => Ok(scans.into_iter().next().expect("len checked == 1")),
        n => bail!("plan has {n} file-scan leaves; scan-level slicing supports a single scan"),
    }
}

/// Repartition `groups` into `partitions` byte-range slices balanced by size.
///
/// `repartition_file_min_size` is set to 1 byte so even the small files in a test — or a lone
/// SF1 table — are split; the 10 MB default would decline to split them and hand every worker the
/// whole file, which is the exact behaviour this module exists to remove.
fn slice_by_size(groups: &[FileGroup], partitions: usize) -> Vec<FileGroup> {
    FileGroupPartitioner::new()
        .with_target_partitions(partitions)
        .with_repartition_file_min_size(1)
        .repartition_file_groups(groups)
        // `None` means nothing to split (e.g. zero total bytes); fall back to the input as-is.
        .unwrap_or_else(|| groups.to_vec())
}

/// Rebuild `plan` with its scan leaf's file groups replaced by the single `group`.
fn rebuild_with_file_group(
    plan: Arc<dyn ExecutionPlan>,
    group: FileGroup,
) -> Result<Arc<dyn ExecutionPlan>> {
    let rewritten = plan
        .transform_down(|node| {
            let Some(config) = file_scan_config(&node) else {
                return Ok(Transformed::no(node));
            };
            let mut config = config.clone();
            config.file_groups = vec![group.clone()];
            let replaced: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
            Ok(Transformed::yes(replaced))
        })
        .map_err(|e| anyhow!("rewriting scan leaf with its slice: {e}"))?;
    Ok(rewritten.data)
}

/// The [`FileScanConfig`] of a node, if it is a file-scan [`DataSourceExec`].
fn file_scan_config(node: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
    node.as_any()
        .downcast_ref::<DataSourceExec>()?
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

    /// Write `rows` integers to a parquet file with a small row-group size, so the file has
    /// several row groups for the byte-range split to divide between slices.
    fn seed_parquet(dir: &std::path::Path, rows: i64) -> Result<std::path::PathBuf> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
        )?;
        let path = dir.join("nums.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(64))
            .build();
        let file = std::fs::File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(path)
    }

    async fn scan_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// The total byte length covered by a plan's scan ranges — the sum over its slice's files of
    /// `range.end - range.start` (or the whole file when no range is set).
    fn scanned_len(plan: &Arc<dyn ExecutionPlan>) -> i64 {
        let mut total = 0;
        plan.apply(|node| {
            if let Some(config) = file_scan_config(node) {
                for group in &config.file_groups {
                    for file in group.iter() {
                        total += match &file.range {
                            Some(r) => r.end - r.start,
                            None => file.object_meta.size as i64,
                        };
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        total
    }

    #[tokio::test]
    async fn splits_a_scan_into_disjoint_ranges_that_cover_the_whole_file() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = seed_parquet(tmp.path(), 1000)?;
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "nums",
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let plan = scan_plan(&ctx, "SELECT n FROM nums").await;
        let whole = scanned_len(&plan);

        let slices = split_scan(Arc::clone(&plan), 4)?;
        assert_eq!(slices.len(), 4, "one plan per requested partition");

        // The slices partition the file: their ranges sum to the whole, and none is empty here
        // (1000 rows at 64 rows/group is many row groups — plenty to spread over four slices).
        let sliced_total: i64 = slices.iter().map(scanned_len).sum();
        assert_eq!(
            sliced_total, whole,
            "slices must cover exactly the file once — not less, not n× more"
        );
        for slice in &slices {
            assert!(scanned_len(slice) > 0, "each slice reads a non-empty range");
            assert!(
                scanned_len(slice) < whole,
                "no slice reads the whole file — that was the bug"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn surplus_partitions_get_empty_slices() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        // A single tiny row group: it cannot be divided many ways.
        let path = seed_parquet(tmp.path(), 4)?;
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "nums",
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
        let plan = scan_plan(&ctx, "SELECT n FROM nums").await;
        let whole = scanned_len(&plan);

        let slices = split_scan(Arc::clone(&plan), 8)?;
        assert_eq!(slices.len(), 8, "always exactly `partitions` plans");
        let sliced_total: i64 = slices.iter().map(scanned_len).sum();
        assert_eq!(
            sliced_total, whole,
            "coverage is preserved even when over-split"
        );
        Ok(())
    }

    #[tokio::test]
    async fn errors_when_there_is_no_scan_to_slice() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("mem", Arc::new(table))?;

        let plan = scan_plan(&ctx, "SELECT n FROM mem").await;
        let err = split_scan(plan, 2).expect_err("a MemTable has no file-scan leaf");
        assert!(err.to_string().contains("no file-scan leaf"), "got: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn errors_when_there_are_multiple_scans() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = seed_parquet(tmp.path(), 128)?;
        let ctx = SessionContext::new();
        ctx.register_parquet("a", path.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
        ctx.register_parquet("b", path.to_str().unwrap(), ParquetReadOptions::default())
            .await?;

        let plan = scan_plan(&ctx, "SELECT a.n FROM a JOIN b ON a.n = b.n").await;
        let err = split_scan(plan, 2).expect_err("two scans is unsupported");
        assert!(err.to_string().contains("file-scan leaves"), "got: {err}");
        Ok(())
    }
}
