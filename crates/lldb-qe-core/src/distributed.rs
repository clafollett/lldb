//! Distributed execution: the hash shuffle.
//!
//! This is the heart of a distributed query engine. To aggregate (or join) across machines
//! you must ensure every row with the same key is processed by the same node. You get there
//! by **hash-partitioning**: `partition = hash(key) % num_reducers`.
//!
//! [`distributed_group_count`] implements a distributed grouped `COUNT(*)` in three stages:
//!
//! 1. **Map** — each worker aggregates a *disjoint slice* of the table (selected by a modulo
//!    predicate) and returns per-group partial counts. These run in parallel over Arrow
//!    Flight, so the aggregation work is genuinely distributed.
//! 2. **Shuffle** — the coordinator routes every partial into a reduce bucket by
//!    `hash(group) % n`. This is the move that makes distributed aggregation correct: every
//!    occurrence of a group lands in the same bucket.
//! 3. **Reduce** — sum the partials within each bucket into the final per-group count.
//!
//! The result is identical to a single-node `GROUP BY`. A hash **join** uses the exact same
//! shuffle, keyed on the join column so matching rows from both sides meet on one node.
//!
//! POC scope, stated plainly:
//! - The map reads the whole file and filters to its slice (compute is distributed, IO is
//!   not). Real engines slice the scan itself.
//! - The reduce runs on the coordinator. Real engines push each shuffle partition to a reduce
//!   worker over Flight `do_exchange`. That worker-to-worker exchange is the documented next
//!   step; here the shuffle is made explicit but co-located.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::SessionContext;
use futures::future::try_join_all;

use crate::flight;

/// A group value and its (partial or final) count.
pub type GroupCount = (String, i64);

/// Distributed grouped `COUNT(*)` over `workers`, keyed on a string `group_col`, sliced for
/// the map stage by a numeric `slice_col`. Returns `(group, count)` pairs sorted by group.
///
/// The result matches `SELECT {group_col}, count(*) FROM {table} GROUP BY {group_col}` run on
/// a single node.
pub async fn distributed_group_count(
    ctx: &SessionContext,
    workers: &[String],
    table: &str,
    group_col: &str,
    slice_col: &str,
) -> Result<Vec<GroupCount>> {
    let n = workers.len();
    assert!(n > 0, "need at least one worker");

    // --- Map: each worker aggregates its disjoint slice, in parallel over Flight. ---
    let map_tasks = (0..n).map(|w| {
        let sql = format!(
            "SELECT {group_col} AS g, count(*) AS cnt FROM {table} \
             WHERE abs({slice_col}) % {n} = {w} GROUP BY {group_col}"
        );
        let url = workers[w].clone();
        async move {
            let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
            let plan = Arc::new(CoalescePartitionsExec::new(plan));
            let batches = flight::fetch(&url, 0, plan).await?;
            anyhow::Ok(extract_group_counts(&batches)?)
        }
    });
    let per_worker: Vec<Vec<GroupCount>> = try_join_all(map_tasks).await?;

    // --- Shuffle: route each partial into a reduce bucket by hash(group) % n. ---
    let mut buckets: Vec<Vec<GroupCount>> = vec![Vec::new(); n];
    for partials in per_worker {
        for (group, cnt) in partials {
            let bucket = (hash_key(&group) % n as u64) as usize;
            buckets[bucket].push((group, cnt));
        }
    }

    // --- Reduce: sum counts per group within each bucket (a group lives in exactly one). ---
    let mut result: Vec<GroupCount> = Vec::new();
    for bucket in buckets {
        let mut totals: HashMap<String, i64> = HashMap::new();
        for (group, cnt) in bucket {
            *totals.entry(group).or_default() += cnt;
        }
        result.extend(totals);
    }
    result.sort();
    Ok(result)
}

/// Deterministic hash of a group key. `DefaultHasher` uses fixed keys, so the same group
/// always routes to the same bucket — the property the shuffle depends on.
fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Pull `(group: Utf8, count: Int64)` pairs out of aggregation result batches.
pub fn extract_group_counts(batches: &[RecordBatch]) -> Result<Vec<GroupCount>> {
    let mut out = Vec::new();
    for batch in batches {
        // Cast defensively: DataFusion/arrow read Parquet strings as Utf8View, and count(*)
        // may be Int64 or a decimal depending on the plan — normalize both here.
        let group_col = cast(batch.column(0), &DataType::Utf8).context("casting group to Utf8")?;
        let count_col =
            cast(batch.column(1), &DataType::Int64).context("casting count to Int64")?;
        let groups = group_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 yields StringArray");
        let counts = count_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cast to Int64 yields Int64Array");
        for i in 0..batch.num_rows() {
            out.push((groups.value(i).to_string(), counts.value(i)));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashing_is_deterministic() {
        assert_eq!(hash_key("A"), hash_key("A"));
    }
}
