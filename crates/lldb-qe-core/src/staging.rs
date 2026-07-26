//! The staging planner: turn an arbitrary physical plan into distributed map/reduce stages.
//!
//! [`crate::distributed`] proved distribution worked, but only for one hand-written aggregation:
//! it built the shuffle by hand, in Rust, for a single grouped `COUNT(*)`. That does not scale to
//! "run *arbitrary* SQL across a fleet". This module is the piece that generalizes it.
//!
//! The idea is the one every distributed engine (Spark, Presto, Ballista) is built on: a physical
//! plan already contains the distribution boundaries — the optimizer inserts a [`RepartitionExec`]
//! (or a partition-collapsing node) exactly where data must be *reshuffled* so that rows sharing a
//! key meet on one node. A `GROUP BY` becomes `FinalAggregate ← Repartition(Hash) ← PartialAggregate`;
//! a hash join becomes `HashJoin(Partitioned) ← Repartition(Hash[left]) ; ← Repartition(Hash[right])`.
//! Those repartition edges are the seams. Cut the plan there, run each side as its own **stage**,
//! and wrap every cross-stage edge in a [`FlightReaderExec`] so the consuming stage pulls the
//! producing stage's output over Flight. What was implicit parallelism inside one process becomes
//! explicit parallelism across a fleet — without any query-specific code.
//!
//! [`plan_distributed`] returns a **coordinator-side** plan: the original plan with its map
//! stage(s) replaced by [`FlightReaderExec`] leaves. The coordinator executes it locally with
//! `collect`; the leaves make the remote calls. Everything *above* the cut is unchanged and runs
//! on the coordinator — which is correct precisely because a `FlightReaderExec` has the same output
//! schema as the sub-plan it replaced, so the parent nodes cannot tell the difference.
//!
//! ```text
//!   coordinator                          workers
//!   ───────────                          ───────
//!   FinalAggregate                       ┌ Partial(slice 0)   (map worker 0)
//!     └─ Repartition(Hash)               ├ Partial(slice 1)   (map worker 1)
//!          └─ UnionExec                   ⋮
//!               ├─ FlightReaderExec(w0) ─┘
//!               └─ FlightReaderExec(w1) ─┘
//! ```
//!
//! # Scope / limitations (POC, stated plainly)
//!
//! - Exactly **one** distribution boundary is handled per plan: a single distributable aggregate,
//!   or a single partitioned hash join. A plan with several (e.g. a `GROUP BY` *over* a join, or
//!   two joins) is rejected with a clear error rather than silently mis-distributed — correctness
//!   over reach. The aggregate map stage is sliced with [`split_scan`], which itself requires a
//!   single scan, so a `GROUP BY` over a join naturally surfaces as an error there too.
//! - If no boundary is found the plan is returned **unchanged**: the query simply runs locally.
//!   A no-op is a valid answer — not every query needs a fleet.
//! - Map producers are pulled by several reduce consumers, but each producer stage executes only
//!   **once**: the worker materializes it into a [`crate::stage_cache::StageCache`] on the first
//!   pull and serves the rest from that buffer (the pull-shuffle hazard documented on
//!   [`FlightReaderExec`], now closed). Shuffle output is buffered in worker memory, not spilled.
//!
//! # Placement and failover are separate questions
//!
//! Every leaf this planner builds names one **primary** worker — the assignment rules below are
//! unchanged — and carries the rest of the fleet as ordered **failover targets** (see
//! [`failover_targets`]). Placement decides where work goes; failover decides where it goes *again*
//! if that node is lost. Keeping them separate is what lets fault tolerance land without touching
//! fan-out behavior.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::union::UnionExec;

use crate::remote::FlightReaderExec;
use crate::scan_split::split_scan;

/// Rewrite `plan` into distributed map/reduce stages fanned across `workers`.
///
/// Returns a coordinator-side plan whose remote stages are [`FlightReaderExec`] leaves. Execute it
/// with `datafusion::physical_plan::collect` on the coordinator's own [`SessionContext`]: the plan
/// is self-contained (scans embed their file paths), so the workers need no table registration.
///
/// The rewrite is chosen by the first distribution boundary found:
///
/// - a `GROUP BY` (an [`AggregateExec`] with [`AggregateMode::Partial`]) → [`distribute_aggregate`];
/// - a partitioned hash join (a [`HashJoinExec`] with [`PartitionMode::Partitioned`]) →
///   [`distribute_hash_join`].
///
/// If neither is present the plan is returned unchanged (it runs locally). If more than one
/// boundary is present the plan is rejected — see the module docs for why.
///
/// [`SessionContext`]: datafusion::prelude::SessionContext
pub fn plan_distributed(
    plan: Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    if workers.is_empty() {
        bail!("need at least one worker to distribute a plan across");
    }

    let partial_aggregates = count(&plan, is_partial_aggregate);
    let partitioned_joins = count(&plan, is_partitioned_hash_join);

    match (partial_aggregates, partitioned_joins) {
        // No distribution boundary: run the whole thing locally. A no-op is a valid answer.
        (0, 0) => Ok(plan),
        (1, 0) => distribute_aggregate(plan, workers),
        (0, 1) => distribute_hash_join(plan, workers),
        // More than one boundary — or one of each — is out of scope for this POC. Erroring beats
        // distributing only one seam and silently running the rest single-node inside a map stage.
        (a, j) => bail!(
            "staging planner handles a single distribution boundary per plan, but found \
             {a} distributable aggregate(s) and {j} partitioned join(s); nesting map/reduce \
             stages across multiple boundaries is not supported yet"
        ),
    }
}

// ---------------------------------------------------------------------------
// Aggregate boundary
// ---------------------------------------------------------------------------

/// Distribute a `GROUP BY` by cutting at its partial aggregate.
///
/// DataFusion plans a grouped aggregation as
/// `FinalAggregate ← Repartition(Hash) ← PartialAggregate ← scan`. The **map stage** is the whole
/// `PartialAggregate ← scan` subtree: it can run independently on a slice of the input because a
/// partial aggregate is, by construction, combinable. We slice its scan into one disjoint
/// byte-range copy per worker with [`split_scan`] (so the fleet's total IO ≈ a single-node scan,
/// not `n`×), ship each slice to its worker as a [`FlightReaderExec`] leaf, and union the leaves.
///
/// Everything above the partial aggregate — the hash repartition and the final aggregate — is left
/// exactly as the optimizer planned it and runs on the coordinator. It re-groups the unioned
/// partials, which is correct because each remote read has the *same schema* as the partial
/// aggregate it stands in for: the final aggregate cannot tell it is now reading combined partials
/// from a fleet rather than from one local child.
fn distribute_aggregate(
    plan: Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let rewritten = plan
        .transform_down(|node| {
            if !is_partial_aggregate(&node) {
                return Ok(Transformed::no(node));
            }

            // Slice the partial-aggregate subtree's scan into one byte-range slice per worker.
            let slices = split_scan(Arc::clone(&node), workers.len()).map_err(to_df)?;

            // Each slice becomes a map stage on its worker. `CoalescePartitionsExec` funnels the
            // slice's (possibly multiple) partitions into the single partition a `FlightReaderExec`
            // exposes.
            let leaves: Vec<Arc<dyn ExecutionPlan>> = slices
                .into_iter()
                .enumerate()
                .map(|(i, slice)| {
                    let coalesced = Arc::new(CoalescePartitionsExec::new(slice));
                    Arc::new(FlightReaderExec::with_fallbacks(
                        workers[i % workers.len()].clone(),
                        failover_targets(workers, i),
                        0,
                        coalesced,
                    )) as Arc<dyn ExecutionPlan>
                })
                .collect();

            let union = UnionExec::try_new(leaves)?;
            // Don't descend into the freshly built map leaves — there is nothing left to rewrite
            // there, and it keeps the single-boundary contract obvious.
            Ok(Transformed::new(union, true, TreeNodeRecursion::Jump))
        })
        .map_err(|e| anyhow!("rewriting aggregate boundary: {e}"))?;
    Ok(rewritten.data)
}

// ---------------------------------------------------------------------------
// Hash-join boundary
// ---------------------------------------------------------------------------

/// Distribute a partitioned hash join by shuffling both sides on the join key.
///
/// DataFusion plans a partitioned hash join as
/// `HashJoin(Partitioned) ← Repartition(Hash[left_keys], N) ; ← Repartition(Hash[right_keys], N)`.
/// Both children expose the same `N` hash partitions, and partition `i` of the left contains
/// exactly the rows whose key hashes to bucket `i` — as does partition `i` of the right. So the
/// join of bucket `i`-left with bucket `i`-right finds every match with `hash(key) % N == i`, and
/// the union over all `i` is the full join. That is the shuffle, read straight off the plan.
///
/// For each partition `i` we build a **reduce stage**: the original `HashJoinExec` with its two
/// children replaced by [`FlightReaderExec`] leaves that pull *partition `i`* from each side's map
/// producer (the untouched `Repartition(Hash)` subtree, which a worker runs to expose all `N`
/// partitions). The reduce stage runs on a worker; the coordinator unions the `N` reduce outputs.
/// This nests a `FlightReaderExec` (map) inside a `FlightReaderExec` (reduce) — the composition
/// [`crate::remote::LldbCodec`] was built to serialize.
///
/// All `N` reduce stages pull *the same* `Arc`-shared map producer (differing only in which
/// partition they request), so a naive worker would re-run that producer `N` times. The worker's
/// [`crate::stage_cache::StageCache`] closes that: the producer materializes once — keyed on the
/// stage id derived from its identical plan bytes — and every reduce partition reads from the one
/// buffered result.
///
/// Note on partitioning: the planner only reaches here when the optimizer chose
/// [`PartitionMode::Partitioned`]. A `CollectLeft` join (small build side) has no repartition seam
/// and is left local by [`plan_distributed`]'s boundary count — raise `target_partitions` and lower
/// the `hash_join_single_partition_threshold*` options to steer the optimizer onto the partitioned
/// path when you want to exercise this.
fn distribute_hash_join(
    plan: Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let rewritten = plan
        .transform_down(|node| {
            if !is_partitioned_hash_join(&node) {
                return Ok(Transformed::no(node));
            }

            let children = node.children();
            // A hash join always has exactly two children (build/probe).
            let left_map = Arc::clone(children[0]);
            let right_map = Arc::clone(children[1]);

            // The number of hash buckets both sides were repartitioned into. Both children carry
            // the same count (that is what makes the join partitioned); use the left's.
            let n_parts = left_map.properties().partitioning.partition_count();
            if n_parts == 0 {
                return Err(to_df(anyhow!(
                    "partitioned hash join child reports zero partitions; nothing to shuffle"
                )));
            }

            let mut reduce_leaves: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(n_parts);
            for i in 0..n_parts {
                // Round-robin the reduce stages over the available workers. Each reduce worker
                // pulls its bucket `i` from the map producers (here co-located on the same worker,
                // reached via a fresh Flight hop — a real fleet would place them apart).
                let worker = &workers[i % workers.len()];
                let fallbacks = failover_targets(workers, i);

                let left_leaf: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::with_fallbacks(
                    worker.clone(),
                    fallbacks.clone(),
                    i as u32,
                    Arc::clone(&left_map),
                ));
                let right_leaf: Arc<dyn ExecutionPlan> =
                    Arc::new(FlightReaderExec::with_fallbacks(
                        worker.clone(),
                        fallbacks.clone(),
                        i as u32,
                        Arc::clone(&right_map),
                    ));

                // Rebuild the join over the two single-partition remote reads. It now joins just
                // bucket `i` of each side and yields a single output partition.
                let reduce_join =
                    Arc::clone(&node).with_new_children(vec![left_leaf, right_leaf])?;
                let coalesced = Arc::new(CoalescePartitionsExec::new(reduce_join));
                reduce_leaves.push(Arc::new(FlightReaderExec::with_fallbacks(
                    worker.clone(),
                    fallbacks,
                    0,
                    coalesced,
                )));
            }

            let union = UnionExec::try_new(reduce_leaves)?;
            Ok(Transformed::new(union, true, TreeNodeRecursion::Jump))
        })
        .map_err(|e| anyhow!("rewriting hash-join boundary: {e}"))?;
    Ok(rewritten.data)
}

// ---------------------------------------------------------------------------
// Failover placement
// ---------------------------------------------------------------------------

/// The rest of the fleet, as ordered failover targets for a stage whose primary is
/// `workers[primary % workers.len()]`.
///
/// Placement is unchanged — the primary is still whatever the assignment rule above chose, because
/// changing it would change fan-out, which is a different question from fault tolerance. This only
/// answers "if that worker is gone, who else?", and every worker is a valid answer: the stage is
/// content-addressed and re-materializes identically wherever it lands
/// ([`crate::stage_cache::StageCache`]).
///
/// The list is **rotated to start just after the primary** rather than always at worker 0. Losing a
/// node fails several stages at once, and if every one of them named worker 0 as its first backup,
/// recovery would stampede the whole query onto a single machine — trading a failure for an
/// overload. Rotating spreads the reassignments the way the primary assignment is already spread.
///
/// A one-worker fleet yields an empty list: no reassignment is possible, and today's behavior is
/// preserved exactly.
fn failover_targets(workers: &[String], primary: usize) -> Vec<String> {
    (1..workers.len())
        .map(|offset| workers[(primary + offset) % workers.len()].clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Boundary detection
// ---------------------------------------------------------------------------

/// True if `node` is the partial aggregate that heads a distributable `GROUP BY` map stage.
fn is_partial_aggregate(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.as_any()
        .downcast_ref::<AggregateExec>()
        .is_some_and(|agg| *agg.mode() == AggregateMode::Partial)
}

/// True if `node` is a hash join the optimizer chose to run in partitioned (shuffle) mode.
fn is_partitioned_hash_join(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.as_any()
        .downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::Partitioned)
}

/// Count the nodes in `plan` for which `pred` holds.
fn count(plan: &Arc<dyn ExecutionPlan>, pred: fn(&Arc<dyn ExecutionPlan>) -> bool) -> usize {
    let mut n = 0;
    // `apply` is infallible here (the closure never errors), so the unwrap cannot fire.
    plan.apply(|node| {
        if pred(node) {
            n += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("counting walk does not error");
    n
}

/// Adapt an `anyhow` error into a `DataFusionError` so it can flow out of a tree-node closure.
fn to_df(e: anyhow::Error) -> DataFusionError {
    DataFusionError::External(e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    const WORKERS: [&str; 2] = ["http://w0:50051", "http://w1:50051"];

    fn workers() -> Vec<String> {
        WORKERS.iter().map(|s| s.to_string()).collect()
    }

    /// Seed `files` small parquet files (each with several row groups) under a fresh directory,
    /// so a listing table over the directory yields multiple scan partitions — the precondition
    /// for the optimizer to insert real hash repartition seams.
    fn seed(dir: &std::path::Path, name: &str, files: usize) -> std::path::PathBuf {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let tdir = dir.join(name);
        std::fs::create_dir_all(&tdir).unwrap();
        for f in 0..files {
            let g: Vec<String> = (0..500)
                .map(|i| format!("k{}", (i + f as i64) % 7))
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(g)),
                    Arc::new(Int64Array::from((0..500).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(128))
                .build();
            let file = std::fs::File::create(tdir.join(format!("part{f}.parquet"))).unwrap();
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        tdir
    }

    /// A session tuned to actually produce distribution boundaries on tiny test data: multiple
    /// target partitions, file scans split down to the byte, and hash joins forced onto the
    /// partitioned path instead of collect-left.
    fn distributing_ctx() -> SessionContext {
        let mut cfg = SessionConfig::new().with_target_partitions(4);
        let opts = cfg.options_mut();
        opts.optimizer.repartition_file_min_size = 1;
        opts.optimizer.hash_join_single_partition_threshold = 0;
        opts.optimizer.hash_join_single_partition_threshold_rows = 0;
        SessionContext::new_with_config(cfg)
    }

    async fn physical(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// Count [`FlightReaderExec`] leaves anywhere in a (coordinator) plan tree.
    fn count_readers(plan: &Arc<dyn ExecutionPlan>) -> usize {
        count(plan, |n| {
            n.as_any().downcast_ref::<FlightReaderExec>().is_some()
        })
    }

    fn contains_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
        count(plan, |n| n.as_any().downcast_ref::<UnionExec>().is_some()) > 0
    }

    /// The number of hash buckets a partitioned hash join shuffled its sides into.
    fn join_hash_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut n = 0;
        plan.apply(|node| {
            if let Some(join) = node.as_any().downcast_ref::<HashJoinExec>() {
                n = join.left().properties().partitioning.partition_count();
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        n
    }

    /// The [`FlightReaderExec`] leaves visible at the top level of a coordinator plan (a tree walk
    /// does not descend into their remote sub-plans, since those are not children).
    fn top_level_readers(plan: &Arc<dyn ExecutionPlan>) -> Vec<FlightReaderExec> {
        let mut readers = Vec::new();
        plan.apply(|node| {
            if let Some(r) = node.as_any().downcast_ref::<FlightReaderExec>() {
                readers.push(r.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        readers
    }

    #[tokio::test]
    async fn group_by_is_rewritten_to_remote_map_leaves() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 1);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT g, count(*) FROM t GROUP BY g").await;
        // Before: no remote reads at all.
        assert_eq!(count_readers(&plan), 0);

        let dist = plan_distributed(plan, &workers()).unwrap();
        // After: one map leaf per worker, unioned under the untouched final aggregate.
        assert_eq!(count_readers(&dist), WORKERS.len());
        assert!(contains_union(&dist));
        // The partial aggregate is gone from the coordinator plan — it now runs on the workers.
        assert_eq!(count(&dist, is_partial_aggregate), 0);
    }

    #[tokio::test]
    async fn hash_join_is_rewritten_with_both_sides_shuffled() {
        let tmp = tempfile::tempdir().unwrap();
        let a = seed(tmp.path(), "a", 2);
        let b = seed(tmp.path(), "b", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("a", a.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
        ctx.register_parquet("b", b.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT a.v, b.v FROM a JOIN b ON a.g = b.g").await;
        assert_eq!(
            count(&plan, is_partitioned_hash_join),
            1,
            "test setup must produce a partitioned hash join"
        );
        // N = the number of hash buckets the optimizer shuffled both sides into.
        let n_parts = join_hash_partitions(&plan);
        assert!(
            n_parts > 1,
            "expected a real multi-way shuffle, got {n_parts}"
        );

        let dist = plan_distributed(plan, &workers()).unwrap();
        // The coordinator sees one reduce leaf per shuffle bucket, unioned. (The two map leaves per
        // bucket live *inside* each reduce leaf's remote sub-plan, so a tree walk of the
        // coordinator plan does not descend into them.)
        assert!(contains_union(&dist));
        assert_eq!(
            count_readers(&dist),
            n_parts,
            "one coordinator-visible reduce leaf per shuffle bucket"
        );
        // The join itself no longer runs on the coordinator; it moved into the reduce stages.
        assert_eq!(count(&dist, is_partitioned_hash_join), 0);
        // Each reduce leaf shuffles both sides: its remote plan nests exactly two map leaves.
        for reader in top_level_readers(&dist) {
            assert_eq!(
                count_readers(reader.inner()),
                2,
                "each reduce stage pulls a left and a right map partition"
            );
        }
    }

    #[test]
    fn failover_targets_are_the_rest_of_the_fleet_rotated_after_the_primary() {
        let fleet: Vec<String> = ["w0", "w1", "w2", "w3"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // Each stage prefers a *different* first backup, so losing a node does not stampede every
        // orphaned stage onto the same machine.
        assert_eq!(failover_targets(&fleet, 0), vec!["w1", "w2", "w3"]);
        assert_eq!(failover_targets(&fleet, 1), vec!["w2", "w3", "w0"]);
        assert_eq!(failover_targets(&fleet, 3), vec!["w0", "w1", "w2"]);
        // The primary never appears in its own failover list.
        for primary in 0..fleet.len() {
            assert!(!failover_targets(&fleet, primary).contains(&fleet[primary]));
        }
    }

    #[test]
    fn a_single_worker_fleet_has_no_failover_targets() {
        // One worker means today's behavior exactly: one target, one chance.
        let fleet = vec!["w0".to_string()];
        assert!(failover_targets(&fleet, 0).is_empty());
    }

    #[tokio::test]
    async fn map_leaves_carry_the_rest_of_the_fleet_as_fallbacks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 1);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT g, count(*) FROM t GROUP BY g").await;
        let dist = plan_distributed(plan, &workers()).unwrap();

        for reader in top_level_readers(&dist) {
            // Two workers: each leaf keeps its primary and gains exactly the other one.
            assert_eq!(reader.fallbacks().len(), WORKERS.len() - 1);
            assert!(
                !reader
                    .fallbacks()
                    .contains(&reader.worker_url().to_string()),
                "a leaf must not list its own primary as a fallback"
            );
        }
    }

    #[tokio::test]
    async fn plan_without_a_boundary_is_returned_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 1);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        // A bare projection/scan has no repartition seam to cut.
        let plan = physical(&ctx, "SELECT g, v FROM t WHERE v > 10").await;
        let dist = plan_distributed(Arc::clone(&plan), &workers()).unwrap();

        assert_eq!(
            count_readers(&dist),
            0,
            "nothing should be shipped remotely"
        );
        assert!(
            Arc::ptr_eq(&plan, &dist),
            "an untouched plan is returned as-is"
        );
    }

    #[tokio::test]
    async fn empty_worker_list_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 1);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
        let plan = physical(&ctx, "SELECT g, count(*) FROM t GROUP BY g").await;

        let err = plan_distributed(plan, &[]).expect_err("no workers is invalid");
        assert!(
            err.to_string().contains("at least one worker"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn multiple_boundaries_error_rather_than_mis_distribute() {
        // A UNION ALL of two GROUP BYs has two independent partial-aggregate boundaries — more than
        // this POC's single-boundary planner will cut, so it must refuse rather than distribute one
        // and silently run the other single-node.
        let tmp = tempfile::tempdir().unwrap();
        let a = seed(tmp.path(), "a", 2);
        let b = seed(tmp.path(), "b", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("a", a.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
        ctx.register_parquet("b", b.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(
            &ctx,
            "SELECT g, count(*) FROM a GROUP BY g \
             UNION ALL SELECT g, count(*) FROM b GROUP BY g",
        )
        .await;
        assert_eq!(
            count(&plan, is_partial_aggregate),
            2,
            "test setup must produce two aggregate boundaries"
        );
        let err = plan_distributed(plan, &workers())
            .expect_err("multiple boundaries are out of scope and must error");
        assert!(
            err.to_string().contains("single distribution boundary"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn aggregate_over_a_non_file_source_errors_cleanly() {
        // A MemTable-backed GROUP BY has a partial-aggregate boundary but no file scan to slice,
        // so the map-stage slicing must surface a clear error, not a panic.
        let ctx = distributing_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("mem", Arc::new(table)).unwrap();

        let plan = physical(&ctx, "SELECT g, count(*) FROM mem GROUP BY g").await;
        let err = plan_distributed(plan, &workers())
            .expect_err("a MemTable aggregate cannot be scan-sliced");
        assert!(err.to_string().contains("no file-scan leaf"), "got: {err}");
    }
}
