//! The staging planner: cut an arbitrary physical plan into a **DAG of distributed stages**.
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
//! # Why the cut recurses
//!
//! A real analytical query has more than one seam: a join feeding a `GROUP BY`, two joins, a sort
//! over an aggregate. So the rewrite is not "find *the* boundary" — it is a **bottom-up rewrite**
//! (a post-order walk, `TreeNode::transform_up`'s order) that cuts every boundary it recognizes,
//! innermost first. At each boundary the subtree below becomes a stage assigned to a worker and is
//! replaced, in its parent, by [`FlightReaderExec`] leaves (unioned when there are several).
//!
//! Cutting bottom-up is what makes the composition work: by the time an outer boundary is
//! considered, the plan beneath it may *already* contain `FlightReaderExec` leaves from an inner
//! cut — and a plan containing a remote read is itself serializable, so it ships to a worker
//! unchanged (see [`crate::remote`]'s `nested_remote_stages_survive`). Stages nest arbitrarily.
//!
//! Two invariants carry the correctness of the whole scheme:
//!
//! 1. **A [`FlightReaderExec`] has the same output schema as the sub-plan it replaces**, so the
//!    parent nodes cannot tell the difference. Break that and everything above a cut is wrong.
//! 2. **A cut preserves rows, and — where a parent could care — partitions.** Each rule below states
//!    which of the two it relies on. A cut that only preserves rows is used only under a parent that
//!    reshuffles anyway (a `Repartition`, a final aggregate); a cut that must preserve partition
//!    *identity* rebuilds partition `i` from partition `i`.
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
//! # The rules, and what each one assumes
//!
//! [`plan_distributed`] tries these at every node, innermost first. The first that matches wins;
//! a node that matches none is left alone and runs where it already was.
//!
//! - **Shuffle seam** — a [`HashJoinExec`] in [`PartitionMode::Partitioned`], or a window
//!   aggregate with a `PARTITION BY`. Every child is hash-partitioned into the same `N` buckets, so
//!   bucket `i` of each child is exactly the set of rows the operator needs to see together. One
//!   reduce stage per bucket, each pulling bucket `i` from every child. Partition-faithful: stage
//!   `i` produces what partition `i` produced.
//! - **Broadcast join** — a [`HashJoinExec`] in [`PartitionMode::CollectLeft`]. The optimizer has
//!   already decided the build side is small enough to collect whole, so there is no shuffle seam to
//!   cut. Instead the small side becomes *one* stage that every join stage pulls (replicated), and
//!   the large side is split without moving it: byte-range slices of its scan, or its existing
//!   remote branches. The large side never crosses the network.
//! - **Aggregate** — a [`AggregateMode::Partial`] aggregate over a single file scan is the classic
//!   map stage: slice the scan into one byte range per worker ([`split_scan`]) so the fleet's total
//!   IO is one scan, not `n`. Row-preserving only, which is sound because a partial aggregate is
//!   combinable by construction and its parent re-groups. An aggregate whose input is *already*
//!   distributed is instead pushed onto each branch's own worker (see below), which is
//!   partition-faithful and so works for `SinglePartitioned` / `FinalPartitioned` modes too.
//! - **Distributed sort** — a [`SortPreservingMergeExec`] over a per-partition [`SortExec`]. Each
//!   worker sorts its own share and the coordinator *merges* the sorted runs. It deliberately does
//!   not re-sort the merged stream: a merge of sorted inputs is the entire reason this is cheaper
//!   than sorting centrally, and re-sorting would paper over a bug in the per-partition sort.
//!
//! ## Pushing an operator onto an already-distributed input
//!
//! Once an inner cut has run, a subtree looks like `Op ← … ← Union(FlightReaderExec …)`. If every
//! operator between `Op` and that union is **partition-wise** — it computes output partition `i`
//! from input partition `i` alone (projection, filter, aggregate, window, per-partition sort) —
//! then rewriting `Op(Union(b₀…bₖ))` into `Union(Op(b₀) … Op(bₖ))` is an
//! identity: same rows, same partitions, same order. So we do exactly that, placing each copy on
//! the worker that already holds its branch. `Repartition`, `CoalescePartitions` and merges are
//! *not* partition-wise and stop the descent — which is precisely why a final aggregate sitting
//! above a hash repartition stays on the coordinator.
//!
//! When a branch turns out to be a whole single-partition stage on the worker we are placing the
//! copy on, we inline it instead of pulling it back through a second Flight hop — `FlightReaderExec(w, 0, X)`
//! evaluated *on `w`* is just `X`.
//!
//! # Scope / limitations (stated plainly)
//!
//! The bargain of this planner is unchanged: **it would rather leave a shape undistributed than
//! distribute it wrongly.** Every rule above is a pattern match with a stated precondition; a plan
//! that matches nothing is returned unchanged and simply runs on the coordinator. A no-op is a
//! valid answer — not every query needs a fleet. What that leaves on the table today:
//!
//! - **Window functions without a `PARTITION BY` run locally.** Such a window needs a single global
//!   ordering over every row; there is no key to hash on and no safe way to split it. DataFusion
//!   plans it as `Window ← SortPreservingMerge ← Sort`, so the *sort* underneath still distributes
//!   and only the window itself stays on the coordinator.
//! - **A sort whose input is separated from its remote branches by a repartition runs locally.**
//!   `GROUP BY … ORDER BY` is the common case: the aggregate half distributes, the sort does not.
//!   Cutting there would need the ordering to be range-partitioned, which nothing in the plan tells
//!   us how to do.
//! - **Final aggregates run on the coordinator.** Only the map half of a plain `GROUP BY` is fanned
//!   out. (A `GROUP BY` *over a join* is different: its aggregate is already hash-partitioned by the
//!   join, so it distributes.)
//! - **Non-file sources are not sliced.** [`split_scan`] slices exactly one file scan, so a subtree
//!   with a `MemTable`, or with several scans and no seam between them, is left local rather than
//!   guessed at. The planner asks [`crate::scan_split::file_scan_count`] up front instead of
//!   calling `split_scan` and interpreting its error.
//! - **Stage placement is round-robin, not cost-based.** There is no statistics-driven scheduler
//!   yet, and a map producer pulled by reduce stages on `k` different workers is materialized on
//!   each of them: `k×` the producer's work (though never `k×` the work *below* it — inner stages
//!   are cached at their own producers). Sizing that properly is the query scheduler's job.
//! - Shuffle output is buffered in worker memory, not spilled.
//!
//! Map producers pulled by several consumers still execute only **once** per worker: the worker
//! materializes each stage into a [`crate::stage_cache::StageCache`] on the first pull and serves
//! the rest from that buffer (the pull-shuffle hazard documented on [`FlightReaderExec`]). That is
//! what makes broadcasting the small side of a join nearly free.
//!
//! # Placement and failover are separate questions
//!
//! Every leaf this planner builds names one **primary** worker — the assignment rules above are
//! unchanged — and carries the rest of the fleet as ordered **failover targets** (see
//! [`failover_targets`]). Placement decides where work goes; failover decides where it goes *again*
//! if that node is lost. Keeping them separate is what lets fault tolerance apply to every rule
//! here without any of them reasoning about it: they all build leaves through [`remote`], so a rule
//! added tomorrow inherits reassignment for free.
//!
//! [`RepartitionExec`]: datafusion::physical_plan::repartition::RepartitionExec

use std::sync::Arc;

use anyhow::{Result, bail};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

use crate::remote::FlightReaderExec;
use crate::scan_split::{file_scan_count, split_scan};

/// Stage plans with the worker each should run on. Placement travels with the plan because it is
/// decided by the same rule that builds it — a stage cut from a remote branch belongs on the worker
/// that already holds that branch, and a scan slice belongs wherever we sent its bytes.
type PlacedStages = Vec<(String, Arc<dyn ExecutionPlan>)>;

/// Rewrite `plan` into a DAG of distributed stages fanned across `workers`.
///
/// Returns a coordinator-side plan whose remote stages are [`FlightReaderExec`] leaves. Execute it
/// with `datafusion::physical_plan::collect` on the coordinator's own [`SessionContext`]: the plan
/// is self-contained (scans embed their file paths), so the workers need no table registration.
///
/// The plan is walked bottom-up and every distribution boundary the planner recognizes is cut, so
/// one plan can yield many stages — a join feeding a `GROUP BY` becomes join stages with aggregate
/// stages layered on top of them. Boundaries it does not recognize are left alone; a plan with no
/// recognizable boundary at all comes back unchanged and runs locally. See the module docs for the
/// rule set and, more importantly, for what it deliberately declines to do.
///
/// # Errors
/// If `workers` is empty, or if rebuilding a stage fails (a malformed plan). A shape this planner
/// cannot distribute is *not* an error — it is left local.
///
/// [`SessionContext`]: datafusion::prelude::SessionContext
pub fn plan_distributed(
    plan: Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    if workers.is_empty() {
        bail!("need at least one worker to distribute a plan across");
    }

    distribute(&plan, workers)
}

/// Post-order rewrite: cut every boundary in the children, then this node's own.
///
/// This is `TreeNode::transform_up`'s traversal, written out by hand for one reason: a rule needs to
/// see the children as the *optimizer* left them, not as we have just rewritten them. Partitioning
/// is the case that forces it — `UnionExec` reports `UnknownPartitioning`, so the moment we replace
/// a `Repartition(Hash)` subtree with a union of remote reads, the `Hash` label the next boundary up
/// needs to recognize its seam is gone. The rows and their grouping are still exactly right; only
/// the label is lost. Keeping the original children alongside the rewritten ones lets a rule check
/// the precondition against the plan the optimizer proved it for, and build the stages out of what
/// the recursion produced.
fn distribute(node: &Arc<dyn ExecutionPlan>, workers: &[String]) -> Result<Arc<dyn ExecutionPlan>> {
    let original: Vec<Arc<dyn ExecutionPlan>> =
        node.children().into_iter().map(Arc::clone).collect();
    let mut rewritten = Vec::with_capacity(original.len());
    let mut changed = false;
    for child in &original {
        let new_child = distribute(child, workers)?;
        changed |= !Arc::ptr_eq(&new_child, child);
        rewritten.push(new_child);
    }
    // Leave the node untouched when nothing below it moved, so a plan with no boundary at all comes
    // back as the very same `Arc` the caller handed in.
    let node = if changed {
        Arc::clone(node).with_new_children(rewritten)?
    } else {
        Arc::clone(node)
    };

    match cut(&node, &original, workers)? {
        Some(stages) => Ok(stages),
        None => Ok(node),
    }
}

/// Try every rule at `node`, returning its replacement or `None` to leave it alone.
///
/// Order matters only where two rules could match the same node, which today is just the two
/// hash-join modes — and those are mutually exclusive. It is written as a chain rather than a match
/// so that adding a rule cannot silently shadow an existing one.
fn cut(
    node: &Arc<dyn ExecutionPlan>,
    original_children: &[Arc<dyn ExecutionPlan>],
    workers: &[String],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(stages) = distribute_shuffle_seam(node, original_children, workers)? {
        return Ok(Some(stages));
    }
    if let Some(stages) = distribute_broadcast_join(node, workers)? {
        return Ok(Some(stages));
    }
    if let Some(stages) = distribute_aggregate(node, workers)? {
        return Ok(Some(stages));
    }
    if let Some(stages) = distribute_sort(node, workers)? {
        return Ok(Some(stages));
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Rule: shuffle seam (partitioned hash join, partitioned window)
// ---------------------------------------------------------------------------

/// Cut an operator whose children the optimizer hash-partitioned into the same `N` buckets.
///
/// DataFusion plans a partitioned hash join as
/// `HashJoin(Partitioned) ← Repartition(Hash[left_keys], N) ; ← Repartition(Hash[right_keys], N)`.
/// Both children expose the same `N` hash partitions, and partition `i` of the left contains
/// exactly the rows whose key hashes to bucket `i` — as does partition `i` of the right. So the
/// join of bucket `i`-left with bucket `i`-right finds every match with `hash(key) % N == i`, and
/// the union over all `i` is the full join. That is the shuffle, read straight off the plan. A
/// window function with a `PARTITION BY` is the same story with one child: the optimizer
/// hash-partitions on the `PARTITION BY` keys precisely so that each partition can be evaluated
/// independently.
///
/// For each bucket `i` we build a **reduce stage**: the original operator with its children replaced
/// by [`FlightReaderExec`] leaves that pull *partition `i`* from each child's map producer (the
/// untouched `Repartition(Hash)` subtree, which a worker runs to expose all `N` partitions). The
/// reduce stage runs on a worker; the coordinator unions the `N` reduce outputs. This nests a
/// `FlightReaderExec` (map) inside a `FlightReaderExec` (reduce) — the composition
/// [`crate::remote::LldbCodec`] was built to serialize.
///
/// All `N` reduce stages pull *the same* `Arc`-shared map producer (differing only in which
/// partition they request), so a naive worker would re-run that producer `N` times. The worker's
/// [`crate::stage_cache::StageCache`] closes that: the producer materializes once — keyed on the
/// stage id derived from its identical plan bytes — and every reduce partition reads from the one
/// buffered result.
///
/// The precondition is checked, not assumed: every child must actually report
/// [`Partitioning::Hash`] with a common bucket count. Anything else (a collect-left join, a window
/// with no `PARTITION BY`, mismatched counts) falls through to the other rules and, failing those,
/// stays local.
fn distribute_shuffle_seam(
    node: &Arc<dyn ExecutionPlan>,
    original_children: &[Arc<dyn ExecutionPlan>],
    workers: &[String],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !is_shuffle_consumer(node) {
        return Ok(None);
    }

    let children: Vec<Arc<dyn ExecutionPlan>> =
        node.children().into_iter().map(Arc::clone).collect();
    if children.len() != original_children.len() {
        return Ok(None);
    }
    // Ask the *pre-rewrite* children about their partitioning: an inner cut may already have
    // replaced one of them with a union, which is partition-faithful but no longer says so.
    let mut buckets: Option<usize> = None;
    for child in original_children {
        match (hash_buckets(child), buckets) {
            (None, _) => return Ok(None),
            (Some(n), None) => buckets = Some(n),
            (Some(n), Some(m)) if n == m => {}
            // Children shuffled into different bucket counts are not a seam we can align.
            (Some(_), Some(_)) => return Ok(None),
        }
    }
    let Some(buckets) = buckets else {
        return Ok(None);
    };

    let mut leaves = Vec::with_capacity(buckets);
    for i in 0..buckets {
        // Round-robin the reduce stages over the available workers. Each reduce worker pulls its
        // bucket `i` from the map producers (here co-located on the same worker, reached via a fresh
        // Flight hop — a real fleet would place them apart).
        let worker = &workers[i % workers.len()];
        let inputs = children
            .iter()
            .map(|child| remote(workers, worker, i as u32, Arc::clone(child)))
            .collect();
        let stage = Arc::clone(node).with_new_children(inputs)?;
        leaves.push(remote(workers, worker, 0, single_partition(stage)));
    }
    Ok(Some(UnionExec::try_new(leaves)?))
}

/// True if `node` consumes a hash-partitioned shuffle — the seam [`distribute_shuffle_seam`] cuts.
///
/// A window aggregate with no `PARTITION BY` is excluded on purpose: it needs one global ordering
/// over every row, so there is no key to split it on. (The check is belt-and-braces — such a window
/// is planned over a single collected partition, which fails the `Partitioning::Hash` test anyway —
/// but the reason deserves to be written down where the decision is made.)
fn is_shuffle_consumer(node: &Arc<dyn ExecutionPlan>) -> bool {
    let any = node.as_any();
    if any.downcast_ref::<HashJoinExec>().is_some() {
        return is_partitioned_hash_join(node);
    }
    if let Some(window) = any.downcast_ref::<BoundedWindowAggExec>() {
        return !window.partition_keys().is_empty();
    }
    if let Some(window) = any.downcast_ref::<WindowAggExec>() {
        return !window.partition_keys().is_empty();
    }
    false
}

// ---------------------------------------------------------------------------
// Rule: broadcast join
// ---------------------------------------------------------------------------

/// Distribute a collect-left hash join by **replicating the small side** instead of shuffling.
///
/// [`PartitionMode::CollectLeft`] is the optimizer telling us the build side is small enough to
/// materialize whole. There is therefore no repartition seam to cut — and cutting one anyway would
/// mean shuffling the *large* side across the fleet to meet a table that fits in memory, which is
/// the trade a broadcast join exists to avoid.
///
/// So: the build side becomes a single stage, shipped once and pulled by every join stage. Every
/// puller sends byte-identical plan bytes, so they all name one stage id and the producing worker
/// materializes it exactly once — [`crate::stage_cache::StageCache`] turns replication into one
/// execution plus `n` cheap streams of a small result. The probe side is split *in place*
/// ([`split_without_shuffling`]): byte-range slices of its own scan, or the remote branches it
/// already has. Its rows are read where they live and joined there; they never cross the network.
///
/// Correctness rests on the join being row-wise in the probe: for an inner or left-outer join,
/// `join(build, probe₀ ∪ probe₁) = join(build, probe₀) ∪ join(build, probe₁)` as long as every
/// stage sees the *whole* build side — which replication guarantees. Splitting the probe by byte
/// range is row-preserving but not partition-faithful, though, and a collect-left join inherits its
/// output partitioning from the probe. So the one case that could bite — a probe that arrived
/// hash-partitioned, leaving a `Hash` label a parent might read as a promise — is checked for and
/// declined rather than reasoned about.
fn distribute_broadcast_join(
    node: &Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(join) = node.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    if *join.partition_mode() != PartitionMode::CollectLeft {
        return Ok(None);
    }

    if hash_buckets(node).is_some() {
        // The probe side arrived hash-partitioned, so this join's output is labelled `Hash` and
        // something above it may be counting on that (an aggregate in `SinglePartitioned` mode, say).
        // Splitting the probe by byte range preserves rows but not buckets, so decline: a broadcast
        // we skip costs a shuffle, a broadcast we get wrong costs the answer.
        return Ok(None);
    }

    let build = Arc::clone(join.left());
    let probe = Arc::clone(join.right());
    let Some(probe_stages) = split_without_shuffling(&probe, workers)? else {
        // We cannot split the probe side without moving it (a MemTable, several scans, no seam), so
        // leave the whole join local rather than turn a broadcast into a hidden shuffle.
        return Ok(None);
    };

    // One `Arc`, shared by every join stage: identical bytes, one stage id, one materialization.
    let build_leaf = remote(workers, &workers[0], 0, single_partition(build));

    let mut leaves = Vec::with_capacity(probe_stages.len());
    for (worker, probe_slice) in probe_stages {
        let stage =
            Arc::clone(node).with_new_children(vec![Arc::clone(&build_leaf), probe_slice])?;
        leaves.push(remote(workers, &worker, 0, single_partition(stage)));
    }
    Ok(Some(UnionExec::try_new(leaves)?))
}

/// Split `subtree` into per-worker pieces **without moving its rows**, or `None` if we cannot.
///
/// Two ways to do that, in preference order: slice its file scan into disjoint byte ranges (the
/// fleet reads the table once between them), or — if it is already distributed — take the remote
/// branches it has. Anything else returns `None`; the caller then leaves its operator local.
fn split_without_shuffling(
    subtree: &Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Option<PlacedStages>> {
    if is_scan_sliceable(subtree) {
        let slices = split_scan(Arc::clone(subtree), workers.len())?;
        return Ok(Some(
            slices
                .into_iter()
                .zip(workers)
                .map(|(slice, worker)| (worker.clone(), slice))
                .collect(),
        ));
    }
    Ok(remote_branch_stages(subtree))
}

// ---------------------------------------------------------------------------
// Rule: aggregates
// ---------------------------------------------------------------------------

/// Distribute a `GROUP BY`, either as a scan-sliced map stage or onto an already-distributed input.
///
/// **Map stage.** DataFusion plans a plain grouped aggregation as
/// `FinalAggregate ← Repartition(Hash) ← PartialAggregate ← scan`. The whole `PartialAggregate ← scan`
/// subtree can run independently on a slice of the input because a partial aggregate is, by
/// construction, combinable. We slice its scan into one disjoint byte-range copy per worker with
/// [`split_scan`] (so the fleet's total IO ≈ a single-node scan, not `n`×), ship each slice to its
/// worker as a [`FlightReaderExec`] leaf, and union the leaves. Everything above — the hash
/// repartition and the final aggregate — is left exactly as the optimizer planned it and runs on the
/// coordinator. It re-groups the unioned partials, which is correct because each remote read has the
/// *same schema* as the partial aggregate it stands in for: the final aggregate cannot tell it is
/// now reading combined partials from a fleet rather than from one local child.
///
/// Slicing is restricted to [`AggregateMode::Partial`] and that restriction is load-bearing. A
/// byte-range slice is not a hash bucket, so a mode that emits *final* rows per partition
/// (`Single`, `SinglePartitioned`) would emit one row per group per slice and the union would
/// double-count. Partial output is combinable; final output is not.
///
/// **Already-distributed input.** When the aggregate sits over a cut boundary — the `GROUP BY` over
/// a join, where DataFusion picks `SinglePartitioned` mode because the join already hash-partitioned
/// the rows by the grouping key — there is no scan left to slice, but there is something better: the
/// input's partitions are the hash buckets. Pushing a copy of the aggregate onto each branch's own
/// worker is an identity rewrite (see the module docs) and it puts the aggregation next to the data
/// that feeds it.
fn distribute_aggregate(
    node: &Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if node.as_any().downcast_ref::<AggregateExec>().is_none() {
        return Ok(None);
    }

    // A byte-range slice is not a hash bucket, so the fan-out below is row-preserving only. That is
    // safe under a partial aggregate — whose parent re-groups — but not under anything that reads
    // this node's partitioning as meaning something, so refuse if the optimizer labelled it `Hash`.
    if is_partial_aggregate(node) && hash_buckets(node).is_none() && is_scan_sliceable(node) {
        let slices = split_scan(Arc::clone(node), workers.len())?;
        let leaves: Vec<Arc<dyn ExecutionPlan>> = slices
            .into_iter()
            .zip(workers)
            .map(|(slice, worker)| remote(workers, worker, 0, single_partition(slice)))
            .collect();
        return Ok(Some(UnionExec::try_new(leaves)?));
    }

    if let Some(stages) = remote_branch_stages(node) {
        let leaves: Vec<Arc<dyn ExecutionPlan>> = stages
            .into_iter()
            .map(|(worker, stage)| remote(workers, &worker, 0, single_partition(stage)))
            .collect();
        return Ok(Some(UnionExec::try_new(leaves)?));
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// Rule: distributed sort
// ---------------------------------------------------------------------------

/// Distribute an `ORDER BY` as per-worker sorts plus one merge.
///
/// DataFusion plans a sort over partitioned input as
/// `SortPreservingMergeExec ← SortExec(preserve_partitioning=true) ← input`: sort each partition,
/// then merge the sorted runs into one ordered stream. That is already the distributed shape — the
/// only question is *where* each half runs. We move the per-partition sort out to the fleet (one
/// stage per byte-range slice, or per existing remote branch) and leave the merge on the
/// coordinator, which now merges `n` remote sorted streams instead of `n` local ones.
///
/// The merge is not re-sorted, and that is the point: merging pre-sorted runs is `O(rows · log n)`
/// against `O(rows · log rows)` for sorting centrally, and it is the entire reason to distribute a
/// sort at all. It also keeps the test honest — a coordinator that re-sorted would produce the right
/// answer even if every worker's sort were broken.
///
/// The rule anchors on the *merge*, not on the sort, and that is deliberate. A
/// `SortExec(preserve_partitioning=true)` promises only "sorted within each partition"; what makes
/// it safe to replace its partitions with a different set of sorted runs is that the node above is a
/// merge, whose result depends on the rows and their order and not on how they were divided. Anchor
/// on the sort alone and a `SortMergeJoin` above it — which needs its input hash-partitioned *and*
/// sorted — would quietly get slices instead of buckets.
fn distribute_sort(
    node: &Arc<dyn ExecutionPlan>,
    workers: &[String],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(merge) = node.as_any().downcast_ref::<SortPreservingMergeExec>() else {
        return Ok(None);
    };
    let sorted_input = Arc::clone(merge.input());
    let Some(sort) = sorted_input.as_any().downcast_ref::<SortExec>() else {
        return Ok(None);
    };
    if !sort.preserve_partitioning() {
        // A global sort already collapsed to one partition: there is nothing to sort in parallel.
        return Ok(None);
    }

    let Some(stages) = split_without_shuffling(&sorted_input, workers)? else {
        return Ok(None);
    };

    // Each stage must hand back exactly one *sorted* partition, so the coordinator's merge sees one
    // sorted run per stage. Where a stage still has several partitions we merge them on the worker —
    // again a merge, never a second sort.
    let leaves: Vec<Arc<dyn ExecutionPlan>> = stages
        .into_iter()
        .map(|(worker, stage)| remote(workers, &worker, 0, merge_sorted(stage, sort)))
        .collect();
    let union = UnionExec::try_new(leaves)?;
    Ok(Some(Arc::clone(node).with_new_children(vec![union])?))
}

/// Collapse `plan` to a single sorted partition by *merging* its runs, never by re-sorting them.
fn merge_sorted(plan: Arc<dyn ExecutionPlan>, sort: &SortExec) -> Arc<dyn ExecutionPlan> {
    if plan.properties().partitioning.partition_count() == 1 {
        return plan;
    }
    Arc::new(SortPreservingMergeExec::new(sort.expr().clone(), plan).with_fetch(sort.fetch()))
}

// ---------------------------------------------------------------------------
// Pushing an operator onto an already-distributed input
// ---------------------------------------------------------------------------

/// Rewrite `subtree` into one stage per remote branch it reads from, with the worker to place each.
///
/// `subtree` must look like `Op ← … ← Union(FlightReaderExec …)` with every operator in the chain
/// [partition-wise](is_partition_wise). Then output partition `i` of `subtree` depends only on
/// branch `i`, so rebuilding the chain over each branch on its own is an identity — same rows, same
/// partitions, same order — and each copy can be placed on the worker that already holds its branch.
/// Returns `None` the moment the shape does not hold; the caller then leaves its operator local.
///
/// One shortcut worth naming: when a branch is `FlightReaderExec(w, 0, X)` and `X` has a single
/// partition, the copy we are about to place *on `w`* would otherwise pull `X` back from `w` over a
/// second Flight hop. Inlining `X` is the same computation with one materialization and one round
/// trip less. It is safe only because the copy is placed on `w` itself, which is how this function
/// chooses placement in the first place.
fn remote_branch_stages(subtree: &Arc<dyn ExecutionPlan>) -> Option<PlacedStages> {
    let mut chain: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let mut node = Arc::clone(subtree);

    let branches: Vec<Arc<dyn ExecutionPlan>> = loop {
        if let Some(union) = node.as_any().downcast_ref::<UnionExec>() {
            break union.inputs().clone();
        }
        if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
            // A one-branch fan-out: `UnionExec::try_new` collapses a single input, so a cut across a
            // one-worker fleet leaves a bare remote read where a union would otherwise be.
            break vec![Arc::clone(&node)];
        }
        if !is_partition_wise(&node) {
            return None;
        }
        let child = {
            let children = node.children();
            if children.len() != 1 {
                return None;
            }
            Arc::clone(children[0])
        };
        chain.push(node);
        node = child;
    };

    let mut stages = Vec::with_capacity(branches.len());
    for branch in branches {
        let reader = branch.as_any().downcast_ref::<FlightReaderExec>()?;
        let worker = reader.worker_url().to_string();
        let inline = reader.remote_partition() == 0
            && reader.inner().properties().partitioning.partition_count() == 1;
        let mut stage = if inline {
            Arc::clone(reader.inner())
        } else {
            Arc::clone(&branch)
        };
        for link in chain.iter().rev() {
            stage = Arc::clone(link).with_new_children(vec![stage]).ok()?;
        }
        stages.push((worker, stage));
    }
    Some(stages)
}

/// True if `node` computes output partition `i` from input partition `i` alone.
///
/// This is the whitelist that makes [`remote_branch_stages`] sound, so it is deliberately a list of
/// operators we have checked rather than a negation of the ones we know are dangerous. A node that
/// is not on it stops the descent, which costs some reach and can never cost correctness. Note what
/// is *absent*: `RepartitionExec`, `CoalescePartitionsExec` and the merges all move rows between
/// partitions, and joins have two children.
fn is_partition_wise(node: &Arc<dyn ExecutionPlan>) -> bool {
    let any = node.as_any();
    any.downcast_ref::<ProjectionExec>().is_some()
        || any.downcast_ref::<FilterExec>().is_some()
        // Every aggregate mode aggregates each input partition independently; whether that is
        // *enough* for a correct answer is the optimizer's business (it inserted the repartition),
        // and repartitions stop this descent anyway.
        || any.downcast_ref::<AggregateExec>().is_some()
        || any.downcast_ref::<BoundedWindowAggExec>().is_some()
        || any.downcast_ref::<WindowAggExec>().is_some()
        || any
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.preserve_partitioning())
}

// ---------------------------------------------------------------------------
// Plan-shape helpers
// ---------------------------------------------------------------------------

/// Wrap `plan` so it runs on `worker`, exposing that worker's `partition` as a local leaf.
///
/// Every rule in this module builds its leaves here, which is deliberately the only place failover
/// is wired: a leaf naming one worker makes that worker a single point of failure for the whole
/// query, and threading the fleet through each rule separately would mean a rule added later
/// silently loses reassignment. `worker`'s position in `workers` sets where the rotation starts —
/// see [`failover_targets`] for why the list is rotated rather than always beginning at worker 0.
fn remote(
    workers: &[String],
    worker: &str,
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let primary = workers.iter().position(|w| w == worker).unwrap_or(0);
    Arc::new(FlightReaderExec::with_fallbacks(
        worker,
        failover_targets(workers, primary),
        partition,
        plan,
    ))
}

/// The rest of the fleet, as ordered failover targets for a stage whose primary is
/// `workers[primary % workers.len()]`.
///
/// Placement is unchanged — the primary is still whatever the assignment rule chose, because
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

/// Funnel `plan` into the single partition a [`FlightReaderExec`] exposes.
///
/// Only used where the consumer wants rows and not order — [`merge_sorted`] is the ordered
/// counterpart, because `CoalescePartitionsExec` interleaves and would destroy a sort.
fn single_partition(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.properties().partitioning.partition_count() == 1 {
        return plan;
    }
    Arc::new(CoalescePartitionsExec::new(plan))
}

/// The number of hash buckets `plan` was shuffled into, if it is hash-partitioned at all.
fn hash_buckets(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    match &plan.properties().partitioning {
        Partitioning::Hash(_, buckets) if *buckets > 0 => Some(*buckets),
        _ => None,
    }
}

/// True if [`split_scan`] can slice `plan`: exactly one file scan, and no remote leaf whose rows
/// would be replicated rather than sliced along with it.
///
/// The second half matters as much as the first. Slicing the one scan in a subtree that *also*
/// reads from a remote stage would hand every slice the full remote input — sound for some
/// operators and quietly wrong for others, so we decline instead of case-splitting.
fn is_scan_sliceable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    file_scan_count(plan) == 1 && count(plan, is_remote_read) == 0
}

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

/// True if `node` is a remote read of another stage.
fn is_remote_read(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.as_any().downcast_ref::<FlightReaderExec>().is_some()
}

/// Count the nodes in `plan` for which `pred` holds.
///
/// A [`FlightReaderExec`] is a leaf, so this never descends into a remote stage — counting what
/// runs *here*, which is the question every caller is asking.
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

    /// Like [`distributing_ctx`] but leaving the collect-left thresholds at their defaults, so a
    /// small build side is planned as a broadcast join.
    fn broadcasting_ctx() -> SessionContext {
        let mut cfg = SessionConfig::new().with_target_partitions(4);
        cfg.options_mut().optimizer.repartition_file_min_size = 1;
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
        count(plan, is_remote_read)
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

    /// Every [`FlightReaderExec`] in the plan *and* in every remote stage it names, flattened —
    /// the whole stage DAG rather than just the coordinator's view of it.
    fn all_readers(plan: &Arc<dyn ExecutionPlan>) -> Vec<FlightReaderExec> {
        let mut out = Vec::new();
        for reader in top_level_readers(plan) {
            out.extend(all_readers(reader.inner()));
            out.push(reader);
        }
        out
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

    #[tokio::test]
    async fn join_feeding_a_group_by_cuts_both_boundaries() {
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
            "SELECT a.g, count(*) FROM a JOIN b ON a.g = b.g GROUP BY a.g",
        )
        .await;
        let n_parts = join_hash_partitions(&plan);
        assert!(n_parts > 1, "test setup must produce a real shuffle");

        let dist = plan_distributed(plan, &workers()).unwrap();
        // Both boundaries are gone from the coordinator's own plan: the join and the aggregate
        // moved into stages, leaving only remote reads under the final projection.
        assert_eq!(count(&dist, is_partitioned_hash_join), 0);
        assert_eq!(
            count(&dist, |n| n
                .as_any()
                .downcast_ref::<AggregateExec>()
                .is_some()),
            0,
            "the aggregate runs on the fleet, not the coordinator"
        );
        assert_eq!(count_readers(&dist), n_parts);
        // Each coordinator-visible leaf is an aggregate stage which *contains* its bucket's join.
        for reader in top_level_readers(&dist) {
            assert_eq!(
                count(reader.inner(), |n| n
                    .as_any()
                    .downcast_ref::<AggregateExec>()
                    .is_some()),
                1,
                "the aggregate was pushed onto the branch's worker"
            );
            assert_eq!(
                count(reader.inner(), is_partitioned_hash_join),
                1,
                "and the join was inlined into that same stage, not pulled back over a hop"
            );
        }
    }

    #[tokio::test]
    async fn two_joins_nest_into_a_stage_dag() {
        let tmp = tempfile::tempdir().unwrap();
        let a = seed(tmp.path(), "a", 2);
        let b = seed(tmp.path(), "b", 2);
        let c = seed(tmp.path(), "c", 2);
        let ctx = distributing_ctx();
        for (name, path) in [("a", &a), ("b", &b), ("c", &c)] {
            ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
                .await
                .unwrap();
        }

        let plan = physical(
            &ctx,
            "SELECT a.v, b.v, c.v FROM a JOIN b ON a.g = b.g JOIN c ON a.g = c.g",
        )
        .await;
        assert_eq!(
            count(&plan, is_partitioned_hash_join),
            2,
            "test setup must produce two join boundaries"
        );

        let dist = plan_distributed(plan, &workers()).unwrap();
        // Neither join is left on the coordinator, and the stage DAG contains both of them —
        // the inner join's stages live inside the outer join's stages.
        assert_eq!(count(&dist, is_partitioned_hash_join), 0);
        let joins_in_stages: usize = all_readers(&dist)
            .iter()
            .map(|r| count(r.inner(), is_partitioned_hash_join))
            .sum();
        assert!(
            joins_in_stages >= 2,
            "both joins must have moved into stages, found {joins_in_stages}"
        );
        assert!(
            all_readers(&dist).len() > count_readers(&dist),
            "the DAG must be deeper than the coordinator's own view of it"
        );
    }

    #[tokio::test]
    async fn broadcast_join_replicates_the_small_side_and_slices_the_large_one() {
        let tmp = tempfile::tempdir().unwrap();
        let big = seed(tmp.path(), "big", 2);
        let small = seed(tmp.path(), "small", 1);
        let ctx = broadcasting_ctx();
        ctx.register_parquet("big", big.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
        ctx.register_parquet(
            "small",
            small.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let plan = physical(
            &ctx,
            "SELECT big.v, small.v FROM small JOIN big ON small.g = big.g",
        )
        .await;
        let collect_left = count(&plan, |n| {
            n.as_any()
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|j| *j.partition_mode() == PartitionMode::CollectLeft)
        });
        assert_eq!(collect_left, 1, "test setup must produce a broadcast join");

        let dist = plan_distributed(plan, &workers()).unwrap();
        assert_eq!(
            count_readers(&dist),
            WORKERS.len(),
            "one join stage per worker, each over its own slice of the large side"
        );
        // Every join stage pulls the *same* small side — one plan, one stage id, one
        // materialization — and reads its slice of the large side directly, with no second hop.
        for reader in top_level_readers(&dist) {
            let inner_readers = top_level_readers(reader.inner());
            assert_eq!(
                inner_readers.len(),
                1,
                "only the small side is pulled; the large side is read in place"
            );
            assert_eq!(inner_readers[0].worker_url(), WORKERS[0]);
        }
    }

    #[tokio::test]
    async fn order_by_becomes_worker_sorts_under_the_coordinator_merge() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT g, v FROM t ORDER BY v").await;
        assert!(
            plan.as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_some(),
            "test setup must plan a merge over per-partition sorts"
        );

        let dist = plan_distributed(plan, &workers()).unwrap();
        // The merge stays on the coordinator; the sorts moved out to one stage per worker.
        assert!(
            dist.as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_some(),
            "the coordinator still merges — it must not re-sort"
        );
        assert_eq!(count_readers(&dist), WORKERS.len());
        assert_eq!(
            count(&dist, |n| n.as_any().downcast_ref::<SortExec>().is_some()),
            0,
            "no sort is left on the coordinator"
        );
        for reader in top_level_readers(&dist) {
            assert_eq!(
                count(reader.inner(), |n| n
                    .as_any()
                    .downcast_ref::<SortExec>()
                    .is_some()),
                1,
                "each worker sorts its own slice"
            );
            // The remote read advertises the sortedness the merge relies on.
            assert!(
                reader.properties().output_ordering().is_some(),
                "a sorted stage's remote read must report its ordering"
            );
        }
    }

    #[tokio::test]
    async fn window_with_partition_by_is_cut_at_its_hash_seam() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(
            &ctx,
            "SELECT g, v, sum(v) OVER (PARTITION BY g ORDER BY v) FROM t",
        )
        .await;
        let windows = count(&plan, |n| {
            n.as_any().downcast_ref::<BoundedWindowAggExec>().is_some()
        });
        assert_eq!(windows, 1, "test setup must plan a window aggregate");

        let dist = plan_distributed(plan, &workers()).unwrap();
        assert_eq!(
            count(&dist, |n| n
                .as_any()
                .downcast_ref::<BoundedWindowAggExec>()
                .is_some()),
            0,
            "the window moved onto the fleet"
        );
        assert!(count_readers(&dist) > 1, "one stage per hash bucket");
    }

    /// A window with no `PARTITION BY` has no key to split on, so the window itself must stay put.
    /// The sort beneath it still distributes — which is the whole point of cutting recursively.
    #[tokio::test]
    async fn window_without_partition_by_stays_local() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT g, v, sum(v) OVER (ORDER BY v) FROM t").await;
        let dist = plan_distributed(plan, &workers()).unwrap();
        assert_eq!(
            count(&dist, |n| n
                .as_any()
                .downcast_ref::<BoundedWindowAggExec>()
                .is_some()),
            1,
            "an unpartitioned window needs a global ordering and must run in one place"
        );
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

    /// Two independent boundaries in one plan. This used to be the planner's documented refusal
    /// case — a single-boundary cut could only have distributed one of the two aggregates and would
    /// have run the other single-node inside a map stage. Recursion removes the dilemma: each
    /// `GROUP BY` is cut on its own, and both halves of the union are distributed.
    #[tokio::test]
    async fn union_all_of_two_aggregates_distributes_both() {
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

        let dist = plan_distributed(plan, &workers()).unwrap();
        assert_eq!(
            count(&dist, is_partial_aggregate),
            0,
            "both map stages moved to the fleet"
        );
        assert_eq!(
            count_readers(&dist),
            2 * WORKERS.len(),
            "one map leaf per worker, per aggregate"
        );
    }

    /// A `MemTable` aggregate has a boundary but nothing sliceable underneath it: no file scan to
    /// cut into byte ranges and no already-distributed input to push onto. The planner leaves it
    /// alone and the query runs on the coordinator, which is where the rows live anyway. (It used to
    /// raise an error here; a local fallback is strictly better once one plan can hold several
    /// boundaries, because one unsliceable aggregate must not veto the boundaries around it.)
    #[tokio::test]
    async fn aggregate_over_a_non_file_source_falls_back_to_local() {
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
        let dist = plan_distributed(Arc::clone(&plan), &workers()).unwrap();

        assert_eq!(count_readers(&dist), 0, "nothing is shipped remotely");
        assert!(
            Arc::ptr_eq(&plan, &dist),
            "the plan is handed back untouched, to run locally"
        );
    }

    /// `GROUP BY … ORDER BY`: the aggregate distributes, the sort does not. A hash repartition sits
    /// between the sort and the aggregate's remote branches, and a repartition is not partition-wise
    /// — so pushing the sort down would be reordering rows we do not control. Half a distributed
    /// plan is the right answer here.
    #[tokio::test]
    async fn sort_over_an_aggregate_falls_back_but_the_aggregate_still_distributes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = seed(tmp.path(), "t", 2);
        let ctx = distributing_ctx();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let plan = physical(&ctx, "SELECT g, count(*) AS c FROM t GROUP BY g ORDER BY g").await;
        let dist = plan_distributed(plan, &workers()).unwrap();

        assert_eq!(
            count(&dist, |n| n.as_any().downcast_ref::<SortExec>().is_some()),
            1,
            "the sort stays on the coordinator"
        );
        assert_eq!(
            count(&dist, is_partial_aggregate),
            0,
            "the aggregate's map stage still went to the fleet"
        );
        assert_eq!(count_readers(&dist), WORKERS.len());
    }

    #[test]
    fn partition_wise_whitelist_excludes_row_moving_operators() {
        // Guarding the list itself: the operators that move rows between partitions must never be
        // treated as partition-wise, or `remote_branch_stages` would rewrite a shuffle away.
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let empty: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::clone(&schema)),
        );
        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&empty)));
        assert!(!is_partition_wise(&coalesce));
        assert!(!is_partition_wise(&empty));

        let repartition: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::repartition::RepartitionExec::try_new(
                empty,
                Partitioning::RoundRobinBatch(2),
            )
            .unwrap(),
        );
        assert!(!is_partition_wise(&repartition));
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
}
