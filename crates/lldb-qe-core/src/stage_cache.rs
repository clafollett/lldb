//! Materialize a shuffle producer **once** and serve its output to many consumers.
//!
//! # The problem this fixes
//!
//! The pull shuffle ([`crate::flight`], [`crate::remote::FlightReaderExec`]) has every consumer
//! open its own `do_get` against a producer, handing over the producer's serialized plan. The
//! naive worker deserializes that plan and calls `plan.execute(partition, ..)` *per request*. So
//! a partitioned hash join that fans a single map producer into `R` reduce stages — the
//! [`crate::staging::distribute_hash_join`] shape, where all `R` reducers pull from one
//! `Arc`-shared `left_map` / `right_map` wrapped in `FlightReaderExec`s that differ only in their
//! `remote_partition` — makes that producer re-run its scan + partial aggregate `R` times. With
//! `M` such producers the fleet pays an `M×R` blowup for what is logically one scan.
//!
//! # The fix: a per-stage materialization cache
//!
//! A **stage** is a producer sub-plan. All consumers that want different output partitions of the
//! *same* producer send byte-identical plan bytes (only the partition selector differs), so a
//! stable hash of those bytes — the `stage_id` carried in the Flight ticket — names the stage
//! without any coordinator-side bookkeeping. This cache keys on that `stage_id`, executes the
//! producer's plan exactly once, buffers **all** of its output partitions in memory, and then
//! answers every consumer — for any partition — straight from that buffer. The producer runs
//! once; the reducers each get complete, correct output.
//!
//! ## Single-flight
//!
//! `R` reducers can hit a brand-new stage at the same instant. A [`tokio::sync::OnceCell`] per
//! stage collapses that race: the first caller runs the materialization while the rest await the
//! same cell, and all `R` observe the one result. The map from `stage_id` to cell is guarded by a
//! plain `std::sync::Mutex` held only long enough to look up / insert the cell — the expensive
//! `collect_partitioned` runs *outside* the lock, inside `get_or_try_init`, so a slow producer
//! never blocks lookups for other stages.
//!
//! ## Why materialize every partition together
//!
//! We drive [`collect_partitioned`], not a loop of `collect(execute(i))`. A producer whose root is
//! a `RepartitionExec` reads its input **once** and fans rows into one channel per output
//! partition; draining those channels one at a time can deadlock (partition 0's consumer blocks
//! waiting for rows that sit behind partition 1's unread channel). `collect_partitioned` drives
//! every partition concurrently, which is the only safe way to buffer a repartition's full output.
//!
//! ## Eviction
//!
//! The cache is bounded to [`StageCache::capacity`] stages and evicts least-recently-used entries
//! when it would overflow. Correctness never depends on retention: an evicted stage simply
//! re-materializes (and re-increments the execution counter) the next time it is pulled. The bound
//! keeps a long-lived worker from accumulating every shuffle it has ever served in memory. This is
//! deliberately the simplest policy that satisfies option (1) of the issue — no size accounting, no
//! TTL; a reviewer weighing memory pressure should see [`StageCache::capacity`] as the single knob.

use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::OnceCell;

/// A producer stage's output, buffered in full: the schema plus one batch vector per output
/// partition. Kept behind an `Arc` so every consumer shares the one materialization rather than
/// cloning the batches.
pub struct MaterializedStage {
    /// The producer plan's output schema. Held explicitly so a partition with zero batches still
    /// encodes a valid (schema-only) Flight stream.
    pub schema: SchemaRef,
    /// `partitions[i]` is the complete set of batches for output partition `i`.
    pub partitions: Vec<Vec<RecordBatch>>,
}

impl MaterializedStage {
    /// Number of output partitions this stage produced.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// The stable 64-bit stage identity of a serialized producer plan.
///
/// A plain, fixed-key [`DefaultHasher`] — deterministic, no random seed — so every consumer that
/// ships byte-identical plan bytes derives the same id and shares one cache entry, and every worker
/// in an identical build agrees on it. It is content-addressing, not a security hash: collisions
/// only ever mean two *different* producers would share a cache slot, which the ticket format makes
/// astronomically unlikely for real plans and which this POC accepts.
pub fn stage_id_of(plan_bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(plan_bytes);
    hasher.finish()
}

/// Per-stage single-flight cell: `None` until the first caller materializes the stage.
type StageCell = Arc<OnceCell<Arc<MaterializedStage>>>;

/// Guarded lookup structure. Split out so the `Mutex` wraps exactly the map + LRU order and
/// nothing async happens while it is held.
struct Inner {
    /// `stage_id` → its single-flight cell.
    cells: std::collections::HashMap<u64, StageCell>,
    /// LRU order, least-recently-used at the front. Eviction pops the front.
    lru: VecDeque<u64>,
}

/// A worker-lifetime cache that materializes each producer stage once and serves it to many
/// `do_get` consumers. See the module docs for the design.
///
/// Lives for the worker's lifetime (held as `Arc<StageCache>` by the Flight service) so its
/// entries survive across independent consumer requests — that is the whole point. Tests construct
/// one, hand a clone to the worker, and read [`execution_count`](Self::execution_count) to assert a
/// producer ran exactly once.
pub struct StageCache {
    inner: Mutex<Inner>,
    /// Number of stages to retain before evicting least-recently-used.
    capacity: usize,
    /// Count of *actual* materializations (cache misses). Incremented once inside the single-flight
    /// closure, so concurrent first-pulls of one stage bump it exactly once.
    materializations: AtomicUsize,
}

impl StageCache {
    /// Default number of stages to retain before LRU eviction.
    pub const DEFAULT_CAPACITY: usize = 128;

    /// A cache with the [default capacity](Self::DEFAULT_CAPACITY).
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// A cache bounded to `capacity` stages. `capacity` is clamped up to 1 — a zero-capacity cache
    /// would evict every entry it inserts and defeat single-flight, so we forbid it.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                cells: std::collections::HashMap::new(),
                lru: VecDeque::new(),
            }),
            capacity: capacity.max(1),
            materializations: AtomicUsize::new(0),
        }
    }

    /// The retention bound (number of stages).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many times a stage has actually been materialized (i.e. cache misses). This is what the
    /// "producer runs once" tests assert: `N` consumers of one stage leave this at `1`.
    pub fn execution_count(&self) -> usize {
        self.materializations.load(Ordering::SeqCst)
    }

    /// Number of stages currently cached (after any eviction). Exposed for tests of the eviction
    /// bound.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("stage cache mutex").cells.len()
    }

    /// Whether the cache currently holds no stages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the materialized output for `stage_id`, running `init` exactly once on a miss.
    ///
    /// Single-flight: concurrent callers for the same new `stage_id` share one `init` run. The
    /// execution counter is bumped inside the once-closure, so it counts materializations, not
    /// calls. `init` produces the [`MaterializedStage`] (deserialize + `collect_partitioned`) and
    /// only runs on a miss.
    pub async fn get_or_materialize<F, Fut>(
        &self,
        stage_id: u64,
        init: F,
    ) -> Result<Arc<MaterializedStage>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<MaterializedStage>>>,
    {
        // Grab (or insert) the cell under the sync lock, then release it before awaiting: the
        // materialization must not hold the map lock, or one slow producer would stall every other
        // stage's lookups.
        let cell = self.cell_for(stage_id);

        let stage = cell
            .get_or_try_init(|| async {
                // Reached by exactly one caller per materialization (OnceCell serializes the rest).
                self.materializations.fetch_add(1, Ordering::SeqCst);
                init().await
            })
            .await?;
        Ok(Arc::clone(stage))
    }

    /// Look up or insert the single-flight cell for `stage_id`, touching LRU order and evicting if
    /// over capacity. Runs entirely under the sync lock and does nothing async.
    fn cell_for(&self, stage_id: u64) -> StageCell {
        let mut inner = self.inner.lock().expect("stage cache mutex");

        if let Some(cell) = inner.cells.get(&stage_id) {
            let cell = Arc::clone(cell);
            touch(&mut inner.lru, stage_id);
            return cell;
        }

        let cell: StageCell = Arc::new(OnceCell::new());
        inner.cells.insert(stage_id, Arc::clone(&cell));
        inner.lru.push_back(stage_id);

        // Evict least-recently-used entries until back within the bound. The entry we just pushed
        // is at the back, so it is never the one evicted (capacity >= 1).
        while inner.cells.len() > self.capacity {
            if let Some(victim) = inner.lru.pop_front() {
                inner.cells.remove(&victim);
            } else {
                break;
            }
        }
        cell
    }
}

impl Default for StageCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Move `stage_id` to the most-recently-used end of `lru` (removing any earlier occurrence).
fn touch(lru: &mut VecDeque<u64>, stage_id: u64) {
    if let Some(pos) = lru.iter().position(|&id| id == stage_id) {
        lru.remove(pos);
    }
    lru.push_back(stage_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    /// Build a two-partition materialized stage for tests.
    fn sample_stage() -> Arc<MaterializedStage> {
        Arc::new(MaterializedStage {
            schema: schema(),
            partitions: vec![vec![batch(vec![1, 2])], vec![batch(vec![3, 4, 5])]],
        })
    }

    #[test]
    fn stage_id_is_stable_and_content_addressed() {
        assert_eq!(stage_id_of(b"plan-a"), stage_id_of(b"plan-a"));
        assert_ne!(stage_id_of(b"plan-a"), stage_id_of(b"plan-b"));
    }

    #[tokio::test]
    async fn materializes_once_under_repeated_access() {
        let cache = StageCache::new();
        let runs = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let runs = Arc::clone(&runs);
            let stage = cache
                .get_or_materialize(42, || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok(sample_stage())
                })
                .await
                .unwrap();
            // Each caller gets complete, correct output.
            assert_eq!(stage.partition_count(), 2);
            assert_eq!(stage.partitions[0][0].num_rows(), 2);
            assert_eq!(stage.partitions[1][0].num_rows(), 3);
        }

        // The closure ran once; the metric agrees.
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(cache.execution_count(), 1);
    }

    #[tokio::test]
    async fn concurrent_first_pulls_materialize_once() {
        let cache = Arc::new(StageCache::new());
        let runs = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let runs = Arc::clone(&runs);
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_materialize(7, || async move {
                        // A little work so the racers actually overlap on the OnceCell.
                        tokio::task::yield_now().await;
                        runs.fetch_add(1, Ordering::SeqCst);
                        Ok(sample_stage())
                    })
                    .await
                    .unwrap()
            }));
        }
        for h in handles {
            let stage = h.await.unwrap();
            assert_eq!(stage.partition_count(), 2);
        }

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(cache.execution_count(), 1);
    }

    #[tokio::test]
    async fn different_stages_each_materialize() {
        let cache = StageCache::new();
        for id in [1u64, 2, 3] {
            cache
                .get_or_materialize(id, || async { Ok(sample_stage()) })
                .await
                .unwrap();
        }
        assert_eq!(cache.execution_count(), 3);
        assert_eq!(cache.len(), 3);
    }

    #[tokio::test]
    async fn eviction_bounds_the_cache_and_forces_rematerialization() {
        let cache = StageCache::with_capacity(2);

        // Fill two slots, then insert a third: the least-recently-used (id 1) is evicted.
        for id in [1u64, 2, 3] {
            cache
                .get_or_materialize(id, || async { Ok(sample_stage()) })
                .await
                .unwrap();
        }
        assert_eq!(cache.len(), 2, "capacity is 2");
        assert_eq!(cache.execution_count(), 3);

        // Re-pulling the evicted stage re-materializes it — correctness does not depend on
        // retention, only performance does.
        cache
            .get_or_materialize(1, || async { Ok(sample_stage()) })
            .await
            .unwrap();
        assert_eq!(cache.execution_count(), 4);
    }

    #[tokio::test]
    async fn recent_use_protects_a_stage_from_eviction() {
        let cache = StageCache::with_capacity(2);
        cache
            .get_or_materialize(1, || async { Ok(sample_stage()) })
            .await
            .unwrap();
        cache
            .get_or_materialize(2, || async { Ok(sample_stage()) })
            .await
            .unwrap();
        // Touch id 1 so id 2 becomes the least-recently-used.
        cache
            .get_or_materialize(1, || async { Ok(sample_stage()) })
            .await
            .unwrap();
        // Inserting id 3 should now evict id 2, not id 1.
        cache
            .get_or_materialize(3, || async { Ok(sample_stage()) })
            .await
            .unwrap();

        // id 1 is still cached (no re-materialization); id 2 was evicted (re-materializes).
        let before = cache.execution_count();
        cache
            .get_or_materialize(1, || async { panic!("id 1 must still be cached") })
            .await
            .unwrap();
        assert_eq!(cache.execution_count(), before, "id 1 served from cache");
        cache
            .get_or_materialize(2, || async { Ok(sample_stage()) })
            .await
            .unwrap();
        assert_eq!(
            cache.execution_count(),
            before + 1,
            "id 2 was evicted and re-materialized"
        );
    }
}
