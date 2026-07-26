//! **Admission control** — how many queries a warehouse runs at once, and what happens to the rest.
//!
//! # The problem
//!
//! A warehouse is a fixed pool of workers. Running one query on it uses all of them (the plan fans
//! across the whole fleet); running fifty at once does not make the pool fifty times bigger, it
//! makes every query contend for the same CPU, the same memory and the same stage caches, and the
//! usual outcome is that all fifty are slow and some die on memory. A warehouse that accepts
//! unbounded concurrency has no performance model at all.
//!
//! So the coordinator bounds it. `K` queries run; the rest wait in line; the line itself is
//! bounded so a client that never stops submitting gets told "no" instead of quietly consuming the
//! coordinator's memory. That is the whole of this module: a counted, fair, bounded gate.
//!
//! # The shape
//!
//! One [`Admission`] per warehouse, holding a [`tokio::sync::Semaphore`] with `K` permits:
//!
//! - **The bound** is the permit count. A query holds exactly one permit for exactly as long as it
//!   executes, and the permit is released by [`QuerySlot`]'s `Drop` — not by a call at the end of
//!   the happy path. That distinction is the single most important line in this file: a slot
//!   released only on success leaks one permit per failed query, and a server that leaks permits
//!   degrades to serial and then stops accepting work entirely. `Drop` runs on the error path, on
//!   an early `?`, and on a panic.
//! - **Fairness** is tokio's: `Semaphore::acquire` is FIFO, so waiters are admitted in submission
//!   order and a stream of cheap queries cannot indefinitely jump the expensive one that arrived
//!   first. The fast path uses `try_acquire`, which cannot barge a waiter because a released
//!   permit is handed straight to the head of the queue rather than back to the pool.
//! - **Isolation** is the per-warehouse split: saturating `analytics` does not make `etl` wait.
//!   That is the same promise [`crate::warehouse`] makes about compute, kept at the scheduler.
//! - **The queue bound** is [`AdmissionLimits::max_queued`]. Past it, submission is refused with
//!   [`AdmissionError::QueueFull`] — backpressure a client can see, rather than a wait it cannot.
//!
//! # What this does NOT do — read this before deploying two coordinators
//!
//! **Admission is per coordinator *process*, not fleet-wide.** The semaphore lives in one
//! process's memory. Two coordinators pointed at the same warehouse, each configured with `K = 4`,
//! will run up to **8** queries on it, and neither can see the other. Nothing here detects that,
//! and the concurrency numbers in query history are therefore only meaningful *within* one value
//! of the `queries.coordinator` column.
//!
//! This is a deliberate scope line, not an oversight. Fleet-wide admission means the permit count
//! becomes shared state — an advisory lock or a leased leader in Postgres, a lease renewal loop, a
//! decision about what happens when a slot-holder dies without releasing, and a fast path that now
//! costs a round trip per query. That is its own issue. Until it lands, the supported deployment
//! is **one coordinator per warehouse**, and an operator running more should size `K` as the
//! per-warehouse budget divided by the number of coordinators.
//!
//! Also absent, and worth naming:
//!
//! - **No priorities, no preemption, no cost model.** FIFO is the entire policy. A query that has
//!   been admitted runs to completion; nothing evicts it to make room.
//! - **No per-tenant fairness.** The queue is per *warehouse*. Two tenants sharing a warehouse
//!   share one FIFO line, so a burst from one delays the other by its own length. Warehouses are
//!   already the isolation boundary this system offers; a second fairness dimension inside one
//!   would need weights and a scheduler that is no longer FIFO.
//! - **No dynamic resize.** A warehouse's limit is fixed when this process first schedules a query
//!   on it. Resizing the warehouse row changes the desired *compute*, and takes effect for
//!   admission on the next coordinator restart — [`Scheduler::admission_for`] logs a warning when
//!   it sees a size it is not honouring, so the drift is visible rather than silent.
//! - **No cancellation.** Dropping the future that awaits [`Admission::acquire`] removes a waiter
//!   (that much is tokio's), but there is no way to cancel a query that has already started.
//!
//! # Why this is testable without a database or a network
//!
//! Nothing here knows what a query *is*. It hands out slots. That is what lets the bound, the
//! fairness, the queue cap and — most importantly — the release-on-failure behaviour be proven by
//! unit tests with no Postgres, no workers and no Flight, which is where a bug in any of them
//! would otherwise only show up as a production server that mysteriously went serial.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Concurrency limit used for a target with no warehouse row to size it from — i.e. a query
/// routed at a raw `--workers` fleet.
///
/// Four rather than one: the point of the default path is that it keeps working exactly as it
/// always did, and a bound of one would turn a coordinator serving a laptop demo into a
/// bottleneck nobody asked for. Four is small enough to still be a bound.
pub const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 4;

/// How many queries may wait per warehouse before submission is refused.
///
/// The number is not load-bearing; the *existence* of a cap is. An unbounded queue converts a
/// client that submits faster than the warehouse drains into a coordinator that runs out of
/// memory, hours later, far from the cause.
pub const DEFAULT_MAX_QUEUED_QUERIES: usize = 128;

/// The key an [`Admission`] is filed under when a query names no warehouse.
///
/// `<` is not legal in a warehouse name ([`crate::warehouse::validate_warehouse_name`] permits
/// only `[a-z0-9-]`), so this cannot collide with a real one.
pub const DEFAULT_FLEET_KEY: &str = "<workers>";

/// What a warehouse's gate is configured to allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Queries that may execute simultaneously. Clamped to at least 1 by [`Admission::new`] —
    /// a limit of zero is a warehouse that can never run anything, which is always a bug.
    pub max_concurrent: usize,
    /// Queries that may wait. Past this, submission is refused.
    pub max_queued: usize,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_MAX_CONCURRENT_QUERIES,
            max_queued: DEFAULT_MAX_QUEUED_QUERIES,
        }
    }
}

/// Why a query was not admitted.
///
/// Both variants are refusals *before* execution, and both are distinguishable from an execution
/// failure on purpose: a client seeing [`AdmissionError::QueueFull`] should back off and retry,
/// while one seeing an execution error should not.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    /// The warehouse already has `max_queued` queries waiting.
    #[error(
        "warehouse `{warehouse}` has {max_queued} queries already queued; \
         retry once the queue drains (raise --max-queued-queries to change the cap)"
    )]
    QueueFull {
        warehouse: String,
        max_queued: usize,
    },
    /// The coordinator is shutting down and will not start new work.
    #[error("coordinator is shutting down; query was not started on warehouse `{warehouse}`")]
    ShuttingDown { warehouse: String },
}

/// A point-in-time reading of one warehouse's gate. Cheap, lock-free, and the thing tests assert
/// on — in particular [`AdmissionSnapshot::peak_running`], which is how "the bound was never
/// exceeded" becomes an assertion rather than a hope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    /// Permits configured.
    pub max_concurrent: usize,
    /// Queries executing right now.
    pub running: usize,
    /// Queries waiting for a slot right now.
    pub queued: usize,
    /// The most that were ever running at once, since this gate was created.
    pub peak_running: usize,
    /// The most that were ever waiting at once.
    pub peak_queued: usize,
    /// Total admitted over this gate's life.
    pub admitted: u64,
    /// Total refused — queue full or shutting down.
    pub refused: u64,
}

/// One warehouse's gate.
///
/// Always held behind an `Arc`: [`QuerySlot`] keeps a clone so it can decrement the running count
/// on drop, which is what makes the accounting survive a failed query.
#[derive(Debug)]
pub struct Admission {
    warehouse: String,
    limits: AdmissionLimits,
    permits: Arc<Semaphore>,
    running: AtomicUsize,
    queued: AtomicUsize,
    peak_running: AtomicUsize,
    peak_queued: AtomicUsize,
    admitted: AtomicU64,
    refused: AtomicU64,
}

impl Admission {
    /// A gate for `warehouse` allowing `limits`.
    ///
    /// `max_concurrent` is clamped to at least one: a zero here would be a warehouse that accepts
    /// queries and never runs them, which is indistinguishable from a hang.
    pub fn new(warehouse: impl Into<String>, limits: AdmissionLimits) -> Arc<Self> {
        let limits = AdmissionLimits {
            max_concurrent: limits.max_concurrent.max(1),
            ..limits
        };
        Arc::new(Self {
            warehouse: warehouse.into(),
            limits,
            permits: Arc::new(Semaphore::new(limits.max_concurrent)),
            running: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            peak_running: AtomicUsize::new(0),
            peak_queued: AtomicUsize::new(0),
            admitted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
        })
    }

    /// The warehouse this gate belongs to.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// The limits in force.
    pub fn limits(&self) -> AdmissionLimits {
        self.limits
    }

    /// Wait for a slot, or be refused.
    ///
    /// The fast path (`try_acquire`) matters for more than latency: a query that is admitted
    /// immediately was never *queued*, and counting it as queued would make `peak_queued` — and
    /// the queue cap — meaningless under a burst that the warehouse could actually absorb. Tokio
    /// hands a released permit directly to the head of the waiter queue rather than returning it
    /// to the pool, so this fast path cannot jump an existing waiter.
    pub async fn acquire(self: &Arc<Self>) -> Result<QuerySlot, AdmissionError> {
        match Arc::clone(&self.permits).try_acquire_owned() {
            Ok(permit) => return Ok(self.occupy(permit)),
            Err(TryAcquireError::Closed) => return Err(self.shutting_down()),
            Err(TryAcquireError::NoPermits) => {}
        }

        // Full. Take a place in line, or be turned away.
        self.reserve_queue_slot()?;
        let permit = Arc::clone(&self.permits).acquire_owned().await;
        self.queued.fetch_sub(1, Ordering::AcqRel);
        match permit {
            Ok(permit) => Ok(self.occupy(permit)),
            // `close()` was called while we waited: the coordinator is going away.
            Err(_) => Err(self.shutting_down()),
        }
    }

    /// Stop admitting. Every waiter wakes with [`AdmissionError::ShuttingDown`] and every later
    /// call is refused; queries already holding a slot are untouched and run to completion.
    ///
    /// This is what makes shutdown honest rather than a hang: without it, a queued query would sit
    /// in line while the server drains, and the client would wait for a slot that is never coming.
    pub fn close(&self) {
        self.permits.close();
    }

    /// True once [`Admission::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.permits.is_closed()
    }

    /// A consistent-enough reading of the counters. Each is atomic; the set is not a snapshot
    /// under a lock, which is the right trade for a gauge that exists to be logged and asserted on
    /// after the storm has passed.
    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            max_concurrent: self.limits.max_concurrent,
            running: self.running.load(Ordering::Acquire),
            queued: self.queued.load(Ordering::Acquire),
            peak_running: self.peak_running.load(Ordering::Acquire),
            peak_queued: self.peak_queued.load(Ordering::Acquire),
            admitted: self.admitted.load(Ordering::Acquire),
            refused: self.refused.load(Ordering::Acquire),
        }
    }

    /// Take a place in the queue, or refuse. The compare-and-swap is what keeps the cap exact
    /// under a simultaneous burst — a plain `load` then `store` would let `N` submitters all read
    /// `max_queued - 1` and all decide there was room.
    fn reserve_queue_slot(self: &Arc<Self>) -> Result<(), AdmissionError> {
        let previous = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued < self.limits.max_queued).then_some(queued + 1)
            });
        match previous {
            Ok(previous) => {
                self.peak_queued.fetch_max(previous + 1, Ordering::AcqRel);
                Ok(())
            }
            Err(_) => {
                self.refused.fetch_add(1, Ordering::AcqRel);
                Err(AdmissionError::QueueFull {
                    warehouse: self.warehouse.clone(),
                    max_queued: self.limits.max_queued,
                })
            }
        }
    }

    /// Book a permit into the running count and wrap it in the guard that will give it back.
    fn occupy(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> QuerySlot {
        let running = self.running.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_running.fetch_max(running, Ordering::AcqRel);
        self.admitted.fetch_add(1, Ordering::AcqRel);
        QuerySlot {
            admission: Arc::clone(self),
            _permit: permit,
        }
    }

    fn shutting_down(self: &Arc<Self>) -> AdmissionError {
        self.refused.fetch_add(1, Ordering::AcqRel);
        AdmissionError::ShuttingDown {
            warehouse: self.warehouse.clone(),
        }
    }
}

/// The right to execute one query, released when this is dropped.
///
/// Deliberately has no `release()` method. The only way to give a slot back is to drop the guard,
/// which happens on every exit path a query has — success, `?`, panic, or a cancelled future — so
/// there is no path on which a caller can forget.
#[derive(Debug)]
pub struct QuerySlot {
    admission: Arc<Admission>,
    /// Held for the guard's lifetime; dropping it returns the permit to the semaphore, which
    /// hands it straight to the next waiter in line.
    _permit: OwnedSemaphorePermit,
}

impl QuerySlot {
    /// The gate this slot came from — for logging which warehouse a query is occupying.
    pub fn warehouse(&self) -> &str {
        self.admission.warehouse()
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        self.admission.running.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Every warehouse's gate, in one place.
///
/// A registry rather than a single gate because "which warehouse" is only known per query: a
/// coordinator serves whatever its clients name, and a warehouse that is never queried should not
/// cost anything. Gates are therefore created lazily, on first sight, and live for the process.
#[derive(Debug)]
pub struct Scheduler {
    default_limits: AdmissionLimits,
    /// `std::sync::Mutex`, not tokio's: the critical section is a hash lookup and never awaits, so
    /// an async mutex would add a scheduling point for nothing.
    warehouses: Mutex<HashMap<String, Arc<Admission>>>,
    closed: AtomicBool,
}

impl Scheduler {
    /// A scheduler whose gates default to `limits` — `max_queued` for every warehouse, and
    /// `max_concurrent` for any target that has no warehouse row to be sized from.
    pub fn new(default_limits: AdmissionLimits) -> Self {
        Self {
            default_limits,
            warehouses: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    /// The gate for `warehouse`, creating it with `max_concurrent` permits on first sight.
    ///
    /// The limit is set **once**. A later call naming a different `max_concurrent` — because the
    /// warehouse was resized in the services database while this process was running — logs a
    /// warning and keeps the existing gate. Growing a live semaphore is easy; shrinking one that
    /// has permits outstanding is not, and a limit that changes underneath a running queue is a
    /// worse thing to debug than a stale one that says so in the log. See the module docs.
    pub fn admission_for(&self, warehouse: &str, max_concurrent: usize) -> Arc<Admission> {
        let mut gates = self
            .warehouses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = gates.get(warehouse) {
            let in_force = existing.limits().max_concurrent;
            if in_force != max_concurrent.max(1) {
                tracing::warn!(
                    warehouse,
                    limit_in_force = in_force,
                    limit_requested = max_concurrent,
                    "the warehouse's concurrency limit changed since this coordinator started; \
                     the running limit is unchanged until it is restarted"
                );
            }
            return Arc::clone(existing);
        }

        let admission = Admission::new(
            warehouse,
            AdmissionLimits {
                max_concurrent,
                max_queued: self.default_limits.max_queued,
            },
        );
        // Born closed if the scheduler already is, so a query arriving during shutdown is refused
        // rather than admitted into a server that is going away.
        if self.closed.load(Ordering::Acquire) {
            admission.close();
        }
        tracing::info!(
            warehouse,
            max_concurrent = admission.limits().max_concurrent,
            max_queued = admission.limits().max_queued,
            "admission control ready for warehouse"
        );
        gates.insert(warehouse.to_string(), Arc::clone(&admission));
        admission
    }

    /// The gate for the default (`--workers`) fleet, sized by [`AdmissionLimits::max_concurrent`].
    pub fn default_admission(&self) -> Arc<Admission> {
        self.admission_for(DEFAULT_FLEET_KEY, self.default_limits.max_concurrent)
    }

    /// The limits new gates are built from.
    pub fn default_limits(&self) -> AdmissionLimits {
        self.default_limits
    }

    /// Stop admitting anywhere. Waiters wake refused; running queries finish.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let gates = self
            .warehouses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for admission in gates.values() {
            admission.close();
        }
    }

    /// True once [`Scheduler::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Every gate's counters, keyed by warehouse. `BTreeMap` so a log line or a test assertion
    /// reads in a stable order.
    pub fn snapshot(&self) -> BTreeMap<String, AdmissionSnapshot> {
        let gates = self
            .warehouses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gates
            .iter()
            .map(|(name, admission)| (name.clone(), admission.snapshot()))
            .collect()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(AdmissionLimits::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;

    fn limits(max_concurrent: usize, max_queued: usize) -> AdmissionLimits {
        AdmissionLimits {
            max_concurrent,
            max_queued,
        }
    }

    /// The headline bound: N tasks, K permits, and the *observed* peak never exceeds K.
    ///
    /// Each task holds its slot across an await point, so the runtime genuinely interleaves them —
    /// a bound that only held because nothing ever yielded would prove nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrency_never_exceeds_the_limit() {
        const N: usize = 40;
        const K: usize = 3;
        let admission = Admission::new("wh", limits(K, N));
        // An independent witness: the gate's own counters could agree with themselves and both be
        // wrong, so the tasks count each other too.
        let live = Arc::new(AtomicUsize::new(0));
        let witnessed_peak = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..N {
            let admission = Arc::clone(&admission);
            let live = Arc::clone(&live);
            let witnessed_peak = Arc::clone(&witnessed_peak);
            tasks.push(tokio::spawn(async move {
                let slot = admission.acquire().await.expect("admitted");
                let now = live.fetch_add(1, Ordering::AcqRel) + 1;
                witnessed_peak.fetch_max(now, Ordering::AcqRel);
                tokio::time::sleep(Duration::from_millis(5)).await;
                live.fetch_sub(1, Ordering::AcqRel);
                drop(slot);
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }

        let snapshot = admission.snapshot();
        assert!(
            witnessed_peak.load(Ordering::Acquire) <= K,
            "observed {} concurrent, limit {K}",
            witnessed_peak.load(Ordering::Acquire)
        );
        assert!(snapshot.peak_running <= K, "{snapshot:?}");
        assert_eq!(snapshot.peak_running, K, "the limit should be reached");
        assert_eq!(snapshot.admitted, N as u64);
        // Drained: every slot handed back.
        assert_eq!(snapshot.running, 0, "{snapshot:?}");
        assert_eq!(snapshot.queued, 0, "{snapshot:?}");
        assert!(
            snapshot.peak_queued > 0,
            "N > K must have queued: {snapshot:?}"
        );
    }

    /// The bug this whole design exists to prevent: a query that fails must give its slot back.
    ///
    /// If the guard released only on success, the second batch here would find zero permits and
    /// hang — which is exactly how a real server "degrades to serial and then stops".
    #[tokio::test]
    async fn a_failing_query_does_not_leak_its_slot() {
        let admission = Admission::new("wh", limits(1, 8));

        for i in 0..5 {
            let slot = admission.acquire().await.expect("admitted");
            // Stand in for a query that fails: the slot goes out of scope via an error path.
            let outcome: Result<(), &str> = Err("boom");
            drop(slot);
            assert!(outcome.is_err(), "iteration {i}");
        }
        assert_eq!(admission.snapshot().running, 0);

        // A panic must not wedge it either — `Drop` still runs while unwinding.
        let panicking = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                let _slot = admission.acquire().await.expect("admitted");
                panic!("query blew up");
            })
        };
        assert!(panicking.await.is_err(), "the task should have panicked");
        assert_eq!(
            admission.snapshot().running,
            0,
            "a panicking query must still return its slot"
        );

        // …and the gate is still usable.
        let slot = tokio::time::timeout(Duration::from_secs(5), admission.acquire())
            .await
            .expect("the gate must not be wedged")
            .expect("admitted");
        assert_eq!(admission.snapshot().running, 1);
        drop(slot);
    }

    /// Fairness: waiters are admitted in the order they arrived, so a long queue of cheap queries
    /// cannot starve the expensive one at the front.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiters_are_admitted_in_arrival_order() {
        let admission = Admission::new("wh", limits(1, 16));
        let order = Arc::new(Mutex::new(Vec::new()));

        // Occupy the only slot, so everything below has to queue.
        let blocker = admission.acquire().await.expect("admitted");

        let mut tasks = Vec::new();
        for i in 0..8usize {
            let gate = Arc::clone(&admission);
            let order = Arc::clone(&order);
            tasks.push(tokio::spawn(async move {
                let _slot = gate.acquire().await.expect("admitted");
                order.lock().expect("lock").push(i);
            }));
            // Let task `i` actually reach the semaphore before spawning `i + 1`; without this the
            // "arrival order" the test asserts on would just be spawn order, which proves nothing.
            while admission.snapshot().queued <= i {
                tokio::task::yield_now().await;
            }
        }

        drop(blocker);
        for task in tasks {
            task.await.expect("task");
        }
        assert_eq!(
            *order.lock().expect("lock"),
            (0..8).collect::<Vec<_>>(),
            "tokio's semaphore is FIFO; waiters must run in arrival order"
        );
    }

    /// The queue cap is exact, and refusing is not the same as failing: the refused query never
    /// touched the warehouse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_queue_refuses_instead_of_growing() {
        let admission = Admission::new("analytics", limits(1, 2));
        let blocker = admission.acquire().await.expect("admitted");

        // Fill the two queue places.
        let mut waiters = Vec::new();
        for _ in 0..2 {
            let admission = Arc::clone(&admission);
            waiters.push(tokio::spawn(async move {
                admission.acquire().await.map(|_slot| ())
            }));
        }
        while admission.snapshot().queued < 2 {
            tokio::task::yield_now().await;
        }

        let refused = admission.acquire().await.expect_err("the queue is full");
        assert_eq!(
            refused,
            AdmissionError::QueueFull {
                warehouse: "analytics".to_string(),
                max_queued: 2,
            }
        );
        let message = refused.to_string();
        assert!(message.contains("analytics"), "{message}");
        assert!(message.contains("--max-queued-queries"), "{message}");

        drop(blocker);
        for waiter in waiters {
            waiter.await.expect("task").expect("admitted");
        }
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.refused, 1, "{snapshot:?}");
        assert_eq!(snapshot.admitted, 3, "{snapshot:?}");
        assert_eq!(snapshot.queued, 0, "the queue must drain: {snapshot:?}");
    }

    /// Shutting down wakes the queue instead of stranding it. A queued client gets a clear
    /// "shutting down", not a wait that never ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_wakes_waiters_with_a_clear_reason() {
        let admission = Admission::new("etl", limits(1, 8));
        let blocker = admission.acquire().await.expect("admitted");

        let admission_clone = Arc::clone(&admission);
        let waiter = tokio::spawn(async move { admission_clone.acquire().await.map(|_| ()) });
        while admission.snapshot().queued < 1 {
            tokio::task::yield_now().await;
        }

        admission.close();
        assert_eq!(
            waiter.await.expect("task"),
            Err(AdmissionError::ShuttingDown {
                warehouse: "etl".to_string()
            })
        );
        // A query arriving after the close is refused too, even though a permit is free once the
        // blocker drops.
        drop(blocker);
        assert!(matches!(
            admission.acquire().await,
            Err(AdmissionError::ShuttingDown { .. })
        ));
        assert!(admission.is_closed());
    }

    /// Isolation: warehouses do not share a queue, which is the scheduler half of the promise
    /// `crate::warehouse` makes about compute.
    #[tokio::test]
    async fn saturating_one_warehouse_does_not_block_another() {
        let scheduler = Scheduler::new(limits(2, 4));
        let analytics = scheduler.admission_for("analytics", 1);
        let etl = scheduler.admission_for("etl", 1);

        let _busy = analytics.acquire().await.expect("admitted");
        assert_eq!(
            analytics.snapshot().running,
            analytics.limits().max_concurrent,
            "analytics is saturated"
        );
        // `etl` is untouched by that.
        let etl_slot = tokio::time::timeout(Duration::from_secs(5), etl.acquire())
            .await
            .expect("etl must not wait on analytics")
            .expect("admitted");
        drop(etl_slot);

        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot["analytics"].running, 1);
        assert_eq!(snapshot["etl"].running, 0);
    }

    /// A gate is created once and keeps its limit; a resize is reported, not silently applied.
    #[tokio::test]
    async fn a_warehouses_limit_is_fixed_for_the_life_of_the_process() {
        let scheduler = Scheduler::new(limits(4, 32));
        let first = scheduler.admission_for("analytics", 2);
        let again = scheduler.admission_for("analytics", 9);
        assert!(
            Arc::ptr_eq(&first, &again),
            "one gate per warehouse, not one per lookup"
        );
        assert_eq!(again.limits().max_concurrent, 2, "the limit does not move");
        // The queue cap comes from the scheduler's defaults, not from the warehouse row.
        assert_eq!(again.limits().max_queued, 32);
    }

    /// A limit of zero would be a warehouse that accepts queries and never runs one.
    #[tokio::test]
    async fn a_zero_limit_is_clamped_to_one() {
        let admission = Admission::new("wh", limits(0, 4));
        assert_eq!(admission.limits().max_concurrent, 1);
        let slot = tokio::time::timeout(Duration::from_secs(5), admission.acquire())
            .await
            .expect("a clamped gate still admits")
            .expect("admitted");
        drop(slot);
    }

    /// The default fleet gets a gate too — the `--workers` path is scheduled, not exempt.
    #[tokio::test]
    async fn the_default_fleet_has_its_own_gate() {
        let scheduler = Scheduler::new(limits(3, 16));
        let gate = scheduler.default_admission();
        assert_eq!(gate.warehouse(), DEFAULT_FLEET_KEY);
        assert_eq!(gate.limits().max_concurrent, 3);
        assert!(
            !DEFAULT_FLEET_KEY
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "the default key must be unspellable as a warehouse name"
        );
    }

    /// Closing the scheduler closes gates that do not exist yet, so a query racing shutdown on a
    /// warehouse nobody has touched is refused rather than admitted into a dying server.
    #[tokio::test]
    async fn a_gate_created_after_shutdown_is_born_closed() {
        let scheduler = Scheduler::new(limits(2, 8));
        scheduler.close();
        assert!(scheduler.is_closed());
        let late = scheduler.admission_for("arrived-late", 2);
        assert!(late.is_closed());
        assert!(matches!(
            late.acquire().await,
            Err(AdmissionError::ShuttingDown { .. })
        ));
    }
}
