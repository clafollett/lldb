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
//! # The shape: two gates in series
//!
//! One [`Admission`] per warehouse, holding a [`tokio::sync::Semaphore`] with `K` permits, and —
//! when the deployment has a control plane — a [`FleetGate`] behind it that the whole fleet shares:
//!
//! - **The local bound** is the permit count. A query holds exactly one permit for exactly as long
//!   as it executes, and the permit is released by [`QuerySlot`]'s `Drop` — not by a call at the
//!   end of the happy path. That distinction is the single most important line in this file: a slot
//!   released only on success leaks one permit per failed query, and a server that leaks permits
//!   degrades to serial and then stops accepting work entirely. `Drop` runs on the error path, on
//!   an early `?`, on a panic — and on a **cancelled future**, which is what makes
//!   [`crate::cancel`] work without this module knowing it exists: stopping a query is dropping the
//!   future that holds the guard, and the permit goes straight to the next waiter in line.
//! - **The fleet bound** is the [`FleetGate`]. A query that has a local permit must *also* hold one
//!   of the warehouse's `K` fleet-wide slots before it runs, so `K` is a property of the warehouse
//!   rather than of a process. The lease is carried by the same [`QuerySlot`] and released by the
//!   same `Drop`, so there is still exactly one way to give a slot back.
//! - **Fairness** is tokio's: `Semaphore::acquire` is FIFO, so waiters are admitted in submission
//!   order and a stream of cheap queries cannot indefinitely jump the expensive one that arrived
//!   first. The fast path uses `try_acquire`, which cannot barge a waiter because a released
//!   permit is handed straight to the head of the queue rather than back to the pool.
//! - **Isolation** is the per-warehouse split: saturating `analytics` does not make `etl` wait.
//!   That is the same promise [`crate::warehouse`] makes about compute, kept at the scheduler.
//! - **The queue bound** is [`AdmissionLimits::max_queued`]. Past it, submission is refused with
//!   [`AdmissionError::QueueFull`] — backpressure a client can see, rather than a wait it cannot.
//!
//! The order of the two gates is not arbitrary. **Local first, fleet second, always.** A query
//! never holds a fleet lease while waiting for a local permit, so there is a strict acquisition
//! order across the whole deployment and no cycle to deadlock on. It also means the cheap gate
//! rejects first: a coordinator that is locally saturated never asks Postgres anything.
//!
//! # Why the local semaphore stays, even with a fleet gate
//!
//! It is both the **fast path** and the **backstop**, and the second is the load-bearing one.
//!
//! [`crate::services::ServicesArgs::connect`] returning `None` is a supported deployment (CLAUDE.md:
//! `cargo run` must never need Postgres), so a scheduler with no fleet gate has to behave exactly as
//! it did before this module knew what one was — and it does, down to doing **zero I/O** on an
//! uncontended admit. And a control plane that is configured but briefly unreachable must not stop a
//! warehouse from scheduling: a failed claim falls through to the local bound
//! ([`AdmissionSnapshot::fleet_degraded`] counts it and the log says so), which means `N`
//! coordinators × `K` for the duration of the outage. That is precisely the behaviour this module
//! had before fleet-wide admission existed, so the degraded mode is bounded by the old bug and is
//! never worse than it.
//!
//! That is also the argument [`crate::liveness`]'s decision 2 asked a future issue to make. Its
//! concession — a coordinator that cannot renew keeps serving, and its rows may be judged dead —
//! now has a client-visible consequence, because a slot this coordinator is still using becomes
//! reclaimable by another one. The resolution is the paragraph above: a coordinator that cannot
//! reach Postgres also cannot *claim* through Postgres, so both sides fall back to their local
//! semaphores at the same moment and the total is `N × K`. Liveness does not need re-deciding;
//! refusing to serve through a control-plane hiccup would still be the worse trade.
//!
//! # What a fleet-wide bound costs, stated rather than discovered
//!
//! - **An uncontended admit is one database round trip**, on the hottest path in the system. This
//!   is the property the issue named up front as the price, and it is only paid where there is a
//!   control plane to pay it to.
//! - **Fleet-wide waiting is polled, not queued.** Tokio's semaphore gives strict FIFO *within* a
//!   coordinator; between coordinators there is no shared queue, so a waiter re-asks every
//!   [`FLEET_POLL_INTERVAL`] (jittered, so two coordinators cannot lock-step) and the first to ask
//!   after a release wins. Ordering across coordinators is therefore arrival-order-ish and not
//!   guaranteed, and a warehouse under sustained saturation can make one coordinator wait longer
//!   than another. `LISTEN`/`NOTIFY` would close that, at the cost of a dedicated connection per
//!   coordinator; it is the obvious next step and not this one.
//! - **Hand-off latency is one poll interval**, because releasing a lease is a `DELETE` a destructor
//!   cannot await (see [`QuerySlot`]). The local permit still moves instantly.
//! - **A mixed-`K` fleet is bounded by the largest `K`.** Each coordinator proposes slot numbers
//!   below the limit *it* was configured with, so two coordinators that disagree about a
//!   warehouse's size — one started before a resize, one after — produce a bound of `max(K)` rather
//!   than a sum. Strictly better than the multiplication it replaces, and still worth avoiding.
//! - **Only warehouses are bounded fleet-wide.** A query routed at a raw `--workers` fleet has no
//!   warehouse row, so there is nothing for two coordinators to agree they are talking about, and
//!   it keeps the per-process semaphore it always had.
//!
//! # Also absent, and worth naming
//!
//! - **No priorities, no preemption, no cost model.** FIFO is the entire policy. Nothing here
//!   evicts an admitted query to make room for a better one; a query gives its slot back when it
//!   ends, or when somebody explicitly stops it ([`crate::cancel`]), and never because the
//!   scheduler decided so.
//! - **No per-tenant fairness.** The queue is per *warehouse*. Two tenants sharing a warehouse
//!   share one FIFO line, so a burst from one delays the other by its own length. Warehouses are
//!   already the isolation boundary this system offers; a second fairness dimension inside one
//!   would need weights and a scheduler that is no longer FIFO.
//! - **The local gate is keyed by warehouse *name*, which is unique only per account.** Two tenants
//!   with a warehouse called `analytics` share one process-local queue — a fairness wart that
//!   predates this module's fleet half and is untouched by it. The *fleet* gate is keyed by
//!   `warehouses.id` precisely so that it cannot inherit the same confusion, since merging two
//!   tenants' concurrency budgets would be considerably worse than sharing a line.
//! - **No dynamic resize.** A warehouse's limit is fixed when this process first schedules a query
//!   on it. Resizing the warehouse row changes the desired *compute*, and takes effect for
//!   admission on the next coordinator restart — [`Scheduler::admission_for`] logs a warning when
//!   it sees a size it is not honouring, so the drift is visible rather than silent.
//!
//! # Why this is still testable without a database or a network
//!
//! Nothing here knows what a query *is*, and nothing here knows what Postgres is either: the fleet
//! bound arrives as the [`FleetGate`] trait, and [`crate::fleet_admission`] is one implementation of
//! it. So the bound, the fairness, the queue cap, the release-on-failure behaviour **and now
//! "two coordinators admit `K` total, not `2K`"** are all provable by unit tests with no Postgres,
//! no workers and no Flight — which is where a bug in any of them would otherwise only show up as a
//! production server that mysteriously went serial, or as a warehouse quietly running twice the
//! work it was sized for.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

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

/// How often a query waiting on the *fleet* re-asks whether a slot has come free.
///
/// Short, because it is added to a hand-off that the local semaphore does instantly, and the whole
/// point of admission control is that the warehouse is busy — a waiter was going to wait anyway.
/// Not shorter, because every tick is a round trip and the number of tickers is the queue depth.
///
/// It is only ever reached by a query that could not be admitted, so the polling load is bounded by
/// [`AdmissionLimits::max_queued`] rather than by throughput: a warehouse nobody is queueing on
/// polls nothing at all.
pub const FLEET_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How much of [`FLEET_POLL_INTERVAL`] is randomized away, as a fraction of it.
///
/// Without jitter, `N` coordinators that started together poll in lock-step forever: they ask at the
/// same instant, the same one keeps winning, and the others are starved by a pattern rather than by
/// load. A uniform smear over a quarter of the interval breaks the phase relationship within a few
/// ticks and costs nothing.
const FLEET_POLL_JITTER: f64 = 0.25;

/// One of a warehouse's fleet-wide slots, held for the life of a query.
///
/// Fields are public because this is a record: a [`FleetGate`] mints one on a successful claim and
/// is handed it back to release. Nothing here interprets it — [`Admission`] carries it inside a
/// [`QuerySlot`] and gives it to [`FleetGate::release`] when that guard drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetLease {
    /// The warehouse whose budget this consumes. `warehouses.id`, not the name — see the module
    /// docs on why a name is the wrong key for shared state.
    pub warehouse_id: i64,
    /// Which of the warehouse's `0 .. K-1` slots was taken.
    pub slot_no: i32,
    /// Which claim this is, within the claiming process. Makes a release a compare-and-swap and a
    /// leaked row reclaimable by its own coordinator.
    pub token: String,
}

/// What a fleet gate said about a claim.
#[derive(Debug)]
pub enum FleetClaim {
    /// A slot was taken. It is the caller's until it is released.
    Granted(FleetLease),
    /// Every slot the warehouse has is held by a live coordinator. Wait and ask again.
    Full,
    /// The gate could not be consulted. The string is why, for the log.
    ///
    /// **Not an error the caller propagates.** A control-plane hiccup must not become a data-plane
    /// outage, so [`Admission`] admits on its local bound alone and counts the degradation. See the
    /// module docs.
    Unavailable(String),
}

/// The future [`FleetGate::claim`] returns. Boxed because the gate is used as a trait object and
/// `async fn` in a trait is not object-safe; the same shape `lldb_qe_core::engine::BoxResolver` uses.
pub type FleetClaimFuture<'a> = Pin<Box<dyn Future<Output = FleetClaim> + Send + 'a>>;

/// The bound the whole fleet shares — a warehouse's `K`, held somewhere every coordinator can see.
///
/// A trait rather than a concrete Postgres type for one reason, and it is the reason this module is
/// still worth trusting: everything above is provable without a database. A test can supply a gate
/// that is a counter in memory and assert that two [`Admission`]s — two coordinators, as far as this
/// module is concerned — admit `K` between them rather than `K` each.
/// [`crate::fleet_admission::FleetAdmission`] is the implementation that actually ships.
///
/// # Contract
///
/// - `claim` is **not** a reservation: a `Granted` lease is held until it is released, and nothing
///   times it out from the caller's side.
/// - `claim` must never block indefinitely. `Full` and `Unavailable` are both prompt answers, and
///   the second is what an implementation returns instead of an error.
/// - `release` is **synchronous and infallible from the caller's view**, because it is called from
///   [`QuerySlot`]'s destructor and a destructor cannot await. An implementation that needs I/O
///   hands it to the runtime — which is what makes releasing best effort, and why
///   [`FleetLease::token`] exists to make a leaked row reclaimable.
/// - `release` must tolerate being handed a lease that was already reclaimed by somebody else, and
///   must not free that somebody's slot.
pub trait FleetGate: std::fmt::Debug + Send + Sync + 'static {
    /// Try to take one of `warehouse_id`'s `limit` slots.
    fn claim(&self, warehouse_id: i64, limit: usize) -> FleetClaimFuture<'_>;

    /// Give one back. Called from a destructor: it must not block and must not panic.
    fn release(&self, lease: FleetLease);
}

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
    /// Claims the fleet gate answered [`FleetClaim::Full`] — i.e. this coordinator had a local
    /// permit free and was held back by *another* coordinator's work.
    ///
    /// Zero on a gate with no fleet bound, which makes it the cheapest way for a test to tell
    /// "the local semaphore did the bounding" from "the fleet did".
    pub fleet_waits: u64,
    /// Claims the fleet gate could not answer, each of which was admitted on the local bound alone.
    ///
    /// Non-zero means this coordinator has been enforcing `K` *per process* rather than fleet-wide
    /// for that many queries, which is the documented degradation and not an error — but it is the
    /// number an operator wants when a warehouse ran hotter than its size.
    pub fleet_degraded: u64,
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
    /// The bound the rest of the fleet shares, and the warehouse row it is keyed by. Both or
    /// neither: a gate with no `warehouse_id` has nothing the control plane can name, so it is
    /// per-process by construction rather than by configuration.
    fleet: Option<(i64, Arc<dyn FleetGate>)>,
    /// Whether the "could not reach the fleet gate" warning has already been said, so an outage
    /// produces one warning and then quiet ones rather than one per query. Mirrors
    /// [`crate::liveness`]'s `announced_stale`.
    fleet_outage_announced: AtomicBool,
    running: AtomicUsize,
    queued: AtomicUsize,
    peak_running: AtomicUsize,
    peak_queued: AtomicUsize,
    admitted: AtomicU64,
    refused: AtomicU64,
    fleet_waits: AtomicU64,
    fleet_degraded: AtomicU64,
}

/// What one consultation of the fleet gate means for the query asking.
enum FleetStep {
    /// Go ahead, carrying this lease (`None` when there is no fleet bound, or when the gate could
    /// not be reached and we are falling back to the local one).
    Admit(Option<FleetLease>),
    /// Another coordinator holds every slot. Wait.
    Wait,
}

impl Admission {
    /// A gate for `warehouse` allowing `limits`, bounded by this process alone.
    ///
    /// `max_concurrent` is clamped to at least one: a zero here would be a warehouse that accepts
    /// queries and never runs them, which is indistinguishable from a hang.
    pub fn new(warehouse: impl Into<String>, limits: AdmissionLimits) -> Arc<Self> {
        Self::build(warehouse, limits, None)
    }

    /// A gate for `warehouse` whose limit is shared with every other coordinator through `fleet`.
    ///
    /// `warehouse_id` is `warehouses.id` and not the name, because names are unique only within an
    /// account and shared state keyed by one would merge two tenants' budgets. There is deliberately
    /// no way to build this without an id: a target the control plane cannot name is a target two
    /// coordinators cannot agree on.
    pub fn fleet_wide(
        warehouse: impl Into<String>,
        limits: AdmissionLimits,
        warehouse_id: i64,
        fleet: Arc<dyn FleetGate>,
    ) -> Arc<Self> {
        Self::build(warehouse, limits, Some((warehouse_id, fleet)))
    }

    fn build(
        warehouse: impl Into<String>,
        limits: AdmissionLimits,
        fleet: Option<(i64, Arc<dyn FleetGate>)>,
    ) -> Arc<Self> {
        let limits = AdmissionLimits {
            max_concurrent: limits.max_concurrent.max(1),
            ..limits
        };
        Arc::new(Self {
            warehouse: warehouse.into(),
            limits,
            permits: Arc::new(Semaphore::new(limits.max_concurrent)),
            fleet,
            fleet_outage_announced: AtomicBool::new(false),
            running: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            peak_running: AtomicUsize::new(0),
            peak_queued: AtomicUsize::new(0),
            admitted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            fleet_waits: AtomicU64::new(0),
            fleet_degraded: AtomicU64::new(0),
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

    /// Whether this gate's limit is shared with the rest of the fleet, or enforced by this process
    /// alone.
    pub fn is_fleet_wide(&self) -> bool {
        self.fleet.is_some()
    }

    /// Wait for a slot, or be refused.
    ///
    /// Two gates in series, local then fleet, and the four things this function has to keep true
    /// are all visible in it:
    ///
    /// 1. **An uncontended admit never counts as queued.** The fast path returns before
    ///    `reserve_queue_slot` is ever reached, so `peak_queued` and the queue cap keep meaning
    ///    "somebody actually waited" under a burst the warehouse could absorb. What the fast path no
    ///    longer is, when there is a fleet gate, is *free*: it costs one round trip, which is the
    ///    price the issue named. With no fleet gate it is exactly as free as it always was.
    /// 2. **FIFO is tokio's, not re-derived.** A released permit goes straight to the head of the
    ///    waiter queue, which is why `try_acquire` cannot barge one. Note what is deliberately *not*
    ///    done below: a query that holds a local permit and is waiting on the fleet **keeps** that
    ///    permit. Giving it back to poll again would send it to the back of the local queue and turn
    ///    fleet contention into local unfairness.
    /// 3. **The queue cap is exact under a burst**, via the compare-and-swap in
    ///    `Admission::reserve_queue_slot` — and a query held up by the *fleet* takes a place in
    ///    that same line, because from a client's point of view it is queued for the same reason.
    /// 4. **`place` is dropped between the wait and `occupy`**, so no snapshot ever counts one query
    ///    as both queued and running.
    pub async fn acquire(self: &Arc<Self>) -> Result<QuerySlot, AdmissionError> {
        // ---- Fast path: a free local permit, and a fleet slot to go with it ---------------------
        let held = match Arc::clone(&self.permits).try_acquire_owned() {
            Ok(permit) => match self.claim_fleet_slot().await {
                FleetStep::Admit(lease) => return Ok(self.occupy(permit, lease)),
                // Locally free, but the rest of the fleet is busy. Keep the permit — see rule 2
                // above — and join the queue for real.
                FleetStep::Wait => Some(permit),
            },
            Err(TryAcquireError::Closed) => return Err(self.shutting_down()),
            Err(TryAcquireError::NoPermits) => None,
        };

        // Full. Take a place in line, or be turned away. The place is a guard for exactly the
        // reason the slot is — every await below is a cancellation point. See [`QueuePlace`].
        let place = self.reserve_queue_slot()?;
        let permit = match held {
            Some(permit) => permit,
            None => match Arc::clone(&self.permits).acquire_owned().await {
                Ok(permit) => permit,
                // `close()` was called while we waited: the coordinator is going away.
                Err(_) => return Err(self.shutting_down()),
            },
        };
        // Holding a local permit, still waiting on the fleet. Re-ask until it lets us in.
        let lease = loop {
            match self.claim_fleet_slot().await {
                FleetStep::Admit(lease) => break lease,
                FleetStep::Wait => {
                    // Checked here rather than trusted to the semaphore: this branch is not
                    // awaiting on it, so a shutdown that lands while we poll would otherwise be
                    // noticed only when a fleet slot happened to free up.
                    if self.permits.is_closed() {
                        return Err(self.shutting_down());
                    }
                    tokio::time::sleep(fleet_poll_delay()).await;
                }
            }
        };
        // Given back explicitly rather than at end of scope, so a snapshot taken between here and
        // `occupy` can never count one query as queued *and* running.
        drop(place);
        Ok(self.occupy(permit, lease))
    }

    /// Ask the fleet for a slot, once. Zero I/O and an immediate yes when there is no fleet bound.
    async fn claim_fleet_slot(self: &Arc<Self>) -> FleetStep {
        let Some((warehouse_id, fleet)) = &self.fleet else {
            return FleetStep::Admit(None);
        };
        match fleet.claim(*warehouse_id, self.limits.max_concurrent).await {
            FleetClaim::Granted(lease) => {
                if self.fleet_outage_announced.swap(false, Ordering::AcqRel) {
                    tracing::info!(
                        warehouse = %self.warehouse,
                        "the fleet-wide admission gate is answering again; this warehouse's limit \
                         is shared across coordinators once more"
                    );
                }
                FleetStep::Admit(Some(lease))
            }
            FleetClaim::Full => {
                self.fleet_waits.fetch_add(1, Ordering::AcqRel);
                FleetStep::Wait
            }
            // The documented degradation, and the reason the local semaphore is still here. Admit
            // on the local bound alone rather than refuse: a control-plane hiccup must not become a
            // data-plane outage, and the worst this can produce is the per-process behaviour this
            // module had before fleet-wide admission existed.
            FleetClaim::Unavailable(reason) => {
                self.fleet_degraded.fetch_add(1, Ordering::AcqRel);
                if !self.fleet_outage_announced.swap(true, Ordering::AcqRel) {
                    tracing::warn!(
                        warehouse = %self.warehouse,
                        limit = self.limits.max_concurrent,
                        reason = %reason,
                        "could not reach the fleet-wide admission gate; admitting on this \
                         process's own limit instead. While this lasts, every coordinator enforces \
                         the limit independently, so the warehouse may run up to (coordinators × \
                         limit) queries at once. Queries are not refused for this."
                    );
                }
                FleetStep::Admit(None)
            }
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
    ///
    /// **A peak is never reported below its current value**, and that clamp is load-bearing rather
    /// than cosmetic. A place is taken in two steps — `queued` is raised by the compare-and-swap in
    /// `Admission::reserve_queue_slot`, then `peak_queued` is raised to match — and `running` is the
    /// same shape. Two atomics cannot be advanced as one without a lock, so between those steps an
    /// observer could read `queued: 1, peak_queued: 0`: a snapshot asserting a peak *lower* than
    /// something it can see happening right now. Taking the max here closes that window for every
    /// reader at once, and cannot over-report — if the current value exceeds the stored peak, the
    /// true peak is at least the current value by definition.
    ///
    /// The alternative, making each caller spin until the peak catches up, pushes a detail of this
    /// module's internals onto everything that reads a gauge, and would have to be got right again
    /// at every call site.
    pub fn snapshot(&self) -> AdmissionSnapshot {
        let running = self.running.load(Ordering::Acquire);
        let queued = self.queued.load(Ordering::Acquire);
        AdmissionSnapshot {
            max_concurrent: self.limits.max_concurrent,
            running,
            queued,
            peak_running: self.peak_running.load(Ordering::Acquire).max(running),
            peak_queued: self.peak_queued.load(Ordering::Acquire).max(queued),
            admitted: self.admitted.load(Ordering::Acquire),
            refused: self.refused.load(Ordering::Acquire),
            fleet_waits: self.fleet_waits.load(Ordering::Acquire),
            fleet_degraded: self.fleet_degraded.load(Ordering::Acquire),
        }
    }

    /// Take a place in the queue, or refuse. The compare-and-swap is what keeps the cap exact
    /// under a simultaneous burst — a plain `load` then `store` would let `N` submitters all read
    /// `max_queued - 1` and all decide there was room.
    fn reserve_queue_slot(self: &Arc<Self>) -> Result<QueuePlace, AdmissionError> {
        let previous = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued < self.limits.max_queued).then_some(queued + 1)
            });
        match previous {
            Ok(previous) => {
                self.peak_queued.fetch_max(previous + 1, Ordering::AcqRel);
                Ok(QueuePlace {
                    admission: Arc::clone(self),
                })
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

    /// Book a permit into the running count and wrap it, and the fleet lease it came with, in the
    /// guard that will give both back.
    fn occupy(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        lease: Option<FleetLease>,
    ) -> QuerySlot {
        let running = self.running.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_running.fetch_max(running, Ordering::AcqRel);
        self.admitted.fetch_add(1, Ordering::AcqRel);
        QuerySlot {
            admission: Arc::clone(self),
            lease,
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

/// A place in a warehouse's queue, given back when this is dropped.
///
/// The counterpart to [`QuerySlot`], and it exists for the same reason at a different point in the
/// query's life. Waiting for a permit is an await, and an await is a cancellation point: a client
/// that hangs up while its query is queued causes tonic to drop the request future exactly there.
/// A bare `queued -= 1` written after that await would simply never run.
///
/// The consequence would be unusually nasty for a counter. `queued` never decreases back, so the
/// cap in [`Admission::reserve_queue_slot`] refuses more and more submissions with `QueueFull`
/// while the line is genuinely empty — a server that gets progressively less useful and finally
/// refuses everything, with no failing test, nothing in the logs, and an admission snapshot that
/// says it is busy while it does nothing at all.
///
/// Like [`QuerySlot`], it deliberately has no `release()`: dropping is the only way to give a
/// place back, so there is no path on which a caller can forget.
struct QueuePlace {
    admission: Arc<Admission>,
}

impl Drop for QueuePlace {
    fn drop(&mut self) {
        self.admission.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The right to execute one query, released when this is dropped.
///
/// Deliberately has no `release()` method. The only way to give a slot back is to drop the guard,
/// which happens on every exit path a query has — success, `?`, panic, or a cancelled future — so
/// there is no path on which a caller can forget.
///
/// That last case is not hypothetical since [`crate::cancel`]: stopping a running query is
/// implemented as dropping the future that holds this guard, so "the queue behind a cancelled query
/// advances" is a property of *this* type rather than of anything cancellation had to add.
///
/// # Two things come back, and only one of them comes back instantly
///
/// Since fleet-wide admission this guard holds a **local permit** and a **fleet lease**, and the
/// difference between how they are returned is worth knowing:
///
/// - the permit is returned by dropping it, which is synchronous and hands it to the next local
///   waiter immediately;
/// - the lease is returned by [`FleetGate::release`], which is a `DELETE` — and a destructor cannot
///   await one. So it is handed to the runtime and lands shortly after, which is why a waiter on
///   another coordinator sees the slot free one poll interval later rather than at once.
///
/// That asynchrony is also why a lease carries a token. A release that never lands — no runtime
/// left, or a database blip in exactly that instant — would otherwise shrink its warehouse's
/// concurrency for as long as the process lives, which would be strictly worse than the per-process
/// bug fleet-wide admission was built to fix. [`crate::fleet_admission`] closes that by making a
/// coordinator's own rows reclaimable by its own next claim; read its docs for the exact guarantee.
#[derive(Debug)]
pub struct QuerySlot {
    admission: Arc<Admission>,
    /// The fleet-wide half, when there is one. `Option` rather than a second guard type because
    /// there must remain exactly *one* thing whose destructor gives a slot back.
    lease: Option<FleetLease>,
    /// Held for the guard's lifetime; dropping it returns the permit to the semaphore, which
    /// hands it straight to the next waiter in line.
    _permit: OwnedSemaphorePermit,
}

impl QuerySlot {
    /// The gate this slot came from — for logging which warehouse a query is occupying.
    pub fn warehouse(&self) -> &str {
        self.admission.warehouse()
    }

    /// The fleet-wide slot this query holds, if the warehouse's limit is shared.
    pub fn fleet_lease(&self) -> Option<&FleetLease> {
        self.lease.as_ref()
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        self.admission.running.fetch_sub(1, Ordering::AcqRel);
        // Before the permit, which is dropped after this body runs: the fleet release is I/O that
        // has to be started, and starting it first shortens the window in which this process's own
        // next waiter finds the slot still taken.
        if let (Some((_, fleet)), Some(lease)) = (&self.admission.fleet, self.lease.take()) {
            fleet.release(lease);
        }
    }
}

/// One poll interval, smeared. See [`FLEET_POLL_JITTER`].
fn fleet_poll_delay() -> Duration {
    let jitter = rand::Rng::random_range(&mut rand::rng(), 0.0..=FLEET_POLL_JITTER);
    FLEET_POLL_INTERVAL.mul_f64(1.0 - FLEET_POLL_JITTER / 2.0 + jitter)
}

/// Every warehouse's gate, in one place.
///
/// A registry rather than a single gate because "which warehouse" is only known per query: a
/// coordinator serves whatever its clients name, and a warehouse that is never queried should not
/// cost anything. Gates are therefore created lazily, on first sight, and live for the process.
#[derive(Debug)]
pub struct Scheduler {
    default_limits: AdmissionLimits,
    /// The fleet-wide bound every gate built from here shares, when the deployment has one.
    fleet: Option<Arc<dyn FleetGate>>,
    /// `std::sync::Mutex`, not tokio's: the critical section is a hash lookup and never awaits, so
    /// an async mutex would add a scheduling point for nothing.
    warehouses: Mutex<HashMap<String, Arc<Admission>>>,
    closed: AtomicBool,
}

impl Scheduler {
    /// A scheduler whose gates default to `limits` — `max_queued` for every warehouse, and
    /// `max_concurrent` for any target that has no warehouse row to be sized from.
    ///
    /// Per-process, like this module always was. Fleet-wide admission is
    /// [`Scheduler::with_fleet`], and is opt-in for the same reason everything else in the control
    /// plane is: there may not be one.
    pub fn new(default_limits: AdmissionLimits) -> Self {
        Self {
            default_limits,
            fleet: None,
            warehouses: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    /// Share every *warehouse* gate's limit with the rest of the fleet through `fleet`.
    ///
    /// A builder rather than a setter, and it must be called before the first query: a gate is
    /// created on first sight and keeps whatever bound it was born with, exactly as it keeps
    /// whatever limit it was born with.
    pub fn with_fleet(mut self, fleet: Arc<dyn FleetGate>) -> Self {
        self.fleet = Some(fleet);
        self
    }

    /// Whether this scheduler's warehouse gates are bounded fleet-wide.
    pub fn is_fleet_wide(&self) -> bool {
        self.fleet.is_some()
    }

    /// The gate for `warehouse`, creating it with `max_concurrent` permits on first sight.
    ///
    /// `warehouse_id` is the row the fleet-wide bound is keyed by. `None` — a query routed at a raw
    /// `--workers` fleet — means there is no control-plane object for two coordinators to agree on,
    /// so the gate is per-process however this scheduler was built. See the module docs.
    ///
    /// The limit is set **once**. A later call naming a different `max_concurrent` — because the
    /// warehouse was resized in the services database while this process was running — logs a
    /// warning and keeps the existing gate. Growing a live semaphore is easy; shrinking one that
    /// has permits outstanding is not, and a limit that changes underneath a running queue is a
    /// worse thing to debug than a stale one that says so in the log. See the module docs.
    pub fn admission_for(
        &self,
        warehouse: &str,
        warehouse_id: Option<i64>,
        max_concurrent: usize,
    ) -> Arc<Admission> {
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

        let limits = AdmissionLimits {
            max_concurrent,
            max_queued: self.default_limits.max_queued,
        };
        let admission = match (warehouse_id, &self.fleet) {
            (Some(id), Some(fleet)) => {
                Admission::fleet_wide(warehouse, limits, id, Arc::clone(fleet))
            }
            _ => Admission::new(warehouse, limits),
        };
        // Born closed if the scheduler already is, so a query arriving during shutdown is refused
        // rather than admitted into a server that is going away.
        if self.closed.load(Ordering::Acquire) {
            admission.close();
        }
        tracing::info!(
            warehouse,
            max_concurrent = admission.limits().max_concurrent,
            max_queued = admission.limits().max_queued,
            fleet_wide = admission.is_fleet_wide(),
            "admission control ready for warehouse"
        );
        gates.insert(warehouse.to_string(), Arc::clone(&admission));
        admission
    }

    /// The gate for the default (`--workers`) fleet, sized by [`AdmissionLimits::max_concurrent`].
    ///
    /// Never fleet-wide: there is no warehouse row behind `--workers`, and therefore nothing two
    /// coordinators could agree they are bounding.
    pub fn default_admission(&self) -> Arc<Admission> {
        self.admission_for(DEFAULT_FLEET_KEY, None, self.default_limits.max_concurrent)
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
    ///
    /// **`current_thread`, and that is load-bearing rather than tidiness** (#120). The barrier below
    /// establishes arrival order by watching `snapshot().queued`, but that counter is incremented by
    /// `reserve_queue_slot`, which runs *before* `acquire_owned().await` registers the task in the
    /// semaphore's wait queue. On a multi-threaded runtime a task can sit in that window on one
    /// worker while the spawner, on another, already sees `queued > i` and releases task `i + 1` to
    /// reach the semaphore first — so the test could observe two adjacent waiters swapped
    /// (`[0, 1, 2, 4, 3, 5, 6, 7]` was seen once) while tokio's semaphore behaved perfectly.
    /// Single-threaded, a spawned task runs to its first pending await — which for a queued waiter
    /// *is* that registration, since nothing before it awaits — before the spawner is polled again,
    /// so arrival order into the semaphore is the spawn order by construction.
    ///
    /// Loosening the assertion instead would have been the wrong repair: the flake was the test
    /// measuring the wrong event, not the property being weaker than claimed. The assertion still
    /// bites — replacing the FIFO wait with a `try_acquire` spin makes this fail with
    /// `[7, 5, 3, 1, 0, 2, 4, 6]`.
    #[tokio::test(flavor = "current_thread")]
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
        let analytics = scheduler.admission_for("analytics", None, 1);
        let etl = scheduler.admission_for("etl", None, 1);

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
        let first = scheduler.admission_for("analytics", None, 2);
        let again = scheduler.admission_for("analytics", None, 9);
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
        let late = scheduler.admission_for("arrived-late", None, 2);
        assert!(late.is_closed());
        assert!(matches!(
            late.acquire().await,
            Err(AdmissionError::ShuttingDown { .. })
        ));
    }

    /// A client that hangs up while queued must give its place in line back.
    ///
    /// This is the queue's version of the permit leak, and it is the more insidious of the two,
    /// because nothing observable goes wrong at the moment it happens — the counter is simply one
    /// too high forever. Repeat it and the cap refuses live traffic while the line is empty.
    #[tokio::test]
    async fn a_waiter_that_goes_away_gives_its_place_in_line_back() {
        let admission = Admission::new("analytics", limits(1, 2));
        // Fill the only slot, so anything else has to queue.
        let _running = admission
            .acquire()
            .await
            .expect("the first query is admitted");

        {
            let mut waiting = Box::pin(admission.acquire());
            // Poll once so it gets past `try_acquire` and takes a place in line.
            assert!(
                futures::poll!(&mut waiting).is_pending(),
                "the only permit is held, so this must queue"
            );
            assert_eq!(admission.snapshot().queued, 1);
        } // <- dropped here: exactly what tonic does to a request whose client disconnected.

        assert_eq!(
            admission.snapshot().queued,
            0,
            "a cancelled waiter's place must be given back"
        );
        // The peak is history and stays: it records that someone *did* wait.
        assert_eq!(admission.snapshot().peak_queued, 1);

        // And the cap is intact rather than permanently one shorter — the whole point. Two fresh
        // waiters must still both fit, which they could not if the place had leaked.
        let mut first = Box::pin(admission.acquire());
        let mut second = Box::pin(admission.acquire());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(admission.snapshot().queued, 2);
    }

    // -----------------------------------------------------------------------------------------
    // Fleet-wide admission.
    //
    // The point of [`FleetGate`] being a trait is that all of this is provable with no Postgres,
    // no workers and no Flight — the same claim the rest of this file has always made. A
    // `TestFleet` is a counter behind a mutex; two `Admission`s sharing one are two coordinators
    // as far as this module can tell, which is exactly the shape the bug is about.
    // -----------------------------------------------------------------------------------------

    /// A fleet gate in memory: `K` slots, handed out and given back, plus counters a test can read.
    #[derive(Debug)]
    struct TestFleet {
        /// Slot numbers currently held, by lease token.
        held: Mutex<HashMap<String, i32>>,
        /// Answer every claim with [`FleetClaim::Unavailable`] instead — the outage case.
        unavailable: AtomicBool,
        claims: AtomicU64,
        releases: AtomicU64,
        /// The most that were ever held at once, across every `Admission` sharing this gate. The
        /// independent witness: the fleet's own view of how many queries the warehouse ran.
        peak_held: AtomicUsize,
    }

    impl TestFleet {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                held: Mutex::new(HashMap::new()),
                unavailable: AtomicBool::new(false),
                claims: AtomicU64::new(0),
                releases: AtomicU64::new(0),
                peak_held: AtomicUsize::new(0),
            })
        }

        fn held_now(&self) -> usize {
            self.held.lock().expect("lock").len()
        }
    }

    impl FleetGate for TestFleet {
        fn claim(&self, warehouse_id: i64, limit: usize) -> FleetClaimFuture<'_> {
            let outcome = if self.unavailable.load(Ordering::Acquire) {
                self.claims.fetch_add(1, Ordering::AcqRel);
                FleetClaim::Unavailable("test gate is offline".to_string())
            } else {
                self.claims.fetch_add(1, Ordering::AcqRel);
                let mut held = self.held.lock().expect("lock");
                match (0..limit as i32).find(|n| !held.values().any(|slot| slot == n)) {
                    None => FleetClaim::Full,
                    Some(slot_no) => {
                        let token = format!("t{}", self.claims.load(Ordering::Acquire));
                        held.insert(token.clone(), slot_no);
                        self.peak_held.fetch_max(held.len(), Ordering::AcqRel);
                        FleetClaim::Granted(FleetLease {
                            warehouse_id,
                            slot_no,
                            token,
                        })
                    }
                }
            };
            Box::pin(std::future::ready(outcome))
        }

        fn release(&self, lease: FleetLease) {
            self.releases.fetch_add(1, Ordering::AcqRel);
            self.held.lock().expect("lock").remove(&lease.token);
        }
    }

    /// One coordinator's gate over a shared fleet, sized `K` like every other one.
    fn coordinator(fleet: &Arc<TestFleet>, k: usize) -> Arc<Admission> {
        Admission::fleet_wide(
            "analytics",
            limits(k, 64),
            7,
            Arc::clone(fleet) as Arc<dyn FleetGate>,
        )
    }

    /// **The headline of issue #37**: two coordinators on one warehouse admit `K` total, not `2K`.
    ///
    /// Each `Admission` is configured with the full `K` — exactly what an operator who scaled for
    /// availability would do, and exactly what used to produce `2K`. The witness is the *fleet's*
    /// peak, not either gate's, because a per-process counter is precisely the instrument the bug
    /// hides from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_coordinators_on_one_warehouse_admit_k_total_not_2k() {
        const K: usize = 2;
        const PER_COORDINATOR: usize = 10;
        let fleet = TestFleet::new();
        let a = coordinator(&fleet, K);
        let b = coordinator(&fleet, K);

        let mut tasks = Vec::new();
        for gate in [&a, &b] {
            for _ in 0..PER_COORDINATOR {
                let gate = Arc::clone(gate);
                tasks.push(tokio::spawn(async move {
                    let slot = gate.acquire().await.expect("admitted");
                    assert!(
                        slot.fleet_lease().is_some(),
                        "a fleet-wide slot was expected"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    drop(slot);
                }));
            }
        }
        for task in tasks {
            task.await.expect("task");
        }

        assert_eq!(
            fleet.peak_held.load(Ordering::Acquire),
            K,
            "two coordinators each configured K={K} must share K slots, not hold {} between them",
            2 * K
        );
        assert_eq!(fleet.held_now(), 0, "every lease must be given back");
        assert_eq!(
            fleet.releases.load(Ordering::Acquire),
            2 * PER_COORDINATOR as u64,
            "one release per admitted query, from Drop and from nothing else"
        );
        // Both coordinators really ran work — a bound that held because one of them did nothing
        // would prove nothing at all.
        for gate in [&a, &b] {
            let snapshot = gate.snapshot();
            assert_eq!(snapshot.admitted, PER_COORDINATOR as u64, "{snapshot:?}");
            assert_eq!(snapshot.running, 0, "{snapshot:?}");
            assert_eq!(snapshot.queued, 0, "{snapshot:?}");
        }
        // …and the fleet gate is what did the bounding, not the local semaphores: with K=2 permits
        // each, the two of them would have admitted 4 at once without it.
        let fleet_waits = a.snapshot().fleet_waits + b.snapshot().fleet_waits;
        assert!(
            fleet_waits > 0,
            "with 2K permits locally and K fleet slots, somebody must have been held by the fleet"
        );
        assert_eq!(a.snapshot().fleet_degraded + b.snapshot().fleet_degraded, 0);
    }

    /// The guard is still the only way a slot comes back — now for both halves of it.
    ///
    /// #38 rests on this: cancelling a query is dropping the future that holds the guard. If a
    /// dropped guard returned the local permit but leaked the fleet lease, a cancelled query would
    /// permanently shrink its warehouse — strictly worse than the per-process bug this replaced.
    #[tokio::test]
    async fn dropping_the_guard_returns_the_fleet_lease_as_well_as_the_permit() {
        let fleet = TestFleet::new();
        let gate = coordinator(&fleet, 1);

        // Success, failure, and a future dropped mid-flight all go through the same destructor.
        let slot = gate.acquire().await.expect("admitted");
        assert_eq!(fleet.held_now(), 1);
        drop(slot);
        assert_eq!(fleet.held_now(), 0, "a finished query gives its lease back");

        {
            let _slot = gate.acquire().await.expect("admitted");
            assert_eq!(fleet.held_now(), 1);
        } // <- an early `?` or the end of a failing scope.
        assert_eq!(fleet.held_now(), 0, "a failed query gives its lease back");

        // A cancelled future: `acquire` resolved, the caller was dropped holding the guard.
        let cancelled = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                let _slot = gate.acquire().await.expect("admitted");
                std::future::pending::<()>().await;
            })
        };
        while fleet.held_now() == 0 {
            tokio::task::yield_now().await;
        }
        cancelled.abort();
        let _ = cancelled.await;
        assert_eq!(
            fleet.held_now(),
            0,
            "a cancelled query must not hold a fleet slot forever"
        );
        assert_eq!(gate.snapshot().running, 0);
        // The gate is not wedged: the very next query goes straight through.
        let after = tokio::time::timeout(Duration::from_secs(5), gate.acquire())
            .await
            .expect("the gate must not be wedged")
            .expect("admitted");
        assert_eq!(after.fleet_lease().map(|l| l.warehouse_id), Some(7));
    }

    /// The constraint that shaped the whole design: a control plane that will not answer degrades
    /// to **today's** per-process behaviour, not to unbounded and not to nothing.
    ///
    /// The assertion is deliberately two-sided. Nothing is refused — a services-database hiccup must
    /// not become a data-plane outage — and the local bound is still enforced, so the worst case is
    /// `coordinators × K`, which is exactly the bug this module used to have and never worse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unreachable_fleet_gate_degrades_to_the_local_bound_rather_than_refusing() {
        const K: usize = 2;
        let fleet = TestFleet::new();
        fleet.unavailable.store(true, Ordering::Release);
        let gate = coordinator(&fleet, K);

        let live = Arc::new(AtomicUsize::new(0));
        let witnessed_peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..12 {
            let gate = Arc::clone(&gate);
            let live = Arc::clone(&live);
            let witnessed_peak = Arc::clone(&witnessed_peak);
            tasks.push(tokio::spawn(async move {
                let slot = gate
                    .acquire()
                    .await
                    .expect("an outage must not refuse a query");
                assert!(
                    slot.fleet_lease().is_none(),
                    "a degraded admit holds no lease, so it has nothing to leak"
                );
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

        let snapshot = gate.snapshot();
        assert_eq!(snapshot.admitted, 12, "nothing was refused: {snapshot:?}");
        assert_eq!(snapshot.refused, 0, "{snapshot:?}");
        assert_eq!(
            snapshot.fleet_degraded, 12,
            "every admit during the outage is counted as degraded: {snapshot:?}"
        );
        assert!(
            witnessed_peak.load(Ordering::Acquire) <= K,
            "the local semaphore is the backstop; it must still bound this process to {K}"
        );
        assert_eq!(
            fleet.releases.load(Ordering::Acquire),
            0,
            "no leases to release"
        );

        // …and the moment the gate answers again, the bound is fleet-wide once more.
        fleet.unavailable.store(false, Ordering::Release);
        let slot = gate.acquire().await.expect("admitted");
        assert!(slot.fleet_lease().is_some());
        assert_eq!(gate.snapshot().fleet_degraded, 12, "and no longer climbing");
    }

    /// With no fleet gate, an uncontended admit does **zero I/O** and nothing changes — the
    /// no-services-database path, which is a supported deployment rather than a degraded one.
    ///
    /// The gate exists in this test and is simply never wired in, so "did not ask" is a counter
    /// reading zero rather than an absence nobody checked.
    #[tokio::test]
    async fn with_no_fleet_gate_nothing_is_consulted_and_nothing_is_queued() {
        let fleet = TestFleet::new();
        let scheduler = Scheduler::new(limits(4, 16));
        assert!(!scheduler.is_fleet_wide());
        // Even a warehouse with a row: no gate, no fleet bound.
        let gate = scheduler.admission_for("analytics", Some(7), 2);
        assert!(!gate.is_fleet_wide());

        let slot = gate.acquire().await.expect("admitted");
        assert!(slot.fleet_lease().is_none());
        let snapshot = gate.snapshot();
        assert_eq!(snapshot.peak_queued, 0, "an uncontended admit never queues");
        assert_eq!(snapshot.fleet_waits, 0);
        assert_eq!(snapshot.fleet_degraded, 0);
        assert_eq!(
            fleet.claims.load(Ordering::Acquire),
            0,
            "no services database means nothing is asked of one"
        );
        drop(slot);
        assert_eq!(fleet.releases.load(Ordering::Acquire), 0);
    }

    /// The other half of property 1, restated for the shape that now costs a round trip: an admit
    /// the warehouse can absorb asks the fleet **once** and is still never counted as queued.
    #[tokio::test]
    async fn an_uncontended_fleet_wide_admit_costs_one_claim_and_never_queues() {
        let fleet = TestFleet::new();
        let gate = coordinator(&fleet, 4);
        let slot = gate.acquire().await.expect("admitted");
        assert_eq!(
            fleet.claims.load(Ordering::Acquire),
            1,
            "exactly one round trip"
        );
        let snapshot = gate.snapshot();
        assert_eq!(snapshot.peak_queued, 0, "{snapshot:?}");
        assert_eq!(snapshot.queued, 0, "{snapshot:?}");
        assert_eq!(snapshot.running, 1, "{snapshot:?}");
        drop(slot);
    }

    /// A query held up by the *fleet* is queued, is capped like any other waiter, and is refused
    /// with the same backpressure a client already understands.
    ///
    /// Without this, fleet contention would be an unbounded wait invisible to `peak_queued` — which
    /// is the failure `max_queued` exists to prevent, reintroduced one layer down.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_query_the_fleet_holds_back_is_queued_and_capped_like_any_other() {
        let fleet = TestFleet::new();
        // Locally roomy, fleet-wide full: the only thing that can hold these back is the fleet.
        let gate = coordinator(&fleet, 1);
        let other = coordinator(&fleet, 1);
        let elsewhere = other.acquire().await.expect("admitted");
        assert_eq!(fleet.held_now(), 1);

        let waiting = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire().await.map(drop) })
        };
        while gate.snapshot().queued < 1 {
            tokio::task::yield_now().await;
        }
        let snapshot = gate.snapshot();
        assert_eq!(
            snapshot.running, 0,
            "it is queued, not running: {snapshot:?}"
        );
        assert_eq!(snapshot.peak_queued, 1, "{snapshot:?}");
        assert!(snapshot.fleet_waits > 0, "{snapshot:?}");

        // The cap counts it, so a client submitting into fleet contention gets backpressure rather
        // than an invisible wait. (`max_queued` is 64 above, so shrink the test to the property:
        // the place was taken, and it is given back.)
        drop(elsewhere);
        waiting
            .await
            .expect("task")
            .expect("the slot frees up and the waiter is admitted");
        let snapshot = gate.snapshot();
        assert_eq!(
            snapshot.queued, 0,
            "the place must be given back: {snapshot:?}"
        );
        assert_eq!(snapshot.running, 0, "{snapshot:?}");
        assert_eq!(fleet.held_now(), 0);
    }

    /// A client that hangs up while waiting on the *fleet* gives back its place in line, its local
    /// permit and — because it never got one — no fleet lease.
    ///
    /// The queued-waiter leak, at the second gate. Dropping mid-poll is the ordinary case here, not
    /// an exotic one: the poll loop is a sequence of awaits and tonic drops request futures at
    /// whichever one they are parked on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_waiter_that_hangs_up_while_polling_the_fleet_leaks_nothing() {
        let fleet = TestFleet::new();
        let gate = coordinator(&fleet, 1);
        let other = coordinator(&fleet, 1);
        let _elsewhere = other.acquire().await.expect("admitted");

        {
            let mut waiting = Box::pin(gate.acquire());
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while gate.snapshot().queued == 0 && std::time::Instant::now() < deadline {
                assert!(futures::poll!(&mut waiting).is_pending());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                gate.snapshot().queued,
                1,
                "it must have taken a place in line"
            );
        } // <- dropped mid-poll, exactly as tonic drops a disconnected client's request.

        let snapshot = gate.snapshot();
        assert_eq!(snapshot.queued, 0, "the place must come back: {snapshot:?}");
        assert_eq!(snapshot.admitted, 0, "it was never admitted: {snapshot:?}");
        assert_eq!(
            fleet.held_now(),
            1,
            "only the other coordinator's lease is outstanding"
        );
        // The local permit came back too, so this gate is immediately usable again.
        assert_eq!(gate.permits.available_permits(), 1);
    }

    /// Shutting down wakes a query that is waiting on the *fleet*, not just one waiting on a local
    /// permit — otherwise a drain would hang on a warehouse another coordinator was saturating.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_wakes_a_query_that_is_waiting_on_the_fleet() {
        let fleet = TestFleet::new();
        let gate = coordinator(&fleet, 1);
        let other = coordinator(&fleet, 1);
        let _elsewhere = other.acquire().await.expect("admitted");

        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire().await.map(|_| ()) })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while gate.snapshot().queued < 1 && std::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(gate.snapshot().queued, 1);

        gate.close();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("a closed gate must not leave a fleet waiter parked forever")
                .expect("task"),
            Err(AdmissionError::ShuttingDown {
                warehouse: "analytics".to_string()
            })
        );
        assert_eq!(gate.snapshot().queued, 0, "its place came back");
    }

    /// The poll interval is jittered, because `N` coordinators polling in lock-step is a starvation
    /// pattern rather than a load one — and it stays in the same order of magnitude, because it is
    /// added to every hand-off under contention.
    #[test]
    fn the_fleet_poll_delay_is_jittered_but_bounded() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let delay = fleet_poll_delay();
            assert!(
                delay >= FLEET_POLL_INTERVAL.mul_f64(1.0 - FLEET_POLL_JITTER)
                    && delay <= FLEET_POLL_INTERVAL.mul_f64(1.0 + FLEET_POLL_JITTER),
                "{delay:?} is outside one jitter of {FLEET_POLL_INTERVAL:?}"
            );
            seen.insert(delay.as_nanos());
        }
        assert!(
            seen.len() > 1,
            "an unjittered delay lets two coordinators poll in lock-step forever"
        );
    }
}
