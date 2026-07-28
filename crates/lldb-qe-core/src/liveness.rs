//! **Coordinator liveness** — the one place the fleet answers "is coordinator `X` still alive?".
//!
//! # Why this exists at all
//!
//! Nothing in this system could tell a dead coordinator from a slow one. [`crate::query_log`] says
//! so in its own module docs — *"treat a `running` row from a coordinator that is no longer alive
//! as unknown, not as running"* — and then has no way to find out which it is. Migration `0004`
//! added `queries.coordinator` explicitly so a future reaper could find a dead coordinator's rows,
//! a forward reference with nothing behind it.
//!
//! Two separate pieces of work need the answer before either can be built: reaping stranded query
//! rows, and fleet-wide admission control (a slot held by a coordinator that died has to be
//! released by *something*). Two implementations of "alive" would disagree about where the line is,
//! and nothing in the build would notice. So the model is decided once, here, and the four
//! decisions it turns on are written down below rather than only in the issue that asked for them.
//!
//! # Decision 1 — what a coordinator registers, and what its identity *is*
//!
//! A row in `coordinators`, keyed by a **pair**: a stable `slot` and a per-process `incarnation`.
//! The pair exists because the identity this repo already had is ambiguous in two opposite
//! directions at once. `queries.coordinator` comes from `--coordinator-id`, which defaults to the
//! bound socket address, so:
//!
//! - a coordinator that restarts on a different port is a brand-new coordinator as far as history
//!   is concerned, though it is the same deployment slot doing the same job; and
//! - a coordinator that restarts onto the *same* address inherits the previous process's identity —
//!   and its in-flight rows — without having run any of them.
//!
//! A design assuming that id is stable across restarts is therefore wrong, and one assuming it is
//! unique per process is *also* wrong. [`CoordinatorIdentity`] fixes both by refusing to conflate
//! them: `slot` is the operator's stable name and survives a restart; `incarnation` is 128 bits of
//! CSPRNG minted at startup, never reused, and deliberately does not. A restart reads as a restart —
//! not as a new coordinator and not as an uninterrupted continuation.
//!
//! The pair is only useful if the rows a coordinator writes carry it, so
//! `queries.coordinator_incarnation` records the writing *process* beside the slot. Without that
//! column, a reaper looking at a stranded row would see a live registration for the slot and
//! conclude the row is live, when it in fact belongs to a process that died and was replaced —
//! precisely the case that has to be got right.
//!
//! What is **not** registered: a warehouse. The issue that asked for this suggested one, and it is
//! wrong for this codebase — `lldb-qe-server` serves whatever warehouse a request names, so a
//! coordinator has no warehouse, it has a set of them that changes per query. Nor an account, for
//! the same reason: a coordinator belongs to the deployment, not to a tenant.
//!
//! # Decision 2 — how it renews, and what a failed renewal does to the process
//!
//! A background task renews on a fixed interval with a conditional `UPDATE ... WHERE slot = $1 AND
//! incarnation = $2` — the same CAS shape [`crate::dml`] commits a snapshot with, for the same
//! reason: one row, one serialization point, and the loser learns it lost from `rows_affected == 0`
//! rather than from an error.
//!
//! The interesting part is the failure branch, and there are two distinct failures behind that zero:
//!
//! - **The database would not answer.** The renewal is counted as failed, logged, and *retried at
//!   the same interval*; the process keeps serving. This is the deliberate choice and it is the one
//!   worth arguing about. Refusing to serve would convert every control-plane hiccup into a
//!   data-plane outage, in a codebase whose standing rule is the opposite: history writes after
//!   acceptance are best effort so "a query that is already executing must not be killed because the
//!   services database hiccuped" ([`crate::server::Coordinator::run_query`]), and the result cache's
//!   rule is that not caching is always a legal answer. Continuing is not free, and the honest cost
//!   is named rather than hidden: past the threshold this coordinator's rows *may* be concluded dead
//!   by whoever reads this table, so [`CoordinatorRegistration::is_stale`] goes true and the log
//!   line moves from `warn` to `error`. What that costs today is history rows being resolved
//!   pessimistically — the queries themselves still answer their clients correctly. If a later
//!   issue makes liveness load-bearing for something a *client* can observe (fleet-wide admission
//!   handing out a slot this coordinator still holds), that issue owns re-deciding this, and this
//!   paragraph is the thing it has to argue against.
//! - **Another process took the slot.** Zero rows and the row still exists under a different
//!   incarnation means two processes are configured with one `--coordinator-id`. Re-registering
//!   would make them flap the slot between each other forever, so this one *stops* renewing, sets
//!   [`CoordinatorRegistration::is_evicted`], and logs an error naming both incarnations. The
//!   process keeps serving, for the reason above.
//!
//! Telling those two apart needs a read, because a conditional `UPDATE` cannot distinguish "the row
//! is gone" from "the row is someone else's" — both update zero rows. That is exactly the
//! limitation [`crate::warehouse`]'s `transition_warehouse` documents, and the resolution here is
//! the cheaper half of the same idea: re-read the row and branch on what is actually there. A row
//! that is simply *absent* (an operator cleaned the table) is benign and re-registers.
//!
//! # Decision 3 — what "not seen recently" means
//!
//! `last_seen_at` older than [`MISSED_RENEWALS_BEFORE_DEAD`] × the coordinator's **own** renewal
//! interval. Two consequences, both intentional:
//!
//! - **There is no threshold setting.** Not a defaulted one, not an overridable one. Two knobs would
//!   let an operator configure a threshold shorter than the renewal it is judging, and the failure
//!   mode of that is reaping the queries of coordinators that are working perfectly. One knob
//!   (`renew_interval`) cannot be inconsistent with itself.
//! - **The interval is stored per row**, so a reader judges each coordinator by the cadence that
//!   coordinator actually renews at, not by the reader's own configuration. A fleet mid-rollout with
//!   two intervals in play is judged correctly rather than approximately.
//!
//! The bound this buys, stated so it can be tested: a coordinator killed outright stops renewing
//! immediately, so its `last_seen_at` freezes at its final renewal and it is observably not-live
//! within **[`MISSED_RENEWALS_BEFORE_DEAD`] × the renewal interval** of that renewal — at most that
//! long after the kill, and at least (`MISSED_RENEWALS_BEFORE_DEAD` − 1) × interval. A coordinator
//! that exits *cleanly* does not wait for any of that: [`CoordinatorRegistration::shut_down`] stamps
//! `shutdown_at` and the row is not live on the very next read.
//!
//! # Decision 4 — who evaluates it
//!
//! Not a coordinator, and specifically not a coordinator at startup. A process that swept for dead
//! peers as it booted would be doing the most dangerous possible version of this: a fleet restarting
//! together, each member evaluating the others through a lease that none of them have renewed yet,
//! all of them concluding the others are dead. Two coordinators reaping each other's live queries is
//! the failure this decision exists to prevent.
//!
//! So the evaluation lives on the *reader* side and this module ships only the predicate —
//! [`ServicesDb::is_coordinator_live`], [`ServicesDb::live_coordinators`],
//! [`ServicesDb::list_coordinators`]. Whatever acts on the answer runs out of process, as a one-shot
//! in the style of `lldb-qe-migrate`, which is how this repo already treats anything that mutates
//! state the whole fleet shares. Nothing here writes to `queries`.
//!
//! [`crate::reaper`] is the first thing to take that answer up, as `lldb-qe-reap`. It is worth
//! reading as the proof that this shape holds: the predicate is used verbatim (`LIVE_PREDICATE` is
//! spliced into its statement rather than re-spelled), the `(slot, incarnation)` pair is what makes
//! its decision correct rather than plausible, and the resulting sweep is a one-shot binary that no
//! coordinator ever calls.
//!
//! The one thing a coordinator does with the predicate is *log* it, once, at startup: how many peers
//! the control plane believes are live. That is a read, it cannot reap anything, and it makes the
//! "admission control is per coordinator process" caveat visible in an operator's logs at the moment
//! it starts being true.
//!
//! # No services database is still legal, and that is load-bearing
//!
//! [`crate::services::ServicesArgs::connect`] returning `None` is a supported deployment, not a
//! degraded one. With no control plane there is nothing to register with, nobody to reap and no
//! fleet to coordinate, so the correct behaviour is *exactly what it was before this module existed*:
//! no row, no background task, no warning per query. [`CoordinatorRegistration::start_if_configured`]
//! is the whole of that rule — `None` in, `None` out, nothing spawned — and it is a function rather
//! than an `if` at the call site so the rule can be tested without a database.
//!
//! Note what is *not* here as a result: liveness is never consulted on the path that admits or runs
//! a query. [`crate::scheduler`] does not know this module exists, which is what keeps its bound,
//! its fairness and its release-on-failure behaviour provable with no Postgres, no workers and no
//! Flight. "Check the lease, then admit" is the natural shape and it is the wrong one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::BUILD_VERSION;
use crate::services::ServicesDb;

/// How often a coordinator renews its registration, when nothing says otherwise.
///
/// Ten seconds is a compromise between two costs that pull in opposite directions: one round trip
/// per interval per coordinator against the [`MISSED_RENEWALS_BEFORE_DEAD`] multiple of it that a
/// dead coordinator's rows stay unresolved. At ten seconds a killed coordinator is detectable within
/// thirty, which is well inside the time a human takes to notice, and the write load is negligible
/// next to a single query's history writes.
pub const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// How many renewals a coordinator may miss before it is concluded dead.
///
/// Three, not one: a single missed renewal is a garbage-collection pause, a slow query holding the
/// runtime, or one lost packet, and concluding death from it would mean declaring healthy
/// coordinators dead routinely. Three consecutive misses is a process that is not coming back on any
/// timescale the rest of the system cares about.
///
/// This is a build constant on purpose. See the module docs, decision 3: an independently
/// configurable threshold can be set shorter than the renewal it judges, and that reaps the queries
/// of coordinators that are working perfectly.
pub const MISSED_RENEWALS_BEFORE_DEAD: u32 = 3;

/// How long since its last renewal a coordinator may go before it is not live.
///
/// A pure function of the renewal interval, which is the whole point — there is no second knob for
/// this to drift away from.
pub fn death_threshold(renew_interval: Duration) -> Duration {
    renew_interval * MISSED_RENEWALS_BEFORE_DEAD
}

/// Bytes of randomness behind an incarnation. Sixteen because it has to be unguessable *and*
/// non-colliding across every restart the deployment will ever perform, and 128 bits is the point
/// past which nobody has to think about either again.
const INCARNATION_BYTES: usize = 16;

/// Who a coordinator is: a **stable slot** plus a **per-process incarnation**.
///
/// The two are not interchangeable and the difference is the whole of decision 1 (see the module
/// docs). `slot` answers "which coordinator in the deployment", survives a restart, and is what
/// `queries.coordinator` has always held. `incarnation` answers "which *process*", is minted fresh
/// by [`CoordinatorIdentity::new`], and deliberately does not survive anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorIdentity {
    slot: String,
    incarnation: String,
}

impl CoordinatorIdentity {
    /// A fresh identity for `slot`, minting a new incarnation.
    ///
    /// Every call mints a different one, including two calls naming the same slot — that is the
    /// property that makes a restart onto the same address distinguishable from the process it
    /// replaced, and it holds whether or not the resulting identity is ever registered anywhere.
    pub fn new(slot: impl Into<String>) -> Self {
        let mut bytes = [0u8; INCARNATION_BYTES];
        // The same thread-local CSPRNG `crate::auth` mints API tokens from. This value is not a
        // secret — it is written to a table any operator can read — but it must never repeat, and
        // "never repeats" is exactly what a CSPRNG gives for free, unlike a timestamp or a pid.
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        Self {
            slot: slot.into(),
            incarnation: hex::encode(bytes),
        }
    }

    /// Rebuild an identity whose incarnation is already known — reading one back out of the
    /// database, or naming another process's.
    pub fn with_incarnation(slot: impl Into<String>, incarnation: impl Into<String>) -> Self {
        Self {
            slot: slot.into(),
            incarnation: incarnation.into(),
        }
    }

    /// The stable deployment identity. This is what `queries.coordinator` records.
    pub fn slot(&self) -> &str {
        &self.slot
    }

    /// The per-process identity. This is what `queries.coordinator_incarnation` records.
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

impl std::fmt::Display for CoordinatorIdentity {
    /// `slot#incarnation` — one token an operator can grep for in logs and match against either
    /// column. Deliberately *not* the stored form: the two halves are separate columns precisely so
    /// that "same slot, different process" is a query rather than a string comparison.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.slot, self.incarnation)
    }
}

/// One row of `coordinators` — what the control plane believes about a coordinator.
///
/// Fields are public because this is a record. Whether it is *live* is not one of them, because
/// liveness is a fact about the row and the clock together: ask [`CoordinatorRow::is_live_at`], or
/// let the database answer with [`ServicesDb::live_coordinators`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorRow {
    pub slot: String,
    pub incarnation: String,
    /// When this incarnation took the slot.
    pub registered_at: DateTime<Utc>,
    /// Last successful renewal, from the database's clock.
    pub last_seen_at: DateTime<Utc>,
    /// Set iff this coordinator exited cleanly.
    pub shutdown_at: Option<DateTime<Utc>>,
    /// The cadence this coordinator renews at — the unit its threshold is a multiple of.
    pub renew_interval_secs: i32,
    /// `version+sha` of the binary that registered.
    pub build_version: Option<String>,
}

impl CoordinatorRow {
    /// This row's own threshold: [`MISSED_RENEWALS_BEFORE_DEAD`] × the interval *it* renews at.
    pub fn death_threshold(&self) -> Duration {
        death_threshold(Duration::from_secs(self.renew_interval_secs.max(1) as u64))
    }

    /// Whether this coordinator is live as of `now`.
    ///
    /// The same predicate [`ServicesDb::live_coordinators`] applies in SQL, kept here as well so a
    /// caller holding rows can filter them without a second round trip — and so the rule is
    /// expressible in a unit test with no database in sight.
    pub fn is_live_at(&self, now: DateTime<Utc>) -> bool {
        if self.shutdown_at.is_some() {
            return false;
        }
        match chrono::TimeDelta::from_std(self.death_threshold()) {
            Ok(threshold) => self.last_seen_at > now - threshold,
            // Unreachable for any interval an `INTEGER` column can hold; refusing to guess beats
            // rounding a nonsense threshold into "alive".
            Err(_) => false,
        }
    }
}

/// The column list every coordinator lookup returns, in the order [`CoordinatorRowTuple`] expects.
const COORDINATOR_COLUMNS: &str = "slot, incarnation, registered_at, last_seen_at, shutdown_at, \
                                   renew_interval_secs, build_version";

/// The raw row shape, named so the `query_as` turbofishes stay readable.
type CoordinatorRowTuple = (
    String,
    String,
    DateTime<Utc>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    i32,
    Option<String>,
);

/// The liveness predicate, in SQL. `$1` is the missed-renewal multiple; the interval comes from the
/// row, so every coordinator is judged by its own cadence (decision 3).
///
/// Shared with [`crate::reaper`], which splices it into a correlated subquery over `queries` —
/// hence the table-qualified column names, which are redundant in this module's own single-table
/// statements and load-bearing there. Two spellings of "alive" would eventually disagree and
/// nothing in the build would notice, so there is exactly one.
pub(crate) const LIVE_PREDICATE: &str = "coordinators.shutdown_at IS NULL \
     AND coordinators.last_seen_at > now() - make_interval(secs => \
         coordinators.renew_interval_secs::DOUBLE PRECISION * $1)";

impl From<CoordinatorRowTuple> for CoordinatorRow {
    fn from(row: CoordinatorRowTuple) -> Self {
        let (
            slot,
            incarnation,
            registered_at,
            last_seen_at,
            shutdown_at,
            renew_interval_secs,
            build_version,
        ) = row;
        Self {
            slot,
            incarnation,
            registered_at,
            last_seen_at,
            shutdown_at,
            renew_interval_secs,
            build_version,
        }
    }
}

/// Seconds this interval renews at, as the column stores it. Clamped to at least one: the column's
/// `CHECK` refuses zero, and a sub-second interval rounding to zero would make the threshold zero
/// and every coordinator instantly dead.
fn interval_secs(renew_interval: Duration) -> i32 {
    renew_interval.as_secs().clamp(1, i32::MAX as u64) as i32
}

impl ServicesDb {
    /// Claim `identity`'s slot, or take it over from whoever held it.
    ///
    /// An upsert on the slot, not an insert: a coordinator restarting into the same deployment slot
    /// is the normal case, and the row it finds is its own predecessor's. Taking over rewrites the
    /// incarnation and clears `shutdown_at`, so the previous incarnation stops being live at the
    /// same instant this one starts — there is never a moment when two incarnations of one slot are
    /// both live, which is what makes "the slot is alive but this row's writer is gone" a question
    /// with one answer.
    pub async fn register_coordinator(
        &self,
        identity: &CoordinatorIdentity,
        renew_interval: Duration,
    ) -> Result<CoordinatorRow> {
        let row = sqlx::query_as::<_, CoordinatorRowTuple>(&format!(
            "INSERT INTO coordinators \
                 (slot, incarnation, renew_interval_secs, build_version) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (slot) DO UPDATE SET \
                 incarnation         = EXCLUDED.incarnation, \
                 registered_at       = now(), \
                 last_seen_at        = now(), \
                 shutdown_at         = NULL, \
                 renew_interval_secs = EXCLUDED.renew_interval_secs, \
                 build_version       = EXCLUDED.build_version \
             RETURNING {COORDINATOR_COLUMNS}"
        ))
        .bind(identity.slot())
        .bind(identity.incarnation())
        .bind(interval_secs(renew_interval))
        .bind(BUILD_VERSION)
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("registering coordinator `{identity}`"))?;
        Ok(CoordinatorRow::from(row))
    }

    /// Renew a registration. `false` means this incarnation no longer holds the slot — the row was
    /// deleted, or another process took it over.
    ///
    /// The conditional `UPDATE ... WHERE slot = ... AND incarnation = ...` is the CAS this whole
    /// mechanism rests on: it cannot renew a lease that is not ours, so a stale process cannot keep
    /// a slot alive on behalf of the one that replaced it. Distinguishing the two reasons for
    /// `false` needs a read — see [`CoordinatorRegistration`], which does it.
    pub async fn renew_coordinator(&self, identity: &CoordinatorIdentity) -> Result<bool> {
        let updated = sqlx::query(
            "UPDATE coordinators SET last_seen_at = now() WHERE slot = $1 AND incarnation = $2",
        )
        .bind(identity.slot())
        .bind(identity.incarnation())
        .execute(self.pool())
        .await
        .with_context(|| format!("renewing coordinator `{identity}`"))?
        .rows_affected();
        Ok(updated > 0)
    }

    /// Record that this coordinator exited cleanly. `false` if it no longer held the slot.
    ///
    /// The row is kept rather than deleted. Deleting would also make the coordinator promptly
    /// not-live, but it would throw away the only record that the slot ever existed — and an
    /// operator asking "did that coordinator stop, or did it die?" is asking a question the answer
    /// to which is precisely this timestamp.
    pub async fn deregister_coordinator(&self, identity: &CoordinatorIdentity) -> Result<bool> {
        let updated = sqlx::query(
            "UPDATE coordinators SET shutdown_at = now() \
             WHERE slot = $1 AND incarnation = $2 AND shutdown_at IS NULL",
        )
        .bind(identity.slot())
        .bind(identity.incarnation())
        .execute(self.pool())
        .await
        .with_context(|| format!("deregistering coordinator `{identity}`"))?
        .rows_affected();
        Ok(updated > 0)
    }

    /// The registration currently holding `slot`, whatever its state.
    pub async fn coordinator_by_slot(&self, slot: &str) -> Result<Option<CoordinatorRow>> {
        let row = sqlx::query_as::<_, CoordinatorRowTuple>(&format!(
            "SELECT {COORDINATOR_COLUMNS} FROM coordinators WHERE slot = $1"
        ))
        .bind(slot)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up coordinator slot `{slot}`"))?;
        Ok(row.map(CoordinatorRow::from))
    }

    /// Every registration this control plane has ever been told about, live or not, oldest first.
    pub async fn list_coordinators(&self) -> Result<Vec<CoordinatorRow>> {
        let rows = sqlx::query_as::<_, CoordinatorRowTuple>(&format!(
            "SELECT {COORDINATOR_COLUMNS} FROM coordinators ORDER BY registered_at, slot"
        ))
        .fetch_all(self.pool())
        .await
        .context("listing coordinators")?;
        Ok(rows.into_iter().map(CoordinatorRow::from).collect())
    }

    /// The coordinators that are live right now, by the rule in decision 3.
    ///
    /// Evaluated in the database, against the database's clock, deliberately: every `last_seen_at`
    /// was stamped by that clock, so comparing them against it is the only comparison that means
    /// anything when the coordinators' own clocks disagree.
    pub async fn live_coordinators(&self) -> Result<Vec<CoordinatorRow>> {
        let rows = sqlx::query_as::<_, CoordinatorRowTuple>(&format!(
            "SELECT {COORDINATOR_COLUMNS} FROM coordinators \
             WHERE {LIVE_PREDICATE} ORDER BY registered_at, slot"
        ))
        .bind(f64::from(MISSED_RENEWALS_BEFORE_DEAD))
        .fetch_all(self.pool())
        .await
        .context("listing live coordinators")?;
        Ok(rows.into_iter().map(CoordinatorRow::from).collect())
    }

    /// Is this coordinator live?
    ///
    /// `incarnation` is what makes the answer usable by a reaper. Pass `None` to ask about the
    /// **slot** ("is anything serving under this name"); pass `Some` to ask about the **process**
    /// ("is the thing that wrote this row still there"). Those are different questions with
    /// different answers exactly when it matters — a coordinator that died and was replaced on the
    /// same address answers `true` to the first and `false` to the second, and a row it left behind
    /// is stranded, not running.
    pub async fn is_coordinator_live(&self, slot: &str, incarnation: Option<&str>) -> Result<bool> {
        let live: Option<(i32,)> = sqlx::query_as(&format!(
            "SELECT 1 FROM coordinators \
             WHERE slot = $2 AND ($3::TEXT IS NULL OR incarnation = $3) AND {LIVE_PREDICATE}"
        ))
        .bind(f64::from(MISSED_RENEWALS_BEFORE_DEAD))
        .bind(slot)
        .bind(incarnation)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("checking whether coordinator `{slot}` is live"))?;
        Ok(live.is_some())
    }
}

/// Counters and clocks the renewal loop keeps, shared with the handle that owns it.
#[derive(Debug)]
struct RenewalState {
    /// When the loop started. `Instant` rather than a wall clock because everything read off this
    /// is an elapsed duration, and a wall clock that steps backwards would make one read negative.
    started: Instant,
    /// Milliseconds after `started` at which the last renewal succeeded. An integer rather than a
    /// `Mutex<Instant>` so the failure path — which runs inside a destructor-adjacent log line —
    /// never blocks.
    last_success_millis: AtomicU64,
    renewals: AtomicU64,
    failures: AtomicU64,
    evicted: AtomicBool,
    /// Set by [`CoordinatorRegistration::shut_down`], read by `Drop` — so the destructor of a
    /// handle that *was* shut down cleanly does not also report it as having been dropped.
    deregistered: AtomicBool,
}

impl RenewalState {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            last_success_millis: AtomicU64::new(0),
            renewals: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            evicted: AtomicBool::new(false),
            deregistered: AtomicBool::new(false),
        }
    }

    fn record_success(&self) {
        self.last_success_millis.store(
            self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::Release,
        );
        self.renewals.fetch_add(1, Ordering::AcqRel);
    }

    /// How long since the last renewal that actually landed. Registration counts as one, so this is
    /// measured from startup on a coordinator that has never yet renewed.
    fn since_last_success(&self) -> Duration {
        let last = Duration::from_millis(self.last_success_millis.load(Ordering::Acquire));
        self.started.elapsed().saturating_sub(last)
    }
}

/// A live registration: the row, plus the background task keeping it fresh.
///
/// Holding one of these is what makes a coordinator visible to the rest of the fleet. Dropping it
/// stops the renewals but does **not** deregister — that needs a round trip and `Drop` cannot await
/// — so a coordinator that wants to be promptly not-live must call [`Self::shut_down`]. A dropped
/// registration simply expires on the threshold, which is the same outcome as being killed, and is
/// logged as such so it does not look like a clean stop.
pub struct CoordinatorRegistration {
    db: ServicesDb,
    identity: CoordinatorIdentity,
    renew_interval: Duration,
    state: Arc<RenewalState>,
    task: tokio::task::JoinHandle<()>,
}

impl CoordinatorRegistration {
    /// Register and start renewing — but only if there is a control plane to register with.
    ///
    /// **This is the no-services-database rule, as a function.** `None` in, `None` out: no row, no
    /// task, no log line, nothing. It is not an `if` at the call site because that rule is the one
    /// an otherwise-reasonable implementation is most likely to break, and a rule in a function can
    /// be tested with no database present. See the module docs.
    pub async fn start_if_configured(
        db: Option<ServicesDb>,
        identity: CoordinatorIdentity,
        renew_interval: Duration,
    ) -> Result<Option<Self>> {
        match db {
            None => Ok(None),
            Some(db) => Self::start(db, identity, renew_interval).await.map(Some),
        }
    }

    /// Register and start renewing.
    ///
    /// The initial registration is awaited and its failure is returned, rather than being retried in
    /// the background: a configured services database that will not take a registration is a
    /// startup failure, exactly as `lldb-qe-server` already treats one that will not answer a health
    /// check. Every failure *after* this point is the background loop's, and is not fatal — see
    /// decision 2.
    pub async fn start(
        db: ServicesDb,
        identity: CoordinatorIdentity,
        renew_interval: Duration,
    ) -> Result<Self> {
        let row = db.register_coordinator(&identity, renew_interval).await?;
        let threshold = death_threshold(renew_interval);
        tracing::info!(
            slot = %identity.slot(),
            incarnation = %identity.incarnation(),
            renew_interval_secs = row.renew_interval_secs,
            death_threshold_secs = threshold.as_secs(),
            "registered this coordinator in the services database"
        );

        let state = Arc::new(RenewalState::new());
        let task = tokio::spawn(renewal_loop(
            db.clone(),
            identity.clone(),
            renew_interval,
            Arc::clone(&state),
        ));
        Ok(Self {
            db,
            identity,
            renew_interval,
            state,
            task,
        })
    }

    /// Who this registration says we are.
    pub fn identity(&self) -> &CoordinatorIdentity {
        &self.identity
    }

    /// The renewal cadence, and therefore (times [`MISSED_RENEWALS_BEFORE_DEAD`]) the threshold.
    pub fn renew_interval(&self) -> Duration {
        self.renew_interval
    }

    /// Renewals that landed, since registration.
    pub fn renewals(&self) -> u64 {
        self.state.renewals.load(Ordering::Acquire)
    }

    /// Renewals that could not be written — a services database that would not answer.
    pub fn failures(&self) -> u64 {
        self.state.failures.load(Ordering::Acquire)
    }

    /// True once another process has taken this slot. The renewal loop has stopped; the process is
    /// still serving. See decision 2.
    pub fn is_evicted(&self) -> bool {
        self.state.evicted.load(Ordering::Acquire)
    }

    /// True when the last successful renewal is older than the threshold — i.e. whoever reads
    /// `coordinators` is now entitled to conclude this coordinator is dead, while it demonstrably is
    /// not.
    ///
    /// This is the visible half of decision 2: a coordinator that keeps serving through a
    /// control-plane outage does not get to pretend nothing is wrong.
    pub fn is_stale(&self) -> bool {
        self.state.since_last_success() > death_threshold(self.renew_interval)
    }

    /// Stop renewing and record a clean exit, so this coordinator is not-live on the next read
    /// rather than in `MISSED_RENEWALS_BEFORE_DEAD` intervals' time.
    ///
    /// Best effort, and it consumes the handle: a coordinator that is on its way out must not fail
    /// its shutdown over a services-database error, and the worst case of not getting the write in
    /// is the threshold expiring normally.
    pub async fn shut_down(self) {
        self.task.abort();
        self.state.deregistered.store(true, Ordering::Release);
        match self.db.deregister_coordinator(&self.identity).await {
            Ok(true) => tracing::info!(
                slot = %self.identity.slot(),
                "deregistered this coordinator; it is no longer live"
            ),
            Ok(false) => tracing::debug!(
                slot = %self.identity.slot(),
                "this coordinator's registration was already gone at shutdown"
            ),
            Err(error) => tracing::warn!(
                slot = %self.identity.slot(),
                error = %format!("{error:#}"),
                "could not deregister this coordinator; its registration will expire on the threshold"
            ),
        }
    }
}

impl Drop for CoordinatorRegistration {
    /// Stop the renewal loop. Deliberately *not* a deregistration: that is a round trip and a
    /// destructor cannot await one. A registration dropped rather than shut down expires like a
    /// killed coordinator's, which is the honest outcome — so it says so, rather than leaving an
    /// operator to wonder why a process that stopped tidily took thirty seconds to disappear.
    fn drop(&mut self) {
        self.task.abort();
        if !self.state.deregistered.load(Ordering::Acquire)
            && !self.state.evicted.load(Ordering::Acquire)
        {
            tracing::debug!(
                slot = %self.identity.slot(),
                "coordinator registration dropped without a clean shutdown; it will expire on the \
                 liveness threshold"
            );
        }
    }
}

impl std::fmt::Debug for CoordinatorRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorRegistration")
            .field("identity", &self.identity)
            .field("renew_interval", &self.renew_interval)
            .field("renewals", &self.renewals())
            .field("failures", &self.failures())
            .field("evicted", &self.is_evicted())
            .finish()
    }
}

/// The renewal loop. Runs until the task is aborted, or until another process takes the slot.
async fn renewal_loop(
    db: ServicesDb,
    identity: CoordinatorIdentity,
    renew_interval: Duration,
    state: Arc<RenewalState>,
) {
    let threshold = death_threshold(renew_interval);
    // Tracks whether the "you may now be reaped" error has already been said, so a long outage
    // produces one error and then quiet warnings rather than an error per interval.
    let mut announced_stale = false;

    loop {
        tokio::time::sleep(renew_interval).await;

        match db.renew_coordinator(&identity).await {
            Ok(true) => {
                if announced_stale {
                    tracing::info!(
                        slot = %identity.slot(),
                        "coordinator registration renewed again; it is live once more"
                    );
                    announced_stale = false;
                }
                state.record_success();
            }
            Ok(false) => {
                // Zero rows means one of two very different things, and a conditional UPDATE cannot
                // tell them apart (the limitation `warehouse::transition_warehouse` documents). Read
                // the row and branch on what is actually there.
                match db.coordinator_by_slot(identity.slot()).await {
                    Ok(None) => {
                        tracing::warn!(
                            slot = %identity.slot(),
                            "this coordinator's registration has been deleted; re-registering"
                        );
                        match db.register_coordinator(&identity, renew_interval).await {
                            Ok(_) => state.record_success(),
                            Err(error) => {
                                state.failures.fetch_add(1, Ordering::AcqRel);
                                tracing::warn!(
                                    slot = %identity.slot(),
                                    error = %format!("{error:#}"),
                                    "could not re-register this coordinator"
                                );
                            }
                        }
                    }
                    Ok(Some(row)) if row.incarnation != identity.incarnation() => {
                        state.evicted.store(true, Ordering::Release);
                        tracing::error!(
                            slot = %identity.slot(),
                            ours = %identity.incarnation(),
                            theirs = %row.incarnation,
                            "another process has taken this coordinator's slot: two coordinators \
                             are configured with the same --coordinator-id (LLDB_COORDINATOR_ID). \
                             This one has stopped renewing and its queries may be treated as \
                             abandoned; give each coordinator a distinct id. Queries already \
                             running are unaffected and will finish."
                        );
                        return;
                    }
                    // Same incarnation but the UPDATE matched nothing: not reachable through this
                    // API, so treat it as the transient oddity it would have to be rather than
                    // asserting.
                    Ok(Some(_)) | Err(_) => {
                        state.failures.fetch_add(1, Ordering::AcqRel);
                        tracing::warn!(
                            slot = %identity.slot(),
                            "could not confirm this coordinator's registration; retrying"
                        );
                    }
                }
            }
            Err(error) => {
                state.failures.fetch_add(1, Ordering::AcqRel);
                let since = state.since_last_success();
                if since > threshold && !announced_stale {
                    announced_stale = true;
                    // Decision 2, said out loud: still serving, but no longer defensible as live.
                    tracing::error!(
                        slot = %identity.slot(),
                        seconds_since_last_renewal = since.as_secs(),
                        threshold_secs = threshold.as_secs(),
                        error = %format!("{error:#}"),
                        "this coordinator has not renewed its registration within the liveness \
                         threshold; the control plane may now treat its queries as abandoned. It \
                         is still serving — a services-database outage must not take a working \
                         coordinator down — and will re-register as soon as the database answers."
                    );
                } else {
                    tracing::warn!(
                        slot = %identity.slot(),
                        error = %format!("{error:#}"),
                        "could not renew this coordinator's registration; retrying"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(last_seen: DateTime<Utc>, interval_secs: i32) -> CoordinatorRow {
        CoordinatorRow {
            slot: "127.0.0.1:50050".to_string(),
            incarnation: "abcd".to_string(),
            registered_at: last_seen,
            last_seen_at: last_seen,
            shutdown_at: None,
            renew_interval_secs: interval_secs,
            build_version: Some("test".to_string()),
        }
    }

    /// Decision 1, and the property the whole design rests on: a restart onto the *same* slot is a
    /// different coordinator. Nothing here touches a database — the distinguishability is minted,
    /// not stored.
    #[test]
    fn two_identities_for_one_slot_are_never_the_same_process() {
        let first = CoordinatorIdentity::new("127.0.0.1:50050");
        let second = CoordinatorIdentity::new("127.0.0.1:50050");
        assert_eq!(first.slot(), second.slot(), "the slot is the stable half");
        assert_ne!(
            first.incarnation(),
            second.incarnation(),
            "a process restarting onto the same address must not inherit the old identity"
        );
        assert_ne!(first, second);
        // …and a restart onto a *different* address is still recognisably a different slot, which
        // is the other half of the ambiguity: neither is silently equated with the other.
        let moved = CoordinatorIdentity::new("127.0.0.1:50060");
        assert_ne!(moved.slot(), first.slot());
    }

    #[test]
    fn an_incarnation_is_128_bits_of_hex() {
        let identity = CoordinatorIdentity::new("slot");
        assert_eq!(identity.incarnation().len(), INCARNATION_BYTES * 2);
        assert!(
            identity
                .incarnation()
                .chars()
                .all(|c| c.is_ascii_hexdigit()),
            "{identity}"
        );
        // The rendered form names both halves, so one log line is greppable against either column.
        assert_eq!(
            identity.to_string(),
            format!("slot#{}", identity.incarnation())
        );
    }

    #[test]
    fn an_identity_read_back_out_of_the_database_round_trips() {
        let identity = CoordinatorIdentity::with_incarnation("slot", "deadbeef");
        assert_eq!(identity.slot(), "slot");
        assert_eq!(identity.incarnation(), "deadbeef");
    }

    /// Decision 3: the threshold is a multiple of the renewal interval and there is no other way to
    /// express it. If a separate setting is ever added, this is the test that should stop it.
    #[test]
    fn the_threshold_is_a_multiple_of_the_renewal_interval_and_nothing_else() {
        for secs in [1u64, 5, 10, 60] {
            let interval = Duration::from_secs(secs);
            assert_eq!(
                death_threshold(interval),
                interval * MISSED_RENEWALS_BEFORE_DEAD
            );
        }
        // One missed renewal is a GC pause, a slow query holding the runtime, or a lost packet —
        // not a death. Checked at compile time because the value is a build constant.
        const {
            assert!(
                MISSED_RENEWALS_BEFORE_DEAD >= 2,
                "one missed renewal is a GC pause, not a death"
            )
        };
    }

    #[test]
    fn a_row_is_judged_by_its_own_interval() {
        let now = Utc::now();
        // Renewing every second: 4 seconds of silence is over a 3-second threshold.
        let brisk = row(now - chrono::TimeDelta::seconds(4), 1);
        assert!(!brisk.is_live_at(now), "{brisk:?}");
        // Renewing every 10 seconds: the same 4 seconds is well inside a 30-second threshold. A
        // reader applying its *own* cadence would get this backwards.
        let leisurely = row(now - chrono::TimeDelta::seconds(4), 10);
        assert!(leisurely.is_live_at(now), "{leisurely:?}");
        assert_eq!(leisurely.death_threshold(), Duration::from_secs(30));
    }

    #[test]
    fn a_clean_exit_is_not_live_however_recent_the_renewal() {
        let now = Utc::now();
        let mut stopped = row(now, 10);
        assert!(stopped.is_live_at(now), "a fresh renewal is live");
        stopped.shutdown_at = Some(now);
        assert!(
            !stopped.is_live_at(now),
            "a clean exit must not wait for the threshold"
        );
    }

    #[test]
    fn a_sub_second_interval_cannot_round_the_threshold_to_zero() {
        // The column refuses a zero interval; this is the clamp that keeps a sub-second Duration
        // from becoming one on the way in.
        assert_eq!(interval_secs(Duration::from_millis(50)), 1);
        assert_eq!(interval_secs(Duration::from_secs(7)), 7);
    }

    /// The no-services-database rule, testable because it is a function rather than an `if` at a
    /// call site: nothing is registered, nothing is spawned, and the caller gets back a `None` it
    /// cannot accidentally treat as a registration.
    #[tokio::test]
    async fn with_no_services_database_nothing_is_registered_at_all() {
        let registration = CoordinatorRegistration::start_if_configured(
            None,
            CoordinatorIdentity::new("local"),
            DEFAULT_RENEW_INTERVAL,
        )
        .await
        .expect("no database is not an error");
        assert!(
            registration.is_none(),
            "an unconfigured services database must produce no registration whatsoever"
        );
    }
}
