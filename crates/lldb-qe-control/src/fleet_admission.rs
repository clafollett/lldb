//! **Fleet-wide admission control** — a warehouse's concurrency limit, held where every coordinator
//! can see it.
//!
//! # The bug this closes
//!
//! [`crate::scheduler`]'s `Admission` is a `tokio::sync::Semaphore` in **one process's memory**. Two
//! coordinators pointed at the same warehouse, each configured `K = 4`, run up to **8** queries on
//! it and neither can see the other. A warehouse's limit was therefore not a property of the
//! warehouse at all: it was a property of a process, multiplied by however many coordinators
//! happened to be running — which is exactly the number an operator scaling for *availability*
//! would increase without expecting a change in load on their compute.
//!
//! This module is the shared half. `scheduler.rs` keeps the local semaphore (fast path, and
//! backstop when this module cannot answer) and asks a [`FleetGate`] before it lets a query run;
//! [`FleetAdmission`] is that gate, over the services database.
//!
//! # Decision 1 — `K` rows, not a counter
//!
//! A warehouse of size `K` has slots numbered `0 .. K-1`, and a claim is a *row*. The primary key
//! `(warehouse_id, slot_no)` is what arbitrates, so over-admission is impossible by construction
//! rather than by argument: there cannot be a `K+1`-th row.
//!
//! The alternative — one counter per warehouse, read, compared, incremented — is wrong under
//! `READ COMMITTED` without a lock, because `N` coordinators can all read `K-1`, all conclude there
//! is room, and all admit. Making it right needs `pg_advisory_xact_lock` or a `SELECT ... FOR
//! UPDATE` on a parent row, and both need an explicit transaction: three round trips on the hottest
//! path in the system, to arrive somewhere a unique index already is.
//!
//! So a claim is **one statement**. It picks a claimable slot number, `INSERT ... ON CONFLICT DO
//! UPDATE`s it, and sees whether a row comes back. Two coordinators racing for the same number both
//! reach the index; one wins, and the other is turned into an `ON CONFLICT` whose `WHERE` repeats
//! the whole claimable predicate — the same repeated-predicate compare-and-swap [`crate::reaper`]
//! sweeps with, and for the same reason. The loser gets no row and waits, exactly as it would have
//! if the warehouse had genuinely been full an instant earlier.
//!
//! The candidate is chosen with `ORDER BY random()` rather than lowest-first, which is a small
//! thing with a real effect: lowest-first makes every simultaneous claimant on a warehouse *with
//! room* pick the same number and all but one lose a race they did not need to have.
//!
//! # Decision 2 — expiry is [`crate::liveness`]'s, and there is no second lease
//!
//! The shape a lease usually takes is `expires_at` plus a renewal loop. This one has neither,
//! because the answer already exists: `coordinators` is a renewed lease over the *process*, and a
//! query slot is held by a process. A row records its holder as the `(slot, incarnation)` pair, and
//! a slot is claimable exactly when no **live** `coordinators` row matches that pair —
//! [`crate::liveness`]'s `LIVE_PREDICATE`, spliced in verbatim rather than re-spelled, the same way
//! `reaper.rs` splices it. Two definitions of "alive" would eventually disagree and nothing in the
//! build would notice, so there is exactly one.
//!
//! Three things follow, and the first two are why the issue said this and #36 should share a
//! mechanism:
//!
//! 1. **One renewal per coordinator, not one per running query.** A fleet running a thousand
//!    queries writes the same number of heartbeats as one running none.
//! 2. **No sweep.** A dead coordinator's slots are reclaimed by the next coordinator that wants
//!    one, on the ordinary claim path. There is nothing to schedule and nothing to forget to run.
//! 3. **The incarnation is load-bearing, exactly as it is for the reaper.** A coordinator that died
//!    and restarted onto the same address has a live registration under the *slot* its abandoned
//!    claims name; matching on the slot alone would see a live coordinator and strand those slots
//!    forever.
//!
//! **A holder must therefore be registered.** A claim written by a process with no `coordinators`
//! row is never live, so it would be reclaimable by everyone the instant it was written and
//! fleet-wide admission would silently do nothing at all. That is why the only constructors here
//! take a [`CoordinatorRegistration`]: it is not possible to build this gate without one.
//!
//! # Decision 3 — releasing is a compare-and-swap, and a leaked row is reclaimable by its owner
//!
//! A lease is released by a `DELETE` issued from [`crate::scheduler::QuerySlot`]'s destructor, and a
//! destructor cannot await one — so it is handed to the runtime, which makes it **best effort**,
//! exactly like every other write this codebase issues from a `Drop` (`lldb_qe_core::server`'s
//! `ActiveQuery` is the precedent). Two consequences had to be designed for rather than hoped about,
//! because a leased slot that is *not* released is strictly worse than the per-process bug this
//! replaced: it shrinks a warehouse permanently instead of enlarging it.
//!
//! - **The `DELETE` is conditional on still being the holder.** A coordinator partitioned long
//!   enough to be judged dead has its slots reclaimed by somebody else; when it comes back and its
//!   query finishes, an unconditional delete would free the *successor's* slot. So the statement
//!   carries `holder_incarnation` and [`FleetLease::token`], and a release that no longer owns the
//!   row is a no-op.
//! - **A coordinator's own stale rows are claimable by its own next claim.** This gate knows exactly
//!   which tokens it is holding, so any row of its own carrying a different token is provably a
//!   leak, and the claim statement takes it. That is what stops a run of failed releases from
//!   wedging a warehouse for as long as the process lives.
//!
//! The release also retries a small number of times before giving up, so the ordinary blip costs a
//! few hundred milliseconds rather than a leak. What is left is stated rather than hidden: **a slot
//! leaked by a coordinator that stays alive and never claims on that warehouse again is reclaimed
//! when that coordinator's registration goes away.** That is the same bounded guarantee
//! [`crate::reaper`] offers for stranded query rows, argued the same way — every process eventually
//! restarts, so it terminates, but it is not immediate.
//!
//! # Decision 4 — a claim's reconciliation is exact, which costs one lock
//!
//! The "my own stale rows" clause above compares against a set this process holds in memory, and a
//! set read at the wrong moment would be *worse than not having one*: a claim that snapshotted the
//! set before a sibling claim inserted its row would judge that sibling's row a leak and take its
//! slot, leaving one process running two queries against one fleet slot.
//!
//! So a `tokio::sync::Mutex` per warehouse serializes this process's claims *for that warehouse*
//! across the round trip, which makes the snapshot exact at statement time. It is per warehouse and
//! not global, so `analytics` and `etl` never wait on each other, and the queries it serializes were
//! already bounded to `K` by the local semaphore before they got here.
//!
//! # No services database is still legal
//!
//! Nothing here exists without one, and that is the whole of it. [`crate::scheduler::Scheduler`]
//! with no [`FleetGate`] is the module it always was — zero I/O on an uncontended admit — and
//! [`FleetAdmission::start_if_registered`] is that rule as a function, `None` in and `None` out, so
//! it is testable with no database in sight.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::liveness::{
    CoordinatorIdentity, CoordinatorRegistration, LIVE_PREDICATE, MISSED_RENEWALS_BEFORE_DEAD,
};
use crate::scheduler::{FleetClaim, FleetClaimFuture, FleetGate, FleetLease};
use crate::services::ServicesDb;

/// Bytes of randomness behind a lease token. Sixteen, for [`CoordinatorIdentity`]'s reason: it has
/// to never repeat across every claim the deployment will ever make, and 128 bits is the point past
/// which nobody has to think about that again.
const TOKEN_BYTES: usize = 16;

/// How many times a release re-attempts its `DELETE` before conceding the row is leaked.
///
/// Three, so an ordinary blip costs a few hundred milliseconds rather than a slot. Not more, because
/// a release that is still failing after this is failing for a reason that will outlast any number
/// a destructor's spawned task is willing to wait for — and the leak is recoverable (decision 3).
const RELEASE_ATTEMPTS: u32 = 3;

/// Pause between release attempts.
const RELEASE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// One row of `admission_slots` — a query slot somebody is holding on a warehouse.
///
/// Fields are public because this is a record. Whether the holder is still *live* is not one of
/// them, for [`crate::liveness::CoordinatorRow`]'s reason: that is a fact about the row and the
/// clock together, and the database is the only thing holding both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionSlotRow {
    pub warehouse_id: i64,
    pub slot_no: i32,
    /// The holder's stable deployment slot — `queries.coordinator`'s value.
    pub holder_slot: String,
    /// The holder's *process*. The half that makes a restart onto the same address distinguishable.
    pub holder_incarnation: String,
    pub holder_token: String,
    pub claimed_at: DateTime<Utc>,
}

/// The column list every `admission_slots` lookup returns, in [`AdmissionSlotTuple`]'s order.
const SLOT_COLUMNS: &str =
    "warehouse_id, slot_no, holder_slot, holder_incarnation, holder_token, claimed_at";

type AdmissionSlotTuple = (i64, i32, String, String, String, DateTime<Utc>);

impl From<AdmissionSlotTuple> for AdmissionSlotRow {
    fn from(row: AdmissionSlotTuple) -> Self {
        let (warehouse_id, slot_no, holder_slot, holder_incarnation, holder_token, claimed_at) =
            row;
        Self {
            warehouse_id,
            slot_no,
            holder_slot,
            holder_incarnation,
            holder_token,
            claimed_at,
        }
    }
}

/// When an existing row may be taken, as SQL over `alias`.
///
/// The two disjuncts are decisions 2 and 3, in the order they matter:
///
/// 1. **its holder is not live** — [`LIVE_PREDICATE`], spliced rather than re-spelled, matched on
///    the `(slot, incarnation)` pair so a coordinator that restarted onto the same address does not
///    keep its predecessor's claims alive; or
/// 2. **its holder is me, and its token is one I no longer hold** — a release of mine that did not
///    land. Provably a leak, because a claim publishes its token before the row exists and a release
///    withdraws it only after the `DELETE` has been attempted.
///
/// `$4` is this claimer's incarnation and `$7` the tokens it holds; `$1` is the missed-renewal
/// multiple [`LIVE_PREDICATE`] wants. Columns are table-qualified because this string is spliced
/// into a statement with `admission_slots` in scope twice under two different aliases.
fn claimable(alias: &str) -> String {
    format!(
        "(NOT EXISTS ( \
                 SELECT 1 FROM coordinators \
                  WHERE coordinators.slot = {alias}.holder_slot \
                    AND coordinators.incarnation = {alias}.holder_incarnation \
                    AND {LIVE_PREDICATE}) \
          OR ({alias}.holder_incarnation = $4 AND {alias}.holder_token <> ALL($7::TEXT[])))"
    )
}

/// The claim, as text. A function so the compare-and-swap it rests on is assertable without a
/// database — see `the_claim_repeats_its_predicate_as_a_compare_and_swap`.
///
/// Binds: `$1` missed-renewal multiple, `$2` warehouse id, `$3` holder slot, `$4` holder
/// incarnation, `$5` this claim's token, `$6` the limit `K`, `$7` the tokens this process holds.
fn claim_statement() -> String {
    let candidate_is_claimable = claimable("existing");
    let still_claimable = claimable("slots");
    format!(
        "INSERT INTO admission_slots AS slots \
             (warehouse_id, slot_no, holder_slot, holder_incarnation, holder_token) \
         SELECT $2, candidate.slot_no, $3, $4, $5 \
           FROM (SELECT gs.n AS slot_no \
                   FROM generate_series(0, $6::INT - 1) AS gs(n) \
                   LEFT JOIN admission_slots existing \
                          ON existing.warehouse_id = $2 AND existing.slot_no = gs.n \
                  WHERE existing.slot_no IS NULL OR {candidate_is_claimable} \
                  ORDER BY random() \
                  LIMIT 1) AS candidate \
         ON CONFLICT (warehouse_id, slot_no) DO UPDATE \
            SET holder_slot        = EXCLUDED.holder_slot, \
                holder_incarnation = EXCLUDED.holder_incarnation, \
                holder_token       = EXCLUDED.holder_token, \
                claimed_at         = now() \
          WHERE {still_claimable} \
         RETURNING slot_no"
    )
}

impl ServicesDb {
    /// Take one of `warehouse_id`'s `limit` slots for `identity`, or report that there was none.
    ///
    /// `token` names this claim; `held` is every token `identity` currently holds on this
    /// warehouse, which is what lets the statement reclaim `identity`'s own leaked rows (decision 3)
    /// without touching the ones it is legitimately using. Passing an empty slice is legal and
    /// means "I hold nothing here", which makes every one of my rows a leak — correct, and what a
    /// freshly started process should say.
    ///
    /// `Ok(None)` is a full warehouse *or* a lost race for one slot number, deliberately not
    /// distinguished: both mean "ask again shortly", and telling them apart would cost a second
    /// round trip to answer a question nobody acts on differently.
    pub async fn claim_admission_slot(
        &self,
        warehouse_id: i64,
        identity: &CoordinatorIdentity,
        token: &str,
        limit: usize,
        held: &[String],
    ) -> Result<Option<i32>> {
        let slot_no: Option<(i32,)> = sqlx::query_as(&claim_statement())
            .bind(f64::from(MISSED_RENEWALS_BEFORE_DEAD))
            .bind(warehouse_id)
            .bind(identity.slot())
            .bind(identity.incarnation())
            .bind(token)
            // Clamped for the same reason `Admission::new` clamps it: a limit of zero is a
            // warehouse that accepts queries and never runs one, and here it would also make
            // `generate_series(0, -1)` empty and every claim fail forever.
            .bind(limit.clamp(1, i32::MAX as usize) as i32)
            .bind(held)
            .fetch_optional(self.pool())
            .await
            .with_context(|| {
                format!("claiming an admission slot on warehouse {warehouse_id} for `{identity}`")
            })?;
        Ok(slot_no.map(|(slot_no,)| slot_no))
    }

    /// Give a slot back. `false` means the row was already gone, or had been reclaimed by somebody
    /// else — both of which are fine, and neither of which is an error.
    ///
    /// The `holder_incarnation` + `holder_token` qualification is the compare-and-swap decision 3
    /// argues for: a coordinator that was judged dead, had its slot taken, and then finished its
    /// query must not delete its successor's row.
    pub async fn release_admission_slot(&self, lease: &FleetLease, holder: &str) -> Result<bool> {
        let deleted = sqlx::query(
            "DELETE FROM admission_slots \
              WHERE warehouse_id = $1 AND slot_no = $2 \
                AND holder_incarnation = $3 AND holder_token = $4",
        )
        .bind(lease.warehouse_id)
        .bind(lease.slot_no)
        .bind(holder)
        .bind(&lease.token)
        .execute(self.pool())
        .await
        .with_context(|| {
            format!(
                "releasing admission slot {} on warehouse {}",
                lease.slot_no, lease.warehouse_id
            )
        })?
        .rows_affected();
        Ok(deleted > 0)
    }

    /// Every slot currently claimed on `warehouse_id`, whoever holds it and whether or not that
    /// holder is still live. The operator's view, and what the acceptance tests assert on.
    pub async fn admission_slots(&self, warehouse_id: i64) -> Result<Vec<AdmissionSlotRow>> {
        let rows = sqlx::query_as::<_, AdmissionSlotTuple>(&format!(
            "SELECT {SLOT_COLUMNS} FROM admission_slots WHERE warehouse_id = $1 ORDER BY slot_no"
        ))
        .bind(warehouse_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing admission slots on warehouse {warehouse_id}"))?;
        Ok(rows.into_iter().map(AdmissionSlotRow::from).collect())
    }
}

/// This process's claims on one warehouse.
#[derive(Debug, Default)]
struct WarehouseClaims {
    /// Serializes this process's claims for this warehouse across the round trip, so the token
    /// snapshot below is exact at statement time. See decision 4 — an inexact snapshot is worse
    /// than no snapshot, because it lets one process take its own live slot.
    ///
    /// `tokio::sync::Mutex`, unlike everywhere else in this codebase, precisely because it is held
    /// across an await.
    claiming: tokio::sync::Mutex<()>,
    /// The tokens this process holds here. Published *before* the row exists and withdrawn *after*
    /// the release has been attempted, so a token in this set is never a leak and a row whose token
    /// is absent always is.
    held: std::sync::Mutex<HashSet<String>>,
}

/// The fleet-wide bound, over the services database.
///
/// One per coordinator process, shared by every [`crate::scheduler::Admission`] it builds. Held
/// behind an `Arc` because [`crate::scheduler::QuerySlot`]'s destructor reaches it.
pub struct FleetAdmission {
    db: ServicesDb,
    identity: CoordinatorIdentity,
    /// `std::sync::Mutex` because the critical section is a hash lookup that never awaits; the
    /// awaiting is done under the per-warehouse lock inside.
    warehouses: std::sync::Mutex<HashMap<i64, Arc<WarehouseClaims>>>,
    granted: AtomicU64,
    full: AtomicU64,
    unavailable: AtomicU64,
    /// Behind an `Arc` because they are the two the *spawned release task* updates, and that task
    /// cannot borrow `self`: `release` is reached through an `Arc<dyn FleetGate>` from a
    /// destructor, so there is no `Arc<Self>` in scope to clone.
    released: Arc<AtomicU64>,
    leaked: Arc<AtomicU64>,
}

impl FleetAdmission {
    /// A fleet gate for the process `registration` registers.
    ///
    /// Taking the registration rather than a bare [`CoordinatorIdentity`] is the type-level spelling
    /// of decision 2's rule: a holder that is not registered is not live, so its claims would be
    /// reclaimable the instant they were written and this whole mechanism would quietly do nothing.
    pub fn for_registration(db: ServicesDb, registration: &CoordinatorRegistration) -> Arc<Self> {
        Arc::new(Self {
            db,
            identity: registration.identity().clone(),
            warehouses: std::sync::Mutex::new(HashMap::new()),
            granted: AtomicU64::new(0),
            full: AtomicU64::new(0),
            unavailable: AtomicU64::new(0),
            released: Arc::new(AtomicU64::new(0)),
            leaked: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Build one only if there is a control plane *and* a registration in it.
    ///
    /// **This is the no-services-database rule, as a function**, in
    /// [`CoordinatorRegistration::start_if_configured`]'s style and for its reason: it is the rule
    /// an otherwise-reasonable implementation is most likely to break, and a rule in a function can
    /// be tested with no database present. `None` out means [`crate::scheduler`] stays exactly the
    /// module it was before fleet-wide admission existed.
    pub fn start_if_registered(
        db: Option<ServicesDb>,
        registration: Option<&CoordinatorRegistration>,
    ) -> Option<Arc<Self>> {
        match (db, registration) {
            (Some(db), Some(registration)) => Some(Self::for_registration(db, registration)),
            _ => None,
        }
    }

    /// Who this gate claims as.
    pub fn identity(&self) -> &CoordinatorIdentity {
        &self.identity
    }

    /// Slots granted over this gate's life.
    pub fn granted(&self) -> u64 {
        self.granted.load(Ordering::Acquire)
    }

    /// Claims answered "every slot is taken" — the fleet holding this coordinator back, which is
    /// the whole point of the mechanism and therefore worth being able to see.
    pub fn full(&self) -> u64 {
        self.full.load(Ordering::Acquire)
    }

    /// Claims the services database could not answer. Each of these was admitted on the local bound
    /// alone; see [`crate::scheduler::AdmissionSnapshot::fleet_degraded`].
    pub fn unavailable(&self) -> u64 {
        self.unavailable.load(Ordering::Acquire)
    }

    /// Slots given back.
    pub fn released(&self) -> u64 {
        self.released.load(Ordering::Acquire)
    }

    /// Releases that never landed, so the row is still there.
    ///
    /// Recoverable rather than fatal — this coordinator's next claim on that warehouse takes the row
    /// back (decision 3) — but non-zero means a warehouse briefly had less concurrency than its
    /// size, and that is worth an operator knowing.
    pub fn leaked(&self) -> u64 {
        self.leaked.load(Ordering::Acquire)
    }

    /// This process's claim state for one warehouse, created on first sight.
    fn warehouse(&self, warehouse_id: i64) -> Arc<WarehouseClaims> {
        let mut warehouses = self
            .warehouses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(warehouses.entry(warehouse_id).or_default())
    }
}

/// 128 bits of CSPRNG, hex — the same shape and the same reason as an incarnation. Not a secret;
/// it is written to a table any operator can read. It must simply never repeat.
fn mint_token() -> String {
    let mut bytes = [0u8; TOKEN_BYTES];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
    hex::encode(bytes)
}

impl std::fmt::Debug for FleetAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetAdmission")
            .field("identity", &self.identity)
            .field("granted", &self.granted())
            .field("full", &self.full())
            .field("unavailable", &self.unavailable())
            .field("released", &self.released())
            .field("leaked", &self.leaked())
            .finish()
    }
}

impl FleetGate for FleetAdmission {
    fn claim(&self, warehouse_id: i64, limit: usize) -> FleetClaimFuture<'_> {
        Box::pin(async move {
            let claims = self.warehouse(warehouse_id);
            // Decision 4: one claim of ours per warehouse at a time, so the snapshot below is
            // exact when the statement reads it.
            let _claiming = claims.claiming.lock().await;

            let token = mint_token();
            // Published before the row can exist, so this very statement's reconciliation cannot
            // mistake the row it is about to write for a leak.
            let held: Vec<String> = {
                let mut held = claims
                    .held
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                held.insert(token.clone());
                held.iter().cloned().collect()
            };
            let withdraw = || {
                claims
                    .held
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&token);
            };

            match self
                .db
                .claim_admission_slot(warehouse_id, &self.identity, &token, limit, &held)
                .await
            {
                Ok(Some(slot_no)) => {
                    self.granted.fetch_add(1, Ordering::AcqRel);
                    tracing::debug!(
                        warehouse_id,
                        slot_no,
                        limit,
                        "claimed a fleet-wide admission slot"
                    );
                    FleetClaim::Granted(FleetLease {
                        warehouse_id,
                        slot_no,
                        token,
                    })
                }
                Ok(None) => {
                    withdraw();
                    self.full.fetch_add(1, Ordering::AcqRel);
                    FleetClaim::Full
                }
                // Never propagated as an error: `Admission` turns this into "admit on the local
                // bound", which is the degradation the whole design is built around. See
                // `crate::scheduler`'s module docs.
                Err(error) => {
                    withdraw();
                    self.unavailable.fetch_add(1, Ordering::AcqRel);
                    FleetClaim::Unavailable(format!("{error:#}"))
                }
            }
        })
    }

    fn release(&self, lease: FleetLease) {
        let claims = self.warehouse(lease.warehouse_id);
        let db = self.db.clone();
        let holder = self.identity.incarnation().to_string();
        let slot = self.identity.slot().to_string();

        // `Handle::try_current` rather than a bare `tokio::spawn`, for `lldb_qe_core::server`'s
        // `ActiveQuery` reason: dropping a guard after the runtime has gone (shutdown, a test)
        // would otherwise panic inside a destructor. A process that is exiting is also one whose
        // registration is about to stop renewing, so the rows it leaves behind become claimable on
        // the liveness threshold — which is precisely the case decision 2 exists to cover.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            claims
                .held
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&lease.token);
            self.leaked.fetch_add(1, Ordering::AcqRel);
            tracing::debug!(
                warehouse_id = lease.warehouse_id,
                slot_no = lease.slot_no,
                "no runtime left to release a fleet admission slot; it will be reclaimed when this \
                 coordinator's registration expires"
            );
            return;
        };

        let released = Arc::clone(&self.released);
        let leaked = Arc::clone(&self.leaked);
        handle.spawn(async move {
            let mut outcome = Ok(false);
            for attempt in 1..=RELEASE_ATTEMPTS {
                outcome = db.release_admission_slot(&lease, &holder).await;
                match &outcome {
                    Ok(_) => break,
                    Err(error) if attempt == RELEASE_ATTEMPTS => tracing::warn!(
                        warehouse_id = lease.warehouse_id,
                        slot_no = lease.slot_no,
                        coordinator = %slot,
                        error = %format!("{error:#}"),
                        "could not release a fleet-wide admission slot after {RELEASE_ATTEMPTS} \
                         attempts; the warehouse is one slot short until this coordinator's next \
                         query on it reclaims the row, or until its registration expires"
                    ),
                    Err(_) => tokio::time::sleep(RELEASE_RETRY_DELAY).await,
                }
            }
            // Withdrawn whichever way it went. On success the row is gone and the token means
            // nothing; on failure withdrawing is exactly what makes the leaked row claimable by
            // this coordinator's next claim (decision 3).
            claims
                .held
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&lease.token);
            match outcome {
                Ok(_) => released.fetch_add(1, Ordering::AcqRel),
                Err(_) => leaked.fetch_add(1, Ordering::AcqRel),
            };
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decision 2, checked where it is cheapest to check: a claim is claimable-by-liveness, and the
    /// rule is [`crate::liveness`]'s rather than a second copy of it.
    ///
    /// If a future change re-spells "alive" here, this is what should stop it — two definitions
    /// would eventually disagree about where the line is and nothing in the build would notice.
    #[test]
    fn claimability_is_liveness_of_the_holder_and_never_the_age_of_the_claim() {
        let predicate = claimable("slots");
        assert!(predicate.contains("NOT EXISTS"), "{predicate}");
        assert!(predicate.contains(LIVE_PREDICATE), "{predicate}");
        assert!(
            predicate.contains("coordinators.incarnation = slots.holder_incarnation"),
            "matching on the slot alone would keep a dead coordinator's claims alive forever, \
             because --coordinator-id defaults to the bound address and a restart re-takes it: \
             {predicate}"
        );
        // A long query is indistinguishable from an abandoned one by age — `reaper`'s decision 1,
        // and exactly as true one layer down. A slot that expired on a clock would be taken from a
        // query that was still running, and the fleet would then be over-admitted by design.
        for age_comparison in [
            "slots.claimed_at <",
            "slots.claimed_at +",
            "claimed_at < now()",
        ] {
            assert!(
                !predicate.contains(age_comparison),
                "a slot must never expire on the age of the claim: {predicate}"
            );
        }
    }

    /// Decision 3's other half: my own row carrying a token I do not hold is a leak I may take back.
    #[test]
    fn a_holder_may_reclaim_its_own_rows_and_only_its_own() {
        let predicate = claimable("slots");
        assert!(
            predicate.contains("slots.holder_incarnation = $4"),
            "reclamation is scoped to the claiming *process*; without this a coordinator could take \
             a live peer's slot on the strength of not recognising its token: {predicate}"
        );
        assert!(
            predicate.contains("slots.holder_token <> ALL($7::TEXT[])"),
            "{predicate}"
        );
    }

    /// The compare-and-swap, checked in the statement text — `reaper`'s idiom, and the same
    /// temptation to "remove the duplication".
    #[test]
    fn the_claim_repeats_its_predicate_as_a_compare_and_swap() {
        let statement = claim_statement();
        // Once to choose a candidate, once as the `ON CONFLICT`'s own qualification so a row that
        // was taken between the scan and the write is re-evaluated and left alone. Deleting the
        // second is what would let two coordinators believe they hold the same slot.
        assert!(statement.contains(&claimable("existing")), "{statement}");
        assert!(statement.contains(&claimable("slots")), "{statement}");
        assert!(
            statement.contains("ON CONFLICT (warehouse_id, slot_no) DO UPDATE"),
            "the primary key is what makes over-admission impossible rather than unlikely: \
             {statement}"
        );
        // The bound is structural: a claim only ever proposes a number below the limit.
        assert!(
            statement.contains("generate_series(0, $6::INT - 1)"),
            "{statement}"
        );
        // Lowest-first would make every simultaneous claimant pick the same number.
        assert!(statement.contains("ORDER BY random()"), "{statement}");
        assert!(statement.contains("LIMIT 1"), "{statement}");
    }

    #[test]
    fn a_token_is_128_bits_of_hex_and_never_repeats() {
        let first = mint_token();
        assert_eq!(first.len(), TOKEN_BYTES * 2);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()), "{first}");
        assert_ne!(first, mint_token());
    }

    /// The no-services-database rule, testable because it is a function rather than an `if` at a
    /// call site: no control plane means no fleet gate, and therefore a scheduler that behaves
    /// exactly as it did before this module existed.
    #[test]
    fn with_no_services_database_there_is_no_fleet_gate_at_all() {
        assert!(
            FleetAdmission::start_if_registered(None, None).is_none(),
            "an unconfigured services database must produce no fleet gate whatsoever"
        );
    }
}
