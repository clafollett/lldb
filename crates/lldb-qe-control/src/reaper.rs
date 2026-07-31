//! **The query reaper** — resolving history rows whose writer is gone.
//!
//! # The bug this closes
//!
//! A row in `queries` is moved to a terminal state by the coordinator task that owns it, and by
//! nothing else. Two things can take that task away before it gets there:
//!
//! 1. **The coordinator dies outright.** Everything it had in flight stays `queued` or `running`
//!    forever. Nothing in that process can clean up after it — that is *why*
//!    [`crate::query_log`]'s rows record which coordinator wrote them.
//! 2. **The insert-to-guard window.** `lldb_qe_core::server`'s `ActiveQuery` guard closes a row out when
//!    a request is dropped, but the row is created by an `await` that commits *before* the future
//!    is resumed with the new id, so a cancellation landing in between leaves a row with nothing
//!    watching it. The guard's own docs name this window; it cannot be closed from there, because
//!    building a guard needs an id only the completed insert can supply.
//!
//! Left alone those rows are not merely untidy. `list_active_queries` — what an operator reads to
//! see what a coordinator is doing — accumulates queries that will never finish, and
//! [`crate::query_log::peak_concurrency`] treats a row with no `finished_at` as still running, so
//! **one stranded row makes every later query look concurrent with it**. That is the sweep-line
//! instrument #18 uses to prove the admission bound was respected, biased silently upward, exactly
//! when you most want to trust it.
//!
//! # Decision 1 — what makes a row reapable, and what emphatically does not
//!
//! Reapable means **the process that wrote this row is provably gone**, expressed as a join onto
//! [`crate::liveness`]'s registry:
//!
//! ```text
//! state IN ('queued','running')
//!   AND coordinator IS NOT NULL AND coordinator_incarnation IS NOT NULL
//!   AND NOT EXISTS (a LIVE coordinators row with that slot AND that incarnation)
//! ```
//!
//! The **incarnation** is the half that makes this correct rather than plausible. A coordinator
//! that died and restarted onto the same address — the overwhelmingly common case, since
//! `--coordinator-id` defaults to the bound socket address — has a live registration under the
//! *slot* its stranded rows name. Matching on the slot alone would see a live coordinator, conclude
//! the rows are live, and strand them forever. Matching on the pair resolves them the moment the
//! replacement takes the slot. This module is [`crate::liveness`]'s first consumer and the pair is
//! the reason that module records two columns instead of one.
//!
//! What is **not** in that predicate is age, and its absence is the whole safety argument. A
//! legitimately long-running query and an abandoned one are indistinguishable by age, so any rule
//! containing "…and it has been running for more than N minutes" kills live work on a slow
//! afternoon. `liveness`'s `a_live_coordinator_is_never_concluded_dead` exists to make that
//! provable and this module extends it: `a_live_coordinators_long_running_query_is_never_reaped`
//! sweeps repeatedly, for several multiples of the liveness threshold, against a query that is
//! still running, and demands that nothing is ever taken. `LIMIT` is the only number here that
//! bounds anything, and it bounds work per sweep, never eligibility.
//!
//! A NULL `coordinator_incarnation` is **not** reapable, and that is deliberate rather than an
//! oversight: history predating the column, and any writer that legitimately never registered, read
//! as "liveness says nothing about this row's writer" — which is not the same claim as "its writer
//! is dead". Migration `0006` says so on the column itself. The cost is that such rows are never
//! resolved; the alternative is failing rows belonging to processes that may be working perfectly.
//!
//! # Decision 2 — the honest limit: a live coordinator's own stranded row waits
//!
//! Cause 2 above strands a row on a coordinator that is *still alive*, and this predicate will not
//! touch it while that is true. There is no fix for that from out of process: nothing an external
//! sweeper can read distinguishes "row 41 is stranded" from "row 41 is a query that has been
//! running for an hour", and inventing a distinction is exactly decision 1's forbidden shape. So
//! the guarantee this module actually offers is bounded, not immediate:
//!
//! > every stranded row is resolved at the latest when the incarnation that wrote it goes away.
//!
//! Every process eventually restarts, so that terminates — but a long-lived coordinator can carry
//! one of its own stranded rows for as long as it lives. Closing *that* gap belongs in-process, on
//! the submit path (the guard, made constructible before the insert commits), not here.
//!
//! # Decision 3 — the reaper is a second writer, and it is the one that has to prove itself
//!
//! [`crate::query_log`] used to justify taking no lock on the grounds that a query row has exactly
//! one writer. It has two now, so the justification had to be replaced rather than deleted — and
//! the replacement is an asymmetry, not a lock:
//!
//! - **The owning coordinator writes unconditionally.** It is the authority on its own query: it
//!   knows whether the rows were delivered, and its word is the truth even if it arrives late.
//! - **The reaper writes a compare-and-swap** — the same idiom `lldb_qe_core::dml` commits a snapshot
//!   with and [`crate::warehouse`] transitions a warehouse with. Its `UPDATE` repeats the full
//!   reapable predicate in its own `WHERE`, so under Postgres's `READ COMMITTED` recheck a row that
//!   moved between the scan and the update is re-evaluated against its *new* version and skipped.
//!
//! Both interleavings of "the reaper decides a row is dead" and "its coordinator writes
//! `succeeded`" therefore end at `succeeded`:
//!
//! - coordinator first → the reaper's CAS sees a terminal state and does not fire, so the terminal
//!   state is never clobbered;
//! - reaper first → the coordinator's later write corrects the row, which is what an authority is
//!   for. Reaching this at all means a coordinator kept serving through a lease it could not renew,
//!   which [`crate::liveness`]'s decision 2 explicitly concedes can happen: a control-plane outage
//!   must not take a working coordinator down, and the price named there is that its rows may be
//!   resolved pessimistically. A row briefly reading `failed` and then reading the truth is that
//!   price, and it is cheaper than either alternative (blocking the coordinator's write, or never
//!   reaping at all).
//!
//! `query_reaper::a_reaper_never_clobbers_a_terminal_state` asserts both orderings, deterministically
//! and then under real contention.
//!
//! # Decision 4 — `finished_at` is the last moment the writer was known alive, not now
//!
//! A reaped row needs a `finished_at`, and the tempting value — `now()` — is a claim the reaper
//! cannot support: it says the query was running right up until the sweep, so a row stranded on
//! Monday and swept on Friday overlaps the entire week and re-creates the `peak_concurrency` bias
//! this module exists to remove, merely shortened.
//!
//! The best-supported instant is the writer's **last successful renewal** — after that moment
//! nothing is known to have been alive to run anything — clamped into a sane interval:
//! `GREATEST(started_at, LEAST(now(), last_seen_at))`. The clamps matter. The lower one keeps a
//! query that started after its coordinator's final renewal from acquiring a negative execution
//! time; the upper one is belt and braces against a clock. Where the coordinator's registration is
//! gone entirely, or its slot has been taken over so the surviving `last_seen_at` belongs to the
//! *successor*, this falls back to `now()` and the estimate is an over-estimate again — stated
//! rather than hidden, and never an under-estimate.
//!
//! # Decision 5 — one bounded sweep, out of process
//!
//! The sweep takes a `LIMIT`, following `lldb_qe_core::result_cache`'s prune: an unbounded `UPDATE`
//! makes one run proportional to every stranded row a deployment has ever produced, which is the
//! shape that turns a routine maintenance task into an incident during the first backlog. A run
//! that fills its batch says so and can simply be run again; two reapers running concurrently is
//! also safe, because the CAS makes a double reap a no-op rather than a conflict.
//!
//! And it runs **out of process**, as `lldb-qe-reap`, which is [`crate::liveness`]'s decision 4
//! rather than a new one: a coordinator sweeping at startup is the dangerous shape, because a fleet
//! restarting together would have every member judging the others through a lease none of them had
//! renewed yet. Nothing in the query path calls into this module.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::liveness::{LIVE_PREDICATE, MISSED_RENEWALS_BEFORE_DEAD};
use crate::query_log::{
    MAX_ERROR_LEN, QUERY_COLUMNS, QueryRecord, QueryRow, active_states_sql, query_from_row,
};
use crate::services::ServicesDb;

/// Rows one sweep will resolve, when the caller does not say otherwise.
///
/// Large enough that an ordinary deployment's whole backlog fits in one run, small enough that the
/// statement holds a bounded number of row locks and a bad day cannot produce a single `UPDATE`
/// that runs for minutes. A caller with more than this to do runs the sweep again — see decision 5.
pub const DEFAULT_REAP_BATCH: usize = 500;

/// The largest batch a single sweep will accept, however large a number is passed.
///
/// The point of a bound is that it cannot be configured away. Ten thousand rows is far past any
/// plausible real backlog while still being one statement Postgres finishes promptly.
pub const MAX_REAP_BATCH: usize = 10_000;

/// What is written into `queries.error` for a row that never started.
///
/// Distinguishing this from [`DIED_MID_FLIGHT`] is a requirement of the issue and not cosmetic: the
/// two describe different facts about the world — one query consumed no compute and returned
/// nothing, the other may have run for an hour and had its results thrown away — and an operator
/// reading history to explain a gap needs to know which happened.
pub const NEVER_STARTED: &str = "reaped: this query never started. It was still waiting for an \
     admission slot when the coordinator process that accepted it stopped renewing its \
     registration (see queries.coordinator and queries.coordinator_incarnation, and the \
     coordinators table). Nothing was executed and nothing was returned.";

/// What is written into `queries.error` for a row that was executing.
pub const DIED_MID_FLIGHT: &str = "reaped: this query was running when the coordinator process \
     executing it stopped renewing its registration, so it was abandoned mid-flight (see \
     queries.coordinator and queries.coordinator_incarnation, and the coordinators table). How far \
     it got is unknown; no results reached the client.";

/// Why a row was reaped — the distinction the issue asks for, as a type rather than a string
/// comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapReason {
    /// `started_at IS NULL`: it was queued and never admitted.
    NeverStarted,
    /// It held a slot and was executing when its coordinator disappeared.
    DiedMidFlight,
}

impl ReapReason {
    /// Which of the two a row is, from the one column that decides it.
    ///
    /// `started_at` is exactly the fact `query_log` documents as "a row with `started_at IS NULL`
    /// and a terminal state never ran at all", so the classification is a read of the schema rather
    /// than an inference.
    pub fn of(started_at: Option<DateTime<Utc>>) -> Self {
        match started_at {
            None => ReapReason::NeverStarted,
            Some(_) => ReapReason::DiedMidFlight,
        }
    }

    /// The message stored on the row.
    pub fn as_str(self) -> &'static str {
        match self {
            ReapReason::NeverStarted => NEVER_STARTED,
            ReapReason::DiedMidFlight => DIED_MID_FLIGHT,
        }
    }
}

impl std::fmt::Display for ReapReason {
    /// A short tag for logs and the CLI's table — the full sentence lives on the row.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ReapReason::NeverStarted => "never-started",
            ReapReason::DiedMidFlight => "died-mid-flight",
        })
    }
}

/// One row this sweep resolved.
///
/// Returned rather than counted because a reaper that says "17" tells an operator nothing they can
/// act on: which tenant lost work, and which coordinator process took it down with it, are the
/// questions that follow immediately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapedQuery {
    pub id: i64,
    pub account_id: i64,
    /// The slot named by the row. Guaranteed non-null by the predicate that selected it.
    pub coordinator: String,
    /// The *process* named by the row — the half that made this row reapable.
    pub coordinator_incarnation: String,
    pub reason: ReapReason,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    /// What the sweep stamped: the last instant the writing process was known alive. See decision 4.
    pub finished_at: DateTime<Utc>,
}

/// The reapable predicate, once, so the dry run, the sweep's scan and the sweep's compare-and-swap
/// cannot drift apart. `$1` is the missed-renewal multiple [`LIVE_PREDICATE`] wants.
///
/// Columns are table-qualified throughout because this string is spliced into a statement with two
/// tables in scope and, in the sweep, into a subquery over `queries` nested inside an `UPDATE` of
/// `queries`. Unqualified names would resolve by accident rather than by intent.
const STRANDED_PREDICATE: &str = "queries.state IN ({ACTIVE}) \
     AND queries.coordinator IS NOT NULL \
     AND queries.coordinator_incarnation IS NOT NULL \
     AND NOT EXISTS ( \
             SELECT 1 FROM coordinators \
              WHERE coordinators.slot = queries.coordinator \
                AND coordinators.incarnation = queries.coordinator_incarnation \
                AND {LIVE})";

/// [`STRANDED_PREDICATE`] with the liveness rule and the active-state set spliced in. A function
/// rather than a `const` because both come from other modules.
///
/// `{ACTIVE}` is [`active_states_sql`] rather than a literal `'queued', 'running'` for the same
/// reason `{LIVE}` is not re-spelled here: a state added to [`crate::query_log::QueryState`] that a
/// query can be *stranded in* must reach this predicate, and the failure mode of it not doing so is
/// silent — rows in that state would simply never be resolved by anything, forever. The converse is
/// covered too: a terminal state (`cancelled`, since #38) is excluded by construction, so a query
/// somebody deliberately stopped can never look abandoned to a sweep.
fn stranded_predicate() -> String {
    STRANDED_PREDICATE
        .replace("{ACTIVE}", &active_states_sql())
        .replace("{LIVE}", LIVE_PREDICATE)
}

/// Optional narrowing to one tenant, as a bind rather than a branch — `NULL` means every account.
///
/// A sweep is naturally fleet-wide, because a dead coordinator strands whatever it happened to be
/// running and does not care whose it was. This exists for the two cases where "everything" is the
/// wrong blast radius: an operator resolving one tenant's history after an incident, and the
/// integration tests, which share a database with every other test in this binary (and possibly
/// with someone's dev instance) and must touch only the rows they made.
fn account_filter(placeholder: &str) -> String {
    format!("({placeholder}::BIGINT IS NULL OR queries.account_id = {placeholder})")
}

/// When the writing process was last known to be alive — see decision 4.
///
/// `COALESCE(..., now())` covers both cases where the registration cannot answer: it was deleted,
/// or the slot was taken over by a later incarnation so the surviving `last_seen_at` is the
/// successor's and says nothing about this row's writer.
const LAST_KNOWN_ALIVE: &str = "GREATEST(queries.started_at, LEAST(now(), COALESCE( \
         (SELECT coordinators.last_seen_at FROM coordinators \
           WHERE coordinators.slot = queries.coordinator \
             AND coordinators.incarnation = queries.coordinator_incarnation), \
         now())))";

/// The raw shape of a reaped row, in the order the sweep's `RETURNING` lists.
///
/// `coordinator` and `coordinator_incarnation` are non-optional here even though the columns are
/// nullable: the predicate that selected the row already excluded NULLs, and decoding them as
/// `String` is that guarantee restated somewhere it would fail loudly if it ever stopped holding.
type ReapedRow = (
    i64,
    i64,
    String,
    String,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
);

/// Keep a caller's batch size inside the bounds decision 5 argues for. Zero would make a sweep that
/// silently does nothing, which is worse than useless in a scheduled job.
fn batch_size(limit: usize) -> i64 {
    limit.clamp(1, MAX_REAP_BATCH) as i64
}

/// The sweep, as text. A function so the compare-and-swap it rests on is assertable without a
/// database — see `the_sweep_repeats_its_predicate_as_a_compare_and_swap`.
fn reap_statement() -> String {
    let predicate = stranded_predicate();
    let account = account_filter("$6");
    format!(
        "UPDATE queries SET \
             state = 'failed', \
             finished_at = {LAST_KNOWN_ALIVE}, \
             error = left(CASE WHEN queries.started_at IS NULL THEN $3::TEXT ELSE $4::TEXT END, \
                          $5), \
             result_rows = NULL \
         WHERE queries.id IN ( \
                 SELECT queries.id FROM queries \
                  WHERE {predicate} AND {account} \
                  ORDER BY queries.submitted_at, queries.id \
                  LIMIT $2) \
           AND {predicate} \
         RETURNING id, account_id, coordinator, coordinator_incarnation, submitted_at, \
                   started_at, finished_at"
    )
}

impl ServicesDb {
    /// The rows a sweep *would* resolve, changing nothing — `lldb-qe-reap --dry-run`.
    ///
    /// Same predicate, same order, same bound as [`Self::reap_stranded_queries`], so what this
    /// prints is what that would take. It is still only a snapshot: a coordinator can die, or a
    /// query finish, between the two calls.
    ///
    /// `account_id` narrows the sweep to one tenant; `None` is every account, which is the ordinary
    /// case (see `account_filter`).
    pub async fn list_stranded_queries(
        &self,
        account_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<QueryRecord>> {
        let rows = sqlx::query_as::<_, QueryRow>(&format!(
            "SELECT {QUERY_COLUMNS} FROM queries WHERE {} AND {} \
             ORDER BY queries.submitted_at, queries.id LIMIT $2",
            stranded_predicate(),
            account_filter("$3")
        ))
        .bind(f64::from(MISSED_RENEWALS_BEFORE_DEAD))
        .bind(batch_size(limit))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .context("listing query-history rows stranded by a coordinator that is no longer live")?;
        rows.into_iter().map(query_from_row).collect()
    }

    /// Resolve up to `limit` stranded rows, and report exactly which.
    ///
    /// One statement, deliberately: the scan, the liveness check and the compare-and-swap are
    /// evaluated against one database snapshot, so there is no window between "this coordinator is
    /// dead" and "therefore fail its rows" for the coordinator to come back to life in. The
    /// predicate is repeated in the outer `WHERE` — that repetition *is* the CAS (decision 3), and
    /// removing it as duplication is what would let a reaper overwrite a terminal state a live
    /// coordinator had just written.
    ///
    /// A run that returns exactly `limit` rows means the batch was filled and there may be more;
    /// running it again is always safe — and so is running two of them at once, since the CAS turns
    /// a double reap into a no-op rather than a conflict.
    ///
    /// `account_id` narrows the sweep to one tenant; `None` is every account.
    pub async fn reap_stranded_queries(
        &self,
        account_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<ReapedQuery>> {
        let rows = sqlx::query_as::<_, ReapedRow>(&reap_statement())
            .bind(f64::from(MISSED_RENEWALS_BEFORE_DEAD))
            .bind(batch_size(limit))
            .bind(NEVER_STARTED)
            .bind(DIED_MID_FLIGHT)
            // The same cap `query_log::truncate_error` applies, enforced in SQL because these
            // messages are composed there. Neither constant is anywhere near it; the bound is for
            // the day a future message, or a `left()`-less rewrite of this, makes one of them long.
            .bind(MAX_ERROR_LEN as i32)
            .bind(account_id)
            .fetch_all(self.pool())
            .await
            .context(
                "reaping query-history rows stranded by a coordinator that is no longer live",
            )?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    account_id,
                    coordinator,
                    coordinator_incarnation,
                    submitted_at,
                    started_at,
                    finished_at,
                )| ReapedQuery {
                    id,
                    account_id,
                    coordinator,
                    coordinator_incarnation,
                    reason: ReapReason::of(started_at),
                    submitted_at,
                    started_at,
                    finished_at,
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_reasons_say_different_things_and_both_fit_on_a_row() {
        assert_eq!(ReapReason::of(None), ReapReason::NeverStarted);
        assert_eq!(ReapReason::of(Some(Utc::now())), ReapReason::DiedMidFlight);
        assert_ne!(NEVER_STARTED, DIED_MID_FLIGHT);
        assert_eq!(ReapReason::NeverStarted.as_str(), NEVER_STARTED);
        assert_eq!(ReapReason::DiedMidFlight.as_str(), DIED_MID_FLIGHT);
        // "never started" and "died mid-flight" are different facts about the world, so the
        // messages have to be distinguishable by a human skimming history, not just by an enum.
        assert!(NEVER_STARTED.contains("never started"), "{NEVER_STARTED}");
        assert!(
            DIED_MID_FLIGHT.contains("abandoned mid-flight"),
            "{DIED_MID_FLIGHT}"
        );
        for message in [NEVER_STARTED, DIED_MID_FLIGHT] {
            // Stored, so bounded by the same cap every other error message is.
            assert!(message.chars().count() < MAX_ERROR_LEN, "{message}");
            // Greppable as a reaped row, whichever reason it carries.
            assert!(message.starts_with("reaped: "), "{message}");
            // Points at the columns that explain it, since the message alone cannot name the
            // coordinator — the row already does.
            assert!(message.contains("coordinator_incarnation"), "{message}");
        }
        assert_eq!(ReapReason::NeverStarted.to_string(), "never-started");
        assert_eq!(ReapReason::DiedMidFlight.to_string(), "died-mid-flight");
    }

    /// Decision 1, as a test that will fail if anyone reintroduces the shape it forbids.
    ///
    /// A reaper is safe because eligibility is decided by *liveness of the writing process*, and by
    /// nothing else. If a future change adds "…and it has been running for more than N minutes",
    /// this is what should stop it: there must be no comparison of a `queries` timestamp against a
    /// bound anywhere in the predicate.
    #[test]
    fn eligibility_is_liveness_of_the_writer_and_never_the_age_of_the_query() {
        let predicate = stranded_predicate();
        assert!(predicate.contains("NOT EXISTS"), "{predicate}");
        assert!(
            predicate.contains("coordinators.incarnation = queries.coordinator_incarnation"),
            "matching on the slot alone would strand every row of a coordinator that restarted \
             onto the same address: {predicate}"
        );
        assert!(
            predicate.contains("queries.state IN ('queued', 'running')"),
            "{predicate}"
        );
        // The liveness rule itself is spliced in from `liveness`, never re-spelled here — two
        // definitions of "alive" would disagree and nothing in the build would notice.
        assert!(predicate.contains(LIVE_PREDICATE), "{predicate}");
        assert!(!predicate.contains("{LIVE}"), "unsubstituted placeholder");
        assert!(!predicate.contains("{ACTIVE}"), "unsubstituted placeholder");
        for age_comparison in [
            "queries.submitted_at <",
            "queries.started_at <",
            "queries.submitted_at +",
            "queries.started_at +",
        ] {
            assert!(
                !predicate.contains(age_comparison),
                "a legitimately long-running query is indistinguishable from an abandoned one by \
                 age; reaping on `{age_comparison}` kills live work: {predicate}"
            );
        }
    }

    /// A query somebody deliberately stopped must never look abandoned to a sweep.
    ///
    /// Issue #38 added a fifth state and this is the interaction it has with this module — verified
    /// rather than assumed, and stated as a rule over *every* state so a sixth one cannot slip
    /// through: a reapable row is exactly a non-terminal one. `cancelled` is terminal, so it is
    /// excluded from both halves of the compare-and-swap by construction, and the race that
    /// worried the issue (a query cancelled at the same moment its coordinator dies) resolves the
    /// same way either interleaving falls — the sweep either sees a row still `running` and reaps
    /// it, or sees `cancelled` and skips it. Both end terminal, and neither loses an admission
    /// slot, because the slot was returned by dropping the future long before either write.
    #[test]
    fn every_terminal_state_is_outside_the_reapable_predicate() {
        use crate::query_log::QUERY_STATES;
        let predicate = stranded_predicate();
        for state in QUERY_STATES {
            let quoted = format!("'{}'", state.as_str());
            assert_eq!(
                predicate.contains(&quoted),
                !state.is_terminal(),
                "a reapable row is exactly a non-terminal one; `{state}` is on the wrong side of \
                 that: {predicate}"
            );
        }
        // Named explicitly as well, because this is the one the issue asks about by name.
        assert!(!predicate.contains("'cancelled'"), "{predicate}");
    }

    #[test]
    fn a_sweep_is_bounded_however_it_is_called() {
        assert_eq!(batch_size(0), 1, "a sweep that does nothing is not a sweep");
        assert_eq!(batch_size(7), 7);
        assert_eq!(batch_size(usize::MAX), MAX_REAP_BATCH as i64);
        // Checked at compile time, because both are build constants: a default larger than the cap
        // would mean the documented default is silently not what a run actually does.
        const {
            assert!(
                DEFAULT_REAP_BATCH <= MAX_REAP_BATCH,
                "the default batch must be reachable"
            )
        };
    }

    /// The compare-and-swap, checked where it is cheapest to check: the statement text.
    ///
    /// The predicate has to appear **twice** — once to choose the rows, once as the `UPDATE`'s own
    /// qualification so Postgres re-evaluates it against any row that moved underneath the scan.
    /// Deleting the second occurrence looks like removing duplication and is what would let this
    /// module overwrite a terminal state.
    #[test]
    fn the_sweep_repeats_its_predicate_as_a_compare_and_swap() {
        let statement = reap_statement();
        let predicate = stranded_predicate();
        assert_eq!(
            statement.matches(predicate.as_str()).count(),
            2,
            "the scan and the update must both carry the predicate: {statement}"
        );
        // The reaper only ever moves a row to `failed` — it never resurrects one, and it never
        // touches a terminal row (the predicate above excludes them).
        assert!(statement.contains("state = 'failed'"), "{statement}");
        // Bounded, always. An unbounded sweep is decision 5's forbidden shape.
        assert!(statement.contains("LIMIT $2"), "{statement}");
        // Decision 4: the closing timestamp is the writer's last renewal, not the sweep's clock.
        assert!(
            statement.contains("coordinators.last_seen_at"),
            "{statement}"
        );
        // Tenant scoping is a bind, so "every account" and "this account" are the same statement
        // and the same plan rather than two spellings that could drift.
        assert!(statement.contains(&account_filter("$6")), "{statement}");
    }
}
