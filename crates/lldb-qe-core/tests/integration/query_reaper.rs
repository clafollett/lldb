//! **The query reaper**, against a real Postgres: rows whose coordinator is gone are resolved, rows
//! whose coordinator is alive are never touched, and neither writer can clobber the other.
//!
//! Most of these are the issue's "done when" bullets; the rest are the invariants the issue creates
//! by existing. In rough order of how much they matter:
//!
//! - [`a_live_coordinators_long_running_query_is_never_reaped`] is the hazard. A reaper that decides
//!   a row is abandoned because it has been `running` for a while is not a reaper, it is a timeout,
//!   and it deletes work that is still going. This sweeps repeatedly for several multiples of the
//!   liveness threshold against a query that is still running and demands that nothing is ever
//!   taken — then kills the coordinator and demands the *same row* is taken, so the test cannot
//!   pass by never reaping anything.
//! - [`a_reaper_never_clobbers_a_terminal_state`] is the second-writer problem, in both orderings.
//! - [`a_dead_coordinators_queries_are_resolved`] is the bug being fixed, plus idempotence.
//! - [`a_replaced_coordinators_rows_are_reaped_though_its_slot_is_live`] is the case a rule matching
//!   on the coordinator *slot* alone gets exactly backwards, and the reason liveness records a pair.
//! - [`peak_concurrency_over_reaped_history_reports_the_true_bound`] is the *reason* the bug matters:
//!   the instrument #18 proves the admission bound with is biased by exactly one stranded row.
//! - [`a_row_whose_writer_cannot_be_judged_is_never_reaped`] is the honest limit, asserted rather
//!   than promised.
//!
//! Gating is the usual three-way one ([`crate::support::resolve_target`]). Every test works in its
//! own account and its own coordinator slot, and **every sweep is scoped to that account**, because
//! an unscoped sweep is fleet-wide by design: run against a shared CI database it would happily
//! resolve the stranded row `coordinator_liveness` deliberately leaves lying around.
//!
//! Like `coordinator_liveness`, these are slower than the rest of the binary on purpose:
//! `renew_interval_secs` is whole seconds, so the shortest threshold that can exist is 3s and a test
//! that has to outlive one really does take that long.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration query_reaper

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use lldb_qe_core::liveness::{CoordinatorIdentity, CoordinatorRegistration, death_threshold};
use lldb_qe_core::query_log::{QueryState, peak_concurrency};
use lldb_qe_core::reaper::{DEFAULT_REAP_BATCH, ReapReason};
use lldb_qe_core::services::ServicesDb;

use crate::support::{Cleanup, DbCleanup, Target, resolve_target, unique_name};

/// The fastest cadence the schema can express, so the threshold under test is 3s.
const RENEW: Duration = Duration::from_secs(1);

/// Connect and migrate, or say why the test is skipping.
async fn database(target: &Target) -> Result<Option<ServicesDb>> {
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise the query reaper"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;
    Ok(Some(db))
}

/// Delete exactly what a test made: its account (queries cascade with it) and its slots.
///
/// A `Drop` guard rather than a call at the end of each body, and registered at the top of each
/// test rather than the bottom: an assertion that fails unwinds straight past anything written down
/// there, so the old shape cleaned up after a *passing* run and left a failing one's rows for the
/// re-run you are about to do while debugging. See [`DbCleanup`].
fn clean_up(target: &Target, account_id: i64, slots: &[&str]) -> DbCleanup {
    let mut cleanup = DbCleanup::new(target.url().expect("a connected database"));
    cleanup.add(Cleanup::CoordinatorSlots(
        slots.iter().map(|s| s.to_string()).collect(),
    ));
    cleanup.account(account_id);
    cleanup
}

/// Block until the control plane stops believing `identity` is live — i.e. until its lease expires.
///
/// Polled rather than slept for, because the threshold is a property of the row's own interval and
/// waiting a fixed wall-clock duration would encode a second, unrelated one.
async fn await_expiry(db: &ServicesDb, identity: &CoordinatorIdentity) -> Result<()> {
    let deadline = Instant::now() + death_threshold(RENEW) + RENEW * 4;
    while db
        .is_coordinator_live(identity.slot(), Some(identity.incarnation()))
        .await?
    {
        assert!(
            Instant::now() < deadline,
            "coordinator `{identity}` never expired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// The bug: a coordinator dies with queries in flight, and something resolves the rows it left.
///
/// Both shapes are present because they are different facts and the issue asks for them to be
/// distinguishable: one query never got an admission slot, the other was executing. And the sweep is
/// run twice, because a maintenance job that is not idempotent is a maintenance job nobody can
/// schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_coordinators_queries_are_resolved() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-dead")).await?;
    let identity = CoordinatorIdentity::new(unique_name("reap-dead"));
    let slot = identity.slot().to_string();
    let _cleanup = clean_up(&target, account.id, &[&slot]);

    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;
    let queued = db
        .submit_query(account.id, None, "SELECT never_admitted()", Some(&identity))
        .await?;
    let running = db
        .submit_query(account.id, None, "SELECT in_flight()", Some(&identity))
        .await?;
    let running = db.mark_query_running(running.id).await?;
    // Killed, not stopped: no deregistration, no terminal write for either row. This is exactly
    // what SIGKILL leaves behind.
    drop(registration);

    // Before the lease expires there is nothing to reap — the process might simply be busy.
    assert!(
        db.list_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
            .await?
            .is_empty(),
        "a coordinator that has not yet missed its renewals is not dead"
    );

    await_expiry(&db, &identity).await?;

    // The dry run sees exactly what the sweep will take, and changes nothing.
    let candidates = db
        .list_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert_eq!(candidates.len(), 2, "{candidates:?}");
    assert!(
        candidates.iter().all(|c| !c.state.is_terminal()),
        "the dry run must not have written anything: {candidates:?}"
    );

    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert_eq!(reaped.len(), 2, "{reaped:?}");
    for row in &reaped {
        assert_eq!(row.coordinator, slot);
        assert_eq!(
            row.coordinator_incarnation,
            identity.incarnation(),
            "the row is attributed to the process that wrote it, not merely to the slot"
        );
    }

    // "Never started" and "died mid-flight" are different facts, and history now says which.
    let queued_row = db.query_by_id(queued.id).await?.expect("row");
    assert_eq!(queued_row.state, QueryState::Failed);
    assert_eq!(
        queued_row.started_at, None,
        "it never ran, and still has not"
    );
    assert!(queued_row.finished_at.is_some());
    let reason = queued_row
        .error
        .clone()
        .expect("a reaped row explains itself");
    assert!(reason.contains("never started"), "{reason}");
    assert_eq!(
        reaped.iter().find(|r| r.id == queued.id).map(|r| r.reason),
        Some(ReapReason::NeverStarted)
    );

    let running_row = db.query_by_id(running.id).await?.expect("row");
    assert_eq!(running_row.state, QueryState::Failed);
    assert_eq!(
        running_row.started_at, running.started_at,
        "reaping must not smear the timings it did not observe"
    );
    let reason = running_row
        .error
        .clone()
        .expect("a reaped row explains itself");
    assert!(reason.contains("abandoned mid-flight"), "{reason}");
    assert_eq!(
        reaped.iter().find(|r| r.id == running.id).map(|r| r.reason),
        Some(ReapReason::DiedMidFlight)
    );
    // Decision 4: closed at the last instant the writer was known alive, not at the sweep's clock.
    let finished = running_row.finished_at.expect("stamped");
    let last_seen = db
        .coordinator_by_slot(&slot)
        .await?
        .expect("the registration row survives a kill")
        .last_seen_at;
    assert!(
        finished <= chrono::Utc::now()
            && finished <= last_seen.max(running_row.started_at.unwrap()),
        "finished_at {finished} should not claim the query outlived its coordinator's last \
         renewal at {last_seen}"
    );

    // Neither row is active any more — which is the whole point, since this is what an operator
    // reads to see what a coordinator is doing.
    assert!(db.list_active_queries(account.id).await?.is_empty());

    // Idempotent: nothing is eligible twice, because terminal rows are not eligible at all.
    let second = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert!(
        second.is_empty(),
        "a second sweep must be a no-op: {second:?}"
    );

    Ok(())
}

/// The case a slot-only rule gets exactly backwards: a coordinator that died and was **replaced on
/// the same address**.
///
/// `--coordinator-id` defaults to the bound socket address, so this is not an exotic scenario, it is
/// what a restart normally looks like. The replacement's registration is live under the very slot
/// the dead process's rows name, so "is this row's coordinator alive?" answers *yes* if the question
/// is asked of the slot — and the rows are stranded forever. Asking it of the `(slot, incarnation)`
/// pair answers no, which is why `queries.coordinator_incarnation` exists.
///
/// Both halves are asserted, because either one alone can be satisfied by a broken rule: the dead
/// incarnation's rows are taken, and the live incarnation's row — same slot, same account, same
/// sweep — is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_coordinators_rows_are_reaped_though_its_slot_is_live() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-restart")).await?;
    let slot = unique_name("reap-restart");
    let _cleanup = clean_up(&target, account.id, &[&slot]);

    // The process that dies, with a query in flight.
    let first = CoordinatorIdentity::new(&slot);
    let first_registration =
        CoordinatorRegistration::start(db.clone(), first.clone(), RENEW).await?;
    let stranded = db
        .submit_query(account.id, None, "SELECT orphaned()", Some(&first))
        .await?;
    let stranded = db.mark_query_running(stranded.id).await?;
    drop(first_registration);

    // Its replacement takes the same slot, immediately — so the slot is never observably dead.
    let second = CoordinatorIdentity::new(&slot);
    let second_registration =
        CoordinatorRegistration::start(db.clone(), second.clone(), RENEW).await?;
    let healthy = db
        .submit_query(account.id, None, "SELECT still_going()", Some(&second))
        .await?;
    let healthy = db.mark_query_running(healthy.id).await?;

    // No waiting for a threshold: the previous incarnation stopped being live the instant the
    // replacement took the slot over, because there is only ever one row per slot.
    assert!(
        db.is_coordinator_live(&slot, None).await?,
        "something is serving under this name — that is the trap"
    );
    assert!(
        !db.is_coordinator_live(&slot, Some(first.incarnation()))
            .await?
    );

    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert_eq!(
        reaped.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![stranded.id],
        "the dead process's row must be taken and the live process's row must not, though both \
         name the same slot: {reaped:?}"
    );
    assert_eq!(
        reaped[0].coordinator_incarnation,
        first.incarnation(),
        "attributed to the process that died, not to the one that replaced it"
    );
    assert_eq!(
        db.query_by_id(healthy.id).await?.expect("row").state,
        QueryState::Running,
        "the replacement's own query is untouched"
    );

    second_registration.shut_down().await;
    Ok(())
}

/// **The hazard, and the reason this issue is dangerous rather than fiddly.**
///
/// A query runs for many multiples of the liveness threshold on a coordinator that is perfectly
/// healthy, and the reaper is run against it over and over throughout. Nothing may ever be taken.
/// This is the assertion that a rule containing "…and it has been running for more than N minutes"
/// cannot pass, and it is the direct extension of `coordinator_liveness`'s
/// `a_live_coordinator_is_never_concluded_dead` from the predicate to the thing that acts on it.
///
/// The second half is what stops the test being vacuous: the same row, the same query, the same
/// sweep — after the coordinator dies it *is* reaped. So "never reaped" above is a property of the
/// coordinator being alive, not of the sweep being broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_coordinators_long_running_query_is_never_reaped() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-live")).await?;
    let identity = CoordinatorIdentity::new(unique_name("reap-live"));
    let slot = identity.slot().to_string();
    let _cleanup = clean_up(&target, account.id, &[&slot]);

    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;
    let long = db
        .submit_query(account.id, None, "SELECT long_running()", Some(&identity))
        .await?;
    let long = db.mark_query_running(long.id).await?;
    // A second row that never got a slot: "queued for a long time" must be as safe as "running for
    // a long time", and it is the shape a saturated warehouse produces by the hundred.
    let waiting = db
        .submit_query(account.id, None, "SELECT still_queued()", Some(&identity))
        .await?;

    let threshold = death_threshold(RENEW);
    let run_for = threshold * 3 + RENEW;
    let started = Instant::now();
    let mut sweeps = 0usize;
    while started.elapsed() < run_for {
        let reaped = db
            .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
            .await?;
        assert!(
            reaped.is_empty(),
            "a live coordinator's query was reaped after {:?} (threshold {threshold:?}): \
             {reaped:?} — {registration:?}",
            started.elapsed()
        );
        assert!(
            db.list_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
                .await?
                .is_empty(),
            "…and it was not even a candidate"
        );
        sweeps += 1;
        tokio::time::sleep(RENEW / 4).await;
    }
    assert!(
        started.elapsed() > threshold,
        "the query has to outlive the threshold or this test proves nothing"
    );
    assert!(sweeps > 8, "too few sweeps to mean anything: {sweeps}");
    assert_eq!(
        db.query_by_id(long.id).await?.expect("row").state,
        QueryState::Running,
        "the row is untouched, not merely unreported"
    );
    assert_eq!(
        db.query_by_id(waiting.id).await?.expect("row").state,
        QueryState::Queued
    );

    // The query finishes normally, having never been interfered with.
    let done = db.mark_query_succeeded(long.id, 42).await?;
    assert_eq!(done.state, QueryState::Succeeded);
    assert_eq!(done.result_rows, Some(42));

    // And now the other half, so "never reaped" cannot mean "this sweep never reaps anything":
    // kill the coordinator and the *still-queued* row is resolved.
    drop(registration);
    await_expiry(&db, &identity).await?;
    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert_eq!(
        reaped.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![waiting.id],
        "the same sweep must take the row once its coordinator is gone: {reaped:?}"
    );
    assert_eq!(
        db.query_by_id(long.id).await?.expect("row").state,
        QueryState::Succeeded,
        "a terminal row is never eligible, whatever happened to its coordinator"
    );

    Ok(())
}

/// The second-writer problem, in both orderings.
///
/// `query_log::set_query_state` used to justify taking no lock on the grounds that a query row has
/// exactly one writer. It has two now, and the replacement claim — that the *reaper* is the one that
/// must prove the row has not moved, via a compare-and-swap carrying the whole reapable predicate —
/// is only worth anything if it is tested.
///
/// Two parts. First, deterministically: a coordinator writes `succeeded` and *then* the sweep runs.
/// The row must be untouched, and the sweep must not even claim it. Deleting the repeated predicate
/// from the `UPDATE`'s own `WHERE` in `reaper.rs` is what this catches. Second, under real
/// contention: many rounds of "mark succeeded" racing "sweep", where whichever lands first, the row
/// must end up saying `succeeded` — the coordinator is the authority on its own query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reaper_never_clobbers_a_terminal_state() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-race")).await?;
    let identity = CoordinatorIdentity::new(unique_name("reap-race"));
    let slot = identity.slot().to_string();
    let _cleanup = clean_up(&target, account.id, &[&slot]);

    // A coordinator that is registered and then killed: every row below is genuinely reapable, so
    // nothing in this test passes because the sweep had nothing to do.
    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;
    drop(registration);
    await_expiry(&db, &identity).await?;

    // ---- Ordering 1: the coordinator gets there first ------------------------------------------
    let finished_first = db
        .submit_query(
            account.id,
            None,
            "SELECT beat_the_reaper()",
            Some(&identity),
        )
        .await?;
    let finished_first = db.mark_query_running(finished_first.id).await?;
    let succeeded = db.mark_query_succeeded(finished_first.id, 7).await?;

    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert!(
        !reaped.iter().any(|r| r.id == finished_first.id),
        "the reaper claimed a row that had already succeeded: {reaped:?}"
    );
    let row = db.query_by_id(finished_first.id).await?.expect("row");
    assert_eq!(row.state, QueryState::Succeeded, "{row:?}");
    assert_eq!(row.error, None, "a succeeded row must carry no explanation");
    assert_eq!(row.result_rows, Some(7), "the count survives");
    assert_eq!(
        row.finished_at, succeeded.finished_at,
        "and so does the clock"
    );

    // ---- Ordering 2: the window itself, held open ----------------------------------------------
    // Ordering 1 is not the dangerous one — the sweep never even selects a row that is already
    // terminal. The clobber lives in the instant *between* the sweep's scan and its write, and this
    // constructs exactly that instant instead of hoping to hit it: an uncommitted transaction holds
    // the row's lock while writing `succeeded`, the sweep starts and blocks on that lock having
    // already seen the row as `running`, and the commit then releases it into the sweep's
    // compare-and-swap. Without the predicate repeated in the `UPDATE`'s own `WHERE`, this is where
    // Postgres's READ COMMITTED recheck would wave the reaper through onto a succeeded row.
    //
    // The `UPDATE` is raw SQL because holding a lock open needs a transaction and
    // `mark_query_succeeded` writes through the pool; it is deliberately the same assignment
    // `set_query_state` makes for a success.
    let contested = db
        .submit_query(account.id, None, "SELECT photo_finish()", Some(&identity))
        .await?;
    let contested = db.mark_query_running(contested.id).await?;

    let mut tx = db.pool().begin().await.context("opening the writer's tx")?;
    sqlx::query(
        "UPDATE queries SET state = 'succeeded', finished_at = now(), error = NULL, \
         result_rows = 3 WHERE id = $1",
    )
    .bind(contested.id)
    .execute(&mut *tx)
    .await
    .context("the coordinator's terminal write, uncommitted")?;

    let sweeper = {
        let db = db.clone();
        let account_id = account.id;
        tokio::spawn(async move {
            db.reap_stranded_queries(Some(account_id), DEFAULT_REAP_BATCH)
                .await
        })
    };
    // Long enough for the sweep to have scanned and be waiting on the row lock. If a very slow
    // machine misses that, the sweep simply runs after the commit and sees a terminal row — the
    // assertions below hold either way, so this is pointed rather than flaky.
    tokio::time::sleep(Duration::from_millis(300)).await;
    tx.commit().await.context("committing the writer")?;

    let reaped = sweeper.await?.context("the sweep raced against a commit")?;
    assert!(
        !reaped.iter().any(|r| r.id == contested.id),
        "the reaper wrote over a terminal state that landed while it was mid-sweep: {reaped:?}"
    );
    let row = db.query_by_id(contested.id).await?.expect("row");
    assert_eq!(row.state, QueryState::Succeeded, "{row:?}");
    assert_eq!(row.result_rows, Some(3), "{row:?}");
    assert_eq!(row.error, None, "{row:?}");

    // ---- Ordering 3: genuine contention --------------------------------------------------------
    // Enough rounds that both interleavings really occur; each round is a fresh row so a win in one
    // direction cannot hide a loss in the other. Removing the compare-and-swap fails this within a
    // couple of rounds, which is how it was checked.
    for round in 0..16 {
        let query = db
            .submit_query(account.id, None, "SELECT photo_finish()", Some(&identity))
            .await?;
        let query = db.mark_query_running(query.id).await?;

        let writer = {
            let db = db.clone();
            let id = query.id;
            tokio::spawn(async move { db.mark_query_succeeded(id, 1).await })
        };
        let sweeper = {
            let db = db.clone();
            let account_id = account.id;
            tokio::spawn(async move {
                db.reap_stranded_queries(Some(account_id), DEFAULT_REAP_BATCH)
                    .await
            })
        };
        // Both must *succeed as operations*: the loser of the race learns it lost from an empty
        // result or a corrected row, never from an error. A `set_query_state` that started failing
        // when it lost would push a retry loop onto every caller of `mark_query_succeeded`.
        writer
            .await?
            .with_context(|| format!("round {round}: the coordinator's write"))?;
        sweeper
            .await?
            .with_context(|| format!("round {round}: the sweep"))?;

        let row = db.query_by_id(query.id).await?.expect("row");
        assert_eq!(
            row.state,
            QueryState::Succeeded,
            "round {round}: however the two interleave, the coordinator is the authority on its \
             own query and the row must end up saying what actually happened: {row:?}"
        );
        assert_eq!(row.error, None, "round {round}: {row:?}");
        assert_eq!(row.result_rows, Some(1), "round {round}: {row:?}");
    }

    Ok(())
}

/// Why the bug matters: the instrument, before and after.
///
/// `peak_concurrency` treats a row with no `finished_at` as still running, so a single stranded row
/// overlaps every query that starts after it — and that sweep-line is one of the two independent
/// instruments #18 uses to prove the admission bound was respected. Here a coordinator dies leaving
/// one `running` row, then three queries run **strictly serially** on a healthy coordinator. The
/// true peak is 1. Before reaping the history reports 2; after, it reports the truth.
///
/// This is also what makes decision 4 load-bearing rather than decorative: closing the stranded
/// interval at `now()` — the moment of the sweep — would leave it overlapping all three of those
/// queries and this test would still read 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peak_concurrency_over_reaped_history_reports_the_true_bound() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-peak")).await?;
    let dead = CoordinatorIdentity::new(unique_name("reap-peak-dead"));
    let live = CoordinatorIdentity::new(unique_name("reap-peak-live"));
    let _cleanup = clean_up(&target, account.id, &[dead.slot(), live.slot()]);

    let doomed = CoordinatorRegistration::start(db.clone(), dead.clone(), RENEW).await?;
    let stranded = db
        .submit_query(account.id, None, "SELECT stranded()", Some(&dead))
        .await?;
    let stranded = db.mark_query_running(stranded.id).await?;
    drop(doomed);
    await_expiry(&db, &dead).await?;

    // Three queries, one at a time, on a coordinator that is alive throughout.
    let healthy = CoordinatorRegistration::start(db.clone(), live.clone(), RENEW).await?;
    for n in 0..3 {
        let q = db
            .submit_query(account.id, None, &format!("SELECT {n}"), Some(&live))
            .await?;
        let q = db.mark_query_running(q.id).await?;
        db.mark_query_succeeded(q.id, 1).await?;
    }

    let before = db.list_queries(account.id, 100).await?;
    assert_eq!(
        peak_concurrency(&before),
        2,
        "one stranded row should be inflating this: {before:?}"
    );

    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert_eq!(
        reaped.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![stranded.id]
    );

    let after = db.list_queries(account.id, 100).await?;
    assert_eq!(
        peak_concurrency(&after),
        1,
        "the true bound: these queries ran one at a time. {after:?}"
    );
    // The reaped row is still in history — resolved, not deleted. History that forgets is worse
    // than history that over-reports.
    assert_eq!(after.len(), 4);

    healthy.shut_down().await;
    Ok(())
}

/// The honest limit, asserted so nobody has to take it on trust.
///
/// A row whose `coordinator_incarnation` is NULL — history written before that column existed, or
/// by a writer that never registered — is **never** reaped. Liveness says nothing about such a row's
/// writer, and "says nothing" is not "is dead". The cost is that those rows are never resolved,
/// which is the right trade: the alternative fails rows belonging to processes that may be working
/// perfectly, and it would do so for every row written by a coordinator that has no services
/// database registration at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_whose_writer_cannot_be_judged_is_never_reaped() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-unknown")).await?;
    let _cleanup = clean_up(&target, account.id, &[]);

    // No coordinator at all: `submit_query(.., None)` is what an embedding with no registration
    // writes, and there has never been a `coordinators` row to judge it by.
    let anonymous = db
        .submit_query(account.id, None, "SELECT anonymous()", None)
        .await?;
    let anonymous = db.mark_query_running(anonymous.id).await?;
    assert_eq!(anonymous.coordinator, None);

    // Named, but from before the incarnation column existed: the slot is unknown to `coordinators`,
    // so a slot-only rule would happily reap this.
    let legacy = db
        .submit_query(account.id, None, "SELECT legacy()", None)
        .await?;
    sqlx::query(
        "UPDATE queries SET coordinator = $2, coordinator_incarnation = NULL WHERE id = $1",
    )
    .bind(legacy.id)
    .bind(unique_name("reap-legacy-slot"))
    .execute(db.pool())
    .await
    .context("back-dating a history row to before coordinator_incarnation existed")?;

    assert!(
        db.list_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
            .await?
            .is_empty()
    );
    let reaped = db
        .reap_stranded_queries(Some(account.id), DEFAULT_REAP_BATCH)
        .await?;
    assert!(
        reaped.is_empty(),
        "a row liveness cannot speak about must be left alone: {reaped:?}"
    );
    assert_eq!(
        db.query_by_id(anonymous.id).await?.expect("row").state,
        QueryState::Running
    );
    assert_eq!(
        db.query_by_id(legacy.id).await?.expect("row").state,
        QueryState::Queued
    );

    Ok(())
}
