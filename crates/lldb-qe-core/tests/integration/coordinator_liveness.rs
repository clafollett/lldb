//! **Coordinator liveness**, against a real Postgres: a coordinator registers, renews while it
//! runs, and is observably not-live when — and *only* when — it stops.
//!
//! Every assertion here is one of the issue's "done when" bullets, because each is a claim about
//! the database's clock and the database's rows and cannot be proven anywhere else. The one that
//! matters most is [`a_live_coordinator_is_never_concluded_dead`]: a mechanism that decides a
//! coordinator is dead because a *query* took a long time is not a liveness mechanism, it is a
//! timeout, and a reaper built on top of a timeout deletes work that is still running. The rest of
//! this file would pass against a timeout; that one would not.
//!
//! Gating is the usual three-way one ([`crate::support::resolve_target`]): an explicit
//! `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a clean skip.
//! Every test names its slot with a pid + nanosecond suffix and deletes exactly the rows it made,
//! so concurrent copies of this file — and anyone else's dev instance — are unaffected.
//!
//! These tests are **slower than the rest of the binary on purpose**. The stored renewal interval
//! is whole seconds (`renew_interval_secs INTEGER`), so the smallest threshold that can exist is
//! `MISSED_RENEWALS_BEFORE_DEAD` seconds, and a test proving something takes longer than the
//! threshold has to actually take longer than the threshold. Shortening them would mean testing a
//! threshold the product cannot be configured with.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration coordinator_liveness

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use lldb_qe_core::liveness::{
    CoordinatorIdentity, CoordinatorRegistration, MISSED_RENEWALS_BEFORE_DEAD, death_threshold,
};
use lldb_qe_core::services::ServicesDb;

use crate::support::{resolve_target, unique_name};

/// The fastest cadence the schema can express: `renew_interval_secs` is whole seconds, so this is
/// also the shortest threshold any real deployment could be configured with.
const RENEW: Duration = Duration::from_secs(1);

/// Connect and migrate, or say why the test is skipping.
async fn database(target: &crate::support::Target) -> Result<Option<ServicesDb>> {
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise coordinator liveness"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;
    Ok(Some(db))
}

/// Remove exactly the registrations this test made. Never touches anything global.
async fn forget(db: &ServicesDb, slot: &str) -> Result<()> {
    sqlx::query("DELETE FROM coordinators WHERE slot = $1")
        .bind(slot)
        .execute(db.pool())
        .await
        .context("deleting the test coordinator registration")?;
    Ok(())
}

/// Registration, renewal, and a clean exit — the happy path end to end.
///
/// The clean-exit half is its own "done when" bullet and is the reason `shutdown_at` exists: a
/// coordinator that stopped on purpose must be not-live on the *next read*, not after the full
/// threshold, because "stopped tidily thirty seconds ago" and "died thirty seconds ago" are
/// different facts and only one of them justifies resolving its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coordinator_registers_renews_and_leaves_promptly() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let identity = CoordinatorIdentity::new(unique_name("live"));
    let slot = identity.slot().to_string();

    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;

    let registered = db
        .coordinator_by_slot(&slot)
        .await?
        .expect("registration wrote a row");
    assert_eq!(registered.incarnation, identity.incarnation());
    assert_eq!(registered.renew_interval_secs, RENEW.as_secs() as i32);
    assert_eq!(registered.shutdown_at, None);
    assert!(
        registered.build_version.is_some(),
        "the row records which build registered: {registered:?}"
    );
    assert!(db.is_coordinator_live(&slot, None).await?);
    assert!(
        db.is_coordinator_live(&slot, Some(identity.incarnation()))
            .await?,
        "the exact process must be live, not merely the slot"
    );

    // Renewal is a background task, so wait for it to actually land rather than for a duration.
    let first_seen = registered.last_seen_at;
    let deadline = Instant::now() + RENEW * 10;
    while registration.renewals() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        registration.renewals() >= 2,
        "the renewal loop did not run: {registration:?}"
    );
    let renewed = db.coordinator_by_slot(&slot).await?.expect("still there");
    assert!(
        renewed.last_seen_at > first_seen,
        "renewal must advance last_seen_at: {first_seen} -> {}",
        renewed.last_seen_at
    );
    assert!(!registration.is_stale(), "{registration:?}");
    assert!(!registration.is_evicted(), "{registration:?}");
    assert_eq!(registration.failures(), 0, "{registration:?}");

    // A clean exit. The row survives — an operator asking "did it stop or did it die?" is asking
    // about exactly this timestamp — but the coordinator is not live any more, at once.
    registration.shut_down().await;
    let stopped = db
        .coordinator_by_slot(&slot)
        .await?
        .expect("a clean exit keeps the row");
    assert!(stopped.shutdown_at.is_some(), "{stopped:?}");
    assert!(
        !db.is_coordinator_live(&slot, None).await?,
        "a clean exit must be observable immediately, not after the threshold"
    );
    assert!(
        !db.live_coordinators()
            .await?
            .iter()
            .any(|row| row.slot == slot)
    );

    forget(&db, &slot).await
}

/// A coordinator killed outright: renewals simply stop, and nothing gets to say goodbye.
///
/// Dropping the registration is exactly what `SIGKILL` does to the database's view of this process
/// — the renewal loop stops mid-flight and no deregistration is written — which is why the handle's
/// `Drop` deliberately does not deregister.
///
/// The bound asserted here is the documented one: not-live within `MISSED_RENEWALS_BEFORE_DEAD` ×
/// the renewal interval. It is checked from both sides, because only the "still live just before"
/// half distinguishes a threshold from a coin flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_coordinator_expires_within_the_documented_bound() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let identity = CoordinatorIdentity::new(unique_name("killed"));
    let slot = identity.slot().to_string();

    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;
    assert!(db.is_coordinator_live(&slot, None).await?);
    let killed_at = Instant::now();
    drop(registration);

    // Still live a whole renewal interval later: a single missed renewal is a GC pause, not a death.
    tokio::time::sleep(RENEW).await;
    assert!(
        db.is_coordinator_live(&slot, None).await?,
        "one interval of silence must not be fatal — that is what the multiple is for"
    );

    // …and gone by the threshold. The generous slack is for the database's clock granularity and
    // for CI's scheduler, not for the bound: the assertion that matters is that this terminates at
    // all rather than that it terminates at a precise instant.
    let threshold = death_threshold(RENEW);
    let deadline = Instant::now() + threshold + RENEW * 3;
    while db.is_coordinator_live(&slot, None).await? {
        assert!(
            Instant::now() < deadline,
            "a killed coordinator was still live {:?} after it stopped renewing; the bound is \
             {MISSED_RENEWALS_BEFORE_DEAD} x {RENEW:?}",
            killed_at.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        killed_at.elapsed() >= threshold - RENEW,
        "declared dead after only {:?}, which is inside the threshold of {threshold:?}",
        killed_at.elapsed()
    );

    // The row is still there and still says it never shut down cleanly. That difference is what a
    // reaper reads: nothing here deleted the evidence.
    let row = db.coordinator_by_slot(&slot).await?.expect("row survives");
    assert_eq!(row.shutdown_at, None, "a kill is not a clean exit");
    assert!(!row.is_live_at(chrono::Utc::now()), "{row:?}");

    forget(&db, &slot).await
}

/// **The assertion that separates a liveness mechanism from a timeout.**
///
/// A coordinator runs one query for many multiples of the liveness threshold, and is never once
/// concluded dead while it does. Without this, a reaper is not safe to build: the shape it would
/// otherwise be tempted into — "this row has been `running` longer than the threshold, so its
/// coordinator must be gone" — passes every other test in this file and destroys live work.
///
/// What is real here and what is not, stated plainly. The *coordinator side* is real: a genuine
/// registration, a genuine background renewal loop, a real `queries` row moved to `running` and
/// held there, on a multi-threaded runtime that is doing other work throughout. What is simulated
/// is the query's own execution — this file has no worker fleet, so the long-running work is a
/// loop rather than a plan. That substitution is sound for what is being proven, because the claim
/// under test is about the renewal loop keeping up while the process is busy for a long time, and
/// the loop cannot tell what is keeping it busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_coordinator_is_never_concluded_dead() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let identity = CoordinatorIdentity::new(unique_name("longquery"));
    let slot = identity.slot().to_string();
    let account = db.create_account(&unique_name("longquery-acct")).await?;

    let registration = CoordinatorRegistration::start(db.clone(), identity.clone(), RENEW).await?;

    // A real history row, owned by this coordinator, held `running` for the whole test.
    let query = db
        .submit_query(account.id, None, "SELECT long_running()", Some(&identity))
        .await?;
    let query = db.mark_query_running(query.id).await?;
    assert_eq!(query.coordinator.as_deref(), Some(slot.as_str()));
    assert_eq!(
        query.coordinator_incarnation.as_deref(),
        Some(identity.incarnation()),
        "the row must name the process, not just the slot"
    );

    let threshold = death_threshold(RENEW);
    // Comfortably longer than the threshold, which is the entire point — a query is allowed to
    // outlive it, and a coordinator running one is not.
    let run_for = threshold * 3 + RENEW;
    let started = Instant::now();
    let mut checks = 0usize;
    while started.elapsed() < run_for {
        // Poll from outside, the way a reaper would, and demand the answer never wavers.
        assert!(
            db.is_coordinator_live(&slot, Some(identity.incarnation()))
                .await?,
            "a live coordinator was concluded dead after {:?} of a query that is still running \
             (threshold {threshold:?}): {registration:?}",
            started.elapsed()
        );
        checks += 1;
        tokio::time::sleep(RENEW / 4).await;
    }
    assert!(
        started.elapsed() > threshold,
        "the query has to outlive the threshold or this test proves nothing"
    );
    assert!(
        checks > 8,
        "too few observations to mean anything: {checks}"
    );
    assert!(!registration.is_stale(), "{registration:?}");
    assert!(
        registration.renewals() >= (run_for.as_secs() / RENEW.as_secs()) / 2,
        "the renewal loop was starved by the workload: {registration:?}"
    );

    // The query finishes normally, having never been interfered with.
    let done = db.mark_query_succeeded(query.id, 1).await?;
    assert_eq!(done.state, lldb_qe_core::query_log::QueryState::Succeeded);

    registration.shut_down().await;
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account.id)
        .execute(db.pool())
        .await
        .context("deleting the test account")?;
    forget(&db, &slot).await
}

/// A restarted coordinator is unambiguously distinguishable from the process it replaced — **on
/// the same address**, which is the case that used to be invisible.
///
/// Two things are asserted, and the second is the one a reaper needs. The slot stays live across
/// the restart (something *is* serving under that name), while the old incarnation does not (the
/// thing that wrote those rows is gone). A design that recorded only the slot would answer "live"
/// to both and strand the first process's rows forever; one that recorded only a per-process id
/// would lose the operator's stable name for the coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_is_distinguishable_from_the_process_it_replaced() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };
    let slot = unique_name("restart");
    let account = db.create_account(&unique_name("restart-acct")).await?;

    // The first process, with a query in flight when it dies.
    let first = CoordinatorIdentity::new(&slot);
    let first_registration =
        CoordinatorRegistration::start(db.clone(), first.clone(), RENEW).await?;
    let stranded = db
        .submit_query(account.id, None, "SELECT 1", Some(&first))
        .await?;
    let stranded = db.mark_query_running(stranded.id).await?;
    // Killed: no deregistration, no terminal write for its query.
    drop(first_registration);

    // The replacement lands on the very same address, which is what `--coordinator-id`'s default
    // makes overwhelmingly likely in practice.
    let second = CoordinatorIdentity::new(&slot);
    assert_eq!(second.slot(), first.slot());
    assert_ne!(second.incarnation(), first.incarnation());
    let second_registration =
        CoordinatorRegistration::start(db.clone(), second.clone(), RENEW).await?;

    let row = db.coordinator_by_slot(&slot).await?.expect("row");
    assert_eq!(
        row.incarnation,
        second.incarnation(),
        "the restart takes the slot over"
    );
    assert!(
        db.is_coordinator_live(&slot, None).await?,
        "something is serving under this name"
    );
    assert!(
        db.is_coordinator_live(&slot, Some(second.incarnation()))
            .await?
    );
    assert!(
        !db.is_coordinator_live(&slot, Some(first.incarnation()))
            .await?,
        "the process that wrote the stranded row is gone, and the slot being live must not hide it"
    );
    // There is exactly one row for the slot, so the two incarnations can never both be live.
    assert_eq!(
        db.list_coordinators()
            .await?
            .iter()
            .filter(|r| r.slot == slot)
            .count(),
        1
    );

    // And the stranded query row carries enough to be attributed to the dead process rather than
    // to the live one — which is the whole reason `coordinator_incarnation` exists.
    let stranded = db.query_by_id(stranded.id).await?.expect("row");
    assert_eq!(stranded.coordinator.as_deref(), Some(slot.as_str()));
    assert_eq!(
        stranded.coordinator_incarnation.as_deref(),
        Some(first.incarnation())
    );
    assert_eq!(stranded.state, lldb_qe_core::query_log::QueryState::Running);

    second_registration.shut_down().await;
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account.id)
        .execute(db.pool())
        .await
        .context("deleting the test account")?;
    forget(&db, &slot).await
}

/// Decision 2, both branches, as behaviour rather than prose.
///
/// A services database that stops answering must not take a working coordinator down — the process
/// keeps renewing and keeps serving — but it must not pretend either, so once the threshold passes
/// the handle says so. And when a *second* process takes the slot, the first stops renewing rather
/// than fighting it, because two processes trading one lease back and forth forever is worse than
/// one of them being visibly wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_renewal_is_survived_and_a_stolen_slot_is_conceded() -> Result<()> {
    let target = resolve_target()?;
    let Some(db) = database(&target).await? else {
        return Ok(());
    };

    // ---- A database that stops answering -----------------------------------------------------
    let identity = CoordinatorIdentity::new(unique_name("outage"));
    let slot = identity.slot().to_string();
    // Its own pool, so closing it simulates an outage for this coordinator alone.
    let doomed = ServicesDb::connect(target.url().expect("checked above")).await?;
    let registration = CoordinatorRegistration::start(doomed.clone(), identity, RENEW).await?;
    doomed.close().await;

    let deadline = Instant::now() + death_threshold(RENEW) + RENEW * 4;
    while !registration.is_stale() {
        assert!(
            Instant::now() < deadline,
            "the registration never noticed it had stopped renewing: {registration:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        registration.failures() > 0,
        "failed renewals must be counted, not swallowed: {registration:?}"
    );
    assert!(
        !registration.is_evicted(),
        "an outage is not an eviction — nobody took the slot: {registration:?}"
    );
    // The loop is still trying: this is the "keeps serving" half, and the failure count proving it
    // is still going is the observable form of it.
    let failures = registration.failures();
    tokio::time::sleep(RENEW * 2).await;
    assert!(
        registration.failures() > failures,
        "the renewal loop gave up instead of retrying: {registration:?}"
    );
    drop(registration);
    forget(&db, &slot).await?;

    // ---- A second process on one slot --------------------------------------------------------
    let shared_slot = unique_name("contested");
    let incumbent = CoordinatorIdentity::new(&shared_slot);
    let incumbent_registration =
        CoordinatorRegistration::start(db.clone(), incumbent.clone(), RENEW).await?;
    let usurper = CoordinatorIdentity::new(&shared_slot);
    let usurper_registration =
        CoordinatorRegistration::start(db.clone(), usurper.clone(), RENEW).await?;

    let deadline = Instant::now() + RENEW * 8;
    while !incumbent_registration.is_evicted() {
        assert!(
            Instant::now() < deadline,
            "the incumbent never noticed its slot was taken: {incumbent_registration:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Conceded, not contested: the row still belongs to the process that took it, and the
    // incumbent is not periodically stealing it back.
    tokio::time::sleep(RENEW * 2).await;
    let row = db.coordinator_by_slot(&shared_slot).await?.expect("row");
    assert_eq!(
        row.incarnation,
        usurper.incarnation(),
        "the slot must not flap between the two"
    );
    assert!(
        !db.is_coordinator_live(&shared_slot, Some(incumbent.incarnation()))
            .await?
    );

    usurper_registration.shut_down().await;
    drop(incumbent_registration);
    forget(&db, &shared_slot).await
}
