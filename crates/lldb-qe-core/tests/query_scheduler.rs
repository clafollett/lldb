//! The query scheduler end-to-end: a **real server**, **real workers**, **real Postgres**, and
//! `N` concurrent queries against a limit of `K < N`.
//!
//! This is issue #18's acceptance criteria stated as facts a machine can settle:
//!
//! 1. **Concurrency is served and bounded.** Submit `N` queries at once against a warehouse whose
//!    limit is `K`; every one returns the right answer, and the number ever *executing* at the
//!    same moment never exceeds `K`.
//! 2. **The queue drains.** The `N - K` that had to wait all eventually ran; nothing was lost and
//!    nothing was left holding a slot.
//! 3. **History is queryable.** Every query has a row with a sane state and sane timings.
//! 4. **A failure is recorded and does not wedge the queue.** The failure mode this design is
//!    most exposed to is a slot leaked on the error path — a server that degrades to serial and
//!    then stops. So a deliberately broken query runs in the middle of a batch of good ones, and
//!    the good ones after it must still complete.
//!
//! # What is real and what is faked
//!
//! Real: the Flight server, the Flight client, the admission control, the DataFusion planning,
//! the distributed execution across in-process workers, the services database, every SQL
//! statement, every state transition and every timestamp.
//!
//! Faked: exactly one link — DNS. `analytics.lldb.local` does not resolve on a laptop, so the
//! coordinator is given the same injected resolver `warehouse_routing.rs` uses, answering the
//! warehouse's name with the addresses of the workers standing in for its tasks. That is
//! precisely what Cloud Map does for an ECS service at `desiredCount: N`.
//!
//! # Two independent instruments for peak concurrency
//!
//! The scheduler's own counter could be self-consistently wrong, so the peak is also recomputed
//! from the `started_at`/`finished_at` timestamps **Postgres** stamped, by
//! [`peak_concurrency`]. Both must respect the bound. One measures the bookkeeping; the other
//! measures what actually happened.
//!
//! The database is found the same way `services_db.rs` finds it (see [`support`]): an explicit
//! `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a clean skip.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test query_scheduler
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test query_scheduler -- --nocapture

mod support;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::engine::BoxResolver;
use lldb_qe_core::query_log::{QueryRecord, QueryState, peak_concurrency};
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, serve_coordinator, submit_query,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::warehouse::{Warehouse, WarehouseState};
use lldb_qe_core::{DEFAULT_WAREHOUSE_ENDPOINT, flight};
use support::{resolve_target, unique_name};

/// Queries submitted at once.
const SUBMISSIONS: usize = 12;
/// Workers behind the warehouse — and therefore, since the limit is sized from the warehouse's
/// row, the concurrency limit `K`. Chosen `< SUBMISSIONS` so the queue is forced to engage.
const WAREHOUSE_SIZE: i32 = 2;

/// Skip-or-connect, shared by every test in this file. Returns `None` when there is no database,
/// having already printed why.
async fn db_or_skip(what: &str) -> Result<Option<(ServicesDb, support::Target)>> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP ({what}): set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with \
             a Docker daemon, to exercise the query scheduler"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    // Idempotent, and every test in this binary needs the schema — whichever runs first applies
    // it, the rest find it applied (sqlx's advisory lock makes the race a wait, not a corruption).
    db.migrate().await.context("applying migrations")?;
    Ok(Some((db, target)))
}

/// Seed a parquet file with several row groups, so a scan can be split between workers.
fn seed_parquet(dir: &Path, rows: i64, groups: i64) -> Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let g: Vec<String> = (0..rows).map(|i| format!("g{}", i % groups)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(g)),
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
        ],
    )?;
    let path = dir.join("rows.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(128))
        .build();
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// A session configured so tiny test data still yields a real distribution boundary.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

/// Start `count` in-process workers, standing in for a warehouse's tasks.
async fn start_workers(count: usize) -> Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            flight::serve_worker(listener, SessionContext::new())
                .await
                .expect("worker serve");
        });
        addrs.push(addr);
    }
    Ok(addrs)
}

/// A resolver that answers one warehouse's DNS name with its tasks' addresses, and errors on
/// anything else — a query must never resolve a warehouse it was not routed to.
fn cloud_map(authority: String, addrs: Vec<SocketAddr>) -> BoxResolver {
    Arc::new(move |asked: String| {
        let answer = if asked == authority {
            Ok(addrs.clone())
        } else {
            Err(anyhow::anyhow!("no warehouse registered as `{asked}`"))
        };
        Box::pin(std::future::ready(answer))
    })
}

/// A running server plus everything needed to talk to it and clean up after it.
struct Harness {
    db: ServicesDb,
    account_id: i64,
    warehouse: String,
    url: String,
    coordinator: Arc<Coordinator>,
    /// Kept alive so the seeded parquet outlives the queries reading it.
    _tmp: tempfile::TempDir,
}

impl Harness {
    /// Stand up: workers, a warehouse row, a catalog, a coordinator, a listening Flight port.
    async fn start(db: ServicesDb, tag: &str) -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let path = seed_parquet(tmp.path(), 1200, 6)?;

        let account = db.create_account(&unique_name(tag)).await?;
        let warehouse_name = unique_name(&format!("wh-{tag}"));
        let warehouse: Warehouse = db
            .create_warehouse(
                account.id,
                &warehouse_name,
                WAREHOUSE_SIZE,
                WarehouseState::Running,
            )
            .await?;

        // The warehouse's own row is what sizes the admission gate, so the fleet standing behind
        // its DNS name is exactly `warehouse.size` tasks — desired and observed state agreeing.
        let workers = start_workers(warehouse.size as usize).await?;
        let authority = format!("{warehouse_name}.lldb.local:50051");
        let resolver = cloud_map(authority, workers);

        let ctx = distributing_ctx();
        ctx.register_parquet(
            "rows",
            path.to_str().expect("utf-8 path"),
            ParquetReadOptions::default(),
        )
        .await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let coordinator = Arc::new(
            Coordinator::new(
                ctx,
                Some(db.clone()),
                CoordinatorConfig {
                    default_account: account.name.clone(),
                    workers: Vec::new(),
                    warehouse_endpoint: vec![DEFAULT_WAREHOUSE_ENDPOINT.to_string()],
                    // Deliberately unset: this is the "sized from the warehouse's row" default,
                    // which is the configuration the issue asks for. The limit under test is
                    // therefore `warehouse.size`.
                    max_concurrent_queries: None,
                    max_queued_queries: 64,
                    coordinator_id: format!("test-{}", addr),
                },
            )
            .with_resolver(resolver),
        );

        let serving = Arc::clone(&coordinator);
        tokio::spawn(async move {
            serve_coordinator(listener, serving, std::future::pending::<()>())
                .await
                .expect("coordinator serve");
        });

        Ok(Self {
            db,
            account_id: account.id,
            warehouse: warehouse_name,
            url: format!("http://{addr}"),
            coordinator,
            _tmp: tmp,
        })
    }

    /// The scheduler's own view of the warehouse's gate.
    fn gate(&self) -> lldb_qe_core::scheduler::AdmissionSnapshot {
        self.coordinator.scheduler().snapshot()[&self.warehouse]
    }

    /// Delete the account, which cascades to its warehouse and its query history.
    async fn cleanup(&self) -> Result<()> {
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(self.account_id)
            .execute(self.db.pool())
            .await
            .context("deleting the test account")?;
        Ok(())
    }
}

/// The workload. Distinct per `i` on purpose: identical queries would hit the workers' stage
/// cache (stages are content-addressed), every submission after the first would be nearly free,
/// and a bound of `K` would look respected because nothing ever ran long enough to contend.
fn workload(i: usize) -> String {
    format!(
        "SELECT a.g, count(*) AS n FROM rows a JOIN rows b ON a.g = b.g \
         WHERE a.v >= {i} GROUP BY a.g ORDER BY a.g"
    )
}

/// Answer the same question locally, with no fleet and no scheduler, as the oracle.
async fn oracle(i: usize) -> Result<Vec<GroupCount>> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 1200, 6)?;
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        path.to_str().expect("utf-8 path"),
        ParquetReadOptions::default(),
    )
    .await?;
    rows_of(&ctx.sql(&workload(i)).await?.collect().await?)
}

/// Flatten `(g, n)` batches into a comparable Vec.
///
/// Goes through [`extract_group_counts`] rather than downcasting by hand because DataFusion reads
/// parquet strings back as `Utf8View`, not `Utf8` — the same normalization every other test in
/// this repo needs.
fn rows_of(batches: &[RecordBatch]) -> Result<Vec<GroupCount>> {
    let mut out = extract_group_counts(batches)?;
    out.sort();
    Ok(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queries_are_bounded_queued_drained_and_recorded() -> Result<()> {
    let Some((db, _target)) = db_or_skip("scheduling").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "sched").await?;
    let result = concurrency_body(&harness).await;
    // Clean up whatever happened, then report.
    harness.cleanup().await?;
    result
}

async fn concurrency_body(harness: &Harness) -> Result<()> {
    let limit = WAREHOUSE_SIZE as usize;

    // Oracles first, so the timing of the concurrent phase is not polluted by computing them.
    let mut expected = Vec::with_capacity(SUBMISSIONS);
    for i in 0..SUBMISSIONS {
        expected.push(oracle(i).await?);
    }

    // ---- Submit them all at once -------------------------------------------------------------
    let mut submissions = Vec::with_capacity(SUBMISSIONS);
    for i in 0..SUBMISSIONS {
        let url = harness.url.clone();
        let warehouse = harness.warehouse.clone();
        submissions.push(tokio::spawn(async move {
            let request = QueryRequest::new(workload(i)).on_warehouse(warehouse);
            submit_query(&url, &request).await.map(|s| (i, s))
        }));
    }

    let mut query_ids = Vec::with_capacity(SUBMISSIONS);
    for task in submissions {
        let (i, submitted) = task.await.expect("submission task")?;
        // 1. Every one of the N answers is correct.
        assert_eq!(
            rows_of(&submitted.batches)?,
            expected[i],
            "query {i} returned the wrong answer"
        );
        let id = submitted
            .query_id
            .with_context(|| format!("query {i} came back with no id"))?;
        query_ids.push(id);
    }
    assert_eq!(query_ids.len(), SUBMISSIONS);

    // ---- 2. The bound held, and the queue engaged and drained --------------------------------
    let gate = harness.gate();
    assert_eq!(
        gate.max_concurrent, limit,
        "the limit must come from the warehouse row: {gate:?}"
    );
    assert!(
        gate.peak_running <= limit,
        "the scheduler ran {} queries at once against a limit of {limit}: {gate:?}",
        gate.peak_running
    );
    assert_eq!(
        gate.peak_running, limit,
        "with {SUBMISSIONS} concurrent submissions the limit should have been reached; if this \
         fails the workload finished too fast to contend: {gate:?}"
    );
    assert!(
        gate.peak_queued > 0,
        "{SUBMISSIONS} submissions against {limit} slots must have queued: {gate:?}"
    );
    assert_eq!(gate.admitted, SUBMISSIONS as u64, "{gate:?}");
    assert_eq!(
        gate.refused, 0,
        "nothing should have been refused: {gate:?}"
    );
    assert_eq!(gate.running, 0, "a slot was leaked: {gate:?}");
    assert_eq!(gate.queued, 0, "the queue did not drain: {gate:?}");
    eprintln!("admission: {gate:?}");

    // ---- 3. History ---------------------------------------------------------------------------
    let history = harness
        .db
        .list_queries(harness.account_id, SUBMISSIONS as i64 * 2)
        .await?;
    assert_eq!(
        history.len(),
        SUBMISSIONS,
        "every submission must have a history row"
    );
    for record in &history {
        assert_eq!(
            record.state,
            QueryState::Succeeded,
            "query {} is {}: {:?}",
            record.id,
            record.state,
            record.error
        );
        assert_eq!(record.error, None, "a succeeded query carries no error");
        assert!(record.result_rows.is_some(), "result_rows was not recorded");
        assert_eq!(
            record.result_rows,
            Some(6),
            "the workload groups into 6 rows"
        );
        assert_eq!(
            record.coordinator.as_deref(),
            Some(format!("test-{}", harness.url.trim_start_matches("http://")).as_str()),
            "the coordinator that ran it must be recorded"
        );
        assert_timings_are_sane(record);
        // The warehouse it ran on is recorded — the other half of criterion 3.
        assert!(
            record.warehouse_id.is_some(),
            "warehouse_id was not recorded"
        );
    }
    // Nothing left in flight, from the database's point of view as well as the scheduler's.
    assert!(
        harness
            .db
            .list_active_queries(harness.account_id)
            .await?
            .is_empty(),
        "history still shows queued/running rows after every query returned"
    );

    // ---- The second, independent instrument --------------------------------------------------
    // Recomputed from Postgres's own clock rather than from the scheduler's counters: if the two
    // disagree, the bookkeeping is lying about what happened.
    let observed = peak_concurrency(&history);
    assert!(
        observed <= limit,
        "history shows {observed} queries executing at once against a limit of {limit}"
    );
    eprintln!(
        "peak concurrency: scheduler {}, history {observed}",
        gate.peak_running
    );

    // At least one query really did wait — the queue is not decorative.
    let waited = history
        .iter()
        .filter(|r| r.queue_time().is_some_and(|d| d.num_milliseconds() > 0))
        .count();
    assert!(
        waited > 0,
        "with {SUBMISSIONS} submissions and {limit} slots, some query must have queued"
    );

    Ok(())
}

/// Every timing invariant a row must satisfy, in one place.
fn assert_timings_are_sane(record: &QueryRecord) {
    let started = record
        .started_at
        .unwrap_or_else(|| panic!("query {} succeeded without a started_at", record.id));
    let finished = record
        .finished_at
        .unwrap_or_else(|| panic!("query {} succeeded without a finished_at", record.id));
    assert!(
        record.submitted_at <= started,
        "query {} started before it was submitted",
        record.id
    );
    assert!(
        started <= finished,
        "query {} finished before it started",
        record.id
    );
    // Server clock, so this should be seconds not hours away from ours.
    let age = chrono::Utc::now().signed_duration_since(record.submitted_at);
    assert!(
        age.num_minutes().abs() < 10,
        "query {}'s submitted_at is {age} away from now",
        record.id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_query_is_recorded_and_does_not_wedge_the_queue() -> Result<()> {
    let Some((db, _target)) = db_or_skip("failure handling").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "fail").await?;
    let result = failure_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn failure_body(harness: &Harness) -> Result<()> {
    let expected = oracle(0).await?;

    // A batch with a poisoned query in the middle. If a failure leaked its admission slot, the
    // gate would shrink by one every time — with a limit of 2, two failures would wedge it
    // outright and the good queries after them would never return.
    let mut submissions = Vec::new();
    for i in 0..8usize {
        let url = harness.url.clone();
        let warehouse = harness.warehouse.clone();
        // Two poisoned queries, enough to exhaust a limit-2 gate if slots leaked.
        let sql = if i == 2 || i == 5 {
            "SELECT * FROM no_such_table".to_string()
        } else {
            workload(0)
        };
        submissions.push(tokio::spawn(async move {
            let request = QueryRequest::new(sql).on_warehouse(warehouse);
            (i, submit_query(&url, &request).await)
        }));
    }

    let mut good = 0;
    let mut bad = 0;
    for task in submissions {
        let (i, outcome) = task.await.expect("submission task");
        if i == 2 || i == 5 {
            let error = outcome.expect_err("a query against a missing table must fail");
            let message = format!("{error:#}");
            assert!(
                message.contains("no_such_table"),
                "the failure must name the cause: {message}"
            );
            bad += 1;
        } else {
            // The point of the test: the good queries still complete, and correctly.
            let submitted = outcome.with_context(|| format!("query {i} should have succeeded"))?;
            assert_eq!(rows_of(&submitted.batches)?, expected, "query {i}");
            good += 1;
        }
    }
    assert_eq!((good, bad), (6, 2));

    // The gate is intact: nothing running, nothing queued, every slot back.
    let gate = harness.gate();
    assert_eq!(gate.running, 0, "a failed query leaked its slot: {gate:?}");
    assert_eq!(gate.queued, 0, "{gate:?}");
    assert_eq!(gate.admitted, 8, "every query was admitted: {gate:?}");
    assert!(gate.peak_running <= WAREHOUSE_SIZE as usize, "{gate:?}");

    // …and one more query goes straight through, which is the operational version of the same
    // claim: the server is still serving after the failures.
    let request = QueryRequest::new(workload(0)).on_warehouse(harness.warehouse.clone());
    let after = submit_query(&harness.url, &request)
        .await
        .context("the server must still serve after a failure")?;
    assert_eq!(rows_of(&after.batches)?, expected);

    // ---- The failures are in history, as failures, with their reason -------------------------
    let history = harness.db.list_queries(harness.account_id, 64).await?;
    assert_eq!(history.len(), 9, "8 submissions plus the follow-up");

    let failures: Vec<_> = history
        .iter()
        .filter(|r| r.state == QueryState::Failed)
        .collect();
    assert_eq!(failures.len(), 2, "both broken queries must be recorded");
    for failure in &failures {
        let error = failure
            .error
            .as_deref()
            .unwrap_or_else(|| panic!("query {} failed with no error text", failure.id));
        assert!(
            error.contains("no_such_table"),
            "the recorded error must say what went wrong: {error}"
        );
        assert_eq!(failure.sql_text, "SELECT * FROM no_such_table");
        assert_eq!(
            failure.result_rows, None,
            "a failed query returned no rows, which is not the same as zero"
        );
        // It was admitted and then failed, so it has both timestamps — a failure *during*
        // execution is distinguishable from one before it ever ran.
        assert_timings_are_sane(failure);
    }
    assert_eq!(
        history
            .iter()
            .filter(|r| r.state == QueryState::Succeeded)
            .count(),
        7
    );
    assert!(
        harness
            .db
            .list_active_queries(harness.account_id)
            .await?
            .is_empty(),
        "nothing should still be queued or running"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutting_down_refuses_new_work_instead_of_stranding_it() -> Result<()> {
    let Some((db, _target)) = db_or_skip("shutdown").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "stop").await?;
    let result = shutdown_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn shutdown_body(harness: &Harness) -> Result<()> {
    // The shutdown contract, stated as a fact: once the scheduler is closed, a submission is
    // refused *immediately* with `UNAVAILABLE` rather than waiting in a queue that will never be
    // served, and the refusal is recorded as a failure rather than being left `queued` forever.
    harness.coordinator.begin_shutdown();

    let request = QueryRequest::new(workload(0)).on_warehouse(harness.warehouse.clone());
    let error = submit_query(&harness.url, &request)
        .await
        .expect_err("a shutting-down coordinator must not start new work");
    let message = format!("{error:#}");
    assert!(message.contains("shutting down"), "{message}");

    let history = harness.db.list_queries(harness.account_id, 8).await?;
    assert_eq!(history.len(), 1, "the refused query is still recorded");
    let refused = &history[0];
    assert_eq!(refused.state, QueryState::Failed);
    assert!(
        refused.started_at.is_none(),
        "a refused query never started, and history must say so"
    );
    assert!(
        refused.finished_at.is_some(),
        "a refused query is terminal, not left queued forever"
    );
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|e| e.contains("shutting down")),
        "{:?}",
        refused.error
    );
    Ok(())
}

/// A client that hangs up mid-query must not leave its history row `queued`/`running` forever.
///
/// This matters beyond tidiness. `list_active_queries` is what an operator reads to see what a
/// coordinator is doing, and the sweep-line over `started_at`/`finished_at` is one of the two
/// instruments this very file uses to *prove* the concurrency bound. A row that never reaches a
/// terminal state corrupts both — silently, and in the direction of "busier than it really is".
/// Clients disconnecting is ordinary rather than exceptional, so this is a routine path.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_abandoned_by_its_client_is_closed_out_rather_than_left_active() -> Result<()> {
    let Some((db, _target)) = db_or_skip("abandonment").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "gone").await?;
    let result = abandonment_body(&harness).await;
    harness.cleanup().await?;
    result
}

/// How long the abandonment test will wait for an asynchronous state change before giving up.
///
/// Deliberately generous, and the generosity costs nothing: both waits below exit the instant the
/// state they want appears, so this bound is only ever reached on a genuine failure. It is sized
/// for the worst case that actually happens — `cargo test --workspace` runs this binary alongside
/// two dozen others on a saturated machine, and the guard's terminal write is a spawned task that
/// has to be scheduled and then make a database round trip. A tight bound here would produce a
/// test that fails under load and passes alone, which is worse than having no test at all.
const PATIENCE: std::time::Duration = std::time::Duration::from_secs(60);

async fn abandonment_body(harness: &Harness) -> Result<()> {
    // Hold every permit the warehouse has, so the query below cannot be admitted and parks at the
    // one await that matters. Taken from the coordinator's own gate, keyed by warehouse name, so
    // these are the very permits it will try to take.
    let gate = harness
        .coordinator
        .scheduler()
        .admission_for(&harness.warehouse, WAREHOUSE_SIZE as usize);
    let mut held = Vec::new();
    for _ in 0..WAREHOUSE_SIZE {
        held.push(gate.acquire().await.expect("the gate starts empty"));
    }

    let request = QueryRequest::new(workload(0)).on_warehouse(harness.warehouse.clone());
    let query_id = {
        let mut running = Box::pin(harness.coordinator.run_query(request));
        // Poll until the row exists and the query is parked in the queue. It has to get through
        // target resolution and the `queued` insert first, both of which are database round trips.
        let mut found = None;
        let deadline = std::time::Instant::now() + PATIENCE;
        while std::time::Instant::now() < deadline {
            assert!(
                futures::poll!(&mut running).is_pending(),
                "no permit is free, so this cannot complete"
            );
            let active = harness.db.list_active_queries(harness.account_id).await?;
            if let Some(record) = active.first() {
                assert_eq!(record.state, QueryState::Queued);
                found = Some(record.id);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        found.context("the query never reached the queue")?
    }; // <- the future is dropped here: exactly what tonic does when a client disconnects.

    // The terminal write is handed to the runtime by the guard's `Drop`, so it lands shortly after
    // rather than synchronously. Poll for it rather than sleeping a guessed interval.
    let mut final_state = None;
    let deadline = std::time::Instant::now() + PATIENCE;
    while std::time::Instant::now() < deadline {
        let history = harness.db.list_queries(harness.account_id, 8).await?;
        if let Some(record) = history.iter().find(|r| r.id == query_id)
            && record.state != QueryState::Queued
        {
            final_state = Some(record.clone());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    let record = final_state.context("the abandoned query was never closed out")?;
    assert_eq!(
        record.state,
        QueryState::Failed,
        "an abandoned query did not succeed, and it is certainly not still waiting"
    );
    assert!(
        record.finished_at.is_some(),
        "a terminal row must carry a finish time"
    );
    assert!(
        record.started_at.is_none(),
        "it was abandoned while queued, so it never started — history must not claim otherwise"
    );
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|e| e.contains("disconnected")),
        "the reason must say what happened, not merely that it failed: {:?}",
        record.error
    );

    // The whole point: nothing is left active, so the operator view and the sweep-line are clean.
    let active = harness.db.list_active_queries(harness.account_id).await?;
    assert!(
        active.is_empty(),
        "no query should still be active, but found {active:?}"
    );

    drop(held);
    Ok(())
}
