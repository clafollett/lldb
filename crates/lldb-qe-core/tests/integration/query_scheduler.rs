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
//! Real, and new since #19: the server authenticates. The harness issues a real API key against a
//! real `api_keys` row and every submission below presents it, because a coordinator with a
//! services database refuses anything else — so this file also happens to prove that the scheduler
//! and access control compose rather than fight.
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
//! Issue #37 turned that second instrument into the *primary* one, because it was already the
//! right shape: `peak_concurrency` runs over `list_queries(account, …)` with **no coordinator
//! filter**, so it has always measured the whole account across every writer. It passed on one
//! coordinator only because there was one. `two_coordinators_on_one_warehouse_admit_k_total_not_2k`
//! is the same computation over a fleet of two, and it is the acceptance criterion.
//!
//! The per-process counter's `peak_running == K` check does **not** generalize with it, and is not
//! ported: under a fleet-wide bound one coordinator's share may legitimately be below `K` while the
//! warehouse is at exactly `K`, so demanding it would be demanding an unfair split. It stays in the
//! single-node test, where one process *is* the fleet and the claim is still exactly true. What
//! replaces it as the "did anything actually contend" check for a fleet is stated on that test.
//!
//! The database is found the same way `services_db.rs` finds it (see [`support`]): an explicit
//! `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a clean skip.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration query_scheduler
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration query_scheduler -- --nocapture

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::support::{self, resolve_target, unique_name};
use anyhow::{Context, Result};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::engine::BoxResolver;
use lldb_qe_core::fleet_admission::FleetAdmission;
use lldb_qe_core::liveness::{
    CoordinatorIdentity, CoordinatorRegistration, DEFAULT_RENEW_INTERVAL,
    MISSED_RENEWALS_BEFORE_DEAD,
};
use lldb_qe_core::query_log::{QueryRecord, QueryState, peak_concurrency};
use lldb_qe_core::rbac::{ObjectRef, ObjectType, Privilege};
use lldb_qe_core::scheduler::FleetLease;
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, serve_coordinator, submit_query_as,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::warehouse::{Warehouse, WarehouseState};
use lldb_qe_core::{DEFAULT_WAREHOUSE_ENDPOINT, flight};

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

/// One coordinator in the harness's fleet.
///
/// The registration is a field rather than something the harness starts and forgets: it is what
/// makes this coordinator's fleet-wide admission slots *count*. A claim's expiry is its holder's
/// liveness lease, so a coordinator that stopped renewing would have its slots reclaimed by its
/// peers — which is exactly what one of the tests below asks for deliberately, and exactly what
/// must not happen by accident in the others.
struct Node {
    url: String,
    coordinator: Arc<Coordinator>,
    /// What this process writes into `queries.coordinator`.
    slot: String,
    _registration: CoordinatorRegistration,
}

/// One or more running servers plus everything needed to talk to them and clean up after them.
///
/// Every node shares one services database, one account, one warehouse row and one injected
/// resolver — which is precisely the deployment issue #37 is about: `N` coordinators in front of
/// one warehouse's compute.
struct Harness {
    db: ServicesDb,
    account_id: i64,
    warehouse: String,
    warehouse_id: i64,
    nodes: Vec<Node>,
    /// The API key every submission presents. Held once here rather than re-issued per query,
    /// exactly as a real client would.
    token: String,
    /// Kept alive so the seeded parquet outlives the queries reading it.
    _tmp: tempfile::TempDir,
}

impl Harness {
    /// One coordinator — the shape most of this file's tests want.
    async fn start(db: ServicesDb, tag: &str) -> Result<Self> {
        Self::start_fleet(db, tag, 1).await
    }

    /// Stand up: workers, a warehouse row, a catalog, `coordinators` coordinators, a listening
    /// Flight port each.
    ///
    /// Each coordinator registers itself and is given a [`FleetAdmission`] built from that
    /// registration, so the warehouse's limit is enforced across all of them rather than by each of
    /// them. That is true even at `coordinators == 1`: the single-node tests below therefore run
    /// through the *same* admission path production uses, rather than through a fallback that only
    /// tests exercise.
    async fn start_fleet(db: ServicesDb, tag: &str, coordinators: usize) -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let path = seed_parquet(tmp.path(), 1200, 6)?;

        let account = db.create_account(&unique_name(tag)).await?;
        let warehouse_name = unique_name(&format!("wh-{tag}"));

        // Identity, before anything else needs it. A user, a role that can read the test table and
        // use the test warehouse, and one key. The grants are deliberately the *narrow* ones a real
        // operator would write — `ALL ON CATALOG` would work too and would prove less.
        let user = db.create_user(account.id, "scheduler-test").await?;
        let role = db.create_role(account.id, "scheduler-test").await?;
        db.assign_role(account.id, user.id, role.id).await?;
        db.grant(
            account.id,
            role.id,
            Privilege::Select,
            &ObjectRef::table("datafusion", "public", "rows"),
        )
        .await?;
        db.grant(
            account.id,
            role.id,
            Privilege::Usage,
            &ObjectRef::new(ObjectType::Warehouse, warehouse_name.clone())?,
        )
        .await?;
        let (_key, token) = db
            .create_api_key(account.id, user.id, "scheduler-test", None)
            .await?;
        let token = token.into_secret();

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
        // One fleet of workers for *every* coordinator, which is the point: `N` schedulers, one
        // pool of compute.
        let workers = start_workers(warehouse.size as usize).await?;
        let authority = format!("{warehouse_name}.lldb.local:50051");

        let mut nodes = Vec::with_capacity(coordinators);
        for _ in 0..coordinators {
            let ctx = distributing_ctx();
            ctx.register_parquet(
                "rows",
                path.to_str().expect("utf-8 path"),
                ParquetReadOptions::default(),
            )
            .await?;

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            // Distinct per node, and it has to be: two coordinators sharing one `--coordinator-id`
            // is a misconfiguration that costs one of them its registration, and a coordinator with
            // no registration holds no fleet-wide slots.
            let identity = CoordinatorIdentity::new(format!("test-{addr}"));
            let registration = CoordinatorRegistration::start(
                db.clone(),
                identity.clone(),
                DEFAULT_RENEW_INTERVAL,
            )
            .await?;
            let fleet = FleetAdmission::for_registration(db.clone(), &registration);

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
                        // therefore `warehouse.size` — for the whole fleet, not per node.
                        max_concurrent_queries: None,
                        max_queued_queries: 64,
                        coordinator: identity.clone(),
                        // Not `true`: this file's submissions all present a real key, so the
                        // scheduler is exercised through the same authenticated path production
                        // uses.
                        allow_anonymous: false,
                    },
                )
                .with_resolver(cloud_map(authority.clone(), workers.clone()))
                .with_fleet_admission(fleet),
            );

            let serving = Arc::clone(&coordinator);
            tokio::spawn(async move {
                serve_coordinator(listener, serving, std::future::pending::<()>())
                    .await
                    .expect("coordinator serve");
            });

            nodes.push(Node {
                url: format!("http://{addr}"),
                coordinator,
                slot: identity.slot().to_string(),
                _registration: registration,
            });
        }

        Ok(Self {
            db,
            account_id: account.id,
            warehouse: warehouse_name,
            warehouse_id: warehouse.id,
            nodes,
            token,
            _tmp: tmp,
        })
    }

    /// The first coordinator — what a single-node test means by "the server".
    fn node(&self) -> &Node {
        &self.nodes[0]
    }

    /// The first coordinator's URL.
    fn url(&self) -> &str {
        &self.node().url
    }

    /// The first coordinator's own view of the warehouse's gate.
    ///
    /// Per *process*, and under a fleet-wide bound that distinction is the whole point: with more
    /// than one node this number is one coordinator's share of the warehouse and may legitimately
    /// be below the limit. [`Harness::fleet_gates`] is the whole fleet's view.
    fn gate(&self) -> lldb_qe_core::scheduler::AdmissionSnapshot {
        self.node().coordinator.scheduler().snapshot()[&self.warehouse]
    }

    /// Every coordinator's view of the warehouse's gate.
    fn fleet_gates(&self) -> Vec<lldb_qe_core::scheduler::AdmissionSnapshot> {
        self.nodes
            .iter()
            .filter_map(|node| {
                node.coordinator
                    .scheduler()
                    .snapshot()
                    .get(&self.warehouse)
                    .copied()
            })
            .collect()
    }

    /// The `queries.coordinator` values this harness's nodes write.
    fn coordinator_slots(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.slot.clone()).collect()
    }

    /// Delete the account, which cascades to its warehouse, its query history and — the reason this
    /// matters here — the warehouse's `admission_slots` rows.
    ///
    /// The `coordinators` rows are not account-scoped, so they are removed by name: this binary
    /// shares a database with every other test in it, and a shared CI instance with whoever else,
    /// and a registration left renewing against a deleted account is litter with a heartbeat.
    async fn cleanup(&self) -> Result<()> {
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(self.account_id)
            .execute(self.db.pool())
            .await
            .context("deleting the test account")?;
        sqlx::query("DELETE FROM coordinators WHERE slot = ANY($1)")
            .bind(self.coordinator_slots())
            .execute(self.db.pool())
            .await
            .context("deleting the test coordinators")?;
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
        let url = harness.url().to_string();
        let warehouse = harness.warehouse.clone();
        let token = harness.token.clone();
        submissions.push(tokio::spawn(async move {
            let request = QueryRequest::new(workload(i)).on_warehouse(warehouse);
            submit_query_as(&url, &request, Some(&token))
                .await
                .map(|s| (i, s))
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
    // Still exactly true here, and only here: this harness runs **one** coordinator, so its own
    // gate is the whole fleet's gate. The multi-coordinator test below cannot make this claim —
    // see the module docs, and see what it asserts instead.
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
        // Set membership rather than one expected value: a harness may run a fleet, and every row
        // must name *some* coordinator of it. Pinning a single value would fail the moment this
        // test's own shape generalized, and asserting nothing would let a NULL through.
        let slots = harness.coordinator_slots();
        assert!(
            record
                .coordinator
                .as_deref()
                .is_some_and(|slot| slots.iter().any(|known| known == slot)),
            "the coordinator that ran it must be recorded, and must be one of {slots:?}: {:?}",
            record.coordinator
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
        let url = harness.url().to_string();
        let warehouse = harness.warehouse.clone();
        let token = harness.token.clone();
        // Two poisoned queries, enough to exhaust a limit-2 gate if slots leaked. They fail in
        // *planning* (the table does not exist), which is before authorization can have an opinion
        // — so this still tests the queue rather than the grant check.
        let sql = if i == 2 || i == 5 {
            "SELECT * FROM no_such_table".to_string()
        } else {
            workload(0)
        };
        submissions.push(tokio::spawn(async move {
            let request = QueryRequest::new(sql).on_warehouse(warehouse);
            (i, submit_query_as(&url, &request, Some(&token)).await)
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
    let after = submit_query_as(harness.url(), &request, Some(&harness.token))
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
    harness.node().coordinator.begin_shutdown();

    let request = QueryRequest::new(workload(0)).on_warehouse(harness.warehouse.clone());
    let error = submit_query_as(harness.url(), &request, Some(&harness.token))
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
    let gate = harness.node().coordinator.scheduler().admission_for(
        &harness.warehouse,
        Some(harness.warehouse_id),
        WAREHOUSE_SIZE as usize,
    );
    let mut held = Vec::new();
    for _ in 0..WAREHOUSE_SIZE {
        held.push(gate.acquire().await.expect("the gate starts empty"));
    }

    let request = QueryRequest::new(workload(0)).on_warehouse(harness.warehouse.clone());
    let query_id = {
        let mut running = Box::pin(
            harness
                .node()
                .coordinator
                .run_query(request, Some(&harness.token)),
        );
        // Drive it until it is **parked in the line**, and wait for that specific signal rather
        // than for the history row to appear.
        //
        // The distinction is the whole test. The row is inserted by an `await` inside
        // `record_submission`, and that insert commits before the future is resumed with its
        // result — so there is a window where the row is visible to this connection while
        // `run_query` has not yet reached the line that constructs its guard. Dropping in that
        // window destroys a future that has nothing to clean up, and the test would be asserting
        // that a guard which never existed did not run. (It is not hypothetical: waiting on the
        // row alone failed roughly one run in six, here and in CI.)
        //
        // `queued == 1` on the gate is the precise signal, because reaching it means the future
        // got past `record_submission` *and* past guard construction, and is now blocked on a
        // permit that this test is holding — so it will stay there until dropped.
        let mut found = None;
        let deadline = std::time::Instant::now() + PATIENCE;
        while std::time::Instant::now() < deadline {
            assert!(
                futures::poll!(&mut running).is_pending(),
                "no permit is free, so this cannot complete"
            );
            if gate.snapshot().queued == 1 {
                let active = harness.db.list_active_queries(harness.account_id).await?;
                let record = active
                    .first()
                    .context("the query is queued on the gate, so its row must exist")?;
                assert_eq!(record.state, QueryState::Queued);
                found = Some(record.id);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        found.context("the query never reached the queue")?
    }; // <- the future is dropped here: exactly what tonic does when a client disconnects.

    // The terminal write is handed to the runtime by the guard's `Drop`, so it lands shortly after
    // rather than synchronously. Wait on the guard's own counters, not just on the row: if this
    // ever fails, the counters say *which* half broke — a guard that never fired, versus a guard
    // that fired and could not write — and that distinction is the whole diagnosis.
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

    let record = final_state.with_context(|| {
        format!(
            "the abandoned query was never closed out (guard closed {}, failed to close {}) — \
             if both are zero the guard never fired; if the second is non-zero it fired and the \
             write failed",
            lldb_qe_core::server::abandoned_closed(),
            lldb_qe_core::server::abandoned_unclosed(),
        )
    })?;
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

// -----------------------------------------------------------------------------------------------
// Issue #37 — fleet-wide admission control.
//
// Everything above this line runs one coordinator, where "the limit is per process" and "the limit
// is per warehouse" are the same sentence. These two are the difference.
// -----------------------------------------------------------------------------------------------

/// Coordinators in the fleet for the acceptance test. Two is the number in the issue, and it is the
/// smallest number that can tell those two sentences apart.
const FLEET_COORDINATORS: usize = 2;

/// **The acceptance criterion of issue #37**: two coordinators on one warehouse admit `K` total,
/// not `2K` — demonstrated with both actually running, not argued.
///
/// Each server is configured exactly as a single one would be: the limit is sized from the
/// warehouse's row, so each of them believes `K = WAREHOUSE_SIZE`. That is precisely what an
/// operator scaling for availability does, and precisely what used to produce `2K`.
///
/// # What is asserted, and why it is the right instrument
///
/// The witness is [`peak_concurrency`] over the account's whole history — Postgres's own
/// `started_at`/`finished_at`, across **every** value of `queries.coordinator`. A per-process
/// counter is exactly the instrument the bug hides from: on the old code both gates would have read
/// `peak_running == 2` and both would have been telling the truth about a warehouse running four
/// queries at once.
///
/// The old per-process contention check (`peak_running == limit`) has no analogue here, because one
/// coordinator's share may legitimately be below `K`. Three assertions replace it, and between them
/// they say what the old one said — *this run really contended*:
///
/// 1. `observed == limit`, so the fleet-wide bound was actually **reached** rather than respected
///    by a workload that finished too fast to overlap;
/// 2. every coordinator admitted work, so the bound did not hold because one of them sat idle; and
/// 3. some query was held back by the **fleet** (`fleet_waits > 0`) rather than by its own
///    process's semaphore — which is the one thing a per-process bound could never produce.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_coordinators_on_one_warehouse_admit_k_total_not_2k() -> Result<()> {
    let Some((db, _target)) = db_or_skip("fleet-wide admission").await? else {
        return Ok(());
    };
    let harness = Harness::start_fleet(db, "fleet", FLEET_COORDINATORS).await?;
    let result = fleet_admission_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn fleet_admission_body(harness: &Harness) -> Result<()> {
    let limit = WAREHOUSE_SIZE as usize;
    assert_eq!(harness.nodes.len(), FLEET_COORDINATORS);

    let mut expected = Vec::with_capacity(SUBMISSIONS);
    for i in 0..SUBMISSIONS {
        expected.push(oracle(i).await?);
    }

    // Round-robin across the coordinators, so both are genuinely serving rather than one taking
    // everything and the other being an idle spectator that makes the bound trivially hold.
    let mut submissions = Vec::with_capacity(SUBMISSIONS);
    for i in 0..SUBMISSIONS {
        let url = harness.nodes[i % FLEET_COORDINATORS].url.clone();
        let warehouse = harness.warehouse.clone();
        let token = harness.token.clone();
        submissions.push(tokio::spawn(async move {
            let request = QueryRequest::new(workload(i)).on_warehouse(warehouse);
            submit_query_as(&url, &request, Some(&token))
                .await
                .map(|s| (i, s))
        }));
    }
    for task in submissions {
        let (i, submitted) = task.await.expect("submission task")?;
        // Sharing a limit must not change the answers.
        assert_eq!(
            rows_of(&submitted.batches)?,
            expected[i],
            "query {i} returned the wrong answer"
        );
        submitted
            .query_id
            .with_context(|| format!("query {i} came back with no id"))?;
    }

    // ---- The bound, measured across the whole fleet -------------------------------------------
    let history = harness
        .db
        .list_queries(harness.account_id, SUBMISSIONS as i64 * 2)
        .await?;
    assert_eq!(history.len(), SUBMISSIONS, "every submission is recorded");
    for record in &history {
        assert_eq!(
            record.state,
            QueryState::Succeeded,
            "query {} is {}: {:?}",
            record.id,
            record.state,
            record.error
        );
    }

    let observed = peak_concurrency(&history);
    let gates = harness.fleet_gates();
    let per_process: usize = gates.iter().map(|gate| gate.peak_running).sum();
    eprintln!("fleet peak {observed} against limit {limit}; per-process peaks {gates:?}");
    assert!(
        observed <= limit,
        "{FLEET_COORDINATORS} coordinators ran {observed} queries at once on a warehouse of size \
         {limit}. Before issue #37 this would have been up to {}, because the limit lived in one \
         process's memory. Per-process peaks: {gates:?}",
        limit * FLEET_COORDINATORS
    );
    // Contention liveness, replacing the per-process `peak_running == limit` check: the bound was
    // reached, not merely not exceeded. Without it this test would also pass on a fleet that ran
    // everything one query at a time.
    assert_eq!(
        observed, limit,
        "the fleet-wide limit should have been reached; if this fails the workload finished too \
         fast to contend: per-process peaks {gates:?}"
    );

    // Both coordinators really worked.
    let admitted: u64 = gates.iter().map(|gate| gate.admitted).sum();
    assert_eq!(admitted, SUBMISSIONS as u64, "{gates:?}");
    for gate in &gates {
        assert!(gate.admitted > 0, "a coordinator sat idle: {gates:?}");
        assert_eq!(gate.running, 0, "a slot was leaked: {gate:?}");
        assert_eq!(gate.queued, 0, "the queue did not drain: {gate:?}");
        assert_eq!(
            gate.refused, 0,
            "nothing should have been refused: {gate:?}"
        );
        assert_eq!(
            gate.fleet_degraded, 0,
            "the services database was up throughout, so nothing should have fallen back to the \
             per-process bound: {gate:?}"
        );
    }
    // …and the FLEET is what bounded them. With `limit` permits each, the two of them would have
    // admitted `2 * limit` at once on their own semaphores alone, which is the whole bug.
    let fleet_waits: u64 = gates.iter().map(|gate| gate.fleet_waits).sum();
    assert!(
        fleet_waits > 0,
        "no query was ever held back by the fleet, so this run does not distinguish a fleet-wide \
         bound from {FLEET_COORDINATORS} independent ones: {gates:?}"
    );
    // The sum of the per-process peaks is what the old instrument would have reported. Printed
    // rather than asserted on, because the point of the fleet-wide instrument is precisely that the
    // two are allowed to disagree.
    eprintln!("sum of per-process peaks: {per_process} (the number the old caveat was about)");

    // Both coordinators are named in history, so `peak_concurrency` above really did span them.
    let writers: std::collections::HashSet<_> = history
        .iter()
        .filter_map(|record| record.coordinator.clone())
        .collect();
    assert_eq!(
        writers.len(),
        FLEET_COORDINATORS,
        "the measurement is only fleet-wide if both coordinators wrote rows: {writers:?}"
    );

    // Nothing is still holding a warehouse slot in the services database.
    assert!(
        harness
            .db
            .admission_slots(harness.warehouse_id)
            .await?
            .is_empty(),
        "every fleet-wide slot must be given back when its query ends"
    );
    Ok(())
}

/// **A coordinator killed holding slots does not permanently consume them** — the issue's second
/// "done when", and the hard half of the design.
///
/// The reason this issue was blocked on #46 is here: a lease needs an expiry, and inventing a
/// second one would have meant a second definition of "alive". So a slot's expiry *is* its holder's
/// liveness registration, and this walks the three states that turn on.
///
/// 1. **A live holder's slots are not taken.** The dangerous failure is the opposite of a leak: a
///    coordinator that is merely busy having its slots stolen would put the warehouse over its
///    limit by design, with two coordinators each certain they held slot 0.
/// 2. **A dead holder's slots are.** No sweep runs and nothing is scheduled — the next coordinator
///    that wants a slot takes it, on the ordinary claim path.
/// 3. **A holder's own leaked row is reclaimable by itself**, which is what stops a release that
///    did not land from shrinking a warehouse for as long as the leaking process lives.
///
/// Plus the two halves of the release: it frees the row, and it refuses to free a *successor's*.
///
/// It is slower than the rest of this file on purpose, for `coordinator_liveness`'s reason:
/// `renew_interval_secs` is whole seconds, so the shortest threshold that can exist is three, and a
/// test that has to outlive one must really take that long.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_coordinator_that_dies_holding_slots_does_not_consume_them_forever() -> Result<()> {
    let Some((db, _target)) = db_or_skip("fleet-wide slot expiry").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "expire").await?;
    let result = slot_expiry_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn slot_expiry_body(harness: &Harness) -> Result<()> {
    let limit = WAREHOUSE_SIZE as usize;
    let warehouse_id = harness.warehouse_id;
    let db = &harness.db;

    // The coordinator that is about to die, and the one that will outlive it. Both register for
    // real — an unregistered holder is never live, so its claims would be reclaimable the instant
    // it wrote them and this test would pass for entirely the wrong reason.
    //
    // The two register at deliberately different cadences, which is `liveness`'s decision 3 doing
    // real work here rather than being quoted: a row is judged by *its own* interval, so the doomed
    // one is dead three seconds after it stops renewing while the survivor stays live for three
    // minutes without a renewal loop. A shared cadence would have both of them expire during the
    // sleep below, and the reclamation this test is about would be indistinguishable from
    // everything expiring at once.
    let doomed = CoordinatorIdentity::new(unique_name("doomed"));
    let survivor = CoordinatorIdentity::new(unique_name("survivor"));
    let brisk = std::time::Duration::from_secs(1);
    db.register_coordinator(&doomed, brisk).await?;
    db.register_coordinator(&survivor, std::time::Duration::from_secs(60))
        .await?;
    let planted = vec![doomed.slot().to_string(), survivor.slot().to_string()];

    let outcome = slot_expiry_assertions(harness, &doomed, &survivor, brisk, limit).await;

    // These two are not this harness's own nodes, so `cleanup` will not take them.
    sqlx::query("DELETE FROM coordinators WHERE slot = ANY($1)")
        .bind(&planted)
        .execute(db.pool())
        .await
        .context("deleting the planted coordinators")?;
    sqlx::query("DELETE FROM admission_slots WHERE warehouse_id = $1")
        .bind(warehouse_id)
        .execute(db.pool())
        .await
        .context("clearing the test warehouse's slots")?;
    outcome
}

async fn slot_expiry_assertions(
    harness: &Harness,
    doomed: &CoordinatorIdentity,
    survivor: &CoordinatorIdentity,
    renew_interval: std::time::Duration,
    limit: usize,
) -> Result<()> {
    let db = &harness.db;
    let warehouse_id = harness.warehouse_id;

    // ---- The doomed coordinator takes every slot the warehouse has ---------------------------
    let mut doomed_tokens = Vec::new();
    for i in 0..limit {
        let token = format!("doomed-{i}");
        doomed_tokens.push(token.clone());
        let slot_no = db
            .claim_admission_slot(warehouse_id, doomed, &token, limit, &doomed_tokens)
            .await?
            .with_context(|| format!("claim {i} of {limit} should have been granted"))?;
        assert!(
            (0..limit as i32).contains(&slot_no),
            "slot {slot_no} is outside 0..{limit}"
        );
    }
    // The bound, in the database rather than in a process: there is no (K+1)-th row to write.
    let mut over = doomed_tokens.clone();
    over.push("doomed-extra".to_string());
    assert_eq!(
        db.claim_admission_slot(warehouse_id, doomed, "doomed-extra", limit, &over)
            .await?,
        None,
        "a warehouse of size {limit} must not hand out a {}th slot",
        limit + 1
    );
    assert_eq!(db.admission_slots(warehouse_id).await?.len(), limit);

    // ---- 1. While its holder is live, a slot is not taken from it -----------------------------
    assert_eq!(
        db.claim_admission_slot(warehouse_id, survivor, "survivor-0", limit, &[])
            .await?,
        None,
        "a live coordinator's slots must never be reclaimed: a busy coordinator is \
         indistinguishable from a slow one by anything except its lease, and taking from it would \
         put the warehouse over its limit with both coordinators certain they were right"
    );

    // ---- 2. Once it stops renewing, they are --------------------------------------------------
    // A kill, not a clean exit: nothing is deregistered, the registration simply goes stale, which
    // is what a SIGKILL, an OOM or a severed network actually looks like from here.
    let threshold = renew_interval * MISSED_RENEWALS_BEFORE_DEAD;
    tokio::time::sleep(threshold + std::time::Duration::from_millis(1500)).await;
    assert!(
        !db.is_coordinator_live(doomed.slot(), Some(doomed.incarnation()))
            .await?,
        "the doomed coordinator should be past its liveness threshold by now"
    );
    assert!(
        db.is_coordinator_live(survivor.slot(), Some(survivor.incarnation()))
            .await?,
        "the survivor must still be live, or everything below would be reclaimable for the wrong \
         reason — this is what the two different renewal cadences above are for"
    );

    let mut survivor_tokens = Vec::new();
    for i in 0..limit {
        let token = format!("survivor-{i}");
        survivor_tokens.push(token.clone());
        db.claim_admission_slot(warehouse_id, survivor, &token, limit, &survivor_tokens)
            .await?
            .with_context(|| {
                format!(
                    "slot {i} was held by a coordinator that is no longer live and must have been \
                     reclaimed"
                )
            })?;
    }
    // Reclaimed, not duplicated: still exactly `limit` rows, and all of them the survivor's.
    let rows = db.admission_slots(warehouse_id).await?;
    assert_eq!(rows.len(), limit, "{rows:?}");
    for row in &rows {
        assert_eq!(
            row.holder_incarnation,
            survivor.incarnation(),
            "the dead coordinator still holds a slot: {row:?}"
        );
    }

    // ---- 3. A holder's own leaked row is reclaimable by itself --------------------------------
    // The survivor "forgets" one of its tokens, which is exactly the state a release that failed to
    // land leaves behind. Its next claim must take that row back rather than report a warehouse
    // that is full forever.
    survivor_tokens.pop().expect("a token to forget");
    let mut with_reclaim = survivor_tokens.clone();
    with_reclaim.push("survivor-reclaim".to_string());
    assert!(
        db.claim_admission_slot(
            warehouse_id,
            survivor,
            "survivor-reclaim",
            limit,
            &with_reclaim
        )
        .await?
        .is_some(),
        "a row carrying a token this process no longer holds is a leaked release and must be \
         reclaimable by the process that leaked it — otherwise one failed DELETE shrinks the \
         warehouse for as long as this coordinator lives"
    );
    assert_eq!(db.admission_slots(warehouse_id).await?.len(), limit);
    // …and a token it *does* still hold is emphatically not a leak.
    let mut still_held = with_reclaim.clone();
    still_held.push("survivor-extra".to_string());
    assert_eq!(
        db.claim_admission_slot(warehouse_id, survivor, "survivor-extra", limit, &still_held)
            .await?,
        None,
        "reclaiming a row whose token this process still holds would let one coordinator run two \
         queries against one fleet slot"
    );

    // ---- Releasing is a compare-and-swap ------------------------------------------------------
    // The dead coordinator comes back and its query finishes. Its release must not free the slot
    // its successor now holds.
    let stolen = db.admission_slots(warehouse_id).await?[0].clone();
    let stale_release = FleetLease {
        warehouse_id,
        slot_no: stolen.slot_no,
        token: doomed_tokens[0].clone(),
    };
    assert!(
        !db.release_admission_slot(&stale_release, doomed.incarnation())
            .await?,
        "a coordinator whose slot was reclaimed must not delete its successor's row"
    );
    assert_eq!(
        db.admission_slots(warehouse_id).await?.len(),
        limit,
        "the successor's slot survived the stale release"
    );

    // A real release does free it, and doing it twice is a no-op rather than an error.
    let real_release = FleetLease {
        warehouse_id,
        slot_no: stolen.slot_no,
        token: stolen.holder_token.clone(),
    };
    assert!(
        db.release_admission_slot(&real_release, survivor.incarnation())
            .await?
    );
    assert!(
        !db.release_admission_slot(&real_release, survivor.incarnation())
            .await?,
        "releasing twice is a no-op, not an error"
    );
    assert_eq!(db.admission_slots(warehouse_id).await?.len(), limit - 1);
    // The freed slot is immediately claimable again — the loop closes.
    assert!(
        db.claim_admission_slot(
            warehouse_id,
            survivor,
            "survivor-after",
            limit,
            &["survivor-after".to_string()]
        )
        .await?
        .is_some()
    );
    Ok(())
}
