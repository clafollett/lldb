//! Cancelling a running query, end to end: a **real server**, a **real worker**, **real Postgres**,
//! real accounts and grants, and one query stopped by id while another waits behind it.
//!
//! This is issue #38's "done when" list stated as facts a machine can settle:
//!
//! 1. **A running query can be cancelled by id, and its slot comes back promptly.**
//! 2. **The queue behind it advances** — the one that matters. Returning the slot is the entire
//!    point of the issue, so a cancellation that marked history and left the slot held would pass
//!    every other assertion here and have achieved nothing. The queued query is required to *start*
//!    and then to *finish with the right answer*.
//! 3. **History distinguishes cancellation from failure**: a `cancelled` row, terminal, carrying who
//!    asked.
//! 4. **A caller cannot cancel another account's query** — and cannot even learn that it exists.
//!
//! Plus two the issue asks for in prose rather than in its checklist: cancelling needs the `CANCEL`
//! grant, and a cancelled row must not look abandoned to `query_reaper`'s sweep.
//!
//! # How a query is held still, and why that is not cheating
//!
//! The hard part of testing cancellation is having something to cancel: a query fast enough to be
//! convenient is too fast to catch, and a query slow enough to catch is a sleep in disguise. So the
//! **worker** is the thing that is held, by a gate in front of a real [`WorkerFlightService`]. That
//! is deliberately the most faithful stall available: the coordinator plans, is admitted, holds a
//! real permit, dispatches over real Flight to a real worker, and blocks exactly where a genuinely
//! expensive query blocks — waiting for a worker to produce. Nothing about the coordinator, the
//! scheduler, the history writes or the cancellation path is faked or shortened, and when the gate
//! opens the same worker answers the query for real.
//!
//! The warehouse is **size 1**, so "the queue advances" is not a statistical claim: there is exactly
//! one slot, and the second query cannot start until the first gives it up.
//!
//! The database is found the same way every other database-gated file finds it (see `support`):
//! an explicit `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a
//! clean skip.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration query_cancel

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, PollInfo, SchemaResult, Ticket,
};
use lldb_qe_core::cancel::CANCEL_ACTION;
use lldb_qe_core::engine::BoxResolver;
use lldb_qe_core::flight::WorkerFlightService;
use lldb_qe_core::liveness::CoordinatorIdentity;
use lldb_qe_core::query_log::{QueryRecord, QueryState};
use lldb_qe_core::rbac::{ObjectRef, ObjectType, Privilege};
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, SubmittedQuery, cancel_query, serve_coordinator,
    submit_query_as,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::warehouse::{Warehouse, WarehouseState};
use lldb_qe_core::{DEFAULT_WAREHOUSE_ENDPOINT, scheduler};
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::support::{resolve_target, unique_name};

/// One slot, so "the queue advanced" is a fact about one permit rather than a statistic.
const WAREHOUSE_SIZE: i32 = 1;

/// How long any wait-for-a-state loop below will run before giving up.
///
/// Generous on purpose, and the generosity is free: every loop exits the instant the state it wants
/// appears, so this bound is only ever reached on a genuine failure. It is sized for this binary
/// running alongside two dozen other test files on a saturated machine — a tight bound would make a
/// test that fails under load and passes alone, which is worse than no test.
const PATIENCE: Duration = Duration::from_secs(60);

/// Skip-or-connect, shared by every test in this file.
async fn db_or_skip(what: &str) -> Result<Option<ServicesDb>> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP ({what}): set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with \
             a Docker daemon, to exercise query cancellation"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;
    Ok(Some(db))
}

// ---------------------------------------------------------------------------
// A real worker, behind a gate the test opens.
// ---------------------------------------------------------------------------

/// A worker that answers nothing until the test says so, and is a completely ordinary worker after
/// that.
///
/// Every method delegates; only `do_get` waits first. Delegating rather than reimplementing is the
/// point — the stall is *in front of* a real worker, so what the coordinator finally receives is
/// what a real worker produces, and "the queued query returned the right answer" is a real claim.
#[derive(Clone)]
struct GatedWorker {
    inner: WorkerFlightService,
    open: watch::Receiver<bool>,
    /// How many `do_get`s have reached the gate. Read by the test to know the query is genuinely
    /// parked on the worker rather than still somewhere on the coordinator.
    arrived: Arc<AtomicUsize>,
}

impl GatedWorker {
    async fn wait_for_the_gate(&self) {
        let mut open = self.open.clone();
        self.arrived.fetch_add(1, Ordering::AcqRel);
        while !*open.borrow_and_update() {
            if open.changed().await.is_err() {
                // The sender is held by the harness for the whole test, so this is unreachable;
                // proceeding is the harmless reading if it ever happens.
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl FlightService for GatedWorker {
    type HandshakeStream = <WorkerFlightService as FlightService>::HandshakeStream;
    type ListFlightsStream = <WorkerFlightService as FlightService>::ListFlightsStream;
    type DoGetStream = <WorkerFlightService as FlightService>::DoGetStream;
    type DoPutStream = <WorkerFlightService as FlightService>::DoPutStream;
    type DoExchangeStream = <WorkerFlightService as FlightService>::DoExchangeStream;
    type DoActionStream = <WorkerFlightService as FlightService>::DoActionStream;
    type ListActionsStream = <WorkerFlightService as FlightService>::ListActionsStream;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.wait_for_the_gate().await;
        self.inner.do_get(request).await
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.inner.handshake(request).await
    }
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.inner.list_flights(request).await
    }
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.inner.get_flight_info(request).await
    }
    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.inner.poll_flight_info(request).await
    }
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.inner.get_schema(request).await
    }
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.inner.do_put(request).await
    }
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.inner.do_exchange(request).await
    }
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.inner.do_action(request).await
    }
    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.inner.list_actions(request).await
    }
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

/// A running coordinator, its gated worker, and one tenant with the grants a submitter needs.
struct Harness {
    db: ServicesDb,
    account_id: i64,
    account_name: String,
    role_id: i64,
    warehouse: String,
    url: String,
    coordinator: Arc<Coordinator>,
    token: String,
    /// Opens the worker's gate. Held for the life of the harness.
    release: watch::Sender<bool>,
    arrived: Arc<AtomicUsize>,
}

impl Harness {
    /// Stand up: a gated worker, a warehouse row of size 1, an identity with `USAGE` + `CANCEL`, a
    /// coordinator on an ephemeral port.
    ///
    /// The workload is `SELECT <n>` — no table, therefore no `SELECT` grant and no parquet to seed.
    /// That is not a shortcut around access control: it keeps this file's subject *cancellation*,
    /// and the grant that is actually under test here (`CANCEL`) is issued and revoked below.
    async fn start(db: ServicesDb, tag: &str) -> Result<Self> {
        let account = db.create_account(&unique_name(tag)).await?;
        let warehouse_name = unique_name(&format!("wh-{tag}"));

        let user = db.create_user(account.id, "cancel-test").await?;
        let role = db.create_role(account.id, "cancel-test").await?;
        db.assign_role(account.id, user.id, role.id).await?;
        // Submitting needs USAGE; stopping needs CANCEL. Two grants on purpose — a single `ALL`
        // would cover both and prove nothing about the split.
        for privilege in [Privilege::Usage, Privilege::Cancel] {
            db.grant(
                account.id,
                role.id,
                privilege,
                &ObjectRef::new(ObjectType::Warehouse, warehouse_name.clone())?,
            )
            .await?;
        }
        let (_key, token) = db
            .create_api_key(account.id, user.id, "cancel-test", None)
            .await?;
        let token = token.into_secret();

        let _warehouse: Warehouse = db
            .create_warehouse(
                account.id,
                &warehouse_name,
                WAREHOUSE_SIZE,
                WarehouseState::Running,
            )
            .await?;

        let (release, open) = watch::channel(false);
        let arrived = Arc::new(AtomicUsize::new(0));
        let worker = start_gated_worker(open, Arc::clone(&arrived)).await?;
        let authority = format!("{warehouse_name}.lldb.local:50051");
        let resolver = cloud_map(authority, vec![worker]);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let coordinator = Arc::new(
            Coordinator::new(
                datafusion::prelude::SessionContext::new(),
                Some(db.clone()),
                CoordinatorConfig {
                    default_account: account.name.clone(),
                    workers: Vec::new(),
                    warehouse_endpoint: vec![DEFAULT_WAREHOUSE_ENDPOINT.to_string()],
                    // Sized from the warehouse's row, which is 1.
                    max_concurrent_queries: None,
                    max_queued_queries: 64,
                    coordinator: CoordinatorIdentity::new(format!("test-{addr}")),
                    allow_anonymous: false,
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
            account_name: account.name,
            role_id: role.id,
            warehouse: warehouse_name,
            url: format!("http://{addr}"),
            coordinator,
            token,
            release,
            arrived,
        })
    }

    /// The scheduler's own view of the warehouse's gate — one of the two instruments this file
    /// uses, the other being the timestamps Postgres stamped.
    ///
    /// Panics if no query has been submitted yet: gates are created lazily, on first sight of a
    /// warehouse, so before that there is genuinely nothing to read rather than an empty gate.
    fn gate(&self) -> scheduler::AdmissionSnapshot {
        self.maybe_gate()
            .expect("no query has been submitted to this warehouse yet")
    }

    /// The gate, or `None` while it does not exist yet. What the wait loop polls.
    fn maybe_gate(&self) -> Option<scheduler::AdmissionSnapshot> {
        self.coordinator
            .scheduler()
            .snapshot()
            .get(&self.warehouse)
            .copied()
    }

    /// Submit `sql` on a background task, exactly as a client would.
    fn submit(&self, sql: &str) -> tokio::task::JoinHandle<Result<SubmittedQuery>> {
        let url = self.url.clone();
        let token = self.token.clone();
        let request = QueryRequest::new(sql.to_string()).on_warehouse(self.warehouse.clone());
        tokio::spawn(async move { submit_query_as(&url, &request, Some(&token)).await })
    }

    /// Wait until `predicate` holds of the gate, or give up after [`PATIENCE`].
    async fn await_gate(
        &self,
        what: &str,
        predicate: impl Fn(&scheduler::AdmissionSnapshot) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.maybe_gate().is_some_and(|gate| predicate(&gate)) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!(
            "timed out waiting for {what}; gate is {:?}",
            self.maybe_gate()
        )
    }

    /// Wait until exactly one query is `running` in history, and return its row.
    async fn await_one_running(&self) -> Result<QueryRecord> {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            let active = self.db.list_active_queries(self.account_id).await?;
            if let Some(record) = active
                .iter()
                .find(|r| r.state == QueryState::Running)
                .cloned()
            {
                return Ok(record);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("no query reached `running` within {PATIENCE:?}")
    }

    /// Wait until query `id` leaves the state it is in now for a terminal one.
    async fn await_terminal(&self, id: i64) -> Result<QueryRecord> {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if let Some(record) = self.db.query_by_id(id).await?
                && record.state.is_terminal()
            {
                return Ok(record);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("query {id} never reached a terminal state within {PATIENCE:?}")
    }

    /// Let the worker answer.
    fn open_the_gate(&self) {
        self.release.send_replace(true);
    }

    async fn cleanup(&self) -> Result<()> {
        // Whatever is still parked on the worker is released first, so no test leaves a task
        // wedged in a shared binary.
        self.open_the_gate();
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(self.account_id)
            .execute(self.db.pool())
            .await
            .context("deleting the test account")?;
        Ok(())
    }
}

/// Start a [`GatedWorker`] on an ephemeral port and return its address.
async fn start_gated_worker(
    open: watch::Receiver<bool>,
    arrived: Arc<AtomicUsize>,
) -> Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let worker = GatedWorker {
        inner: WorkerFlightService::new(datafusion::prelude::SessionContext::new()),
        open,
        arrived,
    };
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(worker))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("gated worker serve");
    });
    Ok(addr)
}

/// A resolver answering one warehouse's DNS name with its tasks' addresses, and erroring on
/// anything else — the same fake `query_scheduler` and `warehouse_routing` use.
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

// ---------------------------------------------------------------------------
// 1 + 2 + 3 — the acceptance test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_running_query_returns_its_slot_and_the_queue_advances() -> Result<()> {
    let Some(db) = db_or_skip("cancellation").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "cancel").await?;
    let result = cancellation_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn cancellation_body(harness: &Harness) -> Result<()> {
    // ---- A query that is genuinely running, and parked on the worker -------------------------
    let first = harness.submit("SELECT 1 AS v");
    harness
        .await_gate("the first query to be admitted", |g| g.running == 1)
        .await?;
    let running = harness.await_one_running().await?;
    let first_id = running.id;
    assert!(
        running.started_at.is_some(),
        "a running query must have a start time"
    );
    // It is on the *worker*, not merely admitted — so the cancellation below is stopping real
    // distributed work rather than a query that had not started yet.
    let deadline = Instant::now() + PATIENCE;
    while harness.arrived.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        harness.arrived.load(Ordering::Acquire),
        1,
        "the query must have reached the worker"
    );

    // ---- …and one more behind it, which cannot start: there is exactly one slot ---------------
    let second = harness.submit("SELECT 2 AS v");
    harness
        .await_gate("the second query to queue", |g| g.queued == 1)
        .await?;
    let gate = harness.gate();
    assert_eq!(gate.max_concurrent, 1, "one slot, from the warehouse row");
    assert_eq!(gate.running, 1, "{gate:?}");
    assert_eq!(gate.queued, 1, "{gate:?}");

    // ---- Cancel the running one by the id its submission would have returned ------------------
    cancel_query(&harness.url, first_id, Some(&harness.token))
        .await
        .context("the cancel action must be accepted")?;

    // 1. The submitter is told, and told it was *cancelled* rather than that it failed.
    let error = first
        .await
        .expect("submission task")
        .expect_err("a cancelled query does not return rows");
    let message = format!("{error:#}");
    assert!(message.contains("cancelled"), "{message}");

    // 2. **The queue advanced.** This is the assertion the issue exists for: the slot came back and
    //    the query behind it started. Asserted on the scheduler's counters *and* — below — on the
    //    timestamps Postgres stamped, because a counter can be self-consistently wrong.
    harness
        .await_gate("the queued query to be admitted", |g| {
            g.queued == 0 && g.running == 1
        })
        .await?;

    // …and it is really the *second* query holding the slot now.
    let now_running = harness.await_one_running().await?;
    assert_ne!(
        now_running.id, first_id,
        "the slot must have gone to the query that was waiting, not back to the cancelled one"
    );
    assert_eq!(now_running.sql_text, "SELECT 2 AS v");

    // 3. History says cancelled, terminally, with who asked — distinguishable from a failure.
    let cancelled = harness.await_terminal(first_id).await?;
    assert_eq!(
        cancelled.state,
        QueryState::Cancelled,
        "a cancelled query must not be recorded as a failure: {:?}",
        cancelled.error
    );
    assert!(
        cancelled.finished_at.is_some(),
        "a terminal row carries a finish time, or peak_concurrency counts it forever"
    );
    assert!(
        cancelled.started_at.is_some(),
        "it was cancelled while running, so history must say it started"
    );
    let reason = cancelled
        .error
        .as_deref()
        .context("a cancelled row must say who stopped it")?;
    assert!(reason.starts_with("cancelled: "), "{reason}");
    assert!(
        reason.contains("cancel-test"),
        "the reason must name the user who asked: {reason}"
    );
    assert_eq!(
        cancelled.result_rows, None,
        "a cancelled query returned no rows, which is not the same as zero"
    );

    // ---- The second query runs to completion, correctly ---------------------------------------
    // The strongest form of "the queue advanced": not merely admitted, but answered.
    harness.open_the_gate();
    let answered = second.await.expect("submission task")?;
    let rows: usize = answered.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "the queued query must produce its answer");

    let finished = harness.await_terminal(now_running.id).await?;
    assert_eq!(finished.state, QueryState::Succeeded);
    assert_eq!(finished.result_rows, Some(1));

    // ---- Nothing leaked ------------------------------------------------------------------------
    harness
        .await_gate("the gate to drain", |g| g.running == 0 && g.queued == 0)
        .await?;
    let gate = harness.gate();
    assert_eq!(
        gate.admitted, 2,
        "both queries held the slot in turn: {gate:?}"
    );
    assert_eq!(
        gate.peak_running, 1,
        "the bound of 1 was never exceeded: {gate:?}"
    );
    assert!(
        harness.coordinator.in_flight().is_empty(),
        "the cancellation registry must not retain a finished query"
    );
    assert!(
        harness
            .db
            .list_active_queries(harness.account_id)
            .await?
            .is_empty(),
        "nothing should still be queued or running"
    );

    // ---- The second instrument, and the one caveat cancellation adds to it ---------------------
    //
    // Recomputed from Postgres's own clock rather than the scheduler's counters. A cancelled row
    // that never got a `finished_at` would read as still running and overlap everything after it —
    // the `peak_concurrency` drift `crate::reaper` exists to remove — so this is the check that a
    // cancellation really closes its interval.
    //
    // It is asserted as `<= 2`, not `== 1`, and the reason is a deliberate ordering rather than
    // slack. The slot is returned by *dropping* the execution future, and the `cancelled` row is
    // written just afterwards, on the same task: so the successor is admitted and stamped
    // `started_at` a fraction of a millisecond before the cancelled row is stamped `finished_at`,
    // and a sweep line sees a sliver of overlap. Holding a warehouse's slot across a control-plane
    // round trip to make a reporting instrument tidier would be exactly the wrong trade — the whole
    // issue is about giving the slot back promptly. What must hold, and is asserted next, is that
    // the overlap is one write's worth of time rather than one query's.
    let history = harness.db.list_queries(harness.account_id, 16).await?;
    assert_eq!(history.len(), 2);
    let observed = lldb_qe_core::query_log::peak_concurrency(&history);
    assert!(
        observed <= 2,
        "history shows {observed} queries executing at once against one slot: {history:?}"
    );
    let overlap = cancelled
        .finished_at
        .context("the cancelled row must have a finish time")?
        - now_running
            .started_at
            .context("the successor must have a start time")?;
    assert!(
        overlap < chrono::TimeDelta::seconds(1),
        "the cancelled row must close as its successor starts, not stay open across it; \
         the overlap was {overlap}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 4 — the tenant boundary
// ---------------------------------------------------------------------------

/// A caller may cancel within its own account and nowhere else.
///
/// The interesting half is the *shape* of the refusal: another tenant's query is answered exactly as
/// a query that is not running here at all, because query ids are consecutive integers from one
/// sequence shared by every tenant. A distinguishable "permission denied" would let any
/// authenticated caller walk the id space and map out which ids belong to whom and when they ran.
/// So this asserts on three things — the refusal, the victim surviving it, and the message naming
/// neither the other account nor its user.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_cannot_cancel_another_accounts_query() -> Result<()> {
    let Some(db) = db_or_skip("cross-account cancellation").await? else {
        return Ok(());
    };
    // Two tenants, two coordinators — but the *cancel* is sent to the coordinator that is running
    // the victim, so the only thing standing between the two is the credential.
    let victim = Harness::start(db.clone(), "victim").await?;
    let attacker = Harness::start(db, "attacker").await?;
    let result = cross_account_body(&victim, &attacker).await;
    victim.cleanup().await?;
    attacker.cleanup().await?;
    result
}

async fn cross_account_body(victim: &Harness, attacker: &Harness) -> Result<()> {
    let running = victim.submit("SELECT 1 AS v");
    victim
        .await_gate("the victim's query to be admitted", |g| g.running == 1)
        .await?;
    let victim_query = victim.await_one_running().await?;

    // The attacker holds a perfectly good credential — for the wrong tenant.
    let error = cancel_query(&victim.url, victim_query.id, Some(&attacker.token))
        .await
        .expect_err("one account must not be able to stop another's query");
    let message = format!("{error:#}");
    assert!(
        message.contains("NotFound") || message.contains("not running on this coordinator"),
        "a cross-account cancel must be indistinguishable from an unknown id: {message}"
    );
    assert!(
        !message.contains(&victim.account_name),
        "the refusal must not disclose whose query it is: {message}"
    );
    assert!(
        !message.contains(&victim.warehouse),
        "…nor which warehouse it is running on: {message}"
    );

    // The victim's query is untouched: still running, still holding its slot.
    assert_eq!(victim.gate().running, 1, "the query must still be running");
    let still = victim
        .db
        .query_by_id(victim_query.id)
        .await?
        .context("the row is still there")?;
    assert_eq!(
        still.state,
        QueryState::Running,
        "a refused cancellation must not have moved the row"
    );

    // …and it answers normally once the worker is released, which is the operational form of the
    // same claim: nothing about the refused cancellation damaged it.
    victim.open_the_gate();
    let answered = running.await.expect("submission task")?;
    assert_eq!(
        answered.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        1
    );
    let finished = victim.await_terminal(victim_query.id).await?;
    assert_eq!(finished.state, QueryState::Succeeded);
    Ok(())
}

// ---------------------------------------------------------------------------
// The grant
// ---------------------------------------------------------------------------

/// Within one account, cancelling still needs `CANCEL` on the warehouse.
///
/// The pair that matters is `USAGE` versus `CANCEL`: the caller here holds `USAGE` throughout — it
/// has to, or it could not have submitted the query — so this proves the two are genuinely separate
/// and that being allowed to *run* work on a warehouse is not being allowed to *kill* work on it.
/// Unlike the cross-account case this one is reported as a denial, because a caller who got this far
/// has already proven which tenant they are and naming the missing grant leaks nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_needs_the_cancel_grant_and_usage_is_not_enough() -> Result<()> {
    let Some(db) = db_or_skip("the cancel grant").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "grant").await?;
    let result = grant_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn grant_body(harness: &Harness) -> Result<()> {
    // Take CANCEL away, leaving USAGE — the exact grant set a plain submitter has.
    let revoked = harness
        .db
        .revoke(
            harness.role_id,
            Privilege::Cancel,
            &ObjectRef::new(ObjectType::Warehouse, harness.warehouse.clone())?,
        )
        .await?;
    assert!(revoked, "the grant was there to revoke");

    let running = harness.submit("SELECT 1 AS v");
    harness
        .await_gate("the query to be admitted", |g| g.running == 1)
        .await?;
    let query = harness.await_one_running().await?;

    let error = cancel_query(&harness.url, query.id, Some(&harness.token))
        .await
        .expect_err("USAGE is not permission to cancel");
    let message = format!("{error:#}");
    assert!(
        message.contains("CANCEL on warehouse"),
        "the denial must name the missing grant: {message}"
    );
    assert!(
        message.contains("lldb-qe-auth grant"),
        "…and the command that adds it: {message}"
    );
    assert_eq!(
        harness
            .db
            .query_by_id(query.id)
            .await?
            .map(|r| r.state)
            .unwrap(),
        QueryState::Running,
        "a denied cancellation must not have stopped anything"
    );

    // Grant it back and the same call succeeds — so the refusal was the grant and nothing else.
    harness
        .db
        .grant(
            harness.account_id,
            harness.role_id,
            Privilege::Cancel,
            &ObjectRef::new(ObjectType::Warehouse, harness.warehouse.clone())?,
        )
        .await?;
    cancel_query(&harness.url, query.id, Some(&harness.token))
        .await
        .context("with CANCEL granted, the same call must work")?;
    assert!(running.await.expect("submission task").is_err());
    let cancelled = harness.await_terminal(query.id).await?;
    assert_eq!(cancelled.state, QueryState::Cancelled);
    Ok(())
}

// ---------------------------------------------------------------------------
// The interaction with #36
// ---------------------------------------------------------------------------

/// A cancelled query must not look abandoned to the reaper.
///
/// `query_reaper` proves the sweep resolves rows whose coordinator is gone; this is the other
/// side of it. The row here is deliberately the *worst* case for the reaper — written by a
/// coordinator identity that never registered at all, so liveness can say nothing good about it —
/// and it is still not taken, because `cancelled` is terminal and terminal rows are outside the
/// reapable predicate. That is the composition the issue asks about: cancellation does not need to
/// fight the reaper's compare-and-swap, because it lands on the other side of the same predicate.
///
/// Written straight to the database rather than through a live cancellation on purpose: the race it
/// stands in for — a query cancelled at the same moment its coordinator dies — is not schedulable
/// from a test, and what actually has to hold is a property of the two statements, which this
/// settles deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_query_is_never_reaped() -> Result<()> {
    let Some(db) = db_or_skip("the reaper interaction").await? else {
        return Ok(());
    };
    let account = db.create_account(&unique_name("reap-cancel")).await?;
    let result = reaper_body(&db, account.id).await;
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account.id)
        .execute(db.pool())
        .await?;
    result
}

async fn reaper_body(db: &ServicesDb, account_id: i64) -> Result<()> {
    // A coordinator that never registered: the most reapable identity there is, short of one that
    // registered and died.
    let identity = CoordinatorIdentity::new(unique_name("dead-coordinator"));

    let cancelled = db
        .submit_query(account_id, None, "SELECT 1", Some(&identity))
        .await?;
    db.mark_query_running(cancelled.id).await?;
    let cancelled = db
        .mark_query_cancelled(cancelled.id, "cancelled: user `dana` stopped this query")
        .await?;
    assert_eq!(cancelled.state, QueryState::Cancelled);

    // A control: an identical row left `running` by the same dead identity. It *is* reapable, which
    // is what makes "the cancelled one was not taken" mean something rather than "the sweep did
    // nothing".
    let stranded = db
        .submit_query(account_id, None, "SELECT 2", Some(&identity))
        .await?;
    db.mark_query_running(stranded.id).await?;

    let would_take = db.list_stranded_queries(Some(account_id), 100).await?;
    let ids: Vec<i64> = would_take.iter().map(|r| r.id).collect();
    assert!(
        !ids.contains(&cancelled.id),
        "a cancelled query is terminal and must not be considered stranded: {ids:?}"
    );
    assert!(
        ids.contains(&stranded.id),
        "the control row must be considered stranded, or this test proves nothing: {ids:?}"
    );

    let reaped = db.reap_stranded_queries(Some(account_id), 100).await?;
    let reaped_ids: Vec<i64> = reaped.iter().map(|r| r.id).collect();
    assert_eq!(reaped_ids, vec![stranded.id], "only the stranded row");

    // The cancelled row is byte-for-byte what it was: same state, same reason, same finish time.
    let after = db
        .query_by_id(cancelled.id)
        .await?
        .context("the cancelled row is still there")?;
    assert_eq!(after, cancelled, "the sweep must not have touched it");
    Ok(())
}

/// `list_actions` advertises the verb, so a Flight client can discover cancellation without reading
/// this repository.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_coordinator_advertises_the_cancel_action() -> Result<()> {
    let Some(db) = db_or_skip("action discovery").await? else {
        return Ok(());
    };
    let harness = Harness::start(db, "actions").await?;
    let result = actions_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn actions_body(harness: &Harness) -> Result<()> {
    use arrow_flight::flight_service_client::FlightServiceClient;
    use futures::TryStreamExt;

    let channel = tonic::transport::Channel::from_shared(harness.url.clone())?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    let actions: Vec<ActionType> = client
        .list_actions(Empty {})
        .await?
        .into_inner()
        .try_collect()
        .await?;
    let names: Vec<&str> = actions.iter().map(|a| a.r#type.as_str()).collect();
    assert_eq!(names, vec![CANCEL_ACTION]);
    assert!(
        actions[0].description.contains("lldb-query-id"),
        "the description must say what the body is: {}",
        actions[0].description
    );

    // …and an action this server does not serve is refused by name rather than silently ignored.
    let status = client
        .do_action(Action {
            r#type: "explode".to_string(),
            body: Vec::new().into(),
        })
        .await
        .expect_err("unknown action");
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert!(status.message().contains("explode"), "{status:?}");

    Ok(())
}
