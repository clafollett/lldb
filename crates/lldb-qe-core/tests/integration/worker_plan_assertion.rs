//! Per-request identity at the worker boundary: a worker refuses a plan it was not asked to run.
//!
//! Issue #34's "done when", proven against real in-process workers over real Flight, with no
//! database and no external data. Four properties, and the second is the one that is easy to get
//! wrong:
//!
//! 1. **A plan with no valid assertion is refused**, and so is one whose files exceed what the
//!    assertion covers — the covering check, without which this would be a second fleet token.
//! 2. **Stage caching still hits across requests from the same tenant.** The assertion is
//!    per-request and the stage id is a content hash of the plan bytes, so an assertion that
//!    travelled *inside* the plan would make every request a cache miss and silently destroy the
//!    materialize-once shuffle. Asserted on the worker's own `StageCache::execution_count`, with two
//!    genuinely different assertions.
//! 3. **Worker-to-worker still works**, with both workers closed — so the reduce worker can only
//!    answer by forwarding the assertion it received to the map worker.
//! 4. **Stage reassignment still works** under a closed fleet: a dead primary, a live fallback, and
//!    an assertion that must reach whichever worker actually serves the stage.
//!
//! # Why the credentials are passed explicitly
//!
//! `LLDB_FLEET_TOKEN` is read once per process by `ambient_fleet_auth`, and `std::env::set_var` is
//! `unsafe` in edition 2024 and would race every other test in this binary (see `main.rs`). So these
//! tests do what `auth_rbac`'s fleet-secret test already does: hand the credential in through the
//! explicit seams (`serve_worker_with_auth`, `fetch_stream_with`), which is the same code path the
//! ambient one reaches. The coordinator's *minting* call site — `engine::run_on_fleet` — reads the
//! ambient posture and is therefore exercised here at the layer directly beneath it, exactly as the
//! fleet secret is.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::TryStreamExt;
use lldb_qe_core::auth::FleetAuth;
use lldb_qe_core::plan_assertion::{self, PlanAssertion, PlanAuth, QueryIdentity, SignedAssertion};
use lldb_qe_core::{FlightReaderExec, Retriability, StageCache, flight, retry};
use tokio::net::TcpListener;

use crate::support::{Servers, nanos};

/// A fleet secret nothing else in this process uses.
fn fleet_secret(tag: &str) -> FleetAuth {
    FleetAuth::Required(format!("fleet-{tag}-{}", nanos()))
}

/// Start an in-process worker that requires `secret` — and therefore, by construction, requires a
/// plan assertion too: the two postures are derived from one value so they cannot disagree.
async fn start_closed_worker(
    servers: &mut Servers,
    secret: &FleetAuth,
    cache: Arc<StageCache>,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let secret = secret.clone();
    servers.spawn(async move {
        flight::serve_worker_with_auth(listener, SessionContext::new(), cache, secret)
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Start a worker that checks the **assertion** but not the fleet token.
///
/// Not a posture production can produce — `WorkerFlightService::new_with_postures` says so at length
/// — and the only way to observe worker-to-worker forwarding in one process. A worker dialling
/// another worker presents this *process's* ambient fleet token, which is unset here and cannot be
/// set (`set_var` is `unsafe`, and `main.rs` forbids a test that mutates the environment). So a
/// second fully-closed worker could never be reached at all, and the fleet-token half of that door
/// is already covered by `auth_rbac::a_worker_with_a_fleet_secret_serves_only_the_fleet`. What is
/// under test here is the half that is new: the assertion, forwarded.
async fn start_assertion_only_worker(
    servers: &mut Servers,
    secret: &FleetAuth,
    cache: Arc<StageCache>,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let plan_auth = PlanAuth::from_fleet_auth(secret);
    servers.spawn(async move {
        flight::serve_worker_with_postures(
            listener,
            SessionContext::new(),
            cache,
            FleetAuth::Open,
            plan_auth,
        )
        .await
        .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Write one parquet file into its own directory — one directory per table, which is how both an
/// Iceberg warehouse and a listing table lay data out, and the granularity the covering check works
/// at.
fn seed_parquet(dir: &std::path::Path, table: &str) -> Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from((1..=5).collect::<Vec<i64>>()))],
    )?;
    let table_dir = dir.join(table);
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("data.parquet");
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// A physical plan scanning one seeded table.
async fn scan_plan(path: &std::path::Path, name: &str) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    Ok(ctx
        .sql(&format!("SELECT v FROM {name}"))
        .await?
        .create_physical_plan()
        .await?)
}

/// What a coordinator would say about the caller.
fn identity(user: &str) -> QueryIdentity {
    QueryIdentity {
        account_id: Some(42),
        user: Some(user.to_string()),
        objects: vec!["SELECT on table lldb.sales.orders".to_string()],
    }
}

/// Mint an assertion for `plan` as `user`, at `now`.
fn mint(
    secret: &FleetAuth,
    user: &str,
    plan: &Arc<dyn ExecutionPlan>,
    now: SystemTime,
) -> SignedAssertion {
    PlanAuth::from_fleet_auth(secret)
        .mint(&identity(user), plan, now)
        .expect("minting succeeds")
        .expect("a fleet with a secret mints an assertion")
}

/// Pull a partition presenting both credentials, collecting the whole stream.
async fn pull(
    url: &str,
    plan: &Arc<dyn ExecutionPlan>,
    secret: &FleetAuth,
    assertion: Option<&SignedAssertion>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = flight::serialize_plan(Arc::clone(plan))?;
    let stream =
        flight::fetch_stream_with(url.to_string(), 0, plan_bytes, secret, assertion).await?;
    Ok(stream.try_collect::<Vec<_>>().await?)
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------
// 1. The refusals
// ---------------------------------------------------------------------------

/// A worker holding the fleet secret still refuses a plan that is not authorized *for this request*.
///
/// The fleet token alone used to be the whole door: anything that could present it could have any
/// plan executed, reading whatever the worker's storage credentials could reach. Every case below
/// presents a **correct** fleet token, so each one is exclusively about the assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_refuses_a_plan_its_assertion_does_not_authorize() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let orders = seed_parquet(tmp.path(), "orders")?;
    let payroll = seed_parquet(tmp.path(), "payroll")?;

    let secret = fleet_secret("refusals");
    let mut fleet = Servers::new();
    let worker = start_closed_worker(&mut fleet, &secret, Arc::new(StageCache::new())).await?;

    let plan = scan_plan(&orders, "orders").await?;
    let other = scan_plan(&payroll, "payroll").await?;
    let now = SystemTime::now();

    // ---- No assertion at all ---------------------------------------------------------------
    let error = pull(&worker, &plan, &secret, None)
        .await
        .expect_err("the fleet token is not authorization to run a plan");
    let message = format!("{error:#}");
    assert!(
        message.contains("requires a plan assertion"),
        "the refusal must name what is missing: {message}"
    );
    // Fatal, not retriable: an identical fleet refuses identically, so a query must not walk it.
    assert_eq!(
        retry::classify(&error),
        Retriability::Fatal,
        "an unauthorized plan must not be replayed across the fleet: {message}"
    );

    // ---- An assertion that covers a *different* table --------------------------------------
    // This is the covering check, and it is what makes this per-request identity rather than a
    // second fleet token: the assertion verifies perfectly and still does not authorize this plan.
    let for_payroll = mint(&secret, "alice", &other, now);
    let error = pull(&worker, &plan, &secret, Some(&for_payroll))
        .await
        .expect_err("an assertion is not a bearer token for arbitrary plans");
    let message = format!("{error:#}");
    assert!(
        message.contains("permission denied") && message.contains("orders"),
        "the refusal must name the location it would not authorize: {message}"
    );
    assert_eq!(retry::classify(&error), Retriability::Fatal, "{message}");

    // ---- An assertion signed by a different fleet ------------------------------------------
    let impostor = fleet_secret("impostor");
    let forged = mint(&impostor, "alice", &plan, now);
    let error = pull(&worker, &plan, &secret, Some(&forged))
        .await
        .expect_err("a MAC from another key must not verify");
    let message = format!("{error:#}");
    assert!(message.contains("signature does not verify"), "{message}");

    // ---- An expired assertion ---------------------------------------------------------------
    // Minted in the past, so it is past its TTL by the time it is presented. Nothing about it is
    // otherwise wrong, which is the point: an assertion is deliberately short-lived, so a captured
    // one stops working.
    let stale = PlanAuth::from_fleet_auth(&secret)
        .sign(&PlanAssertion::for_plan(
            &identity("alice"),
            &plan,
            now - Duration::from_secs(24 * 3600),
            Duration::from_secs(60),
        ))
        .expect("signing succeeds")
        .expect("a fleet with a secret signs");
    let error = pull(&worker, &plan, &secret, Some(&stale))
        .await
        .expect_err("an expired assertion is not a credential");
    let message = format!("{error:#}");
    assert!(message.contains("expired"), "{message}");

    // ---- And the one that is actually authorized -------------------------------------------
    let good = mint(&secret, "alice", &plan, now);
    let batches = pull(&worker, &plan, &secret, Some(&good)).await?;
    assert_eq!(rows(&batches), 5, "the authorized plan runs normally");

    Ok(())
}

// ---------------------------------------------------------------------------
// 2. The trap: caching
// ---------------------------------------------------------------------------

/// Two requests carrying **different** assertions still share one materialization.
///
/// This is the trap issue #34 is built around. `stage_id_of` is a plain content hash of the plan
/// bytes and it is the `StageCache` key, so a per-request value carried *inside* those bytes would
/// give every request its own stage id — every pull a miss, and the materialize-once property that
/// `shuffle_materialization::a_producer_pulled_by_many_consumers_executes_once` proves silently
/// gone. The assertion therefore travels in gRPC metadata, and this is the assertion that it does:
/// the two pulls below present genuinely different header values (different user, different issue
/// time, therefore different MAC) and the producer must still execute exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_caching_still_hits_across_requests_carrying_different_assertions() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let orders = seed_parquet(tmp.path(), "orders")?;

    let secret = fleet_secret("caching");
    let cache = Arc::new(StageCache::new());
    let mut fleet = Servers::new();
    let worker = start_closed_worker(&mut fleet, &secret, Arc::clone(&cache)).await?;

    let plan = scan_plan(&orders, "orders").await?;
    let now = SystemTime::now();

    // Two assertions that are not equal as bytes: a different user and a different issue time, both
    // of the same tenant and both covering the same locations.
    let first = mint(&secret, "alice", &plan, now);
    let second = mint(&secret, "bob", &plan, now + Duration::from_secs(30));
    assert_ne!(
        first.as_header_value(),
        second.as_header_value(),
        "test premise: the two requests must genuinely differ"
    );

    assert_eq!(rows(&pull(&worker, &plan, &secret, Some(&first)).await?), 5);
    assert_eq!(
        rows(&pull(&worker, &plan, &secret, Some(&second)).await?),
        5
    );
    // A third pull with the first assertion again, for good measure.
    assert_eq!(rows(&pull(&worker, &plan, &secret, Some(&first)).await?), 5);

    assert_eq!(
        cache.execution_count(),
        1,
        "three requests, three different (or repeated) assertions, ONE materialization — an \
         assertion inside the plan bytes would make this 3"
    );
    assert_eq!(cache.len(), 1, "and one stage entry, not three");

    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Worker to worker
// ---------------------------------------------------------------------------

/// A worker that pulls from another worker forwards the assertion it was given.
///
/// Both workers are closed, so the reduce worker cannot answer at all unless it presents the
/// assertion to the map worker — and it cannot mint one, because minting is the coordinator's job
/// and the plan does not carry one. The forwarding channel is the `TaskContext` the stage executes
/// under; see `lldb_qe_core::plan_assertion`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_forwards_the_assertion_to_the_worker_it_pulls_from() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let orders = seed_parquet(tmp.path(), "orders")?;

    let secret = fleet_secret("w2w");
    let mut fleet = Servers::new();
    let map_cache = Arc::new(StageCache::new());
    let map_worker =
        start_assertion_only_worker(&mut fleet, &secret, Arc::clone(&map_cache)).await?;
    let reduce_worker =
        start_assertion_only_worker(&mut fleet, &secret, Arc::new(StageCache::new())).await?;

    // Stage 1 on the map worker (the scan); stage 2 on the reduce worker, whose leaf pulls stage 1.
    let scan = Arc::new(CoalescePartitionsExec::new(
        scan_plan(&orders, "orders").await?,
    ));
    let stage2: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::new(&map_worker, 0, scan)?);

    // Minted from the *outer* plan, which is what a coordinator has. It covers the scan's files
    // because the read walk descends into a remote stage's inner plan — without that, this pull
    // would be refused by the map worker as uncovered.
    let assertion = mint(&secret, "alice", &stage2, SystemTime::now());

    let batches = pull(&reduce_worker, &stage2, &FleetAuth::Open, Some(&assertion)).await?;
    assert_eq!(rows(&batches), 5, "rows must survive both hops");
    assert_eq!(
        map_cache.execution_count(),
        1,
        "the map worker really was pulled — this is not a two-hop test that never made hop two"
    );

    // And the forwarding is load-bearing rather than incidental: the map worker refuses the very
    // same inner stage when it is asked for it *without* an assertion.
    let inner = Arc::clone(
        stage2
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .unwrap()
            .inner(),
    );
    let error = pull(&map_worker, &inner, &FleetAuth::Open, None)
        .await
        .expect_err("the map worker requires an assertion, so hop two really carried one");
    assert!(
        format!("{error:#}").contains("requires a plan assertion"),
        "{error:#}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Reassignment
// ---------------------------------------------------------------------------

/// A lost worker is still reassigned to a healthy one, and the assertion follows the stage there.
///
/// The coordinator half of the forwarding path: the plan is executed locally through DataFusion, so
/// its `FlightReaderExec` leaf reads the assertion from the `TaskContext` rather than from the plan
/// bytes — and does so once per candidate, because the same assertion authorizes *what* the stage
/// reads and not *who* runs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reassigned_stage_carries_the_assertion_to_the_worker_that_serves_it() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let orders = seed_parquet(tmp.path(), "orders")?;

    let secret = fleet_secret("reassign");
    let mut fleet = Servers::new();
    let healthy =
        start_assertion_only_worker(&mut fleet, &secret, Arc::new(StageCache::new())).await?;

    // A port nothing listens on: bind it, read it, drop the listener. The cheapest faithful stand-in
    // for a worker that is gone.
    let dead = {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);
        format!("http://{addr}")
    };

    let scan = Arc::new(CoalescePartitionsExec::new(
        scan_plan(&orders, "orders").await?,
    ));
    let staged: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::with_fallbacks(
        &dead,
        vec![healthy.clone()],
        0,
        scan,
    )?);

    let assertion = mint(&secret, "alice", &staged, SystemTime::now());
    let ctx = SessionContext::new();
    let task_ctx = plan_assertion::task_ctx_with(&ctx.task_ctx(), Some(assertion));
    let batches = collect(staged, task_ctx).await?;
    assert_eq!(
        rows(&batches),
        5,
        "the stage must reassign to the healthy worker and be authorized there"
    );

    Ok(())
}
