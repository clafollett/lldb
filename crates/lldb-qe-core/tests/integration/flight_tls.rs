//! **TLS on both Flight boundaries** (issue #33): a query crossing an encrypted client→coordinator
//! hop *and* an encrypted coordinator→worker hop, a plaintext client refused by a TLS server, and
//! the no-configuration path proven still to work.
//!
//! The refusal to *start* insecurely — a checked credential on a plaintext port — is deliberately
//! not here. It is a pure function of flags, so it lives as unit tests in
//! [`lldb_qe_core::tls`] where it needs no socket, no database and no fleet; a test that spent
//! three seconds standing up servers to assert a `bail!` would be slower and prove less.
//!
//! Everything below runs against a CA this binary mints for itself (`support::certs`), never
//! against committed key material.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::support::{Servers, certs};
use anyhow::{Context, Result};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use lldb_qe_core::FleetAuth;
use lldb_qe_core::flight;
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, serve_coordinator_with_tls, submit_query,
};
use lldb_qe_core::stage_cache::StageCache;
use lldb_qe_core::tls::ServerTls;

/// Rows in the one table these tests query. Small: the subject is the transport, not the plan.
const ROWS: i64 = 40;
/// The answer `SELECT sum(v)` must produce — 0 + 1 + … + 39.
const EXPECTED_SUM: i64 = ROWS * (ROWS - 1) / 2;

/// A parquet table on disk, because these plans really are shipped to a worker: an in-memory table
/// would not survive serialization, and the point is a *serialized* plan crossing a TLS hop.
fn seed_table(dir: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from((0..ROWS).collect::<Vec<_>>()))],
    )?;
    let file = std::fs::File::create(dir.join("rows.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// A worker on `127.0.0.1:0` serving `tls`, returning its address.
///
/// The handle goes into the caller's [`Servers`] rather than being dropped: a dropped `JoinHandle`
/// detaches its task instead of stopping it, and this is one binary, so a detached worker would
/// hold its port for the rest of the run. See `support::Servers`.
async fn start_worker(servers: &mut Servers, tls: ServerTls) -> Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    servers.spawn(async move {
        flight::serve_worker_with(
            listener,
            SessionContext::new(),
            Arc::new(StageCache::new()),
            // The fleet secret is a *separate* boundary from the transport and is deliberately
            // untouched by this issue; an open worker keeps the test about TLS and nothing else.
            FleetAuth::Open,
            tls,
        )
        .await
        .expect("worker serve");
    });
    Ok(addr)
}

/// A coordinator on `127.0.0.1:0` serving `tls`, fanning out to `workers`, returning its address.
async fn start_coordinator(
    servers: &mut Servers,
    dir: &Path,
    workers: Vec<String>,
    tls: ServerTls,
) -> Result<std::net::SocketAddr> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        dir.join("rows.parquet").to_str().context("utf-8 path")?,
        ParquetReadOptions::default(),
    )
    .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // No services database: authentication and history are somebody else's tests, and their
    // absence is what makes `CredentialCheck::None` the honest posture for these servers.
    let coordinator = Arc::new(Coordinator::new(
        ctx,
        None,
        CoordinatorConfig {
            workers,
            ..CoordinatorConfig::default()
        },
    ));
    servers.spawn(async move {
        serve_coordinator_with_tls(listener, coordinator, tls, std::future::pending::<()>())
            .await
            .expect("coordinator serve");
    });
    Ok(addr)
}

/// Sum the `v` column out of whatever the server streamed back.
fn sum_of(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 sum")
                .value(0)
        })
        .sum()
}

/// **The acceptance test.** One query, two encrypted hops, the right answer.
///
/// It is one query on purpose rather than two tests of one hop each: the client submits over
/// `https://` to a coordinator that serves TLS, and that coordinator's *only* worker is reachable
/// over `https://` too — so a plan is serialized, shipped inside one TLS session, executed, and its
/// Arrow batches come back through both. Nothing in the path is plaintext, and the assertion is on
/// the rows, so an encrypted channel that silently delivered nothing would fail as a wrong number.
#[tokio::test]
async fn a_query_round_trips_over_tls_on_both_flight_boundaries() -> Result<()> {
    certs::install_test_trust();
    let tmp = tempfile::tempdir()?;
    seed_table(tmp.path())?;

    let mut servers = Servers::new();
    let worker = start_worker(&mut servers, certs::server_tls(tmp.path())?).await?;
    let coordinator = start_coordinator(
        &mut servers,
        tmp.path(),
        vec![format!("https://{worker}")],
        certs::server_tls(tmp.path())?,
    )
    .await?;

    let submitted = submit_query(
        &format!("https://{coordinator}"),
        &QueryRequest::new("SELECT sum(v) AS total FROM rows"),
    )
    .await
    .context("submitting over TLS")?;

    assert_eq!(
        sum_of(&submitted.batches),
        EXPECTED_SUM,
        "the answer must survive both TLS hops intact"
    );
    Ok(())
}

/// A worker that serves TLS must not answer a plaintext caller — and must not leave it hanging.
///
/// Both halves matter. "Refused" is the security property: a client cannot downgrade its way past
/// the encryption by asking nicely, because the *server* decides. "Promptly, with an error" is the
/// operability property: the overwhelmingly likely cause of this failure is an operator who turned
/// on certificates and left a `http://` in `--workers`, and a query that hangs teaches them nothing.
/// The timeout is that assertion — if this ever starts hanging, the test fails instead of stalling
/// CI.
///
/// The plan shipped here is a **valid, executable** one, on purpose. A garbage ticket would make
/// this test pass against a plaintext worker too (it would be rejected as a bad ticket, and the
/// assertion could not tell the two refusals apart) — which is to say it would prove nothing. With a
/// real plan the only reason to fail is the transport, so turning the worker's TLS off turns this
/// test red.
#[tokio::test]
async fn a_plaintext_client_is_refused_by_a_tls_worker() -> Result<()> {
    certs::install_test_trust();
    let tmp = tempfile::tempdir()?;
    seed_table(tmp.path())?;
    let mut servers = Servers::new();
    let worker = start_worker(&mut servers, certs::server_tls(tmp.path())?).await?;
    let plan = shippable_plan(tmp.path()).await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        // `http://`, at a port speaking TLS. The ambient trust is not consulted and that is the
        // point: the scheme decides, so this really is an unencrypted client.
        flight::fetch(&format!("http://{worker}"), 0, plan),
    )
    .await
    .expect("a plaintext client must be refused, not left hanging");

    let error = outcome.expect_err("a TLS port must not answer a plaintext client");
    // Deliberately not asserting on rustls's or tonic's wording — that is a dependency's text, and
    // pinning it here would make a routine upgrade look like a security regression. What is
    // asserted is that the failure names the worker, which is what an operator needs to act.
    assert!(
        format!("{error:#}").contains(&worker.to_string()),
        "the failure must name the worker it could not talk to, got: {error:#}"
    );
    Ok(())
}

/// A single-partition scan of the seeded table, serializable and executable on a bare worker (the
/// plan names its parquet file by absolute path, so the worker needs no catalog).
async fn shippable_plan(dir: &Path) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        dir.join("rows.parquet").to_str().context("utf-8 path")?,
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(Arc::new(CoalescePartitionsExec::new(
        ctx.sql("SELECT v FROM rows")
            .await?
            .create_physical_plan()
            .await?,
    )))
}

/// The hard constraint from CLAUDE.md, proven rather than asserted: **no services database, no
/// flags, no certificates — and everything still works.**
///
/// It runs in the same process, and usually after, the TLS tests above, which is the stronger
/// version of the claim: a CA has by then been installed process-wide, and this still dials
/// `http://` and gets its rows. That is the inertness argument for
/// [`lldb_qe_core::tls::install_client_trust`] turned into an assertion — the ambient trust is
/// consulted only for `https://`, so a TLS-configured process (or a shared test binary) cannot
/// accidentally change what a plaintext caller does.
#[tokio::test]
async fn with_no_certificates_at_all_the_plaintext_path_is_unchanged() -> Result<()> {
    certs::install_test_trust();
    let tmp = tempfile::tempdir()?;
    seed_table(tmp.path())?;

    // Resolved from empty flags rather than named directly — see `certs::no_tls_configured`. This
    // is what `cargo run -p lldb-qe-worker` and a bare `lldb-qe-server` actually produce.
    let mut servers = Servers::new();
    let worker = start_worker(&mut servers, certs::no_tls_configured()?).await?;
    let coordinator = start_coordinator(
        &mut servers,
        tmp.path(),
        vec![format!("http://{worker}")],
        certs::no_tls_configured()?,
    )
    .await?;

    let submitted = submit_query(
        &format!("http://{coordinator}"),
        &QueryRequest::new("SELECT sum(v) AS total FROM rows"),
    )
    .await
    .context("submitting in plaintext, as a checkout does")?;

    assert_eq!(sum_of(&submitted.batches), EXPECTED_SUM);
    Ok(())
}
