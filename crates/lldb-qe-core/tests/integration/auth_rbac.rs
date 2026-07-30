//! Accounts, authentication and RBAC end to end: a **real Flight server**, **real workers**, a
//! **real Postgres**, two tenants, and no shortcuts around the boundary under test.
//!
//! This file exists to settle issue #19's three "done when" claims as facts rather than prose:
//!
//! 1. **An unauthenticated request is rejected; an authenticated one is scoped to its account.**
//!    Submitted with no credential, with a garbage credential, and with a valid credential whose
//!    ticket claims somebody else's tenant. Only the honest one runs, and the row it leaves in
//!    query history belongs to the account the *token* named.
//! 2. **A role without `SELECT` on a table cannot query it; adding the grant enables it.** The same
//!    SQL, the same client, the same server — one `INSERT` into `grants` between the two runs.
//! 3. **Two accounts on one deployment cannot see or query each other's objects.** Both directions:
//!    the plan-time grant check refuses the table, the account-scoped lookup refuses the warehouse,
//!    and the composite foreign keys refuse to wire a credential across tenants at all.
//!
//! Plus the fourth thing the issue asks to be closed or explicitly noted — the **worker trust
//! boundary**. A worker configured with a fleet secret refuses a plan from a caller that does not
//! present it, and serves the same plan to one that does.
//!
//! # What is real and what is faked
//!
//! Real: the Flight server, the Flight client, the API keys (generated, hashed, stored, verified),
//! the grants, the DataFusion planning, the distributed execution across in-process workers, every
//! SQL statement and every constraint.
//!
//! Faked: one link, DNS — `<warehouse>.lldb.local` does not resolve on a laptop, so the coordinator
//! gets the same injected resolver `warehouse_routing.rs` and `query_scheduler.rs` use.
//!
//! Nothing here asserts on an error *string* where a code is available: a message can be reworded,
//! and a test that pins the wording either breaks on a typo fix or, worse, passes because two
//! unrelated failures happen to share a word.
//!
//! The database is found the same way `services_db.rs` finds it (see [`support`]): an explicit
//! `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a clean skip.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration auth_rbac
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration auth_rbac -- --nocapture

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::support::{self, DbCleanup, Servers, resolve_target, unique_name};
use anyhow::{Context, Result};
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::auth::{AUTHORIZATION_HEADER, bearer_header};
use lldb_qe_core::auth::{AuthError, FleetAuth};
use lldb_qe_core::engine::BoxResolver;
use lldb_qe_core::liveness::CoordinatorIdentity;
use lldb_qe_core::rbac::{ObjectRef, ObjectType, Privilege};
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, encode_query_ticket, serve_coordinator,
    submit_query, submit_query_as,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::warehouse::WarehouseState;
use lldb_qe_core::{DEFAULT_WAREHOUSE_ENDPOINT, flight};

/// Workers standing behind each tenant's warehouse.
const WAREHOUSE_SIZE: i32 = 1;

/// Skip-or-connect, shared by every test in this file.
pub(crate) async fn db_or_skip(what: &str) -> Result<Option<(ServicesDb, support::Target)>> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP ({what}): set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a \
             Docker daemon, to exercise authentication and RBAC"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;
    Ok(Some((db, target)))
}

/// Two parquet tables, so a grant can cover one and not the other. Parquet rather than
/// `register_batch` because these plans are really shipped to a worker process — an in-memory table
/// would not survive serialization.
fn seed_tables(dir: &Path) -> Result<()> {
    for (name, rows) in [("rows", 60i64), ("secrets", 12)] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    (0..rows).map(|i| format!("g{}", i % 3)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            ],
        )?;
        let file = std::fs::File::create(dir.join(format!("{name}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(())
}

/// Start `count` in-process workers with the ambient (open) fleet credential — the worker boundary
/// gets its own test below; here the subject is the *coordinator's* front door.
///
/// The handles go into the caller's [`Servers`] rather than being dropped: a dropped `JoinHandle`
/// detaches the task instead of stopping it, and since #44 this is one binary, so a detached worker
/// holds its port for the rest of the run.
async fn start_workers(servers: &mut Servers, count: usize) -> Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        servers.spawn(async move {
            flight::serve_worker(listener, SessionContext::new())
                .await
                .expect("worker serve");
        });
        addrs.push(addr);
    }
    Ok(addrs)
}

/// A resolver answering any of `names` with `addrs`, and erroring on anything else — a query must
/// never resolve a warehouse it was not routed to.
pub(crate) fn cloud_map(names: Vec<String>, addrs: Vec<SocketAddr>) -> BoxResolver {
    Arc::new(move |asked: String| {
        let answer = if names.contains(&asked) {
            Ok(addrs.clone())
        } else {
            Err(anyhow::anyhow!("no warehouse registered as `{asked}`"))
        };
        Box::pin(std::future::ready(answer))
    })
}

/// One tenant, fully wired: an account, a user, a role, a key, a warehouse.
pub(crate) struct Tenant {
    pub(crate) account_id: i64,
    pub(crate) account_name: String,
    pub(crate) user_id: i64,
    pub(crate) role_id: i64,
    pub(crate) warehouse: String,
    pub(crate) token: String,
}

/// Two tenants sharing one server, one catalog and one database — which is the only arrangement in
/// which "they cannot see each other" is a claim worth testing.
struct Harness {
    db: ServicesDb,
    url: String,
    a: Tenant,
    b: Tenant,
    /// The coordinator and the workers behind it. Declared before `_cleanup` so they are aborted
    /// before the rows they might still be writing to are deleted; see [`Servers`].
    _servers: Servers,
    /// Both accounts, deleted on the way out — including when an assertion panicked, which is the
    /// whole reason this is a field and not a `cleanup()` call at the end of each body. See
    /// [`DbCleanup`].
    _cleanup: DbCleanup,
    _tmp: tempfile::TempDir,
}

impl Harness {
    async fn start(db: ServicesDb, url: &str) -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        seed_tables(tmp.path())?;

        let a = provision(&db, "acct-a").await?;
        let b = provision(&db, "acct-b").await?;

        let mut cleanup = DbCleanup::new(url);
        cleanup.account(a.account_id);
        cleanup.account(b.account_id);

        // One fleet behind both warehouses. Deliberate: if isolation depended on the two tenants
        // resolving to *different* workers, this test would prove nothing about access control.
        // They share compute, and are still kept apart.
        let mut servers = Servers::new();
        let workers = start_workers(&mut servers, WAREHOUSE_SIZE as usize).await?;
        let resolver = cloud_map(
            vec![
                format!("{}.lldb.local:50051", a.warehouse),
                format!("{}.lldb.local:50051", b.warehouse),
            ],
            workers.clone(),
        );

        let mut cfg = SessionConfig::new().with_target_partitions(2);
        cfg.options_mut().optimizer.repartition_file_min_size = 1;
        let ctx = SessionContext::new_with_config(cfg);
        for name in ["rows", "secrets"] {
            ctx.register_parquet(
                name,
                tmp.path()
                    .join(format!("{name}.parquet"))
                    .to_str()
                    .expect("utf-8 path"),
                ParquetReadOptions::default(),
            )
            .await?;
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let coordinator = Arc::new(
            Coordinator::new(
                ctx,
                Some(db.clone()),
                CoordinatorConfig {
                    // Set, and deliberately set to a tenant that does not exist: if any code path
                    // still fell back to the configured default instead of the credential's
                    // account, every test below would fail loudly rather than pass by luck.
                    default_account: "nobody".to_string(),
                    workers: workers
                        .iter()
                        .map(|addr| format!("http://{addr}"))
                        .collect(),
                    warehouse_endpoint: vec![DEFAULT_WAREHOUSE_ENDPOINT.to_string()],
                    max_concurrent_queries: Some(2),
                    max_queued_queries: 32,
                    coordinator: CoordinatorIdentity::new(format!("auth-test-{addr}")),
                    allow_anonymous: false,
                },
            )
            .with_resolver(resolver),
        );
        assert!(
            coordinator.requires_authentication(),
            "a coordinator with a services database must require a credential"
        );

        servers.spawn(async move {
            serve_coordinator(listener, coordinator, std::future::pending::<()>())
                .await
                .expect("coordinator serve");
        });

        Ok(Self {
            db,
            url: format!("http://{addr}"),
            a,
            b,
            _servers: servers,
            _cleanup: cleanup,
            _tmp: tmp,
        })
    }
}

/// Create a tenant with a user, a role holding `USAGE` on its own warehouse, and one API key.
///
/// Note what it is **not** granted: any table. Every table grant in this file is made explicitly by
/// the test that needs it, so "denied by default" is the starting state rather than an assumption —
/// which is also why `cache_grant_ordering` reuses it rather than seeding its own tenant.
pub(crate) async fn provision(db: &ServicesDb, tag: &str) -> Result<Tenant> {
    let account = db.create_account(&unique_name(tag)).await?;
    let user = db.create_user(account.id, "operator").await?;
    let role = db.create_role(account.id, "analyst").await?;
    db.assign_role(account.id, user.id, role.id).await?;

    let warehouse = unique_name(&format!("wh-{tag}"));
    db.create_warehouse(
        account.id,
        &warehouse,
        WAREHOUSE_SIZE,
        WarehouseState::Running,
    )
    .await?;
    db.grant(
        account.id,
        role.id,
        Privilege::Usage,
        &ObjectRef::new(ObjectType::Warehouse, warehouse.clone())?,
    )
    .await?;

    let (_key, token) = db.create_api_key(account.id, user.id, "cli", None).await?;
    Ok(Tenant {
        account_id: account.id,
        account_name: account.name,
        user_id: user.id,
        role_id: role.id,
        warehouse,
        token: token.into_secret(),
    })
}

/// `SELECT count(*) FROM <table>` on a tenant's own warehouse.
fn query(tenant: &Tenant, table: &str) -> QueryRequest {
    QueryRequest::new(format!("SELECT count(*) AS n FROM {table}"))
        .on_warehouse(tenant.warehouse.clone())
}

/// Submit through a raw Flight client and hand back the gRPC status.
///
/// [`submit_query_as`] flattens a failure into an `anyhow::Error`, which is right for a caller and
/// useless for a test asserting the *client contract*: "fetch a credential" (`UNAUTHENTICATED`),
/// "ask for a grant" (`PERMISSION_DENIED`) and "file a bug" (`INTERNAL`) are three different
/// actions, and a test that reads only the message text cannot tell them apart — so it would keep
/// passing if every refusal silently became an internal error.
/// Submit with an arbitrary raw `authorization` value, bypassing `bearer_header`.
///
/// The point is to send things a cooperating client never would — a wrong scheme, an empty
/// bearer, a stray header — because that is precisely what the folding bug accepted.
async fn raw_status_of(url: &str, request: &QueryRequest, raw: &str) -> tonic::Status {
    let channel = tonic::transport::Channel::from_shared(url.to_string())
        .expect("valid url")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let mut grpc = tonic::Request::new(Ticket {
        ticket: encode_query_ticket(request).into(),
    });
    grpc.metadata_mut()
        .insert(AUTHORIZATION_HEADER, raw.parse().expect("header value"));
    match client.do_get(grpc).await {
        Ok(_) => tonic::Status::ok("the request succeeded"),
        Err(status) => status,
    }
}

pub(crate) async fn status_of(
    url: &str,
    request: &QueryRequest,
    token: Option<&str>,
) -> tonic::Status {
    let channel = tonic::transport::Channel::from_shared(url.to_string())
        .expect("valid url")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let mut grpc = tonic::Request::new(Ticket {
        ticket: encode_query_ticket(request).into(),
    });
    if let Some(token) = token {
        grpc.metadata_mut().insert(
            AUTHORIZATION_HEADER,
            bearer_header(token).parse().expect("header value"),
        );
    }
    // A `do_get` that succeeds returns a stream, and the test cases here never do — but a success
    // must fail the assertion rather than panic, so it is reported as `Ok`.
    match client.do_get(grpc).await {
        Ok(_) => tonic::Status::ok("the request succeeded"),
        Err(status) => status,
    }
}

/// The single scalar a `count(*)` came back with.
fn scalar(batches: &[RecordBatch]) -> Result<i64> {
    let batch = batches.first().context("no batches")?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("count(*) is not Int64")?;
    Ok(col.value(0))
}

// ---------------------------------------------------------------------------
// 1. Unauthenticated is rejected; authenticated is scoped
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_is_rejected_and_authenticated_is_scoped_to_its_account() -> Result<()> {
    let Some((db, target)) = db_or_skip("authentication").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip only returns a connected database");
    // No explicit cleanup call: the harness deletes its rows in `Drop`, so a failing assertion in
    // the body below still cleans up after itself. See `support::DbCleanup`.
    let harness = Harness::start(db, url).await?;
    authentication_body(&harness).await
}

async fn authentication_body(harness: &Harness) -> Result<()> {
    // The tenant may read `rows`; whether it *is allowed* is settled in the next test, so grant it
    // here and keep this one about identity.
    harness
        .db
        .grant(
            harness.a.account_id,
            harness.a.role_id,
            Privilege::Select,
            &ObjectRef::table("datafusion", "public", "rows"),
        )
        .await?;

    // ---- No credential at all -----------------------------------------------------------------
    let error = submit_query(&harness.url, &query(&harness.a, "rows"))
        .await
        .expect_err("a coordinator with a services database must refuse an anonymous request");
    let message = format!("{error:#}");
    assert!(
        message.to_lowercase().contains("unauthenticated"),
        "the refusal must say what is missing: {message}"
    );
    eprintln!("anonymous submission refused: {message}");
    assert_eq!(
        status_of(&harness.url, &query(&harness.a, "rows"), None)
            .await
            .code(),
        tonic::Code::Unauthenticated,
        "a missing credential must say `get a credential`, not `something broke`"
    );

    // ---- A credential that is not one ----------------------------------------------------------
    for bogus in [
        "lldb_notarealtokenatallnotarealtokenatall00",
        "definitely-not-an-lldb-key",
        "",
    ] {
        let error = submit_query_as(&harness.url, &query(&harness.a, "rows"), Some(bogus))
            .await
            .expect_err("a forged credential must be refused");
        assert!(
            format!("{error:#}")
                .to_lowercase()
                .contains("unauthenticated"),
            "`{bogus}` produced {error:#}"
        );
    }

    // ---- A real credential claiming somebody else's tenant -------------------------------------
    // The hole this issue closes, stated directly: the ticket's `account` field used to be obeyed.
    let impersonation = query(&harness.a, "rows").as_account(harness.b.account_name.clone());
    let error = submit_query_as(&harness.url, &impersonation, Some(&harness.a.token))
        .await
        .expect_err("a ticket must not be able to select a tenant");
    let message = format!("{error:#}");
    assert!(
        message.contains("cannot be chosen by the caller"),
        "{message}"
    );
    eprintln!("cross-tenant ticket refused: {message}");
    assert_eq!(
        status_of(&harness.url, &impersonation, Some(&harness.a.token))
            .await
            .code(),
        tonic::Code::PermissionDenied
    );

    // ---- The honest request ---------------------------------------------------------------------
    let ok = submit_query_as(
        &harness.url,
        &query(&harness.a, "rows"),
        Some(&harness.a.token),
    )
    .await
    .context("an authenticated, granted query must run")?;
    assert_eq!(scalar(&ok.batches)?, 60);

    // …and it is scoped: the history row belongs to the token's account, even though the server's
    // configured default account is a tenant that does not exist.
    let history = harness.db.list_queries(harness.a.account_id, 16).await?;
    assert_eq!(
        history.len(),
        1,
        "exactly one query should have been recorded for account A; refused ones never got a row"
    );
    assert_eq!(history[0].sql_text, query(&harness.a, "rows").sql);
    assert!(
        harness
            .db
            .list_queries(harness.b.account_id, 16)
            .await?
            .is_empty(),
        "account B did not submit anything and must have no history"
    );

    // The key that ran it is recorded as used — the audit trail that makes "revoke the key that did
    // this" answerable.
    let keys = harness.db.list_api_keys(harness.a.account_id).await?;
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].last_used_at.is_some(),
        "a successful authentication must stamp last_used_at"
    );
    // And what is stored is a handle, not the credential.
    assert!(harness.a.token.starts_with(&keys[0].token_prefix));
    assert_ne!(harness.a.token, keys[0].token_prefix);

    // ---- Revocation takes effect immediately, with no restart ----------------------------------
    assert!(
        harness
            .db
            .revoke_api_key(harness.a.account_id, harness.a.user_id, "cli")
            .await?
    );
    let error = submit_query_as(
        &harness.url,
        &query(&harness.a, "rows"),
        Some(&harness.a.token),
    )
    .await
    .expect_err("a revoked key must stop working at once");
    assert!(format!("{error:#}").contains("revoked"), "{error:#}");
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. A missing grant denies; adding it enables
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_without_select_cannot_query_until_the_grant_is_added() -> Result<()> {
    let Some((db, target)) = db_or_skip("grants").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip only returns a connected database");
    let harness = Harness::start(db, url).await?;
    grant_body(&harness).await
}

async fn grant_body(harness: &Harness) -> Result<()> {
    let request = query(&harness.a, "secrets");

    // ---- Before ---------------------------------------------------------------------------------
    let error = submit_query_as(&harness.url, &request, Some(&harness.a.token))
        .await
        .expect_err("no SELECT grant, no rows");
    let message = format!("{error:#}");
    assert!(
        message.contains("SELECT on table datafusion.public.secrets"),
        "the denial must name the privilege and the object: {message}"
    );
    assert!(
        message.contains("lldb-qe-auth grant"),
        "the denial must name the fix: {message}"
    );
    eprintln!("denied before the grant: {message}");
    // The code, not just the text. This refusal is raised deep inside execution and has to survive
    // all the way out as a *refusal*: if the typed probe on `rbac::Denied` ever stopped matching,
    // the message above would be unchanged and this line is the only thing that would notice.
    let status = status_of(&harness.url, &request, Some(&harness.a.token)).await;
    assert_eq!(
        status.code(),
        tonic::Code::PermissionDenied,
        "a missing grant must not be reported as an internal error: {status:?}"
    );

    // Denied, but *recorded* — an access-control refusal that leaves no trace is an audit hole.
    // Two rows, because the two refusals above are two submissions: the check runs after the query
    // is admitted (it needs the logical plan), so a denied query really does reach history.
    let history = harness.db.list_queries(harness.a.account_id, 16).await?;
    assert_eq!(history.len(), 2, "every refusal must still be in history");
    for record in &history {
        assert_eq!(record.state, lldb_qe_core::QueryState::Failed, "{record:?}");
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|e| e.contains("permission denied")),
            "the recorded error must say it was a refusal: {:?}",
            record.error
        );
    }

    // ---- One INSERT into `grants` --------------------------------------------------------------
    harness
        .db
        .grant(
            harness.a.account_id,
            harness.a.role_id,
            Privilege::Select,
            &ObjectRef::table("datafusion", "public", "secrets"),
        )
        .await?;

    // ---- After: same SQL, same client, same server, same process -------------------------------
    let ok = submit_query_as(&harness.url, &request, Some(&harness.a.token))
        .await
        .context("the grant must enable exactly this query")?;
    assert_eq!(scalar(&ok.batches)?, 12);
    eprintln!("allowed after the grant: {} rows", scalar(&ok.batches)?);

    // ---- And it is *that* grant, not a general unlocking ---------------------------------------
    // `rows` was never granted, so it must still be refused. A check that got looser when any
    // grant existed would pass everything above and fail here.
    let error = submit_query_as(
        &harness.url,
        &query(&harness.a, "rows"),
        Some(&harness.a.token),
    )
    .await
    .expect_err("a grant on `secrets` says nothing about `rows`");
    assert!(
        format!("{error:#}").contains("datafusion.public.rows"),
        "{error:#}"
    );

    // ---- Containment: the namespace grant covers what the table grant did not -------------------
    harness
        .db
        .grant(
            harness.a.account_id,
            harness.a.role_id,
            Privilege::Select,
            &ObjectRef::new(ObjectType::Namespace, "datafusion.public")?,
        )
        .await?;
    let ok = submit_query_as(
        &harness.url,
        &query(&harness.a, "rows"),
        Some(&harness.a.token),
    )
    .await
    .context("a namespace grant must reach the tables in it")?;
    assert_eq!(scalar(&ok.batches)?, 60);

    // ---- Revoking the narrow grant does not revoke the broad one -------------------------------
    // The behaviour `lldb-qe-auth revoke` warns about, asserted so the warning stays true.
    assert!(
        harness
            .db
            .revoke(
                harness.a.role_id,
                Privilege::Select,
                &ObjectRef::table("datafusion", "public", "secrets"),
            )
            .await?
    );
    submit_query_as(&harness.url, &request, Some(&harness.a.token))
        .await
        .context("the namespace grant still covers `secrets`")?;

    // ---- A statement whose footprint cannot be named is refused, not allowed --------------------
    // With `ALL` on the namespace this caller can read anything in it; that must still not turn
    // into a read of an arbitrary path on the worker's filesystem.
    let ddl = QueryRequest::new(
        "CREATE EXTERNAL TABLE leak (a BIGINT) STORED AS PARQUET LOCATION '/etc/'",
    )
    .on_warehouse(harness.a.warehouse.clone());
    let error = submit_query_as(&harness.url, &ddl, Some(&harness.a.token))
        .await
        .expect_err("DDL has no expressible privilege and must fail closed");
    assert!(
        format!("{error:#}").contains("refused rather than allowed"),
        "{error:#}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Two accounts cannot see or query each other's objects
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_accounts_cannot_see_or_query_each_others_objects() -> Result<()> {
    let Some((db, target)) = db_or_skip("tenant isolation").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip only returns a connected database");
    let harness = Harness::start(db, url).await?;
    isolation_body(&harness).await
}

async fn isolation_body(harness: &Harness) -> Result<()> {
    let db = &harness.db;

    // Give A a broad grant, so that anything B manages to reach would be reachable *because of A's
    // grant* — the exact leak this test has to rule out.
    db.grant(
        harness.a.account_id,
        harness.a.role_id,
        Privilege::All,
        &ObjectRef::new(ObjectType::Catalog, "datafusion")?,
    )
    .await?;

    // ---- B cannot use A's warehouse ------------------------------------------------------------
    let borrowed = QueryRequest::new("SELECT count(*) AS n FROM rows")
        .on_warehouse(harness.a.warehouse.clone());
    let error = submit_query_as(&harness.url, &borrowed, Some(&harness.b.token))
        .await
        .expect_err("B has no USAGE on A's warehouse");
    assert!(
        format!("{error:#}").contains("USAGE on warehouse"),
        "{error:#}"
    );

    // …and even if an operator mistakenly grants B `USAGE` on a warehouse *name* that belongs to A,
    // the lookup is account-scoped, so there is still nothing there. Two independent barriers.
    db.grant(
        harness.b.account_id,
        harness.b.role_id,
        Privilege::Usage,
        &ObjectRef::new(ObjectType::Warehouse, harness.a.warehouse.clone())?,
    )
    .await?;
    let error = submit_query_as(&harness.url, &borrowed, Some(&harness.b.token))
        .await
        .expect_err("A's warehouse does not exist in B's account");
    let message = format!("{error:#}");
    assert!(message.contains("does not exist for account"), "{message}");
    assert!(
        message.contains(&harness.b.account_name),
        "the error must be phrased in B's world: {message}"
    );

    // ---- B cannot read what only A was granted -------------------------------------------------
    let error = submit_query_as(
        &harness.url,
        &query(&harness.b, "rows"),
        Some(&harness.b.token),
    )
    .await
    .expect_err("A's catalog-wide grant must not reach B");
    assert!(
        format!("{error:#}").contains("SELECT on table datafusion.public.rows"),
        "{error:#}"
    );

    // ---- A's credential cannot be wired to B, at the schema level -------------------------------
    // Not "the application does not do this" but "the database will not store it": the composite
    // foreign keys added by migration 0005 are what make a cross-tenant credential unrepresentable.
    assert!(
        db.assign_role(harness.b.account_id, harness.a.user_id, harness.b.role_id)
            .await
            .is_err(),
        "assigning B's role to A's user must violate a foreign key"
    );
    assert!(
        db.create_api_key(harness.b.account_id, harness.a.user_id, "stolen", None)
            .await
            .is_err(),
        "issuing a key for A's user under B's account must violate a foreign key"
    );
    assert!(
        sqlx::query(
            "INSERT INTO grants (account_id, role_id, privilege, object_type, object_name) \
             VALUES ($1, $2, 'ALL', 'catalog', 'datafusion')"
        )
        .bind(harness.b.account_id)
        .bind(harness.a.role_id)
        .execute(db.pool())
        .await
        .is_err(),
        "a grant naming another tenant's role must violate a foreign key"
    );

    // ---- Listings are scoped ---------------------------------------------------------------------
    let a_users = db.list_users(harness.a.account_id).await?;
    let b_users = db.list_users(harness.b.account_id).await?;
    assert_eq!(a_users.len(), 1);
    assert_eq!(b_users.len(), 1);
    assert!(
        a_users[0].id != b_users[0].id,
        "two accounts may both have an `operator`, and they are different users"
    );
    assert_eq!(
        a_users[0].name, b_users[0].name,
        "same name, different rows"
    );
    assert!(
        db.warehouse_by_name(harness.b.account_id, &harness.a.warehouse)
            .await?
            .is_none(),
        "A's warehouse must be invisible from B"
    );
    // B holds a grant naming A's warehouse (the operator mistake above) and it buys nothing.
    assert!(
        db.effective_grants(harness.b.account_id, harness.b.user_id)
            .await?
            .iter()
            .all(|g| g.account_id == harness.b.account_id),
        "B's effective grants must all be B's"
    );

    // ---- B, granted properly in its own account, works exactly as A does -------------------------
    // The control: isolation must not be "B is broken".
    db.grant(
        harness.b.account_id,
        harness.b.role_id,
        Privilege::Select,
        &ObjectRef::table("datafusion", "public", "rows"),
    )
    .await?;
    let ok = submit_query_as(
        &harness.url,
        &query(&harness.b, "rows"),
        Some(&harness.b.token),
    )
    .await
    .context("B's own grant on B's own warehouse must work")?;
    assert_eq!(scalar(&ok.batches)?, 60);

    let a_history = db.list_queries(harness.a.account_id, 32).await?;
    let b_history = db.list_queries(harness.b.account_id, 32).await?;
    assert!(
        a_history
            .iter()
            .all(|q| q.account_id == harness.a.account_id),
        "history must not cross tenants"
    );
    assert!(
        b_history
            .iter()
            .all(|q| q.account_id == harness.b.account_id),
        "history must not cross tenants"
    );
    eprintln!(
        "tenant isolation: account A has {} history rows, account B has {}",
        a_history.len(),
        b_history.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. The worker trust boundary
// ---------------------------------------------------------------------------

/// A worker's Flight port with a fleet secret on it refuses a caller that does not present it, and
/// serves the identical plan to one that does — **and to one that also carries a plan assertion**,
/// because a closed worker requires both.
///
/// Needs no database — the fleet secret is a deployment-level credential, not a per-user one, which
/// is exactly the limitation [`lldb_qe_core::auth::FleetAuth`] documents. What closes the other half
/// of it is `lldb_qe_core::plan_assertion`, and `worker_plan_assertion` is where that is tested;
/// what this file keeps is the *fleet membership* half, so the assertion here is minted and passed
/// simply because the door now has two locks.
///
/// The credential is passed explicitly rather than through `LLDB_FLEET_TOKEN`, because `set_var` is
/// `unsafe` in edition 2024 and would race every other test in this process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_with_a_fleet_secret_serves_only_the_fleet() -> Result<()> {
    let secret = FleetAuth::Required(format!("fleet-{}", support::nanos()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let serving = secret.clone();
    let mut servers = Servers::new();
    servers.spawn(async move {
        flight::serve_worker_with_auth(
            listener,
            SessionContext::new(),
            Arc::new(lldb_qe_core::StageCache::new()),
            serving,
        )
        .await
        .expect("worker serve");
    });
    let url = format!("http://{addr}");

    // A plan any worker can run without touching storage, so the test is about the door and not
    // about what is behind it.
    let ctx = SessionContext::new();
    let plan = ctx
        .sql("SELECT 1 AS one")
        .await?
        .create_physical_plan()
        .await?;
    let plan_bytes = flight::serialize_plan(Arc::clone(&plan))?;
    // A closed worker checks two credentials, so a caller that means to get past the first one has
    // to carry the second. `SELECT 1` reads no storage, so the assertion covers nothing and is
    // satisfied trivially — which keeps this test about the fleet secret, as it was.
    let assertion = lldb_qe_core::plan_assertion::PlanAuth::from_fleet_auth(&secret)
        .mint(
            &lldb_qe_core::plan_assertion::QueryIdentity::default(),
            &plan,
            std::time::SystemTime::now(),
        )?
        .expect("a fleet with a secret mints one");

    // ---- No credential: refused ------------------------------------------------------------------
    // `fetch` uses this process's ambient credential, which is unset, so this is exactly the shape
    // of a stranger connecting to the port.
    let error = flight::fetch(&url, 0, Arc::clone(&plan))
        .await
        .expect_err("a closed worker must refuse an uncredentialed plan");
    let message = format!("{error:#}");
    assert!(message.contains("LLDB_FLEET_TOKEN"), "{message}");
    // Fatal, not retriable: an identical fleet would refuse identically, so a query must not walk
    // every worker hoping one is misconfigured.
    assert_eq!(
        lldb_qe_core::retry::classify(&error),
        lldb_qe_core::Retriability::Fatal,
        "a rejected credential must not be replayed across the fleet: {message}"
    );

    // ---- The wrong credential: refused ------------------------------------------------------------
    let error = flight::fetch_stream_with(
        url.clone(),
        0,
        plan_bytes.clone(),
        &FleetAuth::Required("not-the-secret".to_string()),
        Some(&assertion),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .or_else(|| Some(String::new()))
    .expect("some outcome");
    assert!(
        error.contains("LLDB_FLEET_TOKEN") || error.contains("unauthenticated"),
        "a wrong secret must be refused, got: {error}"
    );

    // ---- The right credential: served -------------------------------------------------------------
    use futures::TryStreamExt;
    let batches: Vec<RecordBatch> =
        flight::fetch_stream_with(url.clone(), 0, plan_bytes, &secret, Some(&assertion))
            .await?
            .try_collect()
            .await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    eprintln!("closed worker served the credentialled caller");

    // ---- And an open worker is open, which is the no-configuration default ------------------------
    assert!(
        FleetAuth::Open.check(None).is_ok(),
        "the no-configuration default must keep `cargo run -p lldb-qe-worker` working"
    );
    assert!(
        matches!(secret.check(None), Err(AuthError::Missing)),
        "a closed worker reports an absent credential as `Missing`, which maps to UNAUTHENTICATED"
    );
    Ok(())
}

/// Under `--allow-anonymous`, a header that is present but unusable must still be refused.
///
/// This is the one case where folding "cannot parse it" into "did not send one" actually costs
/// something. With anonymous access permitted, `None` means *run with nothing checked* — so a
/// mistyped or truncated token would quietly stop being a credential and its caller would be
/// served as anonymous while believing it was authenticated and scoped to its account. It is not
/// an escalation (that caller could have sent no header at all and got the same access); it is
/// the silent loss of an identity someone intended to present, which is harder to notice and
/// harder to debug.
///
/// The catalog here is deliberately empty, so an *accepted* request fails later in planning. That
/// makes the assertion sharp: `UNAUTHENTICATED` means the credential was judged, anything else
/// means it got past identity.
#[tokio::test]
async fn a_malformed_credential_is_refused_even_when_anonymous_is_allowed() -> Result<()> {
    let Some((db, _target)) = db_or_skip("anonymous").await? else {
        return Ok(());
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let coordinator = Arc::new(Coordinator::new(
        SessionContext::new(),
        Some(db.clone()),
        CoordinatorConfig {
            workers: vec!["http://127.0.0.1:1".to_string()],
            allow_anonymous: true,
            coordinator: CoordinatorIdentity::new(format!("anon-test-{addr}")),
            ..CoordinatorConfig::default()
        },
    ));
    assert!(
        !coordinator.requires_authentication(),
        "the flag is the whole point of this test"
    );
    let mut servers = Servers::new();
    servers.spawn(async move {
        serve_coordinator(listener, coordinator, std::future::pending::<()>())
            .await
            .expect("coordinator serve");
    });
    let url = format!("http://{addr}");
    let request = QueryRequest::new("SELECT * FROM rows");

    // Sending nothing is allowed, so it must get *past* identity and fail on the query itself.
    let anonymous = status_of(&url, &request, None).await.code();
    assert_ne!(
        anonymous,
        tonic::Code::Unauthenticated,
        "with --allow-anonymous, a request carrying no credential must not be refused for identity"
    );

    // Sending something unusable must not be quietly downgraded to that.
    for raw in [
        "Bearer",                     // scheme, no token
        "Bearer ",                    // scheme, empty token
        "Basic dXNlcjpwYXNzd29yZA==", // the wrong scheme entirely
        "lldb_looks_like_a_token_but_has_no_scheme",
        "  ",
    ] {
        let code = raw_status_of(&url, &request, raw).await.code();
        assert_eq!(
            code,
            tonic::Code::Unauthenticated,
            "`{raw}` was accepted as anonymous instead of being refused (got {code:?})"
        );
    }
    Ok(())
}
