//! Issue #42, stated as a test: **the grant check dominates the result-cache lookup.**
//!
//! [`execute_cached`](lldb_qe_core::result_cache::execute_cached) plans, then authorizes, then
//! looks the query up in the cache. That order is a security property, not a stylistic one: a
//! cached result is still that tenant's data, so a caller whose grant was revoked yesterday must be
//! refused rather than served a row they were once entitled to compute.
//!
//! Nothing asserted it. The ordering held *only* as adjacency inside one function body — everything
//! the lookup needs is in scope the instant the logical plan exists, so the whole cache block can be
//! moved above the authorization block and still compile cleanly. And there is a plausible reason
//! someone would: the grant check walks the entire logical plan, so "only pay for authorization when
//! we actually execute" reads like a sensible optimization to anyone who has not read why it is not.
//! This file is what turns that from a review comment into a failing build.
//!
//! # The leak is intra-tenant, and the test has to be shaped for that
//!
//! `ResultCacheKey` holds `account_id`, the build version, the default catalog/schema, the
//! normalized statement and every input's snapshot id — **no user, no role, no grant set**. So a
//! reordering does *not* open a cross-tenant hole: another account composes a different key and is
//! already refused by the key itself, which `result_cache_db.rs` asserts separately. What it opens
//! is the same account, after a revoke, being handed the answer it cached while it was still
//! entitled. That is why every run below is the **same** principal and the **same** account, and
//! why "strengthening" this into a two-tenant test would make it pass for the wrong reason.
//!
//! # The four things that have to be true at once
//!
//! 1. The query is genuinely **cacheable**, which needs an input the cache can version — an Iceberg
//!    snapshot. Plain parquet listing tables have none, which is why this could not have been added
//!    to `auth_rbac.rs`.
//! 2. There really is **a stored result to leak**: run 2 is asserted to be a cache *hit* before
//!    anything is revoked.
//! 3. Revocation leaves the table and its snapshot **untouched**, so the key is unchanged and the
//!    entry stays reachable. The cache is never invalidated by design — a commit would move the
//!    snapshot and compose a *different* key, and a test that accidentally did that would prove
//!    nothing. Two assertions pin this down: the row is still in `result_cache` while the caller is
//!    being refused, and re-granting turns the very next run back into a hit.
//! 4. The refusal is a refusal — `PERMISSION_DENIED`, not an internal error — and **no rows come
//!    back**.
//!
//! # It runs on the fleet, not on the coordinator
//!
//! Since `resolve_iceberg_scans` landed (#28) an Iceberg scan is rewritten into a plain parquet
//! scan over the snapshot's files before the plan is staged, so an Iceberg query can cross the
//! Flight boundary at all. The workers here are real in-process Flight servers holding a
//! [`StageCache`] each, and run 1 is asserted to have materialized a stage on one of them.
//!
//! [`StageCache::rows_served`] is what makes the later runs falsifiable on a *shared* fleet. A
//! worker's stage cache is content-addressed, so a repeat query would leave `execution_count` flat
//! whether the result cache answered it or the worker's own buffer did — but a stage-cache hit still
//! streams its rows back, so `rows_served` moves. A result-cache hit ships nothing in either
//! direction. Flat rows-served therefore means the query never left the coordinator, which is a
//! claim a shared fleet can still make honestly.
//!
//! The database is found the same three ways as everywhere else (see [`crate::support`]): an
//! explicit `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a
//! clean skip.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb \
//!     cargo test -p lldb-qe-core --test integration cache_grant_ordering

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use lldb_qe_core::engine::{TenantSession, TenantSessions};
use lldb_qe_core::liveness::CoordinatorIdentity;
use lldb_qe_core::rbac::{ObjectRef, Privilege};
use lldb_qe_core::result_cache::{ResultCache, ResultCacheConfig};
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, serve_coordinator, submit_query_as,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::tenancy::TenantScope;
use lldb_qe_core::{
    DEFAULT_WAREHOUSE_ENDPOINT, StageCache, StorageConfig, apply_manifest, build_session,
    serve_worker_with_cache,
};

use crate::auth_rbac::{Tenant, cloud_map, db_or_skip, provision, status_of};
use crate::result_cache_db::{NS, insert_rows, manifest};
use crate::support::unique_name;

/// Workers behind the tenant's warehouse. More than one so "the fleet" is not a euphemism for "a
/// worker"; the warehouse row is resized to match so the size/fleet mismatch warning stays quiet.
const WORKERS: usize = 2;

/// In-process Flight workers, each holding a [`StageCache`] this test can read.
///
/// One fleet for the whole test, unlike `result_cache_db.rs`'s fleet-per-query: a [`Coordinator`]
/// takes its worker list once, at construction. See the module docs for why [`StageCache::
/// rows_served`] is still a falsifiable signal under that arrangement.
struct Fleet {
    addrs: Vec<SocketAddr>,
    caches: Vec<Arc<StageCache>>,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

impl Fleet {
    async fn start() -> Result<Self> {
        let mut addrs = Vec::with_capacity(WORKERS);
        let mut caches = Vec::with_capacity(WORKERS);
        let mut servers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            addrs.push(listener.local_addr()?);
            let cache = Arc::new(StageCache::new());
            let served = Arc::clone(&cache);
            servers.push(tokio::spawn(async move {
                // A bare `SessionContext`: no catalog, no credential, no warehouse path. A worker
                // reads only what the plan names, which for a resolved Iceberg scan is the data
                // files the coordinator pinned into it.
                serve_worker_with_cache(listener, SessionContext::new(), served)
                    .await
                    .expect("worker serve");
            }));
            caches.push(cache);
        }
        Ok(Self {
            addrs,
            caches,
            servers,
        })
    }

    fn urls(&self) -> Vec<String> {
        self.addrs
            .iter()
            .map(|addr| format!("http://{addr}"))
            .collect()
    }

    /// Stage materializations across the fleet.
    fn executions(&self) -> usize {
        self.caches.iter().map(|c| c.execution_count()).sum()
    }

    /// Rows the fleet has streamed back to the coordinator, ever.
    fn rows_served(&self) -> usize {
        self.caches.iter().map(|c| c.rows_served()).sum()
    }
}

/// Everything the test drives, wired the way a deployment is: a real Flight front door with
/// authentication on, a real fleet behind it, and a real result cache in Postgres.
struct Harness {
    db: ServicesDb,
    url: String,
    tenant: Tenant,
    /// The catalog name **SQL** uses — the manifest's declared name. This is what the query below
    /// spells and what the grant is written against; under [`TenantScope::account`] it is the same
    /// word for every tenant.
    catalog: String,
    /// The catalog name **storage** uses — `acct_<id>__<declared>`, the `iceberg_tables` row
    /// discriminator. Only cleanup wants this one; asking for it by any other name would delete
    /// nothing and leave this run's rows behind.
    iceberg_catalog: String,
    coordinator: Arc<Coordinator>,
    /// The coordinator's Flight server, retained for the same reason [`Fleet`] retains its
    /// workers': dropping a `JoinHandle` detaches the task rather than stopping it, so a coordinator
    /// spawned with a `pending()` shutdown would hold its listening socket for the rest of the
    /// process. Since #44 that process is the whole `integration` binary — one test's leaked
    /// listener now outlives its test and accumulates across every test that follows.
    server: tokio::task::JoinHandle<()>,
    fleet: Fleet,
    _warehouse: tempfile::TempDir,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    /// The cache the coordinator is actually using — read for its counters, which is where the
    /// hit/miss claims below are settled.
    fn cache(&self) -> &ResultCache {
        self.coordinator
            .result_cache()
            .expect("the coordinator was built with a result cache")
    }

    fn request(&self, sql: &str) -> QueryRequest {
        QueryRequest::new(sql.to_string()).on_warehouse(self.tenant.warehouse.clone())
    }

    /// Delete this run's rows: the account cascades to users, keys, roles, grants, warehouses,
    /// history and cached results, and the Iceberg catalog's own tables are cleaned by name because
    /// they may be shared with a real catalog on this database.
    async fn cleanup(&self) -> Result<()> {
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(self.tenant.account_id)
            .execute(self.db.pool())
            .await
            .context("deleting the test account")?;
        for table in ["iceberg_tables", "iceberg_namespace_properties"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE catalog_name = $1"))
                .bind(&self.iceberg_catalog)
                .execute(self.db.pool())
                .await
                .with_context(|| format!("cleaning up {table}"))?;
        }
        Ok(())
    }
}

/// Values, not row counts: a cache that returned the right shape and the wrong numbers is exactly
/// the bug worth catching.
fn rendered(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches)
        .expect("formatting a result")
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_grant_refuses_a_query_whose_answer_is_already_cached() -> Result<()> {
    let Some((db, target)) = db_or_skip("the cache/grant ordering").await? else {
        return Ok(());
    };
    // The catalog manifest needs the URL as a string of its own: a `sql` catalog opens its own
    // pool, separate from the services DB's.
    let url = target
        .url()
        .expect("db_or_skip only returns a connected database")
        .to_string();
    let harness = start(db, &url).await?;
    let result = ordering_body(&harness).await;
    harness.cleanup().await?;
    result
}

async fn start(db: ServicesDb, url: &str) -> Result<Harness> {
    // An account, a user, a role, an API key and a warehouse with `USAGE` — and deliberately no
    // privilege on any table, so "denied by default" is the starting state.
    let tenant = provision(&db, "cache-order").await?;
    db.resize_warehouse(tenant.account_id, &tenant.warehouse, WORKERS as i32)
        .await?;

    let fleet = Fleet::start().await?;
    let resolver = cloud_map(
        vec![format!("{}.lldb.local:50051", tenant.warehouse)],
        fleet.addrs.clone(),
    );

    // `Local`, not `InMemory`: a resolved Iceberg plan names its data files by path, so every
    // worker has to be able to read the warehouse's object store.
    let warehouse = tempfile::tempdir()?;
    let (ctx, storage) =
        build_session(StorageConfig::Local(warehouse.path().to_path_buf())).await?;
    let catalog = unique_name("cgo").replace('-', "_");
    // The tenant's own scope, not `untenanted()`: this account exists, so this is the shape a
    // multi-tenant front door materializes. It also makes the test able to tell the two catalog
    // names apart — `catalog_name()` stays the declared word the SQL and the grant below spell,
    // while `iceberg_catalog_name()` becomes `acct_<id>__<declared>`. Were `result_cache.rs` to
    // version its inputs against the storage-facing name, the lookup would find nothing and step 2
    // would fail on a miss rather than pass quietly. Under `untenanted()` the two names are the
    // same string and nothing here could distinguish them.
    let scope = TenantScope::account(tenant.account_id);
    let lakehouses = apply_manifest(
        &ctx,
        &storage,
        &manifest(&catalog, url, warehouse.path()),
        &scope,
    )
    .await?;
    assert_eq!(
        lakehouses.len(),
        1,
        "one catalog in, one lakehouse out — and the cache versions its key against this handle, \
         so an empty list here would make the query uncacheable and every hit assertion below \
         vacuous"
    );

    // One commit, so the table has a snapshot to version the cache key against. Written on the
    // coordinator's own context, which is where a write belongs.
    insert_rows(&ctx, &catalog, "(1, 'a'), (2, 'b'), (3, 'c')").await?;

    let cache = ResultCache::new(
        db.clone(),
        ResultCacheConfig {
            // Long enough that nothing here is time-dependent: the bound under test is the grant,
            // not the clock.
            ttl: Duration::from_secs(3600),
            ..ResultCacheConfig::default()
        },
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // `multi_tenant` + `TenantSessions::fixed`, not `Coordinator::new`: the single-tenant
    // constructor wraps the context in a `TenantSession` with **no lakehouses**, and the result
    // cache versions its key against the lakehouses of the session a query plans in. Built that
    // way this coordinator would be a coordinator whose queries are never cacheable, and every
    // hit/miss claim below would be a claim about nothing.
    let coordinator = Arc::new(
        Coordinator::multi_tenant(
            TenantSessions::fixed(TenantSession::new(ctx, lakehouses)),
            Some(db.clone()),
            CoordinatorConfig {
                // A tenant that does not exist, so any path still falling back to the configured
                // default instead of the credential's account fails loudly rather than by luck.
                default_account: "nobody".to_string(),
                workers: fleet.urls(),
                warehouse_endpoint: vec![DEFAULT_WAREHOUSE_ENDPOINT.to_string()],
                max_concurrent_queries: Some(2),
                max_queued_queries: 32,
                coordinator: CoordinatorIdentity::new(format!("cache-order-{addr}")),
                allow_anonymous: false,
            },
        )
        .with_resolver(resolver)
        .with_result_cache(cache),
    );
    assert!(
        coordinator.requires_authentication(),
        "a coordinator with a services database must require a credential"
    );

    let served = Arc::clone(&coordinator);
    let server = tokio::spawn(async move {
        serve_coordinator(listener, served, std::future::pending::<()>())
            .await
            .expect("coordinator serve");
    });

    Ok(Harness {
        db,
        url: format!("http://{addr}"),
        tenant,
        iceberg_catalog: scope.iceberg_catalog_name(&catalog),
        catalog,
        coordinator,
        server,
        fleet,
        _warehouse: warehouse,
    })
}

async fn ordering_body(h: &Harness) -> Result<()> {
    let table = ObjectRef::table(&h.catalog, NS, "orders");
    let sql = format!(
        "SELECT id, label FROM \"{}\".\"{NS}\".\"orders\" ORDER BY id",
        h.catalog
    );
    let request = h.request(&sql);
    let cache = h.cache();

    // The one grant this test gives and takes away. Nothing broader is ever granted, which matters:
    // revocation is exact rather than covering, so a stray `SELECT ON NAMESPACE` left lying around
    // would keep reaching the table and step 5 would never deny.
    h.db.grant(
        h.tenant.account_id,
        h.tenant.role_id,
        Privilege::Select,
        &table,
    )
    .await?;

    // ---- 1. A miss, executed on the fleet -----------------------------------------------------
    let first = submit_query_as(&h.url, &request, Some(&h.tenant.token))
        .await
        .context("an authenticated, granted query must run")?;
    assert_eq!(cache.miss_count(), 1);
    assert_eq!(cache.hit_count(), 0);
    assert_eq!(cache.execution_count(), 1);
    assert_eq!(cache.store_count(), 1, "a small result must be cached");
    // The query was *keyed*, not merely not-found: a query the cache declines to version at all
    // counts as a skip and never as a miss, and the one way this cache stops working without
    // anything failing is a catalog whose lakehouse handle is absent or another tenant's. Both at
    // zero is what makes every hit assertion below a claim about something.
    assert_eq!(
        cache.skip_count(),
        0,
        "the query must be genuinely cacheable — a skip here would make the rest of this file \
         assert nothing"
    );
    assert_eq!(
        cache.catalog_mismatch_count(),
        0,
        "the session's lakehouse handles must be the ones this query's catalog resolves through"
    );
    assert!(
        h.fleet.executions() > 0,
        "the query must have been dispatched to a worker — if it never left the coordinator, \
         everything below compares a cache against nothing"
    );
    let rows_after_first = h.fleet.rows_served();
    assert!(
        rows_after_first > 0,
        "and the rows came back off that worker"
    );
    assert_eq!(
        rendered(&first.batches),
        "+----+-------+\n\
         | id | label |\n\
         +----+-------+\n\
         | 1  | a     |\n\
         | 2  | b     |\n\
         | 3  | c     |\n\
         +----+-------+",
        "the seeded rows, by value"
    );

    // ---- 2. A hit, so there is a stored result to leak -----------------------------------------
    let second = submit_query_as(&h.url, &request, Some(&h.tenant.token))
        .await
        .context("the repeat query must be served")?;
    assert_eq!(cache.hit_count(), 1, "the second run must be a cache hit");
    assert_eq!(
        cache.execution_count(),
        1,
        "a hit must not have been handed to the engine"
    );
    assert_eq!(
        h.fleet.rows_served(),
        rows_after_first,
        "a result-cache hit ships nothing; had it missed, the worker would have streamed the rows \
         again — out of its own stage cache, but still across the wire"
    );
    assert_eq!(rendered(&second.batches), rendered(&first.batches));

    let (held,): (i64,) = sqlx::query_as("SELECT count(*) FROM result_cache WHERE account_id = $1")
        .bind(h.tenant.account_id)
        .fetch_one(h.db.pool())
        .await?;
    assert_eq!(held, 1, "exactly one entry, and it is this query's answer");

    // ---- 3. Revoke, touching nothing else ------------------------------------------------------
    // No write, no commit, no new snapshot: the cache key the next run composes is byte-identical
    // to the one that just hit, so the entry stays reachable. That is the whole point — a test that
    // let the entry expire or the key change would pass without proving anything.
    assert!(
        h.db.revoke(h.tenant.role_id, Privilege::Select, &table)
            .await?,
        "the grant must actually have been removed"
    );

    // ---- 4. The refusal ------------------------------------------------------------------------
    let error = submit_query_as(&h.url, &request, Some(&h.tenant.token))
        .await
        .expect_err(
            "a revoked caller must be refused — being served the cached answer here is the leak \
             this file exists to prevent",
        );
    let message = format!("{error:#}");
    assert!(
        message.contains(&format!("SELECT on table {}.{NS}.orders", h.catalog)),
        "the denial must name the privilege and the object: {message}"
    );
    eprintln!("denied after the revoke, with the answer still cached: {message}");

    // The code, not just the text: a refusal has to reach the client as "ask for a grant" rather
    // than "file a bug", and this is raised between planning and the lookup, deep inside execution.
    let status = status_of(&h.url, &request, Some(&h.tenant.token)).await;
    assert_eq!(
        status.code(),
        tonic::Code::PermissionDenied,
        "a revoked grant must not be reported as an internal error: {status:?}"
    );

    // No rows came back — neither submission produced a `SubmittedQuery` at all — and the counters
    // say why: the lookup was never performed. This is the ordering, asserted. Were the cache block
    // moved above the authorization check, `hit_count` would be 3 here and both calls above would
    // have returned the rows instead of an error.
    assert_eq!(
        cache.hit_count(),
        1,
        "the cache must not have been consulted for a caller who is about to be refused"
    );
    assert_eq!(
        cache.execution_count(),
        1,
        "and nothing was executed for them either"
    );
    assert_eq!(
        h.fleet.rows_served(),
        rows_after_first,
        "a denied query moves no rows, from the fleet or from the cache"
    );

    // ---- 5. The entry was live the whole time --------------------------------------------------
    // Proof that step 4 refused rather than merely missed: the row is still there, and restoring the
    // grant turns the very next run — same SQL, same key, same session — straight back into a hit.
    let (still_held,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM result_cache WHERE account_id = $1")
            .bind(h.tenant.account_id)
            .fetch_one(h.db.pool())
            .await?;
    assert_eq!(
        still_held, 1,
        "the revoke must not have disturbed the cached entry"
    );

    h.db.grant(
        h.tenant.account_id,
        h.tenant.role_id,
        Privilege::Select,
        &table,
    )
    .await?;
    let restored = submit_query_as(&h.url, &request, Some(&h.tenant.token))
        .await
        .context("re-granting must re-enable exactly this query")?;
    assert_eq!(
        cache.hit_count(),
        2,
        "the entry the revoked caller was refused was reachable all along"
    );
    assert_eq!(cache.execution_count(), 1, "still nothing re-executed");
    assert_eq!(h.fleet.rows_served(), rows_after_first);
    assert_eq!(rendered(&restored.batches), rendered(&first.batches));

    Ok(())
}
