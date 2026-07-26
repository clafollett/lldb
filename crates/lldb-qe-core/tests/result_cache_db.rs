//! Issue #17's "done when", stated as a test: **an identical query over unchanged tables is
//! answered from the cache without executing, and a write to a referenced table makes the next run
//! recompute the right answer.**
//!
//! Both halves need a real database — the cache lives in the services DB — and a real Iceberg
//! catalog, because the invalidation mechanism *is* the snapshot id a commit moves. So this test
//! builds the genuine article: a SQL catalog on Postgres, a `file://` warehouse, tables created
//! through [`apply_manifest`] exactly as a coordinator creates them, and writes committed through
//! DataFusion `INSERT INTO`.
//!
//! # How "no execution" is asserted
//!
//! The same way the shuffle-cache tests do it: with a counter that only a real execution moves.
//! [`execute_cached`] takes the engine's expensive half as a closure — physical planning, the
//! staging rewrite, fleet dispatch — and the test's closure increments an [`AtomicUsize`] before
//! doing any of it. A hit returns before the closure is ever called, so a flat counter means no
//! plan was built and nothing was dispatched. [`ResultCache::execution_count`] agrees
//! independently.
//!
//! The closure runs the plan locally rather than shipping it to an in-process Flight worker, and
//! that is a limitation of the *engine*, not of the cache: `datafusion-proto` has no encoding for
//! `iceberg-datafusion`'s scan node, so an Iceberg plan cannot cross the Flight boundary in this
//! build at all (`LldbCodec` only teaches it about `FlightReaderExec`). A worker-side
//! `StageCache` counter would therefore be asserting on a path no Iceberg query can take. The
//! counter used here sits at exactly the point the coordinator hands work to the fleet, which is
//! the property the issue is about.
//!
//! # Finding a database — the same three ways as `services_db.rs`
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is (CI's service container, or a local server).
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine`, remove it afterwards.
//! 3. **Neither** — print why and pass. `cargo test --workspace` with no Postgres and no Docker
//!    stays green.
//!
//! Safe to rerun and to run concurrently with itself: the accounts and the Iceberg catalog name
//! are suffixed with a pid + nanoseconds, and every row it creates is deleted at the end. It
//! never drops a table — the database it is handed may be someone's dev instance.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb \
//!     cargo test -p lldb-qe-core --test result_cache_db
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test result_cache_db -- --nocapture

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use lldb_qe_core::lakehouse::Lakehouse;
use lldb_qe_core::manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableSource,
};
use lldb_qe_core::result_cache::{ResultCache, ResultCacheConfig, execute_cached};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::{StorageConfig, apply_manifest, build_session};

/// Same image compose and CI run, so a local pass and a CI pass mean the same thing.
const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// The namespace the test's table lives in.
const NS: &str = "sales";

/// How the test got its database — and, for the container case, what to tear down.
enum Target {
    /// Nothing available; the test prints a skip and passes.
    Skipped,
    /// A URL supplied by the environment. Not ours, so we touch only our own rows.
    Provided(String),
    /// A container we started. `Drop` removes it even if an assertion panicked.
    Container { url: String, name: String },
}

impl Drop for Target {
    fn drop(&mut self) {
        if let Target::Container { name, .. } = self {
            let _ = Command::new("docker").args(["rm", "-f", name]).output();
        }
    }
}

impl Target {
    fn url(&self) -> Option<&str> {
        match self {
            Target::Skipped => None,
            Target::Provided(url) | Target::Container { url, .. } => Some(url),
        }
    }
}

/// Resolve a database to test against, per the three-way rule in the module docs.
fn resolve_target() -> Result<Target> {
    if let Ok(url) = std::env::var("LLDB_TEST_POSTGRES_URL")
        && !url.trim().is_empty()
    {
        return Ok(Target::Provided(url));
    }
    if std::env::var("LLDB_DOCKER").ok().as_deref() != Some("1") {
        return Ok(Target::Skipped);
    }
    start_container()
}

/// Start a throwaway Postgres and wait until it answers.
fn start_container() -> Result<Target> {
    let port = free_port()?;
    let name = format!("lldb-resultcache-test-{}-{}", std::process::id(), nanos());

    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "-e",
            "POSTGRES_USER=lldb",
            "-e",
            "POSTGRES_PASSWORD=lldb",
            "-e",
            "POSTGRES_DB=lldb",
            "-p",
            &format!("127.0.0.1:{port}:5432"),
            POSTGRES_IMAGE,
        ])
        .output()
        .context("spawning `docker run` — is Docker installed?")?;
    if !out.status.success() {
        bail!(
            "failed to start {POSTGRES_IMAGE}:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Hand ownership to the guard *before* the readiness poll, so a timeout still cleans up.
    let target = Target::Container {
        url: format!("postgres://lldb:lldb@127.0.0.1:{port}/lldb?sslmode=disable"),
        name: name.clone(),
    };

    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let probe = Command::new("docker")
            .args(["exec", &name, "pg_isready", "-U", "lldb", "-d", "lldb"])
            .output()
            .context("probing the container with pg_isready")?;
        if probe.status.success() {
            return Ok(target);
        }
        if Instant::now() >= deadline {
            let logs = Command::new("docker").args(["logs", &name]).output();
            bail!(
                "{POSTGRES_IMAGE} did not become ready within {}s; container logs:\n{}",
                READY_TIMEOUT.as_secs(),
                logs.map(|l| String::from_utf8_lossy(&l.stdout).into_owned())
                    .unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// A port nothing is listening on right now.
fn free_port() -> Result<u16> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").context("finding a free host port")?;
    Ok(listener.local_addr()?.port())
}

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_nanos()
}

/// A name no other run — or concurrent copy of this run — will pick.
fn unique(tag: &str) -> String {
    format!("lldb-test-{tag}-{}-{}", std::process::id(), nanos())
}

/// One Iceberg table on a shared SQL catalog, declared with an explicit schema so the test needs
/// no generated data on disk.
fn manifest(catalog_name: &str, url: &str, warehouse: &Path) -> Manifest {
    Manifest {
        catalogs: vec![CatalogDef {
            name: catalog_name.to_string(),
            backend: CatalogBackend::Sql {
                uri: Some(url.to_string()),
            },
            warehouse: Some(format!("file://{}", warehouse.display())),
            namespaces: vec![NamespaceDef {
                name: NS.to_string(),
                tables: vec![TableDef {
                    name: "orders".to_string(),
                    format: Default::default(), // Iceberg
                    source: TableSource::Empty,
                    schema: Some(vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: "int64".to_string(),
                            nullable: false,
                        },
                        ColumnDef {
                            name: "label".to_string(),
                            data_type: "string".to_string(),
                            nullable: true,
                        },
                    ]),
                }],
            }],
        }],
    }
}

/// Run a query through the cache. The closure is the engine's expensive half — in the coordinator
/// it is the fleet dispatch — and `executions` counts every time it actually runs.
async fn run_query(
    ctx: &SessionContext,
    cache: Option<&ResultCache>,
    lakehouses: &[Lakehouse],
    account_id: Option<i64>,
    sql: &str,
    executions: &AtomicUsize,
) -> Result<Vec<RecordBatch>> {
    execute_cached(
        ctx,
        cache,
        lakehouses,
        account_id,
        // No authorization to enforce: this file is about the cache in isolation.
        //
        // The interaction between the two — that the grant check must dominate the cache lookup,
        // so a revoked caller cannot be served a stored result — is **not** covered by any test,
        // here or in `auth_rbac.rs`. It holds by construction (`execute_cached` checks before it
        // looks up, see that function) and by review, which is weaker than a test and should be
        // read as such. Proving it needs a cacheable query, which needs an Iceberg snapshot to
        // version the inputs against; the tables in `auth_rbac.rs` are plain parquet and so
        // nothing there is cacheable at all.
        None,
        sql,
        |logical| async move {
            executions.fetch_add(1, Ordering::SeqCst);
            let plan = ctx
                .execute_logical_plan(logical)
                .await?
                .create_physical_plan()
                .await?;
            Ok(collect(plan, ctx.task_ctx()).await?)
        },
    )
    .await
}

/// Results compared by *value*, not by row count: a cache that returned the right shape and the
/// wrong numbers would be the exact bug this test exists to catch.
fn rendered(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches)
        .expect("formatting a result")
        .to_string()
}

/// Append rows through DataFusion — a real Iceberg commit, which is what moves the snapshot id.
async fn insert_rows(ctx: &SessionContext, catalog: &str, values: &str) -> Result<()> {
    ctx.sql(&format!(
        "INSERT INTO \"{catalog}\".\"{NS}\".\"orders\" VALUES {values}"
    ))
    .await?
    .collect()
    .await
    .with_context(|| format!("inserting into {catalog}.{NS}.orders"))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeat_query_is_served_from_cache_and_a_write_invalidates_it() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise the result cache"
        );
        return Ok(());
    };

    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;

    // Two tenants asking the identical question of the identical table.
    let tenant_a = db.ensure_account(&unique("cache-a")).await?;
    let tenant_b = db.ensure_account(&unique("cache-b")).await?;

    let catalog = format!("lldb_rc_{}_{}", std::process::id(), nanos());
    let warehouse = tempfile::tempdir()?;
    let (ctx, storage) = build_session(StorageConfig::InMemory).await?;
    let lakehouses =
        apply_manifest(&ctx, &storage, &manifest(&catalog, url, warehouse.path())).await?;
    assert_eq!(lakehouses.len(), 1, "one catalog in, one lakehouse out");

    insert_rows(&ctx, &catalog, "(1, 'a'), (2, 'b'), (3, 'c')").await?;
    let snapshot_before = lakehouses[0]
        .current_snapshot_id(NS, "orders")
        .await?
        .expect("the seed insert must produce a snapshot");

    let cache = ResultCache::new(
        db.clone(),
        ResultCacheConfig {
            // A short TTL would make the test time-dependent; the bound under test here is the
            // key, not the clock.
            ttl: Duration::from_secs(3600),
            ..ResultCacheConfig::default()
        },
    );
    // A rerun of this test against the same database must not inherit its own past.
    cache.purge_account(tenant_a.id).await?;
    cache.purge_account(tenant_b.id).await?;

    let executions = AtomicUsize::new(0);
    let sql = format!("SELECT id, label FROM \"{catalog}\".\"{NS}\".\"orders\" ORDER BY id");

    // ---- 1. First run: a miss, so the engine executes -------------------------------------
    let first = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant_a.id),
        &sql,
        &executions,
    )
    .await?;
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(cache.execution_count(), 1);
    assert_eq!(cache.miss_count(), 1);
    assert_eq!(cache.hit_count(), 0);
    assert_eq!(cache.store_count(), 1, "a small result must be cached");

    // ---- 2. The identical query over unchanged tables: no execution at all ------------------
    // Deliberately re-spelled with different whitespace and keyword case. Same question, so the
    // normalizer must reach the same key.
    let respelled =
        format!("select   id,\n  label\nfrom \"{catalog}\".\"{NS}\".\"orders\"\n  order by id");
    let second = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant_a.id),
        &respelled,
        &executions,
    )
    .await?;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "the second run executed; it must have been served from the cache"
    );
    assert_eq!(cache.execution_count(), 1);
    assert_eq!(cache.hit_count(), 1);
    assert_eq!(
        rendered(&second),
        rendered(&first),
        "a cache hit must return the same values, not merely the same shape"
    );

    // ---- 3. A write to a referenced table invalidates it -------------------------------------
    insert_rows(&ctx, &catalog, "(4, 'd'), (5, 'e')").await?;
    let snapshot_after = lakehouses[0]
        .current_snapshot_id(NS, "orders")
        .await?
        .expect("the second insert must produce a snapshot");
    assert_ne!(
        snapshot_before, snapshot_after,
        "a commit must move the snapshot — the whole invalidation story rests on this"
    );

    // Ground truth, computed with the cache entirely out of the picture.
    let truth_executions = AtomicUsize::new(0);
    let truth = run_query(&ctx, None, &lakehouses, None, &sql, &truth_executions).await?;
    assert_eq!(truth_executions.load(Ordering::SeqCst), 1);

    let third = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant_a.id),
        &sql,
        &executions,
    )
    .await?;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the write must have forced a recompute"
    );
    assert_eq!(cache.execution_count(), 2);
    assert_eq!(
        rendered(&third),
        rendered(&truth),
        "the recomputed answer must match a freshly computed one — values, not row counts"
    );
    assert_ne!(
        rendered(&third),
        rendered(&first),
        "the write added rows, so the new answer must differ from the cached one"
    );

    // …and the recomputed answer is itself cached, so a fourth run hits again.
    let fourth = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant_a.id),
        &sql,
        &executions,
    )
    .await?;
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(cache.hit_count(), 2);
    assert_eq!(rendered(&fourth), rendered(&truth));

    // ---- 4. A different tenant sees none of it -----------------------------------------------
    // Same session, same catalog, same SQL, same snapshot: the *only* difference is the account.
    let other_tenant = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant_b.id),
        &sql,
        &executions,
    )
    .await?;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        3,
        "tenant B was served tenant A's cached result — that is a data leak, not a cache hit"
    );
    assert_eq!(cache.execution_count(), 3);
    assert_eq!(rendered(&other_tenant), rendered(&truth));

    // Row counts, which show the invalidation model plainly. Tenant A holds *two* entries: the
    // pre-write one and the post-write one. The stale one is not deleted — it is unreachable,
    // because no key will ever be composed that matches it again. That is stronger than deleting
    // it, since there is no window between the writer committing and a deleter running; the cost
    // is a row that the TTL and the LRU bound eventually reclaim. Tenant B, having asked once,
    // holds exactly one.
    for (tenant, expected) in [(&tenant_a, 2i64), (&tenant_b, 1)] {
        let (n,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM result_cache WHERE account_id = $1")
                .bind(tenant.id)
                .fetch_one(db.pool())
                .await?;
        assert_eq!(n, expected, "entries held by tenant {}", tenant.name);
    }
    // The two tenants' entries are genuinely different rows keyed on different material, not one
    // row shared by both.
    let (shared,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM result_cache a JOIN result_cache b USING (key_material) \
          WHERE a.account_id = $1 AND b.account_id = $2",
    )
    .bind(tenant_a.id)
    .bind(tenant_b.id)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(
        shared, 0,
        "two tenants must never share a cache key, even for byte-identical SQL"
    );

    // ---- 5. Uncacheable shapes fall through, every time --------------------------------------
    // No table reference means nothing to invalidate on, so this must execute on every run.
    let skips_before = cache.skip_count();
    for _ in 0..2 {
        run_query(
            &ctx,
            Some(&cache),
            &lakehouses,
            Some(tenant_a.id),
            "SELECT 1 AS one",
            &executions,
        )
        .await?;
    }
    assert_eq!(
        cache.skip_count(),
        skips_before + 2,
        "a query with no versionable input must be declined, not cached"
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        5,
        "a declined query executes normally, every time"
    );

    // ---- 6. No cache configured behaves exactly as before -------------------------------------
    let unconfigured = AtomicUsize::new(0);
    let plain = run_query(&ctx, None, &lakehouses, None, &sql, &unconfigured).await?;
    assert_eq!(unconfigured.load(Ordering::SeqCst), 1);
    assert_eq!(rendered(&plain), rendered(&truth));

    // ---- Cleanup: this run's rows only ---------------------------------------------------------
    // Deleting the accounts cascades the cache rows away, which is also the assertion that the
    // foreign key is doing its job.
    for tenant in [&tenant_a, &tenant_b] {
        sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(tenant.id)
            .execute(db.pool())
            .await
            .context("deleting a test account")?;
    }
    let (survivors,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM result_cache WHERE account_id IN ($1, $2)")
            .bind(tenant_a.id)
            .bind(tenant_b.id)
            .fetch_one(db.pool())
            .await?;
    assert_eq!(
        survivors, 0,
        "deleting a tenant must cascade to its cached results"
    );

    // The Iceberg catalog's own tables belong to `iceberg-catalog-sql` and may be shared with a
    // real catalog on this database, so only this run's uniquely-named rows are removed.
    for table in ["iceberg_tables", "iceberg_namespace_properties"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE catalog_name = $1"))
            .bind(&catalog)
            .execute(db.pool())
            .await
            .with_context(|| format!("cleaning up {table}"))?;
    }

    db.close().await;
    warehouse.close()?;
    Ok(())
}

/// The two bounds, exercised against a real database: the TTL and the per-tenant LRU cap.
///
/// Neither is a correctness mechanism — snapshots already make a stale hit impossible — so what
/// this proves is the [`StageCache`](lldb_qe_core::StageCache) property restated for a persistent
/// cache: **correctness never depends on retention.** Every entry these bounds throw away is
/// simply recomputed, and the recomputed answer is the right one.
#[tokio::test(flavor = "multi_thread")]
async fn ttl_and_the_per_tenant_bound_evict_without_affecting_answers() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!("SKIP: no Postgres (see LLDB_TEST_POSTGRES_URL / LLDB_DOCKER)");
        return Ok(());
    };

    let db = ServicesDb::connect(url).await?;
    db.migrate().await?;
    let tenant = db.ensure_account(&unique("cache-bounds")).await?;

    let catalog = format!("lldb_rcb_{}_{}", std::process::id(), nanos());
    let warehouse = tempfile::tempdir()?;
    let (ctx, storage) = build_session(StorageConfig::InMemory).await?;
    let lakehouses =
        apply_manifest(&ctx, &storage, &manifest(&catalog, url, warehouse.path())).await?;
    insert_rows(&ctx, &catalog, "(1, 'a'), (2, 'b')").await?;

    let table = format!("\"{catalog}\".\"{NS}\".\"orders\"");
    let queries = [
        format!("SELECT id FROM {table} ORDER BY id"),
        format!("SELECT label FROM {table} ORDER BY label"),
        format!("SELECT count(*) FROM {table}"),
    ];

    // ---- The per-tenant LRU bound ----------------------------------------------------------
    let cache = ResultCache::new(
        db.clone(),
        ResultCacheConfig {
            max_entries_per_account: 2,
            ..ResultCacheConfig::default()
        },
    );
    cache.purge_account(tenant.id).await?;
    let executions = AtomicUsize::new(0);

    let mut expected = Vec::new();
    for sql in &queries {
        expected.push(rendered(
            &run_query(
                &ctx,
                Some(&cache),
                &lakehouses,
                Some(tenant.id),
                sql,
                &executions,
            )
            .await?,
        ));
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        3,
        "three distinct queries"
    );

    let (held,): (i64,) = sqlx::query_as("SELECT count(*) FROM result_cache WHERE account_id = $1")
        .bind(tenant.id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(held, 2, "the per-tenant cap is 2 entries");

    // The least-recently-used entry (the first query) was evicted, so it recomputes — and the
    // answer is identical to the one that was thrown away.
    let again = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant.id),
        &queries[0],
        &executions,
    )
    .await?;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        4,
        "the evicted entry must be recomputed"
    );
    assert_eq!(
        rendered(&again),
        expected[0],
        "eviction cannot change an answer"
    );

    // The most recently used entry survived and still hits.
    let survivor = run_query(
        &ctx,
        Some(&cache),
        &lakehouses,
        Some(tenant.id),
        &queries[2],
        &executions,
    )
    .await?;
    assert_eq!(
        executions.load(Ordering::SeqCst),
        4,
        "a recently-used entry must not have been evicted"
    );
    assert_eq!(rendered(&survivor), expected[2]);

    // ---- The TTL ---------------------------------------------------------------------------
    // Zero seconds makes `expires_at` equal to the insert's `now()`, and a lookup requires
    // `expires_at > now()` — so the entry is unreachable the moment it is written. Deterministic,
    // with no sleeping.
    let expiring = ResultCache::new(
        db.clone(),
        ResultCacheConfig {
            ttl: Duration::from_secs(0),
            ..ResultCacheConfig::default()
        },
    );
    expiring.purge_account(tenant.id).await?;
    let ttl_executions = AtomicUsize::new(0);
    for _ in 0..2 {
        let batches = run_query(
            &ctx,
            Some(&expiring),
            &lakehouses,
            Some(tenant.id),
            &queries[0],
            &ttl_executions,
        )
        .await?;
        assert_eq!(rendered(&batches), expected[0]);
    }
    assert_eq!(
        ttl_executions.load(Ordering::SeqCst),
        2,
        "an expired entry must never be served"
    );
    assert_eq!(expiring.hit_count(), 0);

    // ---- Cleanup ----------------------------------------------------------------------------
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(tenant.id)
        .execute(db.pool())
        .await?;
    for t in ["iceberg_tables", "iceberg_namespace_properties"] {
        sqlx::query(&format!("DELETE FROM {t} WHERE catalog_name = $1"))
            .bind(&catalog)
            .execute(db.pool())
            .await?;
    }
    db.close().await;
    warehouse.close()?;
    Ok(())
}
