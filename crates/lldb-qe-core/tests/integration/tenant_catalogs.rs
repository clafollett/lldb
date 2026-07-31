//! **A catalog per tenant**: two accounts, one deployment, one manifest, and the *same qualified
//! table name* — kept apart by structure rather than by a check.
//!
//! Everything the control plane owns was already `account_id`-scoped in the schema. The Iceberg
//! catalog was not: `iceberg_tables` is created and owned by `iceberg-catalog-sql`, carries no
//! account column, and every tenant shared one catalog namespace. So isolation over table *data*
//! rested on the plan-time grant check in `rbac.rs` and on nothing else, and an operator who
//! granted account B a catalog-wide privilege had really given it account A's tables.
//!
//! This file settles the three things issue #35 asks for, as facts:
//!
//! 1. **Two accounts can each own `lldb.sales.orders` without collision** — different rows, and
//!    different rows on disk, from one manifest.
//! 2. **A catalog-wide grant to one account cannot reach another account's tables.** Both tenants
//!    hold `SELECT ON CATALOG lldb`, which is the widest grant expressible over table data, and it
//!    still reaches only their own. The point is what it reaches, not what it is refused: a test
//!    that asserted `PERMISSION_DENIED` would be re-testing `rbac.rs`, which is the mechanism this
//!    issue exists to stop relying on.
//! 3. **The catalog names really are different rows in `iceberg_tables`** — asserted against the
//!    live table, because "we generate a different string" is not the same claim as "the storage
//!    partitions on it".
//!
//! Plus the failure mode the design introduces and has to be watched: the result cache versions a
//! query's inputs by looking up a `Lakehouse` for each catalog the plan named. Under per-tenant
//! catalogs there are now *two* catalog names — the one SQL says and the one storage says — and
//! matching on the wrong one does not fail, it silently stops caching forever. So the cache is on
//! here, hits are asserted per tenant, and `catalog_mismatch_count()` is asserted to be zero.
//!
//! # What is real and what is faked
//!
//! Real: the Flight server and client, the API keys, the grants, two real accounts, a real Postgres
//! catalog shared by both tenants, real Iceberg commits, real distributed execution across
//! in-process workers, and the result cache. Nothing here is faked.
//!
//! # What this does **not** prove
//!
//! Layout, not access. Since resolved Iceberg scans name data files by absolute path, a worker
//! reads warehouse files with its own credentials and cannot tell whose they are — so a worker
//! handed a plan naming tenant B's files will read them. Per-tenant warehouse roots make that
//! *harder to do by accident*; they do not make it impossible on purpose. Per-request identity at
//! the worker boundary is separate work. See `lldb_qe_core::tenancy`.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration tenant_catalogs
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration tenant_catalogs -- --nocapture

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::support::gates;
use crate::support::{self, Cleanup, DbCleanup, Servers, resolve_target, unique_name};
use anyhow::{Context, Result};
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use lldb_qe_core::engine::TenantSessions;
use lldb_qe_core::liveness::CoordinatorIdentity;
use lldb_qe_core::rbac::{ObjectRef, ObjectType, Privilege};
use lldb_qe_core::result_cache::{ResultCache, ResultCacheConfig};
use lldb_qe_core::server::{
    Coordinator, CoordinatorConfig, QueryRequest, serve_coordinator, submit_query_as,
};
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::tenancy::TenantScope;
use lldb_qe_core::{
    CatalogSource, StorageConfig, apply_manifest, build_session, flight, manifest::Manifest,
};

/// How this suite names itself in the skip report.
const SUITE: &str = "tenant_catalogs";

/// The catalog every tenant's manifest declares. Both accounts materialize *this* name, which is
/// the whole point: a query is portable between tenants, the rows behind it are not.
const CATALOG: &str = "lldb";
const NAMESPACE: &str = "sales";
const TABLE: &str = "orders";

/// How many rows each tenant seeds. Different on purpose — a test where both answers were the same
/// number could not tell "isolated" from "sharing one table".
const ROWS_A: i64 = 3;
const ROWS_B: i64 = 7;

/// Skip-or-connect, matching `auth_rbac.rs`.
async fn db_or_skip() -> Result<Option<(ServicesDb, support::Target)>> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        gates::skip(SUITE, &gates::SERVICES_DB);
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    db.migrate().await.context("applying migrations")?;
    Ok(Some((db, target)))
}

/// The manifest both tenants materialize, written to disk because that is what a coordinator is
/// configured with.
///
/// One `sql` catalog, one namespace, one table with an explicit schema (so the test needs no
/// generated parquet on disk). The declared warehouse is a single root — deliberately, since a
/// shared root is precisely the arrangement in which two tenants' `sales/orders` directories would
/// collide if only the catalog name were scoped.
fn write_manifest(dir: &Path, url: &str, warehouse: &Path) -> Result<std::path::PathBuf> {
    let toml = format!(
        r#"
[[catalogs]]
name = "{CATALOG}"
warehouse = "file://{warehouse}"
backend = {{ kind = "sql", uri = "{url}" }}

[[catalogs.namespaces]]
name = "{NAMESPACE}"

[[catalogs.namespaces.tables]]
name = "{TABLE}"
source = {{ type = "empty" }}
schema = [
    {{ name = "id", data_type = "int64", nullable = false }},
]
"#,
        warehouse = warehouse.display(),
    );
    // Parsed before it is written: a malformed manifest here would surface much later, as a
    // session that would not build, and the error would name the coordinator rather than this file.
    Manifest::from_toml_str(&toml).context("the test's own manifest must be valid")?;
    let path = dir.join("manifest.toml");
    std::fs::write(&path, toml)?;
    Ok(path)
}

/// A tenant: an account, a user, a role holding a **catalog-wide** `SELECT`, and an API key.
struct Tenant {
    account_id: i64,
    account_name: String,
    token: String,
}

/// Provision a tenant and grant it `SELECT ON CATALOG lldb`.
///
/// The grant is deliberately the widest one that exists over table data. Under a shared catalog it
/// would have reached every tenant's tables — that is the sentence issue #35 opens with — so
/// granting it to *both* accounts and then showing each one still sees only its own rows is the
/// assertion that the boundary moved from the check to the structure.
async fn provision(db: &ServicesDb, tag: &str) -> Result<Tenant> {
    let account = db.create_account(&unique_name(tag)).await?;
    let user = db.create_user(account.id, "operator").await?;
    let role = db.create_role(account.id, "analyst").await?;
    db.assign_role(account.id, user.id, role.id).await?;
    db.grant(
        account.id,
        role.id,
        Privilege::Select,
        &ObjectRef::new(ObjectType::Catalog, CATALOG.to_string())?,
    )
    .await?;
    let (_key, token) = db.create_api_key(account.id, user.id, "cli", None).await?;
    Ok(Tenant {
        account_id: account.id,
        account_name: account.name,
        token: token.into_secret(),
    })
}

/// Seed one tenant's table, through the same `apply_manifest` path a coordinator uses.
///
/// Done out of band rather than through the server because the subject here is *reads* crossing a
/// tenant boundary; writing through the front door would drag `INSERT` privileges into a test that
/// is not about them. It exercises the real scoping either way: `TenantScope::account` is what the
/// coordinator's session builder passes.
async fn seed(storage_root: &Path, manifest: &Manifest, account_id: i64, rows: i64) -> Result<()> {
    let (ctx, storage) = build_session(StorageConfig::Local(storage_root.to_path_buf())).await?;
    apply_manifest(&ctx, &storage, manifest, &TenantScope::account(account_id)).await?;
    let values = (1..=rows)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "INSERT INTO {CATALOG}.{NAMESPACE}.{TABLE} VALUES {values}"
    ))
    .await?
    .collect()
    .await
    .with_context(|| format!("seeding {rows} rows for account {account_id}"))?;
    Ok(())
}

/// In-process workers with bare sessions — a worker can only read what the plan names.
///
/// The handles go into the caller's [`Servers`] rather than being dropped: a dropped `JoinHandle`
/// detaches the task instead of stopping it, and since #44 this is one binary, so a detached worker
/// holds its port for the rest of the run.
async fn start_workers(servers: &mut Servers, count: usize) -> Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        addrs.push(listener.local_addr()?);
        servers.spawn(async move {
            flight::serve_worker(listener, SessionContext::new())
                .await
                .expect("worker serve");
        });
    }
    Ok(addrs)
}

/// `SELECT count(*)` as one tenant, through the real Flight front door.
async fn count_as(server: &str, tenant: &Tenant) -> Result<i64> {
    let request = QueryRequest::new(format!(
        "SELECT count(*) AS n FROM {CATALOG}.{NAMESPACE}.{TABLE}"
    ));
    let answer = submit_query_as(server, &request, Some(&tenant.token))
        .await
        .with_context(|| format!("querying as {}", tenant.account_name))?;
    let batch = answer
        .batches
        .first()
        .context("count(*) always returns a row")?;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("count(*) is an i64")?
        .value(0))
}

#[tokio::test]
async fn two_accounts_own_the_same_table_name_and_a_catalog_wide_grant_stays_inside_one()
-> Result<()> {
    let Some((db, _target)) = db_or_skip().await? else {
        return Ok(());
    };
    let url = _target.url().expect("a target with a url").to_string();
    let tmp = tempfile::tempdir()?;
    let warehouse = tmp.path().join("wh");
    std::fs::create_dir_all(&warehouse)?;
    let manifest_path = write_manifest(tmp.path(), &url, &warehouse)?;
    let manifest = Manifest::from_path(&manifest_path)?;

    let a = provision(&db, "cat-a").await?;
    let b = provision(&db, "cat-b").await?;

    // Everything below runs whether or not an assertion panics, so a failure does not leave rows in
    // someone's dev database. Registered *here*, before the first thing that can fail — the old
    // shape ran these deletes after the body and so only on the success path. See
    // `support::DbCleanup`.
    let mut cleanup = DbCleanup::new(&url);
    for tenant in [&a, &b] {
        cleanup.account(tenant.account_id);
        cleanup.add(Cleanup::IcebergCatalog(
            TenantScope::account(tenant.account_id).iceberg_catalog_name(CATALOG),
        ));
    }
    // A `Servers` per test, outside the block below so a `?` inside it still stops the workers.
    let mut servers = Servers::new();

    async {
        seed(tmp.path(), &manifest, a.account_id, ROWS_A).await?;
        seed(tmp.path(), &manifest, b.account_id, ROWS_B).await?;

        // --- 1. Two rows in `iceberg_tables`, same table name, different catalogs. ------------
        //
        // Asserted against the live table rather than against our own naming function: the claim
        // is that the *storage* partitions on this column, which is a fact about
        // `iceberg-catalog-sql`'s schema and not about the string we composed.
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT catalog_name, metadata_location FROM iceberg_tables \
             WHERE table_namespace = $1 AND table_name = $2 AND catalog_name = ANY($3) \
             ORDER BY catalog_name",
        )
        .bind(NAMESPACE)
        .bind(TABLE)
        .bind(vec![
            TenantScope::account(a.account_id).iceberg_catalog_name(CATALOG),
            TenantScope::account(b.account_id).iceberg_catalog_name(CATALOG),
        ])
        .fetch_all(db.pool())
        .await
        .context("reading iceberg_tables")?;
        assert_eq!(
            rows.len(),
            2,
            "one row per tenant for the same qualified table name: {rows:?}"
        );
        assert_ne!(rows[0].0, rows[1].0, "the catalog names must differ");
        // …and the *files* must differ too. This is the half that a catalog-name-only change would
        // get wrong: `iceberg-catalog-sql` builds a location as `{warehouse}/{namespace}/{table}`
        // with no catalog name in it, so two tenanted catalogs over one warehouse root would be
        // separated in Postgres and pointed at the same directory.
        assert_ne!(
            rows[0].1, rows[1].1,
            "per-tenant catalogs without per-tenant warehouse roots collide on disk: {rows:?}"
        );
        for (catalog, location) in &rows {
            assert!(
                location.contains(catalog.trim_end_matches(&format!("__{CATALOG}"))),
                "a tenant's table must live under its own warehouse root: {location}"
            );
        }

        // --- 2. The front door: one server, two tenants, one manifest. -------------------------
        let workers = start_workers(&mut servers, 2).await?;
        let sessions = TenantSessions::per_account(
            StorageConfig::Local(tmp.path().to_path_buf()),
            CatalogSource::Manifest(manifest_path.clone()),
        );
        let cache = ResultCache::new(db.clone(), ResultCacheConfig::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let coordinator = Arc::new(
            Coordinator::multi_tenant(
                sessions,
                Some(db.clone()),
                CoordinatorConfig {
                    // A tenant that does not exist: any path still falling back to the configured
                    // default instead of the credential's account fails loudly rather than by luck.
                    default_account: "nobody".to_string(),
                    workers: workers.iter().map(|a| format!("http://{a}")).collect(),
                    coordinator: CoordinatorIdentity::new(format!("tenant-catalogs-{addr}")),
                    ..CoordinatorConfig::default()
                },
            )
            .with_result_cache(cache),
        );
        let served = Arc::clone(&coordinator);
        servers.spawn(async move {
            serve_coordinator(listener, served, std::future::pending::<()>())
                .await
                .expect("coordinator serve");
        });
        let url = format!("http://{addr}");

        // The same SQL, the same catalog name, the same namespace, the same table — two answers.
        // Both tenants hold `SELECT ON CATALOG lldb`, so neither is limited by a grant here; what
        // limits each of them is that the other's catalog is not in their session at all.
        assert_eq!(count_as(&url, &a).await?, ROWS_A);
        assert_eq!(count_as(&url, &b).await?, ROWS_B);

        // --- 3. The other tenant's catalog is not merely denied, it does not resolve. ----------
        //
        // Naming the storage-facing catalog explicitly is the closest thing to an attack this
        // design admits, and it fails at planning: `acct_<b>__lldb` is registered in no session.
        let stolen = QueryRequest::new(format!(
            "SELECT count(*) FROM \"{}\".{NAMESPACE}.{TABLE}",
            TenantScope::account(b.account_id).iceberg_catalog_name(CATALOG)
        ));
        let refused = submit_query_as(&url, &stolen, Some(&a.token)).await;
        assert!(
            refused.is_err(),
            "one tenant must not be able to name another's catalog: {refused:?}"
        );

        // --- 4. The cache is versioned against the right one of the two names. -----------------
        //
        // A repeat query is a hit, per tenant, and the answers stay apart — the key carries the
        // account id, so B's row can never answer A. `catalog_mismatch_count` is the guard against
        // the silent failure this design makes possible: matching a plan's `lldb` against a
        // lakehouse's *storage* name would never match, which does not error — it just turns the
        // cache off forever.
        let cache = coordinator
            .result_cache()
            .expect("the cache was configured");
        let executions_before = cache.execution_count();
        assert_eq!(count_as(&url, &a).await?, ROWS_A);
        assert_eq!(count_as(&url, &b).await?, ROWS_B);
        assert_eq!(
            cache.execution_count(),
            executions_before,
            "a repeat query over unchanged tables must not execute again"
        );
        assert!(cache.hit_count() >= 2, "both repeats must be hits");
        assert_eq!(
            cache.catalog_mismatch_count(),
            0,
            "a catalog with no lakehouse handle means the cache has gone permanently cold"
        );

        Ok::<(), anyhow::Error>(())
    }
    .await
}
