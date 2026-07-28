//! The acceptance criterion for issue #8, stated as a test: **two independently-constructed
//! lakehouses pointed at one Postgres catalog see the same tables and the same snapshot.**
//!
//! With a memory catalog each process builds its own catalog from its own copy of the manifest,
//! so "what tables exist" and "which snapshot is current" are N private opinions. A test that
//! only checked "a table got created" would pass just as happily against that broken world, so
//! this one is written to fail there: every assertion is made from a handle that did **not**
//! perform the write it is observing, and the writing role is deliberately handed back and
//! forth so the property proven is a shared source of truth rather than one lucky direction.
//!
//! Each "worker" here is a separate [`SessionContext`] plus a separate [`Lakehouse`], built by
//! running the same manifest through [`apply_manifest`] — the real config-as-data path a worker
//! process takes, not a hand-built catalog. Same process, different handles: what is under test
//! is whether catalog state is shared through Postgres, and a second OS process would prove
//! nothing more while making the test hostage to a build.
//!
//! It finds a database the same three ways `services_db.rs` does:
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is (CI's service container, or a local server).
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine`, remove it afterwards.
//! 3. **Neither** — print why and pass. `cargo test --workspace` on a laptop with no Postgres
//!    and no Docker stays green.
//!
//! Safe to rerun and to run concurrently with itself: the Iceberg catalog name is suffixed with
//! a pid + nanoseconds, and `iceberg_tables` is keyed by `(catalog_name, namespace, table)`, so
//! two runs cannot see each other's rows. The catalog's own tables are never dropped — they may
//! be shared with a real catalog on someone's dev database — only this run's rows are deleted.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb \
//!     cargo test -p lldb-qe-core --test integration shared_sql_catalog
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration shared_sql_catalog -- --nocapture

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use lldb_qe_core::lakehouse::Lakehouse;
use lldb_qe_core::manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableSource,
};
use lldb_qe_core::tenancy::TenantScope;
use lldb_qe_core::{StorageConfig, apply_manifest, build_session};

/// Same image compose and CI run, so a local pass and a CI pass mean the same thing.
const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// The namespace both "workers" declare.
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
    let name = format!("lldb-sqlcatalog-test-{}-{}", std::process::id(), nanos());

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

/// The manifest both workers apply: one SQL catalog, two Iceberg tables, no data of its own.
///
/// The tables are `Empty` with explicit schemas on purpose — sourcing them from SF1 parquet
/// would make this test skip on a machine without the generated data, and the property under
/// test has nothing to do with where the rows came from.
fn manifest(catalog_name: &str, backend: CatalogBackend, warehouse: &Path) -> Manifest {
    let table = |name: &str| TableDef {
        name: name.to_string(),
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
    };
    Manifest {
        catalogs: vec![CatalogDef {
            name: catalog_name.to_string(),
            backend,
            warehouse: Some(format!("file://{}", warehouse.display())),
            namespaces: vec![NamespaceDef {
                name: NS.to_string(),
                tables: vec![table("orders"), table("returns")],
            }],
        }],
    }
}

/// One "worker": its own session and its own catalog handle, built by applying the manifest
/// exactly as a worker process would. Nothing is shared with any other worker except Postgres
/// and the warehouse directory.
async fn start_worker(
    catalog_name: &str,
    backend: CatalogBackend,
    warehouse: &Path,
) -> Result<(SessionContext, Lakehouse)> {
    let (ctx, storage) = build_session(StorageConfig::InMemory).await?;
    let m = manifest(catalog_name, backend, warehouse);
    let mut lakes = apply_manifest(&ctx, &storage, &m, &TenantScope::untenanted()).await?;
    assert_eq!(lakes.len(), 1, "one catalog in, one lakehouse out");
    Ok((ctx, lakes.remove(0)))
}

/// The backend a worker declares: an explicit URI, or — when the process has been pointed at
/// the same database through `LLDB_METADATA_*` — the `uri`-less form a deployed manifest uses.
///
/// This is not decoration. `{ kind = "sql" }` with no URI is the shape shipped in
/// `manifests/shared-catalog.toml` and the shape compose deploys, and it goes through a
/// different resolution path ([`lldb_qe_core::services::ServicesArgs::from_env`]). Exercising it
/// requires the environment to actually name this database, which only the caller can arrange:
///
///   LLDB_TEST_POSTGRES_URL=$U LLDB_METADATA_URL=$U cargo test --test integration shared_sql_catalog
///
/// When they disagree — the ordinary CI case — the explicit URI is used and the assertions are
/// unchanged. The unit tests in `lakehouse.rs` cover the resolution rules with no database.
fn backend_for(target_url: &str) -> CatalogBackend {
    match std::env::var("LLDB_METADATA_URL") {
        Ok(env_url) if env_url == target_url => CatalogBackend::Sql { uri: None },
        _ => CatalogBackend::Sql {
            uri: Some(target_url.to_string()),
        },
    }
}

/// `SELECT count(*)` through a session, as an `i64`.
async fn count_rows(ctx: &SessionContext, catalog: &str, table: &str) -> Result<i64> {
    let batches = ctx
        .sql(&format!(
            "SELECT count(*) FROM \"{catalog}\".\"{NS}\".\"{table}\""
        ))
        .await?
        .collect()
        .await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) is Int64")
        .value(0))
}

/// Append rows to an Iceberg table through DataFusion — a real commit, not a catalog poke.
async fn insert_rows(ctx: &SessionContext, catalog: &str, table: &str, values: &str) -> Result<()> {
    ctx.sql(&format!(
        "INSERT INTO \"{catalog}\".\"{NS}\".\"{table}\" VALUES {values}"
    ))
    .await?
    .collect()
    .await
    .with_context(|| format!("inserting into {catalog}.{NS}.{table}"))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn two_workers_share_one_catalog() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise the shared SQL catalog"
        );
        return Ok(());
    };

    // A name no other run — or concurrent copy of this run — will pick. `iceberg_tables` is
    // keyed by catalog_name, so this is what keeps runs from seeing each other.
    let catalog = format!("lldb_test_{}_{}", std::process::id(), nanos());
    // One warehouse directory, shared by every worker: the local-filesystem stand-in for the
    // shared object storage a real fleet would have.
    let warehouse = tempfile::tempdir()?;

    // ---- Two workers, built independently, both applying the same manifest -----------------
    let (ctx_a, lake_a) = start_worker(&catalog, backend_for(url), warehouse.path())
        .await
        .context("worker A")?;
    // B applying the same manifest against a catalog that already has these tables must be a
    // clean no-op. If `apply_manifest` were not idempotent this line would fail outright — the
    // ordinary case for a persistent catalog is that the tables are already there.
    let (ctx_b, lake_b) = start_worker(&catalog, backend_for(url), warehouse.path())
        .await
        .context("worker B")?;

    // ---- Same tables ------------------------------------------------------------------------
    for table in ["orders", "returns"] {
        assert!(lake_a.table_exists(NS, table).await?, "A missing {table}");
        assert!(lake_b.table_exists(NS, table).await?, "B missing {table}");
    }
    assert!(
        !lake_b.table_exists(NS, "not_declared").await?,
        "a table nobody declared must not appear"
    );
    // Schemas agree because they came from the same catalog row, not from two parsers.
    assert_eq!(
        lake_a
            .load_table(NS, "orders")
            .await?
            .metadata()
            .current_schema(),
        lake_b
            .load_table(NS, "orders")
            .await?
            .metadata()
            .current_schema(),
    );

    // ---- Same snapshot: before any write, both say "none" ------------------------------------
    assert_eq!(lake_a.current_snapshot_id(NS, "orders").await?, None);
    assert_eq!(lake_b.current_snapshot_id(NS, "orders").await?, None);

    // ---- A writes; B — which did not write — must see it -------------------------------------
    insert_rows(&ctx_a, &catalog, "orders", "(1, 'a'), (2, 'b'), (3, 'c')").await?;
    let snap_after_a = lake_a
        .current_snapshot_id(NS, "orders")
        .await?
        .expect("A's insert must produce a snapshot");
    assert_eq!(
        lake_b.current_snapshot_id(NS, "orders").await?,
        Some(snap_after_a),
        "B was built before the write and never told about it — a shared catalog is how it finds \
         out. A per-process catalog fails exactly here."
    );
    // The other table is untouched in both views: sharing state is not smearing it.
    assert_eq!(lake_b.current_snapshot_id(NS, "returns").await?, None);

    // A third worker, constructed *after* the write, reads the same snapshot and the rows.
    let (ctx_c, lake_c) = start_worker(&catalog, backend_for(url), warehouse.path())
        .await
        .context("worker C")?;
    assert_eq!(
        lake_c.current_snapshot_id(NS, "orders").await?,
        Some(snap_after_a)
    );
    assert_eq!(
        count_rows(&ctx_c, &catalog, "orders").await?,
        3,
        "C resolved the table purely from shared catalog state and read A's rows"
    );
    // Re-applying the manifest (which is what starting C did) must not re-seed or re-commit.
    assert_eq!(
        lake_a.current_snapshot_id(NS, "orders").await?,
        Some(snap_after_a),
        "applying the manifest again moved the snapshot — it is not idempotent"
    );

    // ---- Now C writes, and A — the original writer — must follow -----------------------------
    // The direction matters: a catalog that only ever propagates from the process that created
    // the table would pass every assertion above and still not be a shared source of truth.
    insert_rows(&ctx_c, &catalog, "orders", "(4, 'd'), (5, 'e')").await?;
    let snap_after_c = lake_c
        .current_snapshot_id(NS, "orders")
        .await?
        .expect("C's insert must produce a snapshot");
    assert_ne!(
        snap_after_c, snap_after_a,
        "a commit must move the snapshot"
    );
    assert_eq!(
        lake_a.current_snapshot_id(NS, "orders").await?,
        Some(snap_after_c),
        "A must observe C's commit"
    );
    assert_eq!(
        lake_b.current_snapshot_id(NS, "orders").await?,
        Some(snap_after_c),
        "B must observe C's commit"
    );

    // Snapshot *summaries* agree too, so the handles are reading one metadata file rather than
    // coincidentally-equal ids.
    let summary_a = lake_a.snapshot_summary(NS, "orders").await?;
    assert_eq!(summary_a, lake_b.snapshot_summary(NS, "orders").await?);
    assert_eq!(
        summary_a.get("total-records").map(String::as_str),
        Some("5"),
        "summary: {summary_a:?}"
    );

    // A fourth worker sees all five rows — the end-to-end statement of the whole issue.
    let (ctx_d, _lake_d) = start_worker(&catalog, backend_for(url), warehouse.path())
        .await
        .context("worker D")?;
    assert_eq!(count_rows(&ctx_d, &catalog, "orders").await?, 5);
    assert_eq!(count_rows(&ctx_d, &catalog, "returns").await?, 0);

    // ---- Cleanup: this run's rows only ------------------------------------------------------
    // `iceberg_tables` / `iceberg_namespace_properties` belong to `iceberg-catalog-sql` and may
    // be shared with a real catalog on the database we were handed, so they are never dropped —
    // just emptied of the uniquely-named catalog this run invented.
    let pool = sqlx::postgres::PgPool::connect(url).await?;
    for table in ["iceberg_tables", "iceberg_namespace_properties"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE catalog_name = $1"))
            .bind(&catalog)
            .execute(&pool)
            .await
            .with_context(|| format!("cleaning up {table}"))?;
    }
    pool.close().await;

    // Keep the warehouse alive until here: dropping it earlier would delete the metadata the
    // assertions above depend on.
    drop(ctx_b);
    warehouse.close()?;
    Ok(())
}
