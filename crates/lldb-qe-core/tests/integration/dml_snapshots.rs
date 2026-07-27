//! Issue #21's "done when", stated as a test: **`DELETE`/`UPDATE` produce correct rows and a
//! fresh snapshot, a follow-up read reflects them, and two concurrent writers never lose or
//! double-apply a change.**
//!
//! Both halves need a real database. The commit is a conditional `UPDATE iceberg_tables ... WHERE
//! metadata_location = <what I read>`, and what is under test is precisely that Postgres
//! serializes two of those on one row — a claim no in-process fake can make. So this test finds a
//! database the same three ways `services_db.rs` and `shared_sql_catalog.rs` do:
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is (CI's service container, or a local server).
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine`, remove it afterwards.
//! 3. **Neither** — print why and pass. `cargo test --workspace` with no Postgres and no Docker
//!    stays green; the parse/rewrite/snapshot-assembly logic is unit-tested in `dml.rs` without
//!    one.
//!
//! The concurrency test is the load-bearing one, and it is written so that the *absence of an
//! error* cannot pass it. Four writers each run `UPDATE ... SET qty = qty + 1` against the same
//! table at the same time; the assertion is on the arithmetic. A lost commit leaves a row short,
//! a double-applied one leaves it long, and either shows up as a wrong number rather than as a
//! stack trace. Writers are released together by a barrier so they genuinely overlap instead of
//! politely queueing.
//!
//! Safe to rerun and to run concurrently with itself: the Iceberg catalog name carries a pid +
//! nanosecond suffix, `iceberg_tables` is keyed by `(catalog_name, namespace, table)`, and
//! cleanup deletes only this run's rows — the catalog's own tables are never dropped, since the
//! database we are handed may be someone's dev instance.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb \
//!     cargo test -p lldb-qe-core --test integration dml_snapshots
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration dml_snapshots -- --nocapture

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::prelude::SessionContext;
use lldb_qe_core::dml;
use lldb_qe_core::lakehouse::Lakehouse;
use lldb_qe_core::manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableSource,
};
use lldb_qe_core::{StorageConfig, apply_manifest, build_session};
use tokio::sync::Barrier;

/// Same image compose and CI run, so a local pass and a CI pass mean the same thing.
const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);
/// The namespace the manifest declares.
const NS: &str = "sales";
/// The table every case operates on.
const TABLE: &str = "orders";

/// How the test got its database — and, for the container case, what to tear down.
enum Target {
    Skipped,
    Provided(String),
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

fn start_container() -> Result<Target> {
    let port = free_port()?;
    let name = format!("lldb-dml-test-{}-{}", std::process::id(), nanos());

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

/// One SQL catalog, one namespace, one table with a column of every kind the rewrite has to
/// handle: a non-null key, a nullable string, and a nullable number to do arithmetic on.
fn manifest(catalog_name: &str, url: &str, warehouse: &Path) -> Manifest {
    let column = |name: &str, ty: &str, nullable: bool| ColumnDef {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
    };
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
                    name: TABLE.to_string(),
                    format: Default::default(), // Iceberg
                    source: TableSource::Empty,
                    schema: Some(vec![
                        column("id", "int64", false),
                        column("label", "string", true),
                        column("qty", "int64", true),
                    ]),
                }],
            }],
        }],
    }
}

/// A worker: its own session and its own catalog handle, exactly as a process would build them.
async fn start_worker(
    catalog_name: &str,
    url: &str,
    warehouse: &Path,
) -> Result<(SessionContext, Lakehouse)> {
    let (ctx, storage) = build_session(StorageConfig::InMemory).await?;
    let m = manifest(catalog_name, url, warehouse);
    let mut lakes = apply_manifest(&ctx, &storage, &m).await?;
    assert_eq!(lakes.len(), 1, "one catalog in, one lakehouse out");
    Ok((ctx, lakes.remove(0)))
}

/// The whole table, ordered by id, as `(id, label, qty)` — the shape every assertion is made on.
async fn rows(
    ctx: &SessionContext,
    catalog: &str,
) -> Result<Vec<(i64, Option<String>, Option<i64>)>> {
    let batches = ctx
        .sql(&format!(
            "SELECT id, label, qty FROM \"{catalog}\".\"{NS}\".\"{TABLE}\" ORDER BY id"
        ))
        .await?
        .collect()
        .await?;
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let labels = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label is Utf8");
        let qtys = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("qty is Int64");
        for i in 0..b.num_rows() {
            out.push((
                ids.value(i),
                labels.is_valid(i).then(|| labels.value(i).to_string()),
                qtys.is_valid(i).then(|| qtys.value(i)),
            ));
        }
    }
    Ok(out)
}

async fn insert(ctx: &SessionContext, catalog: &str, values: &str) -> Result<()> {
    ctx.sql(&format!(
        "INSERT INTO \"{catalog}\".\"{NS}\".\"{TABLE}\" VALUES {values}"
    ))
    .await?
    .collect()
    .await
    .context("seeding rows")?;
    Ok(())
}

/// Remove only this run's catalog rows. `iceberg_tables` belongs to `iceberg-catalog-sql` and may
/// be shared with a real catalog on the database we were handed, so it is never dropped.
async fn cleanup(url: &str, catalog: &str) -> Result<()> {
    let pool = sqlx::postgres::PgPool::connect(url).await?;
    for table in ["iceberg_tables", "iceberg_namespace_properties"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE catalog_name = $1"))
            .bind(catalog)
            .execute(&pool)
            .await
            .with_context(|| format!("cleaning up {table}"))?;
    }
    pool.close().await;
    Ok(())
}

/// The commit in `dml.rs` binds `iceberg_tables`' columns by name, and those names are private
/// statics inside `iceberg-catalog-sql`. Pin them: a rename there would otherwise surface as
/// every commit failing at runtime, or — far worse — as a `WHERE` clause that matches nothing and
/// so reports an endless phantom conflict. This is the cheapest possible early warning.
async fn assert_catalog_columns_unchanged(url: &str) -> Result<()> {
    let pool = sqlx::postgres::PgPool::connect(url).await?;
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'iceberg_tables' ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .context("reading iceberg_tables' columns")?;
    pool.close().await;
    // Assert the columns the commit *depends on* are present, not that the set is exactly this.
    // An upstream release that adds a column is backward-compatible and must not fail CI; one that
    // renames or removes a column this CAS binds by name genuinely breaks the commit, and that is
    // what this catches. Testing the intersection tests the contract; testing equality tests the
    // upstream's changelog.
    for required in [
        "catalog_name",
        "table_namespace",
        "table_name",
        "metadata_location",
        "previous_metadata_location",
    ] {
        assert!(
            names.iter().any(|n| n == required),
            "iceberg-catalog-sql no longer has `{required}` on iceberg_tables; dml.rs's commit \
             binds it by name. Columns present: {names:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_and_update_commit_snapshots_a_later_read_sees() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise Iceberg DML"
        );
        return Ok(());
    };

    let catalog = format!("lldb_dml_{}_{}", std::process::id(), nanos());
    let warehouse = tempfile::tempdir()?;
    let (ctx, lake) = start_worker(&catalog, url, warehouse.path()).await?;
    // `start_worker` has now created the catalog's tables, so this is the first moment the
    // assertion can be made.
    assert_catalog_columns_unchanged(url).await?;

    insert(
        &ctx,
        &catalog,
        "(1, 'a', 10), (2, 'b', 20), (3, NULL, 30), (4, 'd', 40), (5, 'e', NULL)",
    )
    .await?;
    let seeded = lake
        .current_snapshot_id(NS, TABLE)
        .await?
        .expect("the seed INSERT commits a snapshot");
    assert_eq!(rows(&ctx, &catalog).await?.len(), 5);

    // ---- DELETE -----------------------------------------------------------------------------
    let out = dml::execute(&lake, &format!("DELETE FROM {NS}.{TABLE} WHERE id = 2"))
        .await?
        .expect("DELETE is a statement dml owns");
    assert_eq!(out.rows_changed, 1);
    assert!(out.committed(), "a DELETE that removed a row must commit");
    let after_delete = out.snapshot_id.expect("a committed DELETE has a snapshot");
    assert_ne!(after_delete, seeded, "the snapshot must move");
    assert_eq!(out.attempts, 1, "nothing else was writing");

    // The follow-up read is the point of the whole exercise: the same session, whose provider
    // reloads from the catalog, must see the new state.
    assert_eq!(
        rows(&ctx, &catalog).await?,
        vec![
            (1, Some("a".into()), Some(10)),
            (3, None, Some(30)),
            (4, Some("d".into()), Some(40)),
            (5, Some("e".into()), None),
        ]
    );
    let summary = lake.snapshot_summary(NS, TABLE).await?;
    assert_eq!(summary.get("total-records").map(String::as_str), Some("4"));

    // A row whose predicate evaluates to NULL is *not* matched, so `label = 'zzz'` must leave the
    // NULL-label row alone rather than sweeping it away with the three-valued-logic default.
    let out = dml::execute(
        &lake,
        &format!("DELETE FROM {NS}.{TABLE} WHERE label = 'zzz'"),
    )
    .await?
    .expect("DELETE");
    assert_eq!(out.rows_changed, 0);
    assert!(
        !out.committed(),
        "a statement that matches nothing must not mint a snapshot"
    );
    assert_eq!(out.snapshot_id, Some(after_delete));
    assert_eq!(rows(&ctx, &catalog).await?.len(), 4);

    // ---- UPDATE -----------------------------------------------------------------------------
    let out = dml::execute(
        &lake,
        &format!("UPDATE {NS}.{TABLE} SET label = 'x', qty = qty + 100 WHERE id >= 4"),
    )
    .await?
    .expect("UPDATE");
    assert_eq!(out.rows_changed, 2);
    let after_update = out.snapshot_id.expect("a committed UPDATE has a snapshot");
    assert_ne!(after_update, after_delete);
    assert_eq!(
        rows(&ctx, &catalog).await?,
        vec![
            (1, Some("a".into()), Some(10)),
            (3, None, Some(30)),
            // Both assignments applied to the matched rows…
            (4, Some("x".into()), Some(140)),
            // …including the one whose `qty + 100` is NULL, which stays NULL rather than becoming 100.
            (5, Some("x".into()), None),
        ]
    );

    // An unconditional UPDATE touches every row, and its right-hand sides read pre-update values.
    let out = dml::execute(&lake, &format!("UPDATE {NS}.{TABLE} SET id = id * 10"))
        .await?
        .expect("UPDATE");
    assert_eq!(out.rows_changed, 4);
    assert_eq!(
        rows(&ctx, &catalog)
            .await?
            .iter()
            .map(|r| r.0)
            .collect::<Vec<_>>(),
        vec![10, 30, 40, 50]
    );

    // ---- A process that did not write must see all of it --------------------------------------
    let (ctx_b, lake_b) = start_worker(&catalog, url, warehouse.path()).await?;
    assert_eq!(
        lake_b.current_snapshot_id(NS, TABLE).await?,
        lake.current_snapshot_id(NS, TABLE).await?
    );
    assert_eq!(rows(&ctx_b, &catalog).await?, rows(&ctx, &catalog).await?);

    // ---- Unconditional DELETE empties the table without losing the table ----------------------
    let out = dml::execute(&lake, &format!("DELETE FROM {NS}.{TABLE}"))
        .await?
        .expect("DELETE");
    assert_eq!(out.rows_changed, 4);
    assert!(out.committed());
    assert!(rows(&ctx_b, &catalog).await?.is_empty());
    // Still a table, still appendable — an empty overwrite must not have orphaned the schema.
    insert(&ctx, &catalog, "(99, 'back', 1)").await?;
    assert_eq!(
        rows(&ctx_b, &catalog).await?,
        vec![(99, Some("back".into()), Some(1))]
    );

    cleanup(url, &catalog).await?;
    drop(ctx_b);
    warehouse.close()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_neither_lose_nor_double_apply() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!("SKIP: no Postgres available for the concurrent-writer test");
        return Ok(());
    };

    /// Enough writers to make the race real, few enough that the worst case (every writer losing
    /// to every other) stays inside `dml::MAX_ATTEMPTS`. Each writer can only lose to another
    /// writer's *successful* commit, and there are `WRITERS - 1` of those.
    const WRITERS: u32 = 4;
    const {
        assert!(
            WRITERS <= dml::MAX_ATTEMPTS,
            "the retry budget must cover the worst case"
        )
    };

    let catalog = format!("lldb_dmlrace_{}_{}", std::process::id(), nanos());
    let warehouse = tempfile::tempdir()?;
    let (ctx, lake) = start_worker(&catalog, url, warehouse.path()).await?;
    insert(&ctx, &catalog, "(1, 'a', 0), (2, 'b', 0), (3, 'c', 0)").await?;
    let before = lake
        .current_snapshot_id(NS, TABLE)
        .await?
        .expect("seeded snapshot");

    // Each writer gets its own `Lakehouse` — its own catalog handle and its own connection —
    // because sharing one would test a mutex, not a database.
    let barrier = Arc::new(Barrier::new(WRITERS as usize));
    let mut tasks = Vec::new();
    for _ in 0..WRITERS {
        let url = url.to_string();
        let catalog = catalog.clone();
        let warehouse = warehouse.path().to_path_buf();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let lake = Lakehouse::open_sql(&catalog, &url, warehouse.to_str().unwrap()).await?;
            // Released together, so the reads genuinely overlap rather than queueing politely.
            barrier.wait().await;
            let out = dml::execute(&lake, &format!("UPDATE {NS}.{TABLE} SET qty = qty + 1"))
                .await?
                .expect("UPDATE");
            anyhow::Ok(out)
        }));
    }

    let mut snapshots = Vec::new();
    let mut total_attempts = 0;
    for task in tasks {
        // Every writer must *succeed*. A losing writer that gave up would be a correct-but-worse
        // outcome; asserting success here is what makes the arithmetic below meaningful.
        let out = task.await.expect("writer task did not panic")?;
        assert_eq!(out.rows_changed, 3, "each UPDATE matches every row");
        assert!(out.committed());
        total_attempts += out.attempts;
        snapshots.push(out.snapshot_id.expect("committed"));
    }

    // Every writer minted a distinct snapshot, and none of them is the one they all started from:
    // proof that the commits serialized rather than clobbering each other's metadata pointer.
    snapshots.sort_unstable();
    let distinct = {
        let mut s = snapshots.clone();
        s.dedup();
        s.len()
    };
    assert_eq!(distinct, WRITERS as usize, "snapshots: {snapshots:?}");
    assert!(!snapshots.contains(&before));
    eprintln!(
        "{WRITERS} concurrent writers took {total_attempts} attempts \
         ({} conflicts retried)",
        total_attempts - WRITERS
    );

    // ---- The assertion that matters: the data ------------------------------------------------
    // `qty` started at 0 and each of the four writers added 1. Exactly 4 means no commit was
    // lost and none was applied twice. Anything else is the bug this test exists to catch, and it
    // shows up as a number rather than as an error — which is the point.
    let (ctx_after, _lake_after) = start_worker(&catalog, url, warehouse.path()).await?;
    let final_rows = rows(&ctx_after, &catalog).await?;
    assert_eq!(final_rows.len(), 3, "no writer may add or drop a row");
    for (id, _, qty) in &final_rows {
        assert_eq!(
            *qty,
            Some(WRITERS as i64),
            "row {id} saw {qty:?} increments, expected {WRITERS}; rows: {final_rows:?}"
        );
    }

    cleanup(url, &catalog).await?;
    warehouse.close()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_deletes_of_different_rows_both_land() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!("SKIP: no Postgres available for the concurrent-delete test");
        return Ok(());
    };

    let catalog = format!("lldb_dmldel_{}_{}", std::process::id(), nanos());
    let warehouse = tempfile::tempdir()?;
    let (ctx, _lake) = start_worker(&catalog, url, warehouse.path()).await?;
    insert(
        &ctx,
        &catalog,
        "(1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', 4)",
    )
    .await?;

    // Two writers deleting *different* rows. A naive last-writer-wins pointer swap passes every
    // "did it error?" check here and still silently resurrects one of the deleted rows, so the
    // assertion is on which rows survive.
    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for id in [2, 3] {
        let url = url.to_string();
        let catalog = catalog.clone();
        let warehouse = warehouse.path().to_path_buf();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let lake = Lakehouse::open_sql(&catalog, &url, warehouse.to_str().unwrap()).await?;
            barrier.wait().await;
            let out = dml::execute(&lake, &format!("DELETE FROM {NS}.{TABLE} WHERE id = {id}"))
                .await?
                .expect("DELETE");
            anyhow::Ok(out)
        }));
    }
    for task in tasks {
        let out = task.await.expect("writer task did not panic")?;
        assert_eq!(out.rows_changed, 1);
        assert!(out.committed());
    }

    let (ctx_after, _lake_after) = start_worker(&catalog, url, warehouse.path()).await?;
    assert_eq!(
        rows(&ctx_after, &catalog)
            .await?
            .iter()
            .map(|r| r.0)
            .collect::<Vec<_>>(),
        vec![1, 4],
        "both deletes must survive — a lost one leaves 2 or 3 behind"
    );

    cleanup(url, &catalog).await?;
    warehouse.close()?;
    Ok(())
}
