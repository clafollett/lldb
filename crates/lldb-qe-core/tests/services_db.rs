//! The services database, exercised against a **real Postgres** — migrations, the accounts API,
//! and the foreign keys that make an account actually scope a warehouse.
//!
//! Issue #14's "done when" is a schema claim, and a schema claim can only be proven by a server:
//! `CREATE TABLE` typos, a `CHECK` that rejects legal values, an `ON DELETE CASCADE` that was
//! never written — none of them show up in a unit test. So this test needs a database, and it
//! finds one in the first of three ways that works:
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is. This is CI's path (the `check` job runs a
//!    `postgres:18.4-alpine` service container) and the path for anyone with a local server.
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine` on an ephemeral host port,
//!    wait for `pg_isready`, and remove it afterwards no matter how the test ends.
//! 3. **Neither** — print why and pass. `cargo test` on a laptop with no Postgres and no Docker
//!    must stay green, the same bargain `distributed_cluster.rs` strikes.
//!
//! The test is safe to run repeatedly against the same database, and concurrently with another
//! copy of itself: every account it creates is named with a pid + nanosecond suffix, and it
//! deletes exactly the rows it made. It never drops anything global — the database it is handed
//! may well be someone's dev instance.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test services_db
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test services_db -- --nocapture

use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use lldb_qe_core::services::ServicesDb;

/// Image the ephemeral container runs — the same version compose and CI use, so "it passed
/// locally" and "it passed in CI" mean the same thing.
const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

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
    let name = format!("lldb-services-test-{}-{}", std::process::id(), nanos());

    // `--rm` is not enough on its own (a killed test leaves the container running), hence the
    // Drop guard as well; belt and braces so a rerun never collides with a stale container.
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

/// A port nothing is listening on right now. Racy in principle; in practice the kernel hands out
/// ephemeral ports round-robin, so the window between closing this and Docker binding it is not
/// a problem worth a lock file.
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
fn unique_account_name(tag: &str) -> String {
    format!("lldb-test-{tag}-{}-{}", std::process::id(), nanos())
}

#[tokio::test(flavor = "multi_thread")]
async fn services_database_migrates_and_scopes_a_warehouse() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise the services database"
        );
        return Ok(());
    };

    let db = ServicesDb::connect(url).await?;
    db.health_check().await?;

    // ---- Migrations ------------------------------------------------------------------------
    db.migrate().await.context("first migration run")?;
    // Re-running must be a clean no-op: the compose `db-migrate` service runs on every `up`,
    // and a deploy step that only works the first time is not a deploy step.
    db.migrate().await.context("second migration run")?;
    db.health_check().await?;

    // Every table the foundation migration promises, stubs included — later issues extend these
    // rather than creating them, so their absence would break #16/#18/#19 at the wrong moment.
    for table in ["accounts", "users", "warehouses", "queries"] {
        let (exists,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(db.pool())
        .await?;
        assert!(exists, "migration did not create the `{table}` table");
    }

    // ---- Accounts --------------------------------------------------------------------------
    let name = unique_account_name("acct");
    let created = db.create_account(&name).await?;
    assert_eq!(created.name, name);
    assert!(created.id > 0, "identity column produced {}", created.id);

    // `created_at` comes from the server's `now()`, so it should be within minutes of ours; a
    // wildly wrong value means the column defaulted from somewhere unexpected.
    let age = chrono::Utc::now().signed_duration_since(created.created_at);
    assert!(
        age.num_minutes().abs() < 10,
        "created_at is {age} away from now: {}",
        created.created_at
    );

    // Round-trip both lookup paths.
    let by_name = db.account_by_name(&name).await?.expect("account by name");
    assert_eq!(by_name, created);
    let by_id = db.account_by_id(created.id).await?.expect("account by id");
    assert_eq!(by_id, created);
    assert!(
        db.account_by_name(&unique_account_name("missing"))
            .await?
            .is_none(),
        "a name that was never created must not resolve"
    );

    // The UNIQUE constraint is load-bearing: two tenants with one name make every scoped lookup
    // ambiguous. `create_account` must fail; `ensure_account` must return the same row instead.
    assert!(
        db.create_account(&name).await.is_err(),
        "a duplicate account name must be rejected"
    );
    let ensured = db.ensure_account(&name).await?;
    assert_eq!(ensured, created, "ensure_account must be idempotent");
    let ensured_twice = db.ensure_account(&name).await?;
    assert_eq!(ensured_twice.id, created.id);

    let listed = db.list_accounts().await?;
    assert!(
        listed.iter().any(|a| a.id == created.id),
        "list_accounts omitted the account we just created"
    );

    // ---- An account scopes a warehouse -----------------------------------------------------
    // The acceptance criterion, stated as SQL: a warehouse belongs to an account, and deleting
    // the account takes the warehouse with it. That is `ON DELETE CASCADE` doing the work the
    // application would otherwise have to remember to do.
    let (warehouse_id,): (i64,) =
        sqlx::query_as("INSERT INTO warehouses (account_id, name) VALUES ($1, $2) RETURNING id")
            .bind(created.id)
            .bind("wh-primary")
            .fetch_one(db.pool())
            .await
            .context("creating a warehouse scoped to the account")?;

    let (scoped_to,): (i64,) = sqlx::query_as("SELECT account_id FROM warehouses WHERE id = $1")
        .bind(warehouse_id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        scoped_to, created.id,
        "warehouse is not scoped to its account"
    );

    // A warehouse under a tenant that does not exist must be impossible.
    assert!(
        sqlx::query("INSERT INTO warehouses (account_id, name) VALUES ($1, $2)")
            .bind(i64::MAX)
            .bind("orphan")
            .execute(db.pool())
            .await
            .is_err(),
        "a warehouse must not be creatable under a nonexistent account"
    );

    // ---- Cleanup, which is also the cascade assertion ---------------------------------------
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(created.id)
        .execute(db.pool())
        .await
        .context("deleting the test account")?;

    let (survivors,): (i64,) = sqlx::query_as("SELECT count(*) FROM warehouses WHERE id = $1")
        .bind(warehouse_id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        survivors, 0,
        "deleting an account must cascade to its warehouses"
    );
    assert!(
        db.account_by_id(created.id).await?.is_none(),
        "the test account should be gone"
    );

    db.close().await;
    Ok(())
}
