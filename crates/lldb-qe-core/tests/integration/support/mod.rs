//! Finding a Postgres to test against — and undoing what a test did, however the test ended.
//!
//! Two of the three things in this module are teardown ([`Servers`], [`DbCleanup`]); the rest is
//! the database resolution described below. They live together because they answer the same
//! question from opposite ends: what does a test in this binary own, and who gives it back?
//!
//! Most database-gated integration tests resolve their database through this module — `services_db`
//! (migrations, accounts, the foreign keys), `warehouse_lifecycle` (the warehouse API and its
//! transitions), `query_scheduler`, `auth_rbac`, `tenant_catalogs`, `coordinator_liveness` and
//! `query_reaper` — and they must agree exactly on *which* server and on how to skip when there is
//! none. The order is first-one-that-works:
//!
//! Be aware that they are **not** the only database-gated tests: `dml_snapshots`, `result_cache_db`
//! and `shared_sql_catalog` each carry their own private copy of this resolution — their own
//! `Target`, `Drop` guard and container bootstrap. That is an accident of history rather than a
//! decision. Each was once its own binary, where taking a dependency on this module was opt-in and
//! copying looked cheap; now that they are all modules of one binary (see `main.rs`) the copies sit
//! side by side in a single compilation unit. They behave identically and name their containers
//! distinctly, so nothing is broken — but four implementations of "find me a Postgres" is three too
//! many, and the next person to touch any of them should migrate it here rather than fix a bug in
//! one copy.
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is. CI's path (the `check` job runs a
//!    `postgres:18.4-alpine` service container) and the path for anyone with a local server.
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine` on an ephemeral host port,
//!    wait for `pg_isready`, and remove it afterwards no matter how the test ends.
//! 3. **Neither** — the caller prints why and passes. `cargo test` on a laptop with no Postgres
//!    and no Docker must stay green, the same bargain `distributed_cluster.rs` strikes.
//!
//! Every test using this must be safe to run repeatedly against the same database *and*
//! concurrently with another copy of itself — the URL it is handed may well be someone's dev
//! instance, and in CI every database-gated test shares one container. Hence [`unique_name`]: a
//! pid + nanosecond suffix, so no run can collide with another, and each test deletes exactly the
//! rows it made and nothing global.
//!
//! This module used to carry `#![allow(dead_code)]`, because it was compiled separately into each
//! integration-test binary and each of them legitimately used only the parts it needed. That
//! stopped being true when those binaries became modules of one (see `main.rs`): there is now a
//! single copy, every item in it has a caller, and the allow — which would have hidden a genuinely
//! unused helper — is gone. Do not put it back to silence a warning; delete the helper instead.

use std::future::Future;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use sqlx::Connection;

/// Image the ephemeral container runs — the same version compose and CI use, so "it passed
/// locally" and "it passed in CI" mean the same thing.
pub const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// How the test got its database — and, for the container case, what to tear down.
pub enum Target {
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
    pub fn url(&self) -> Option<&str> {
        match self {
            Target::Skipped => None,
            Target::Provided(url) | Target::Container { url, .. } => Some(url),
        }
    }
}

/// Resolve a database to test against, per the three-way rule in the module docs.
pub fn resolve_target() -> Result<Target> {
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

pub fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_nanos()
}

/// A name no other run — or concurrent copy of this run — will pick.
///
/// Warehouse names must also be DNS labels (lowercase, `[a-z0-9-]`), which is why this uses only
/// characters legal in both an account name and a warehouse name.
pub fn unique_name(tag: &str) -> String {
    format!("lldb-test-{tag}-{}-{}", std::process::id(), nanos())
}

// ---------------------------------------------------------------------------
// Teardown, part 1: the Flight servers a test spawned
// ---------------------------------------------------------------------------

/// In-process Flight servers a test started, stopped when this value is dropped.
///
/// [`tokio::spawn`] hands back a `JoinHandle`, and **dropping that handle detaches the task rather
/// than cancelling it**: the task keeps running and the `TcpListener` it is accepting on keeps its
/// port. Nothing else stops it either — every server in this directory is spawned with
/// `std::future::pending()` as its shutdown signal, a future that by construction never resolves.
/// So the only lifetime a spawned server has ever had is "until the process exits".
///
/// That was genuinely fine when it was written, and it is worth saying so rather than implying the
/// code was wrong: each of these files used to be its own test *binary*, so "until the process
/// exits" meant a second or two and the OS reclaimed the socket. Issue #44 collapsed 24 binaries
/// into one, which changed the premise underneath. "The process" is now the whole `integration`
/// run, so one test's leaked listener outlives its test and accumulates alongside every other
/// test's — each holding an accept loop, a `SessionContext` and, for a worker, a `StageCache`.
/// Nothing has failed because of it, and the failure it sets up is the expensive kind: a later test
/// contending with a dead test's server for a port or for runtime capacity, surfacing as flakiness
/// with no obvious cause.
///
/// Retaining the handle and aborting it in `Drop` gives a server the same lifetime as the test that
/// started it. `Drop` rather than a `stop()` you have to remember to call, for the reason
/// [`DbCleanup`] spells out at length: an assertion that fails unwinds past anything at the end of
/// the body.
#[derive(Default)]
pub struct Servers {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Servers {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl Servers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn `server` and keep its handle. Taking the future rather than a `JoinHandle` is the
    /// point: there is no way to call this and still end up with a detached task.
    pub fn spawn<F>(&mut self, server: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.push(tokio::spawn(server));
    }
}

// ---------------------------------------------------------------------------
// Teardown, part 2: the rows a test wrote
// ---------------------------------------------------------------------------

/// One kind of row a test leaves behind, and therefore one `DELETE`.
///
/// A closed set rather than free-form SQL, because every cleanup in this directory is already one
/// of these four and naming them makes a missing one visible.
pub enum Cleanup {
    /// An account. Migration 0005's cascades take its users, keys, roles, grants, warehouses,
    /// query history, cached results and admission slots with it.
    Account(i64),
    /// `iceberg_tables` + `iceberg_namespace_properties` for one catalog.
    ///
    /// This is the **storage-facing** name — `TenantScope::iceberg_catalog_name`, not the declared
    /// one — because that is the column those rows carry. Asking by the other name deletes nothing
    /// and leaves the run's rows behind. Deleted by name rather than truncated because
    /// `iceberg-catalog-sql` owns those tables and they may be shared with a real catalog on this
    /// database.
    IcebergCatalog(String),
    /// `coordinators` rows, which are not account-scoped and so survive the cascade. A registration
    /// left renewing against a deleted account is litter with a heartbeat.
    CoordinatorSlots(Vec<String>),
    /// `admission_slots` for a warehouse whose row a test made by hand, or which outlives the
    /// account that would otherwise cascade it away.
    AdmissionSlots(i64),
}

/// Rows a test made, deleted when this value is dropped — **including when an assertion panicked.**
///
/// What this replaces is `async fn cleanup(&self) -> Result<()>`, called with `?` near the end of
/// the test body. That runs on the success path only: a failing `assert!` unwinds straight past it.
/// So a *passing* run cleaned up and a *failing* one — the run you are about to repeat while
/// debugging — left its accounts, grants, `iceberg_tables` and `result_cache` rows behind, which is
/// exactly backwards. It also lets a test that fails once make its own re-run fail differently.
/// `Drop` runs on unwind; that is the whole of the fix, and it is why PR #47 put the coordinator's
/// `abort()` there rather than in a method.
///
/// # Why this blocks a thread instead of spawning a task
///
/// `Drop` cannot `await`, and this repo already answers that twice. `ActiveQuery`'s `Drop`
/// (`server.rs`) hands its write to `tokio::runtime::Handle::try_current()`; that is right *there*,
/// because a coordinator's runtime outlives the drop by hours. Here it would be wrong, and quietly
/// so: this guard is dropped as the test function returns, and a `#[tokio::test]`'s runtime shuts
/// down immediately afterwards, dropping queued tasks without ever polling them. The `DELETE` would
/// usually not run at all — and a teardown that *usually* works is worse than one that visibly does
/// not, because it fails intermittently on a shared CI database. `Handle::block_on` under
/// `block_in_place` is the other tempting shape and needs a multi-threaded runtime; several tests
/// here are plain `#[tokio::test]`, which is not one. Reusing the test's own `PgPool` from a
/// different runtime is worse than either: sqlx's sockets are registered with the reactor of the
/// runtime that opened them, and that reactor is precisely what is going away.
///
/// So this takes the other precedent — `Target`'s blocking `docker rm -f` above — and makes it
/// self-contained: its own OS thread, its own current-thread runtime, its own connection, joined
/// before `drop` returns. It costs one Postgres connect per test teardown, which is test code and
/// not a hot path, and it buys determinism: when the test function returns, the rows are gone.
///
/// It never panics. A destructor that panics while an assertion is already unwinding aborts the
/// process, turning a readable test failure into a core dump, so a failed cleanup is reported on
/// stderr and nothing else. Connection URLs go through [`lldb_qe_core::services::redact_url`]
/// first, the same rule the engine follows.
///
/// **Hold it for at least as long as the [`Target`] it was built from.** Under `LLDB_DOCKER=1` the
/// database is a container `Target::drop` removes, so a guard outliving its target would find
/// nothing to connect to. Locals drop in reverse declaration order, so binding the target first —
/// which every caller here already does — is enough.
pub struct DbCleanup {
    url: String,
    items: Vec<Cleanup>,
}

impl DbCleanup {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            items: Vec::new(),
        }
    }

    /// Register something to delete. Call it as soon as the row exists, not at the end — the point
    /// is to be already registered when an assertion fails.
    pub fn add(&mut self, item: Cleanup) -> &mut Self {
        self.items.push(item);
        self
    }

    /// Shorthand for the overwhelmingly common case.
    pub fn account(&mut self, id: i64) -> &mut Self {
        self.add(Cleanup::Account(id))
    }
}

impl Drop for DbCleanup {
    fn drop(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let url = std::mem::take(&mut self.url);
        let items = std::mem::take(&mut self.items);

        let spawned = std::thread::Builder::new()
            .name("lldb-test-cleanup".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        eprintln!("test cleanup: could not build a runtime: {error}");
                        return;
                    }
                };
                runtime.block_on(run_cleanup(&url, &items));
            });

        match spawned {
            // The join is the whole point: without it this is a spawn-and-hope, which is the shape
            // rejected above.
            Ok(handle) => {
                if handle.join().is_err() {
                    eprintln!("test cleanup: the cleanup thread panicked; rows may remain");
                }
            }
            Err(error) => eprintln!("test cleanup: could not spawn a thread: {error}"),
        }
    }
}

/// Run every registered `DELETE`, reporting rather than propagating.
///
/// Deliberately does not stop at the first failure: the items are independent, and giving up early
/// would leave more behind than the one that failed.
async fn run_cleanup(url: &str, items: &[Cleanup]) {
    let mut conn = match sqlx::postgres::PgConnection::connect(url).await {
        Ok(conn) => conn,
        Err(error) => {
            eprintln!(
                "test cleanup: could not connect to {}: {error}",
                lldb_qe_core::services::redact_url(url)
            );
            return;
        }
    };
    for item in items {
        if let Err(error) = delete(&mut conn, item).await {
            eprintln!("test cleanup: {error:#}");
        }
    }
    let _ = conn.close().await;
}

async fn delete(conn: &mut sqlx::postgres::PgConnection, item: &Cleanup) -> Result<()> {
    match item {
        Cleanup::Account(id) => {
            sqlx::query("DELETE FROM accounts WHERE id = $1")
                .bind(id)
                .execute(&mut *conn)
                .await
                .with_context(|| format!("deleting test account {id}"))?;
        }
        Cleanup::IcebergCatalog(name) => {
            for table in ["iceberg_tables", "iceberg_namespace_properties"] {
                sqlx::query(&format!("DELETE FROM {table} WHERE catalog_name = $1"))
                    .bind(name)
                    .execute(&mut *conn)
                    .await
                    .with_context(|| format!("cleaning up {table} for catalog {name}"))?;
            }
        }
        Cleanup::CoordinatorSlots(slots) => {
            sqlx::query("DELETE FROM coordinators WHERE slot = ANY($1)")
                .bind(slots)
                .execute(&mut *conn)
                .await
                .context("deleting test coordinator registrations")?;
        }
        Cleanup::AdmissionSlots(warehouse_id) => {
            sqlx::query("DELETE FROM admission_slots WHERE warehouse_id = $1")
                .bind(warehouse_id)
                .execute(&mut *conn)
                .await
                .with_context(|| {
                    format!("deleting admission slots for warehouse {warehouse_id}")
                })?;
        }
    }
    Ok(())
}
