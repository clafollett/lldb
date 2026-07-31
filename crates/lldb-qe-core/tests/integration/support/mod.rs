//! What a test in this binary borrows, and who gives it back.
//!
//! Four things live here. The database resolution described below; the teardown guards
//! ([`Servers`], [`DbCleanup`]) that hand back the servers and rows a test owns however the test
//! ended; [`certs`], the one throwaway certificate authority the TLS tests share; and [`gates`],
//! which reports the suites that did not run at all. The first three belong together because they
//! answer the same question from different ends: a test in a *single* binary borrows from a process
//! that outlives it, so everything it takes it must give back.
//!
//! [`gates`] is here because it is the other side of the resolution below — every `Skipped` this
//! module hands back is a suite that will not run, and until issue #112 that fact was written with
//! `eprintln!`, which libtest's output capture discards for a *passing* test. It is the one file
//! here that is **also compiled into `distributed_cluster`** (by `#[path]`), because that target
//! skips silently for want of Docker in exactly the same way.
//!
//! **Every** database-gated integration test resolves its database through this module —
//! `services_db` (migrations, accounts, the foreign keys), `warehouse_lifecycle` (the warehouse API
//! and its transitions), `query_scheduler`, `auth_rbac`, `tenant_catalogs`, `coordinator_liveness`,
//! `query_reaper`, `dml_snapshots`, `result_cache_db` and `shared_sql_catalog` — and they must
//! agree exactly on *which* server and on how to skip when there is none. The order is
//! first-one-that-works:
//!
//! 1. **`LLDB_TEST_POSTGRES_URL`** — use it as-is. CI's path (the `check` job runs a
//!    `postgres:18.4-alpine` service container) and the path for anyone with a local server.
//! 2. **`LLDB_DOCKER=1`** — start a throwaway `postgres:18.4-alpine` on an ephemeral host port,
//!    wait for `pg_isready`, and remove it afterwards no matter how the test ends.
//! 3. **Neither** — the caller reports the skip through [`gates::skip`] and passes. `cargo test` on
//!    a laptop with no Postgres and no Docker must stay green, the same bargain
//!    `distributed_cluster.rs` strikes — unless [`gates::REQUIRE_GATED_ENV`] is set, which is the
//!    opt-in that turns that skip into a failure.
//!
//! The last three joined that list late (issue #121), and the reason they were ever outside it is
//! worth one line: each was once its own test *binary*, where depending on this module was opt-in
//! and copying the resolution looked cheap, so each carried a private `Target`, `Drop` guard and
//! container bootstrap. Collapsing the binaries into one (see `main.rs`) put four implementations of
//! "find me a Postgres" side by side in a single compilation unit, and then
//! [`gates::REQUIRE_GATED_ENV`] raised the stakes: a missed detection is a *failed job* now rather
//! than a quiet skip, so a copy drifting from this one could fail the build for the wrong reason.
//!
//! What the fold dropped was their distinct container-name prefixes, and that turned out to be load
//! bearing in a way worth recording: the prefixes were partitioning a name space that [`NAME_SEQ`]
//! documents as too small, so folding them into one exposed a latent tie. Names carry that counter
//! now, which is strictly stronger than any prefix scheme — what is genuinely lost is only that
//! `docker ps` no longer says which suite owns which container.
//!
//! Every test using this must be safe to run repeatedly against the same database *and*
//! concurrently with another copy of itself — the URL it is handed may well be someone's dev
//! instance, and in CI every database-gated test shares one container. Hence [`unique_name`]: a
//! pid + nanosecond + [`NAME_SEQ`] suffix, so no run can collide with another **and** no two
//! callers within a run can collide with each other (#141 — the clock alone did not manage the
//! second), and each test deletes exactly the rows it made and nothing global.
//!
//! This module used to carry `#![allow(dead_code)]`, because it was compiled separately into each
//! integration-test binary and each of them legitimately used only the parts it needed. That
//! stopped being true when those binaries became modules of one (see `main.rs`): there is now a
//! single copy, every item in it has a caller, and the allow — which would have hidden a genuinely
//! unused helper — is gone. Do not put it back to silence a warning; delete the helper instead.

pub mod certs;
pub mod gates;

use std::future::Future;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use sqlx::Connection;

/// Image the ephemeral container runs — the same version compose and CI use, so "it passed
/// locally" and "it passed in CI" mean the same thing.
pub const POSTGRES_IMAGE: &str = "postgres:18.4-alpine";
/// How long to wait for a fresh container to accept connections before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// What tells two names apart when the clock cannot — container names and [`unique_name`] alike.
///
/// [`nanos`] carries `SystemTime`'s resolution, and on macOS that is **microseconds** — the last
/// three digits are always zero. Threads reaching either caller inside one of those ticks tie on
/// it, and the two ties fail very differently. `docker run --name` refuses the loser with a name
/// conflict, which is loud: while the three private copies of the database resolution existed
/// (#121) each had its own container-name prefix, partitioning the space and hiding the tie, and
/// one prefix shared by every suite exposed it reproducibly at six concurrent containers. Two
/// tests handed the same database, namespace, table or warehouse name do **not** fail at creation
/// — they interfere, and the failure lands on whichever runs second (#141). That one measured
/// 7497 duplicates in 8192 names across 16 threads, and had never been noticed.
///
/// A counter removes the tie rather than making it rarer: two names minted by one process differ
/// here even inside one microsecond, and the pid still separates processes.
///
/// **One counter, not one per caller.** The two name spaces are disjoint by prefix, so a second
/// counter would be correct too; what it would cost is a second entry in `main.rs`'s audit of
/// process-global state carrying a verbatim copy of this argument, which is the shape #121 spent
/// itself removing. Sharing is free because the value is a discriminator and not a count — nothing
/// reads it, so the gaps each caller leaves in the other's run of integers are unobservable.
///
/// It is process-global mutable state, which `main.rs` is deliberately suspicious of — safe
/// because it is monotonic, read by nothing but the names it builds, and asserted on by no test,
/// so ordering permutes which caller gets which integer and nothing else.
static NAME_SEQ: AtomicU64 = AtomicU64::new(0);

/// How the test got its database — and, for the container case, what to tear down.
pub enum Target {
    /// Nothing available; the caller reports a skip through `support::gates` and passes.
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
    let name = format!(
        "lldb-services-test-{}-{}-{}",
        std::process::id(),
        nanos(),
        NAME_SEQ.fetch_add(1, Ordering::Relaxed)
    );

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
        // `-h 127.0.0.1` is load bearing: probe the **same transport the caller will use**.
        //
        // Without it `pg_isready` uses the container's unix socket, and the official postgres
        // entrypoint runs a *temporary* server on that socket — `listen_addresses` empty, so no
        // TCP at all — while it runs initdb and the init scripts. The socket therefore goes green
        // before the real server exists: 227 ms of window, measured on this image. Inside it this
        // function would hand back a `postgres://127.0.0.1:<port>` URL pointing at a docker-proxy
        // with nothing behind it, the caller's connect would be closed at once, and sqlx would
        // report `expected to read 5 bytes, got 0 bytes at EOF` — which reads as a database bug,
        // lands on whichever tests happen to start under load, and moves between runs.
        let probe = Command::new("docker")
            .args([
                "exec",
                &name,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-U",
                "lldb",
                "-d",
                "lldb",
            ])
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

/// A name no other run — or concurrent copy of this run, or concurrent *caller* — will pick.
///
/// The pid separates processes and [`nanos`] separates runs; [`NAME_SEQ`] is what separates two
/// libtest threads calling this inside one tick of a clock whose real resolution is microseconds.
/// Read that static before dropping the counter as redundant: without it this returns the same
/// string to concurrent callers routinely rather than rarely, and the tests that receive it go on
/// to create one account, namespace, table or warehouse between them and quietly interfere.
///
/// Warehouse names must also be DNS labels (lowercase, `[a-z0-9-]`), which is why this uses only
/// characters legal in both an account name and a warehouse name — the counter is decimal digits
/// and stays inside that alphabet.
pub fn unique_name(tag: &str) -> String {
    format!(
        "lldb-test-{tag}-{}-{}-{}",
        std::process::id(),
        nanos(),
        NAME_SEQ.fetch_add(1, Ordering::Relaxed)
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};

    /// The property [`unique_name`]'s name claims, asserted the way it actually breaks.
    ///
    /// Sequentially it holds on every platform and always did; the case that matters is two callers
    /// inside one tick of whatever clock the name is built from, which is what a barrier plus
    /// `THREADS` minting threads produces deliberately. Against the pid+nanosecond name alone this
    /// failed on macOS at these sizes, because `SystemTime` there resolves to microseconds. The
    /// message counts the duplicates rather than naming one, because the count is the diagnosis: a
    /// handful means a coarse clock, thousands means the discriminator is gone entirely.
    #[test]
    fn concurrent_unique_names_never_collide() {
        const THREADS: usize = 16;
        const PER_THREAD: usize = 512;

        let barrier = Arc::new(Barrier::new(THREADS));
        let minting: Vec<_> = (0..THREADS)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    (0..PER_THREAD)
                        .map(|_| unique_name("collision"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let minted: Vec<String> = minting
            .into_iter()
            .flat_map(|thread| thread.join().expect("a minting thread panicked"))
            .collect();
        let distinct: BTreeSet<&str> = minted.iter().map(String::as_str).collect();

        assert_eq!(
            distinct.len(),
            minted.len(),
            "{} of {} concurrently minted names were duplicates — two tests handed the same name \
             for a database, namespace, table or warehouse do not fail at creation, they interfere",
            minted.len() - distinct.len(),
            minted.len(),
        );
    }

    /// Every character has to be legal in an account name, a warehouse name **and** a DNS label, so
    /// no discriminator added to the name may widen its alphabet.
    #[test]
    fn a_unique_name_stays_a_dns_label() {
        let name = unique_name("tag");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "not a DNS label: {name}"
        );
    }
}
