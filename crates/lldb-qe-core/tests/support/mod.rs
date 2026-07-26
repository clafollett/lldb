//! Finding a Postgres to test against, in the three ways that can work.
//!
//! Two integration tests now need a real server — `services_db.rs` (migrations, accounts, the
//! foreign keys) and `warehouse_lifecycle.rs` (the warehouse API and its transitions) — and they
//! must agree exactly on *which* server and on how to skip when there is none. So the resolution
//! lives here rather than being copied, in the order the first one that works wins:
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
//! instance, and in CI both test binaries share one container. Hence [`unique_name`]: a pid +
//! nanosecond suffix, so no run can collide with another, and each test deletes exactly the rows
//! it made and nothing global.
//!
//! `dead_code` is allowed because each integration test binary compiles this module separately
//! and legitimately uses only the parts it needs.
#![allow(dead_code)]

use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

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
