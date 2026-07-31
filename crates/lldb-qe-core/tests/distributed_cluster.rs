//! Cross-container distributed smoke test: bring the whole compose cluster up (MinIO + a
//! worker fleet + coordinator) and prove a query round-trips over **real gRPC/Arrow Flight
//! between containers**, not in-process tokio tasks.
//!
//! This needs a Docker daemon, so it is gated behind `LLDB_DOCKER=1` and skips otherwise —
//! plain `cargo test` on a laptop (or in the fast CI job) stays green without Docker. CI's
//! docker job sets `LLDB_DOCKER=1`.
//!
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test distributed_cluster -- --nocapture
//!
//! # This file is the only thing that runs the real image
//!
//! Which makes it the only place a claim about the *image* is falsifiable. Issue #51 is what that
//! costs when nothing checks: `lldb-qe-auth` was built by the builder stage and never copied into
//! the runtime stage, so `docker-compose.yml`'s `auth-setup` service had an entrypoint that did not
//! exist in the image it ran — and CI stayed green for the whole time, because the test below
//! starts `minio`, `minio-setup`, `worker-1`, `worker-2` and then runs `coordinator`, and none of
//! those five reach `auth-setup`. A missing binary was invisible rather than merely untested.
//!
//! So there are two tests here now: one for the transport, and one for the *contents* of the image.
//! The second one enumerates every binary the cluster invokes by name, so the next binary added
//! without a `COPY` fails here rather than in someone's `docker compose up`.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, MutexGuard};

/// The skip report, shared with the `integration` binary by `#[path]` rather than by a second copy.
///
/// This target is separate for the reasons `tests/integration/main.rs` gives, and none of them is
/// "it gates differently" — it skips silently for want of Docker exactly as the database-gated
/// suites skip for want of Postgres, and issue #112 is about both. Sharing the file is what keeps
/// the two reports one format and the strict switch one rule; a copy here is precisely the second
/// mechanism that would drift.
///
/// One process is one report, so this binary keeps its own `REPORTED` set and prints its own
/// header — which is right, because cargo runs the two binaries as two processes.
#[path = "integration/support/gates.rs"]
mod gates;

/// How this suite names itself in the skip report.
const SUITE: &str = "distributed_cluster";

/// Every binary the compose cluster (and the CDK stack) invokes by name, and therefore every
/// binary the runtime image must contain. Keep in step with `Dockerfile`'s `COPY` list and with the
/// binary targets of the three packages it builds — `lldb-qe-coordinator` (`src/main.rs` +
/// `src/bin/lldb-qe-server.rs`), `lldb-qe-worker`, and `lldb-qe-admin` (`src/bin/`).
///
/// This is a list rather than a directory scan on purpose: the point is to state, in one place, the
/// contract between two files that cannot see each other — the `Dockerfile` and
/// `docker-compose.yml`. A scan would silently accept a binary nobody deploys. It also catches the
/// failure this list's newest hazard produces: the `Dockerfile` names packages with `-p`, so
/// dropping `-p lldb-qe-admin` builds an image missing four of these seven.
const IMAGE_BINARIES: &[&str] = &[
    "lldb-qe-coordinator",
    "lldb-qe-worker",
    "lldb-qe-server",
    "lldb-qe-migrate",
    "lldb-qe-warehouse",
    "lldb-qe-auth",
    "lldb-qe-reap",
];

/// One compose cluster, one host, two tests. `docker compose` is process-global state and the
/// teardown below is `down -v`, so the tests must not interleave: cargo runs a test binary's tests
/// on parallel threads by default and one test's teardown would delete the other's containers
/// mid-run.
static CLUSTER: Mutex<()> = Mutex::new(());

fn exclusive() -> MutexGuard<'static, ()> {
    // A panicking test poisons the lock; the next test still wants to run (and its own teardown
    // will clean up whatever the panicking one left), so take the guard regardless.
    CLUSTER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Run `docker compose <args>` from the repo root, returning (stdout, stderr, success).
fn compose(args: &[&str]) -> (String, String, bool) {
    let out = Command::new("docker")
        .arg("compose")
        .args(args)
        .current_dir(repo_root())
        .output()
        .expect("failed to spawn `docker compose` — is Docker installed?");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

/// `true` when the image was built for us (CI prebuilds it once with layer caching and passes the
/// tag in `LLDB_IMAGE`); `false` when this test has to build it itself.
fn prebuilt() -> bool {
    std::env::var("LLDB_IMAGE").is_ok()
}

/// Skip unless a daemon is expected. Returns the exclusivity guard when the test should proceed.
fn docker_or_skip() -> Option<MutexGuard<'static, ()>> {
    if std::env::var("LLDB_DOCKER").ok().as_deref() != Some("1") {
        gates::skip(SUITE, &gates::DOCKER_CLUSTER);
        return None;
    }
    Some(exclusive())
}

/// Always tear down, even on failure, so a rerun starts clean.
struct Teardown;

impl Drop for Teardown {
    fn drop(&mut self) {
        let _ = compose(&["down", "-v", "--remove-orphans"]);
    }
}

/// Everything a failed `compose up` refuses to tell you on its own.
///
/// `docker compose up` reports only that a dependency "exited (1)" — never *why*. In CI that
/// silence costs a full round trip per guess, because the containers are gone by the time anyone
/// reads the log. So when a cluster assertion fails, attach the per-service logs and the exit
/// codes to the panic message: the failure explains itself the first time.
fn diagnostics() -> String {
    let (ps, ps_err, _) = compose(&["ps", "--all"]);
    let (logs, logs_err, _) = compose(&["logs", "--no-color", "--tail", "80"]);
    format!(
        "\n\n---- docker compose ps ----\n{ps}{ps_err}\n---- docker compose logs ----\n{logs}{logs_err}"
    )
}

#[test]
fn cluster_ships_a_query_across_containers() {
    let Some(_exclusive) = docker_or_skip() else {
        return;
    };
    let _teardown = Teardown;

    // Bring MinIO + workers up (detached). The coordinator is one-shot, so we start the
    // long-running services first, then run the coordinator to completion.
    //
    // When `LLDB_IMAGE` is set the image was already built (CI prebuilds it once with layer
    // caching and passes the tag), so we skip `--build` and reuse it. Locally, with no prebuilt
    // image, we build here.
    let mut up = vec!["up", "-d"];
    if !prebuilt() {
        up.push("--build");
    }
    up.extend(["minio", "minio-setup", "worker-1", "worker-2"]);
    let (_o, e, ok) = compose(&up);
    assert!(ok, "compose up failed:\n{e}{}", diagnostics());

    // Run the coordinator to completion and capture its printed result table.
    let (stdout, stderr, ok) = compose(&["run", "--rm", "coordinator"]);
    assert!(
        ok,
        "coordinator run failed:\nstdout:\n{stdout}\nstderr:\n{stderr}{}",
        diagnostics()
    );

    // The default query is `SELECT 42 AS answer, 'distributed hello' AS greeting`, shipped to a
    // worker over Flight and streamed back — proving the cross-container path.
    assert!(
        stdout.contains("42") && stdout.contains("distributed hello"),
        "expected the query result in coordinator output, got:\n{stdout}"
    );
}

/// Issue #51: the image must actually contain every binary the cluster invokes.
///
/// Two assertions, in order of generality.
///
/// 1. **Every binary in `IMAGE_BINARIES` answers `--version` from inside the image.** This is the
///    one that generalises: it costs no database and no fleet, it names the binary that is missing
///    rather than reporting "exited (1)", and it catches the *next* binary added to `src/bin/`
///    without a matching `COPY`. `--version` rather than `--help` because every one of these
///    binaries stamps `version+git-sha`, so a pass also says the copied binary is a real, current
///    build and not something stale on the `PATH`.
/// 2. **`auth-setup` — the service the issue was actually about — runs to completion.** The
///    `--version` sweep proves the file is present; this proves the demo bootstrap compose ships
///    works end to end against a real Postgres in the real image: create the user, create the role,
///    assign it, write three grants, print them. It is cheap here because `compose run` starts its
///    own dependency chain (`postgres` → `db-migrate` → `warehouse-setup`), which the other test
///    already pays for anyway via `coordinator`.
#[test]
fn image_contains_every_binary_the_cluster_invokes() {
    let Some(_exclusive) = docker_or_skip() else {
        return;
    };
    let _teardown = Teardown;

    // One build for the whole test. `worker-1` is an arbitrary choice — every service here shares
    // the same `x-lldb-build` anchor and the same image tag, so building one builds the image.
    if !prebuilt() {
        let (_o, e, ok) = compose(&["build", "worker-1"]);
        assert!(ok, "compose build failed:\n{e}");
    }

    // ---- 1. every binary is present and reports its build -----------------------------------
    //
    // `--no-deps` so this needs nothing but the image itself: no Postgres, no MinIO, no fleet.
    // `--entrypoint` rather than a trailing command because some services (`auth-setup`,
    // `warehouse-setup`) define an `entrypoint` of their own, and a `run` command would be appended
    // to it as arguments rather than replacing it.
    for binary in IMAGE_BINARIES {
        let (stdout, stderr, ok) = compose(&[
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            binary,
            "worker-1",
            "--version",
        ]);
        assert!(
            ok,
            "`{binary}` is not runnable in the image — is it missing from the Dockerfile's \
             COPY list?\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        // clap prints `<name> <version>`, and the name is set per binary, so this also catches a
        // COPY that landed the wrong binary under the right filename.
        assert!(
            stdout.contains(binary),
            "`{binary} --version` did not identify itself:\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        eprintln!("image contains {}", stdout.trim());
    }

    // ---- 2. the service the issue was about actually runs -------------------------------------
    let (stdout, stderr, ok) = compose(&["run", "--rm", "auth-setup"]);
    assert!(
        ok,
        "the `auth-setup` one-shot failed:\nstdout:\n{stdout}\nstderr:\n{stderr}{}",
        diagnostics()
    );
    // Its last step is `lldb-qe-auth show`, so the demo user, the role it was assigned and the
    // grants written to it all have to appear.
    for expected in ["demo", "analyst", "USERS", "GRANTS"] {
        assert!(
            stdout.contains(expected),
            "`auth-setup` did not report `{expected}`:\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
}
