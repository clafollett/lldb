//! Cross-container distributed smoke test: bring the whole compose cluster up (MinIO + a
//! worker fleet + coordinator) and prove a query round-trips over **real gRPC/Arrow Flight
//! between containers**, not in-process tokio tasks.
//!
//! This needs a Docker daemon, so it is gated behind `LLDB_DOCKER=1` and skips otherwise —
//! plain `cargo test` on a laptop (or in the fast CI job) stays green without Docker. CI's
//! docker job sets `LLDB_DOCKER=1`.
//!
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test distributed_cluster -- --nocapture

use std::path::PathBuf;
use std::process::Command;

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
    if std::env::var("LLDB_DOCKER").ok().as_deref() != Some("1") {
        eprintln!("SKIP: set LLDB_DOCKER=1 (and have a Docker daemon) to run the cluster test");
        return;
    }

    // Always tear down, even on failure, so a rerun starts clean.
    struct Teardown;
    impl Drop for Teardown {
        fn drop(&mut self) {
            let _ = compose(&["down", "-v", "--remove-orphans"]);
        }
    }
    let _teardown = Teardown;

    // Bring MinIO + workers up (detached). The coordinator is one-shot, so we start the
    // long-running services first, then run the coordinator to completion.
    //
    // When `LLDB_IMAGE` is set the image was already built (CI prebuilds it once with layer
    // caching and passes the tag), so we skip `--build` and reuse it. Locally, with no prebuilt
    // image, we build here.
    let prebuilt = std::env::var("LLDB_IMAGE").is_ok();
    let mut up = vec!["up", "-d"];
    if !prebuilt {
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
