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

    // Build images and bring MinIO + workers up (detached). The coordinator is one-shot, so we
    // start the long-running services first, then run the coordinator to completion.
    let (_o, e, ok) = compose(&[
        "up",
        "-d",
        "--build",
        "minio",
        "minio-setup",
        "worker-1",
        "worker-2",
    ]);
    assert!(ok, "compose up failed:\n{e}");

    // Run the coordinator to completion and capture its printed result table.
    let (stdout, stderr, ok) = compose(&["run", "--rm", "coordinator"]);
    assert!(
        ok,
        "coordinator run failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The default query is `SELECT 42 AS answer, 'distributed hello' AS greeting`, shipped to a
    // worker over Flight and streamed back — proving the cross-container path.
    assert!(
        stdout.contains("42") && stdout.contains("distributed hello"),
        "expected the query result in coordinator output, got:\n{stdout}"
    );
}
