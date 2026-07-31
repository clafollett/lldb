//! Stamp the build with its git commit so a running binary can report exactly which build it
//! is. The coordinator and every worker MUST be the identical build — serialized DataFusion
//! physical plans are not cross-version compatible — and a visible `version+sha` is the first,
//! cheapest way for an operator to confirm the whole fleet matches.

use std::process::Command;

fn main() {
    // Prefer an explicitly-injected SHA: Docker image builds pass it as a build arg because the
    // `.git` directory is excluded from the build context (see `.dockerignore`). Fall back to
    // asking git for local `cargo build`, and never fail the build if neither is available.
    println!("cargo:rerun-if-env-changed=LLDB_GIT_SHA");
    let sha = std::env::var("LLDB_GIT_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(git_short_sha)
        .unwrap_or_else(|| "unknown".to_string());

    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    println!("cargo:rustc-env=LLDB_GIT_SHA={sha}");
    println!("cargo:rustc-env=LLDB_BUILD_VERSION={version}+{sha}");
}

fn git_short_sha() -> Option<String> {
    // Re-stamp when HEAD moves; harmless if the path doesn't exist (e.g. inside Docker).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    let out = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let sha = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!sha.is_empty()).then_some(sha)
}
