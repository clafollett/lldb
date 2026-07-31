//! Stamp the build with its git commit so a running binary can report exactly which build it
//! is. The coordinator and every worker MUST be the identical build — serialized DataFusion
//! physical plans are not cross-version compatible — and a visible `version+sha` is the first,
//! cheapest way for an operator to confirm the whole fleet matches.

use std::path::{Path, PathBuf};
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
    watch_files_that_move_the_commit();
    git_stdout(&["rev-parse", "--short=12", "HEAD"])
}

/// Tell cargo which files must change before re-running this script. Two rules shape every line
/// below, and both were learned the hard way (issue #87).
///
/// **A `rerun-if-changed` path that does not exist does not mean "no trigger" — it means "always
/// stale".** Cargo re-runs the script, and rebuilds this crate and everything downstream, on
/// *every* build. The previous hard-coded `../../.git/HEAD` therefore cost a full downstream
/// rebuild per invocation in a linked worktree, where `.git` is a *file* pointing at
/// `.git/worktrees/<name>` and that path does not exist. So every path here is emitted only once
/// it is confirmed to exist, and comes from `git rev-parse --git-path`, which answers correctly
/// in both layouts.
///
/// **`HEAD` alone misses commits.** It holds a symbolic ref (`ref: refs/heads/main`), so it is
/// rewritten when the branch changes and left untouched when a commit is made — the file a commit
/// writes is the branch ref. Watching only `HEAD` means the stamp keeps reporting the *previous*
/// commit, which is worse than no stamp at all: the fleet-match check this exists for would then
/// report agreement between two builds that are not the same.
fn watch_files_that_move_the_commit() {
    // No git (the Docker image excludes `.git`): nothing to watch, and `git_short_sha` is about
    // to return None anyway.
    let Some(head) = git_path("HEAD") else {
        return;
    };
    emit_rerun_if_exists(&head);

    // Detached HEAD has no branch ref — `--symbolic-full-name` answers "HEAD" — and the commit
    // cannot move without rewriting HEAD itself, so watching HEAD alone is complete there.
    let Some(branch) = git_stdout(&["rev-parse", "--symbolic-full-name", "HEAD"]) else {
        return;
    };
    if branch == "HEAD" {
        return;
    }

    let Some(branch_ref) = git_path(&branch) else {
        return;
    };
    if emit_rerun_if_exists(&branch_ref) {
        return;
    }

    // The loose ref is missing, so this branch is packed: `git gc` / `git pack-refs` folds refs
    // into `packed-refs` and deletes the loose files. Watch `packed-refs` instead, rather than
    // emit the loose path anyway — that would resurrect the every-build rebuild above, a certain
    // and permanent cost paid to cover an uncommon gap.
    //
    // The gap, stated plainly because it was measured rather than assumed: a commit writes a
    // *new loose ref* and leaves `packed-refs` untouched, so a commit made while the branch is
    // packed is not seen — and it stays unseen, since this script does not re-run and so never
    // notices the loose ref that now exists. It takes a change to `HEAD` or `packed-refs` (a
    // branch switch, a fetch, another pack), a change to `LLDB_GIT_SHA`, or an edit to this file
    // to recover. Only `git gc` / `git pack-refs` puts a repo into that state, so if a stamp
    // looks stale in a repo that has just been packed, this is why.
    if let Some(packed) = git_path("packed-refs") {
        emit_rerun_if_exists(&packed);
    }
}

/// Resolve a path inside the *real* git directory: `.git/HEAD` in a clone,
/// `<repo>/.git/worktrees/<name>/HEAD` in a linked worktree. Git answers relative to its working
/// directory, which is this script's — the package root — and cargo reads relative
/// `rerun-if-changed` paths from that same directory, so the answer is usable as printed whether
/// git makes it relative or absolute.
fn git_path(relative_to_git_dir: &str) -> Option<PathBuf> {
    git_stdout(&["rev-parse", "--git-path", relative_to_git_dir]).map(PathBuf::from)
}

/// Returns whether the path existed, since "the loose ref is missing" is a case the caller acts on.
fn emit_rerun_if_exists(path: &Path) -> bool {
    let exists = path.exists();
    if exists {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    exists
}

/// Trimmed stdout of a successful `git`, or None — never an error. git may be missing entirely,
/// and a build stamp is not worth failing a build over.
fn git_stdout(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let value = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!value.is_empty()).then_some(value)
}
