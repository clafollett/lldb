# Contributing to lldb

Thanks for your interest in lldb, a distributed analytical query engine written in Rust. Contributions of all kinds are welcome: bug reports, fixes, features, docs, and ideas.

## License

lldb is licensed under the [Apache License, Version 2.0](LICENSE). Contributions are accepted under the same license. By submitting a contribution, you agree that your work is licensed under Apache-2.0 and that you have the right to submit it.

## Workflow: Branch and Pull Request

Every change lands through a pull request. `main` is always releasable, and **nothing is pushed
straight to it** — not by contributors, not by maintainers.

```
feature branch → pull request → review → green CI → squash-merge to main
```

- Branch from the latest `main`. Maintainer branches are named `claude/issue-<n>-<slug>`; anything
  descriptive is fine for an outside contribution.
- Open a pull request and let CI finish. Every PR gets reviewed — Copilot code review at minimum.
- Keep changes small and focused, and rebase on `main` before opening or updating a PR. Avoid
  long-running branches; they drift and rot.
- Squash-merge, so `main` keeps one commit per change.

## Building and Testing

New checkout? Run `./scripts/bootstrap.sh` first — it installs the TPC-H data generator and generates the benchmark data the test suite needs.

Run `cargo fmt --all` to *fix* formatting as you work. Before you push, run the gates — these are
verbatim what CI runs, and all of them must pass:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --features lldb-qe-core/benches -- -D warnings
cargo test --workspace
./scripts/check-path-refs.sh
./scripts/check-dep-dupes.sh
```

The last two are cheap and catch things review reliably misses. `check-path-refs.sh` asserts that
every `crates/<pkg>/…` path mentioned in a tracked file actually exists — module docs carry this
project's design rationale, and `git mv` renames files without rewriting the strings pointing at
them, so a crate split silently rots every reference into it (#90). It runs on **every** PR, not
just Rust ones, because those references live in `.md`, `.yml`, `.toml` and `.sql`. A path that is
deliberately historical — `docs/build-performance.md` records commands run at a named commit — opts
out with `path-refs-allow: <that path>` somewhere in the same file.

`check-dep-dupes.sh` asserts zero duplicate versions of `arrow`, `datafusion`, `object_store` and
`iceberg`. That is the constraint `CLAUDE.md` actually imposes; it replaced a hand-maintained
duplicate *total* that had gone stale and could not express the rule anyway (#78).

Five things that surprise people:

- **The benches are behind a feature, and the clippy gate is what keeps them compiling.** Both
  `[[bench]]` targets carry `required-features = ["benches"]`, so a default build no longer links
  their 629 MB of binaries — `cargo bench` needs `-p lldb-qe-core --features benches` or it builds
  nothing and reports nothing. `--features lldb-qe-core/benches` above is not optional: drop it and
  a bench that stops compiling goes unnoticed. See
  [`docs/build-performance.md`](docs/build-performance.md).

- **A green `cargo test` does not mean everything ran.** Suites that need a service — a Postgres
  services database, Docker — skip *silently* without it. The tell is timing: the `integration`
  binary takes ~20 s with a database and under 2 s without. The prerequisites for each gated suite
  are listed in `.claude/skills/lldb-commands/SKILL.md`.
- **Do not override the build profile on the command line.** `[profile.dev]` in the root
  `Cargo.toml` already sets `debug = 0` and `incremental = false`; passing `CARGO_PROFILE_DEV_DEBUG`
  or `CARGO_INCREMENTAL` yourself invalidates the whole target directory and forces a full rebuild.
  See [`docs/build-performance.md`](docs/build-performance.md).

- **On macOS, every fat binary links with an `__eh_frame` warning, and that is expected.** It means
  debug-build unwinding takes the slower path; correctness is unaffected. **No gate catches it** —
  `cargo clippy` never links, so `-D warnings` there structurally cannot see a linker message, and
  CI runs on Linux where this particular warning does not fire. Do not add `RUSTFLAGS=-Dwarnings` to
  a build step without reading
  [`docs/build-performance.md`](docs/build-performance.md) first: it would be green on CI and red on
  every Mac (#105).

- **If you use git worktrees, sweep them.** Each one is a full checkout with **its own `target/`**,
  which is the point of the isolation and also why they run 3–7 GB apiece. Nothing removes them when
  the PR merges, and eight left behind had accumulated **36 GB** (#107) — more than the whole disk
  budget the build-size work was optimising against.

  ```sh
  ./scripts/sweep-worktrees.sh          # dry run: what would go, and how much it frees
  ./scripts/sweep-worktrees.sh --apply
  ```

  It removes a worktree only when its branch has a **merged PR** and the tree is clean, and never
  one that is `locked`. Merged-ness comes from `gh pr list`, not `git branch --merged` — branches
  here land by squash merge, which `git branch --merged` reports as *unmerged*.

## Dependencies

`datafusion`, `arrow`, `object_store`, `iceberg` and `sqlx` are pinned and move **together**, never
independently — the Arrow Flight plan-serialization boundary requires one version of each tree-wide.
Before adding any dependency, check `cargo tree -d` and confirm it introduces no second copy of any
of them. The rationale is in the root [`Cargo.toml`](Cargo.toml) and `CLAUDE.md`.

## Commit Messages

Write concise, imperative commit messages — describe what the commit does, not what you did.

```
Add columnar scan operator
Fix panic on empty partition
Speed up hash join build phase
```

Keep the subject line short; add a body only when the change needs context.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
