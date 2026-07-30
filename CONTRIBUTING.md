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

```sh
cargo build                                  # build
cargo test                                   # run the test suite
cargo fmt --all                              # format (run before committing)
cargo clippy --workspace --all-targets       # lint (keep it warning-clean)
```

Please make sure `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
and `cargo test --workspace` all pass before you push. Those are exactly what CI runs.

Two things that surprise people:

- **A green `cargo test` does not mean everything ran.** Suites that need a service — a Postgres
  services database, Docker — skip *silently* without it. The tell is timing: the `integration`
  binary takes ~20 s with a database and under 2 s without. The prerequisites for each gated suite
  are listed in `.claude/skills/lldb-commands/SKILL.md`.
- **Do not override the build profile on the command line.** `[profile.dev]` in the root
  `Cargo.toml` already sets `debug = 0` and `incremental = false`; passing `CARGO_PROFILE_DEV_DEBUG`
  or `CARGO_INCREMENTAL` yourself invalidates the whole target directory and forces a full rebuild.
  See [`docs/build-performance.md`](docs/build-performance.md).

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
