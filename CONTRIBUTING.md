# Contributing to lldb

Thanks for your interest in lldb, a distributed analytical query engine written in Rust. Contributions of all kinds are welcome: bug reports, fixes, features, docs, and ideas.

## License

lldb is licensed under the [Apache License, Version 2.0](LICENSE). Contributions are accepted under the same license. By submitting a contribution, you agree that your work is licensed under Apache-2.0 and that you have the right to submit it.

## Workflow: Trunk-Based Development

We practice trunk-based development. `main` is always releasable.

- **Maintainers** commit directly to `main` or use short-lived branches that merge back quickly.
- **Everyone else** is welcome to open a pull request from a short-lived branch. Keep changes small and focused so they're easy to review and merge.
- Rebase on the latest `main` before opening or updating a PR. Avoid long-running branches — they drift and rot.

## Building and Testing

New checkout? Run `./scripts/bootstrap.sh` first — it installs the TPC-H data generator and generates the benchmark data the test suite needs.

```sh
cargo build                                      # build
cargo test                                       # run the test suite
cargo fmt --all                                  # format (run before committing)
cargo clippy --all-targets --all-features        # lint (keep it warning-clean)
```

Please make sure `cargo fmt --all`, `cargo clippy --all-targets --all-features`, and `cargo test` all pass before you push.

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
