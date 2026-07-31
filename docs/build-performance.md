# Build performance — the baseline, and what has already been tried

This file exists so nobody re-derives the same numbers, and so the next change to the build
configuration can be measured against something instead of asserted. Issue #44 is the parent.

Every figure in the baseline section was taken on the machine the issue was filed from, with
nothing else running. The consolidation section that follows it was measured later, on a box of the
same shape that ran ~28% slower — it re-measures its own before column rather than borrowing this
one, and says so at length, because a table of numbers from two machines is worse than no table:

| | |
| - | - |
| cores | 4 |
| RAM | 15 GB |
| toolchain | 1.97.1 (pinned in `rust-toolchain.toml`) |
| profile | `[profile.dev] debug = 0, incremental = false`; `[profile.test]` inherits |
| workspace | 439 crates compiled cold; 24 integration tests + 2 benches in `lldb-qe-core` (the 24 became 2 — see below) |

**Run cargo plainly when reproducing these.** Do not export `CARGO_PROFILE_DEV_DEBUG` or
`CARGO_INCREMENTAL` — they are the committed profile by another route, and a build whose profile
differs from the previous one invalidates the whole target directory. See CLAUDE.md.

**Check that the build stamp fingerprints in the layout you are measuring in — the layout is part
of the protocol.** `lldb-qe-control/build.rs` tells cargo which files must change before it re-runs,
and cargo treats a `rerun-if-changed` path that does *not* exist as permanently stale. Until issue
#87 that path was a hard-coded `../../.git/HEAD`, which does not exist in a linked git worktree
(`.git` is a file there), so the crate and everything downstream recompiled on every invocation: a
no-op `cargo build --workspace` cost **18.7 s in a worktree versus 0.19 s after the fix**, on the
same box in the same session. Any warm number taken in a worktree before that fix was measuring the
bug, and cannot be compared with one taken in a clone. The cheap check before trusting a warm run:
two consecutive no-op builds, and the second must recompile nothing —
`CARGO_LOG=cargo::core::compiler::fingerprint=info` names the culprit when it does.

## Baseline (`main` at 265f2af, no `.cargo/config.toml`)

The warm-cycle rows are the ones that matter: the edit-test loop pays them, and dependencies are
already cached by then. Each was preceded by `touch crates/lldb-qe-core/src/lib.rs`, which forces
`lldb-qe-core` and everything downstream to recompile and relink. `--no-run` isolates compile+link
from test execution.

| Metric | Runs | Notes |
| - | - | - |
| Cold `cargo build --workspace --all-targets` (after `cargo clean`) | 264 s, 235 s | see the note on the 264 s outlier below |
| Warm `cargo test --workspace --no-run` | 63.1 s, 61.7 s, 55.9 s | |
| Warm `cargo clippy --workspace --all-targets -- -D warnings` | 116.9 s, 26.1 s, 27.4 s | the 116.9 s is clippy's *own* first pass over the dependency graph — clippy fingerprints separately from `cargo build`, so it pays a one-off cold pass. 26–27 s is the real warm number |
| `du -sh target` after `--all-targets` | 9.8 GB | 11 GB once `cargo test` and `cargo clippy` have each added their artifacts |

The two cold builds differ by 29 s (11%). Both were `cargo clean` + full rebuild with identical
inputs, so that is the run-to-run variance of a cold build on this box, most of it page cache. It
is worth stating plainly because it is larger than any linker effect measured below — a single cold
sample cannot resolve a 10% change here.

## Where the warm cycle goes

Method: `cargo build --timings --workspace --all-targets` after the same touch. Cargo reports
per-unit wall time, so this attributes cost to *targets*, not to phases. Wall clock for that build
was 63 s; the units sum to 189.7 s of CPU across 4 cores.

| Unit category | n | CPU | share |
| - | - | - | - |
| integration test binaries | 24 | 89.2 s | 47.0% |
| `lldb-qe-core` lib unit-test binary | 1 | 33.2 s | 17.5% |
| binaries (`lldb-qe-*` + their unit-test binaries) | 12 | 30.0 s | 15.8% |
| `lldb-qe-core` lib | 1 | 29.0 s | 15.3% |
| bench binaries | 2 | 8.3 s | 4.4% |

Mean per integration test binary: 3.72 s under 4-way contention.

## How much of that is linking

Cargo's `--timings` will not split a unit into codegen and link, and the `-Z` flags that would are
nightly-only while the toolchain is pinned to stable. Two methods instead, neither perfect:

**1. Replay the real link command.** `cargo rustc -p lldb-qe-core --test first_light -- -C
save-temps --print link-args` emits the exact `cc` invocation and `save-temps` keeps the temporary
inputs alive, so the link step can be re-run standalone and timed. This is *uncontended*, one
binary, so it measures a link step cleanly but not the fleet of them competing for 4 cores.

**2. Touch one test file.** `touch crates/lldb-qe-core/tests/first_light.rs` then
`cargo test -p lldb-qe-core --no-run --test first_light`: 2.7 s / 2.7 s / 2.8 s. That is one test
file compiled plus one 294 MB binary linked. (Both paths are as they were at 265f2af; that file is
now `tests/integration/first_light.rs` and is no longer a target of its own — which is exactly the
change this measurement motivated.)

Method 1 puts the link at ~2.0 s, so method 2 leaves ~0.7 s for compiling the test file itself —
the two agree. Extrapolating: 26 linked test/bench binaries × ~2.0 s ≈ **52 s of the 189.7 s of
unit CPU, about 27%**; at 4-way parallelism roughly 13 s of the ~58 s warm wall clock, about 22%.

Be honest about what that does and does not prove. It is an extrapolation from one binary, the
binaries are not all the same size, and a link under contention is slower than the isolated 2.0 s
measured. Treat "roughly a quarter of the warm cycle is linking" as the right order of magnitude
and not a precise figure.

One consequence worth carrying forward: **`cargo clippy --all-targets` does not link at all.** It
runs the compiler front end and stops, which is the whole reason the clippy gate is 26 s while the
test gate is 58 s. Anything that removes targets from the clippy path saves *compile* time only.

## Rejected: a `.cargo/config.toml` selecting lld

Issue #44's first proposal was
`[target.x86_64-unknown-linux-gnu] rustflags = ["-C", "link-arg=-fuse-ld=lld"]`. **It was measured
and reverted, because rustc already does exactly this.**

`rustc --print link-args` shows the stock 1.97.1 invocation on `x86_64-unknown-linux-gnu` already
carries `-B <sysroot>/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld -fuse-ld=lld`. The `-B` shim
directory is what makes `-fuse-ld=lld` resolve to the toolchain's bundled `rust-lld` rather than to
anything on the system. Adding the flag a second time appends a duplicate argument that resolves to
the same linker: the resulting binary is **byte-for-byte identical** (`cmp` clean, 294,123,272
bytes, both stamped `Linker: LLD 22.1.6` in `.comment`).

Timed link steps for one real test binary, 3 runs each, isolated:

| Linker | Runs | |
| - | - | - |
| bundled `rust-lld` 22.1.6 — the stock default | 1960 ms, 2030 ms, 1974 ms | |
| the proposed rustflags | 2235 ms, 2083 ms, 2101 ms | identical output; the extra millisecond or two is `cc` parsing one more argument |
| system `lld` 18.1.3 (`/usr/bin/ld.lld`, reached by dropping the `-B` shim) | 2110 ms, 2111 ms, 1996 ms | older than the bundled one, no faster, and an external dependency |
| GNU `ld` (bfd) 2.42 | 18463 ms, 16129 ms, 16029 ms | **8x slower** |

Full-workspace numbers with the config applied, same protocol as the baseline:

| Metric | Baseline | With the lld config |
| - | - | - |
| Cold `cargo build --workspace --all-targets` | 264 s, 235 s | 236 s |
| Warm `cargo test --workspace --no-run` | 63.1 s, 61.7 s, 55.9 s | 55.7 s, 55.2 s, 59.8 s |
| Warm `cargo clippy --workspace --all-targets` | 116.9 s (clippy cold), 26.1 s, 27.4 s | 114.1 s (clippy cold), 26.3 s, 26.3 s |
| `du -sh target` | 9.8 GB / 11 GB | 9.8 GB / 11 GB |

Every difference sits inside the run-to-run spread of the baseline itself, which is what the
byte-identical output predicts. The config was not kept: `rustflags` in `.cargo/config.toml`
invalidates every contributor's target directory once for nothing, and it converts a linker that
ships with the pinned toolchain into a soft dependency on whatever `lld` happens to be on `PATH` —
a real regression on any machine where that is older, or missing.

The bfd row is the useful part of this experiment. lld is worth roughly 8x on this workspace's link
steps; we simply already have it, because rustc made it the default for this target. `mold` is not
installed here and was not tried, but it is competing against `rust-lld` at 2.0 s, not against
`bfd` at 16 s, so the headroom left is small.

## What was left, at the end of the baseline

Linking is already about as cheap per binary as a stable toolchain can make it. The remaining lever
is doing it fewer times — 47% of the warm cycle is 24 separate integration test binaries — which is
issue #44's second proposal, and the benches' effect on the clippy gate is its third. Both must be
measured against the baseline table above, or neither result means anything.

Both were then done, and both are below.

# Consolidating the integration tests into one binary

`crates/lldb-qe-core/tests/` was 24 integration-test targets; it is now one (`integration`) plus
`distributed_cluster`, which is deliberately still its own — `tests/integration/main.rs` holds the
reasoning, the process-global-state audit that had to precede the merge, and the rule for adding a
test. This section is only the numbers.

## Read this before comparing to the baseline table above

**The box is not the box the baseline was taken on.** Same 4 cores, same 15 GB, same pinned 1.97.1,
but every figure came out ~28% slower on re-measurement: the baseline's warm cycle reproduced at
75.0–81.7 s rather than 55.9–63.1 s, and its warm clippy at 34.1–37.5 s rather than 26.1–27.4 s.
The scaling is uniform enough across unrelated metrics to be the machine rather than the workspace,
but it means **nothing below may be compared to a number in the baseline table**. So the before
column was re-measured from scratch, in the same session, on the same box, with the tests still
split — `git stash` on the change, `cargo clean`, measure, restore. Every before/after pair here is
internally consistent; a cross-section pair is not.

Protocol, unchanged from the baseline: `touch crates/lldb-qe-core/src/lib.rs` before each warm run,
cargo run plainly, no profile environment variables. The warm rows were taken on a target directory
that had just been cold-built and then had `cargo test --no-run` and `cargo clippy` run once, so
both columns start from the same kind of cache.

## Results

| Metric | Before (24 targets) | After (1 target + `distributed_cluster`) |
| - | - | - |
| Warm `cargo test --workspace --no-run` | 79.8, 75.0, 79.7, 81.7, 78.8 s | 55.9, 56.2, 49.9 s |
| Warm `cargo clippy --workspace --all-targets -- -D warnings` | 37.5, 36.8, 35.4, 36.6 s | 35.8, 36.3, 37.8, 40.9 s |
| Cold `cargo build --workspace --all-targets` after `cargo clean` | 352.1, 353.9 s | 302.9, 318.7 s |
| `du -sh target` after `--all-targets` | 9.8 GB | **4.4 GB** |
| `du -sh target` once `cargo test` and `cargo clippy` have added theirs | 11 GB | 4.6 GB |
| binaries over 100 MB in `target/debug/deps` | 26, totalling 7.62 GB | **7, totalling 2.15 GB** |
| unit CPU, `cargo build --timings --workspace --all-targets` | 344.0 s over 40 units | 237.4 s over 20 units |
| of which, integration test targets | 24 targets, 192.2 s | 2 targets, 32.6 s |

The warm cycle — the number the edit-test loop actually pays — drops about **32%**, and the two
ranges do not overlap. The disk figure is the larger result and the one that was underweighted
going in: **a 55% smaller target directory**, because 23 of those 26 near-identical 300 MB copies
of the whole dependency graph simply stopped existing.

Three honest caveats:

- **Clippy is unchanged, and that is the expected result.** Clippy never links, so consolidation
  cannot save it any link time; all it does is trade 23 independently schedulable check units for
  one large one, which on 4 cores is a wash. The two ranges overlap and their medians differ by
  0.4 s. An earlier batch of after-runs, taken on a target directory still littered with the old
  layout's artifacts, read 37.6–43.8 s and looked like a regression; it did not survive re-running
  under the same protocol as the before column, which is the whole reason the protocol is stated.
- **Per-unit `--timings` durations are not comparable across the two columns.** Cargo reports wall
  time per unit, and wall time per unit depends on what else was competing for the four cores —
  which is precisely what changed. The category rows move in ways that have nothing to do with the
  change (unchanged bench and binary targets appear to double), so only the *total* and the
  *integration-test* row are quoted, and even those are directional. The wall-clock and disk rows
  are the trustworthy ones.
- **The cold build improves by ~10%, which is barely outside its own spread.** The baseline
  measured 11% run-to-run variance on a cold build; 352/354 → 303/319 is a real but modest win, and
  the tighter before pair should not be read as precision.

## Rejected: putting the benches behind a feature

Issue #44's third proposal was to feature-gate the two `criterion` benches so
`cargo clippy --all-targets` stops building their tree. **Measured, and not implemented.**

The experiment needs no code: `--lib --bins --tests` is the same target set the proposal would
produce, so timing it against `--all-targets` prices the change directly. Same protocol, on the
consolidated tree:

| Clippy gate | Runs |
| - | - |
| `--workspace --all-targets` (what CI runs) | 35.8, 36.3, 37.8, 40.9 s |
| `--workspace --lib --bins --tests` (benches excluded) | 34.1, 36.5, 37.4 s |

A median difference of about **0.5 s on a 37 s gate — under 2%, and a quarter of the gate's own
run-to-run spread.** The baseline's framing ("benches are 4.4% of unit CPU") already made this look
marginal; the direct measurement says it is smaller still, and the reason is that removing work is
not the same as shortening the critical path. The two bench units check *in parallel* with the
`lldb-qe-core` lib unit-test binary, which is the longest single unit in the graph and finishes
after them either way. Deleting them from the schedule frees a core; it does not end the build
sooner.

Against that: a `--features benches` gate is a flag every contributor and every CI job has to
remember, a target that stops being compiled by the default gate and therefore starts to rot, and
one more conditional in a workspace that already carries a version wall's worth of them. Not worth
0.5 s. The benches stay in `--all-targets`, where a compile error in them still fails CI.

## What is left now

The remaining warm-cycle cost is concentrated where it cannot be structured away: the
`lldb-qe-core` lib and its unit-test binary are ~40% of the unit CPU between them, and they are one
crate compiled twice because `#[cfg(test)]` changes the crate. Splitting `lldb-qe-core` into
smaller crates would parallelize that — and would also be a far larger change than a build tweak,
with real consequences for the plan-codec boundary, so it belongs to its own issue and its own
argument rather than to this one.
