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

<!-- The path above is deliberately the one that existed at 265f2af; repointing it would misreport
     what was measured. scripts/check-path-refs.sh is told so here rather than in the prose:
     path-refs-allow: crates/lldb-qe-core/tests/first_light.rs -->

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

**Reversed later on a different axis — see "Gating the two benches behind a feature (#97)" at the
end of this file.** Nothing in this section was found to be wrong, and its numbers reproduce. What
changed is that #72 established **disk**, not time, as the binding constraint, and this section
never argued disk. The rot objection above is answered rather than accepted: CI's clippy step now
carries the feature, so a bench that stops compiling still fails the build.

## What is left now

The remaining warm-cycle cost is concentrated where it cannot be structured away: the
`lldb-qe-core` lib and its unit-test binary are ~40% of the unit CPU between them, and they are one
crate compiled twice because `#[cfg(test)]` changes the crate. Splitting `lldb-qe-core` into
smaller crates would parallelize that — and would also be a far larger change than a build tweak,
with real consequences for the plan-codec boundary, so it belongs to its own issue and its own
argument rather than to this one.

That split was then done, as issue #72. Its results are below, and **the headline is a negative
result**.

# Splitting `lldb-qe-core`, and moving the one-shots out of `lldb-qe-coordinator` (#72)

Two extractions, not one, and they came from different packages:

- **`lldb-qe-core` became three crates** — `lldb-qe-types` ← `lldb-qe-control` ← `lldb-qe-core`
  (#76, #85).
- **The four operator one-shots** (`lldb-qe-migrate`, `lldb-qe-warehouse`, `lldb-qe-auth`,
  `lldb-qe-reap`) moved out of **`lldb-qe-coordinator`** into a new `lldb-qe-admin` (#95). They were
  never in `lldb-qe-core`; they were `src/bin/` targets of the coordinator package, which is
  precisely why they compiled DataFusion — Cargo resolves dependencies per package, not per binary.

The workspace went from four crates to six.

## Read this before comparing to anything above

**These numbers came from a 14-core box (10 performance cores). Every figure earlier in this file
came from a 4-core box.** Nothing here may be compared with anything above it. The series below is
internally consistent — same machine, same session, same protocol — and that is the only comparison
it supports.

Protocol unchanged: `cargo clean`, then `cargo build --workspace --all-targets`, cargo run plainly,
no profile environment variables.

## The series

| | pre-split (`770d1dd`) | after types+control (`e90111a`) | after admin (`703077f`) |
| - | - | - | - |
| Cold `cargo build --workspace --all-targets` | 107 s¹ | 101 s | 109 s |
| `du -sh target` | 5.4 GB | **5.4 GB** | **5.4 GB** |

¹ that run carried `--timings`, which adds overhead — so even the 107→101 movement is not real.

**The split bought nothing on cold build or on disk.** Run-to-run spread on a cold build was already
measured at ~11% earlier in this file; 101–109 s sits inside it. Disk did not move at all, to three
significant figures, across three measurements.

This is not surprising in hindsight and is worth stating plainly so nobody re-derives it: *the same
code still compiles, and the same seven binaries still statically link the same dependency graph.*
Moving code between crates changes **who depends on what**, not **how much there is**.

## What it did buy: scoped rebuilds

| Edit → test | Wall |
| - | - |
| `touch types/rbac.rs` → `cargo test -p lldb-qe-types --lib` | **1.5 s** |
| `touch control/auth.rs` → `cargo test -p lldb-qe-control --lib` | **9.0 s** |
| `touch core/catalog.rs` → `cargo test -p lldb-qe-core --lib` | 20.5 s |

Before the split every one of those 33 modules cost the 20 s loop. Thirteen of them now cost 9 s,
and the rbac/storage vocabulary costs 1.5 s.

And the operator one-shots stopped paying for the query engine at all:

| After `touch crates/lldb-qe-core/src/staging.rs` | Wall | `lldb-qe-core` compiled |
| - | - | - |
| `cargo build -p lldb-qe-admin --bin lldb-qe-migrate` | **4.4 s** | **0 times** |
| `cargo build -p lldb-qe-coordinator --bin lldb-qe-server` | 16.9 s | yes |

The second row is the control: it proves the touch was live, so the first row is a real result and
not a stale-cache artifact. Before #95 both cost the 17 s.

Note what this corrects. #72 originally justified this on the one-shots being ~50 MB binaries. That
figure was measuring the wrong thing: `nm` on `lldb-qe-migrate` finds exactly **one** datafusion-named
symbol, from `datafusion_doc`, a proc-macro helper — `--gc-sections` was already stripping the engine
from the artifact. Disk was never the cost. Compiling and linking a graph the binary then discards
was.

## Where the 5.4 GB actually is

| | |
| - | - |
| executables (51) | 2.53 GB |
| rlibs (467) | 1.20 GB |

Seven executables are 2.36 GB of that 2.53 GB:

| Artifact | Size |
| - | - |
| `integration` | 373 MB |
| `lldb_qe_server` | 359 MB |
| `lldb_qe_core` (unit tests) | 353 MB |
| `lldb_qe_coordinator` | 352 MB |
| `distributed` (bench) | 323 MB |
| `tpch` (bench) | 306 MB |
| `lldb_qe_worker` | 296 MB |

Identical before and after the split, as the disk figure implies.

## The conclusion, and what it means for the next attempt

**Disk is not addressable by crate structure.** It is a function of how many artifacts statically
link the whole dependency graph, and the split did not change that count. Anyone reaching for
another restructure to fix a disk ceiling should read this section first.

The remaining disk levers are about *artifacts*, not modules — the two criterion benches alone are
629 MB, 27% of executable bytes, tracked as #97. Note that #44's rejection of gating them was
argued on **time** and remains correct on time; #97 revisits it on **disk**, which was never argued.

**The sibling / port-trait inversion was not built, and nothing measured justifies building it.** The
stacked design needs zero port traits and delivered the entire scoped-rebuild win. Inverting it would
buy parallel compilation of the two halves — worth having only if cold build were the constraint, and
the table above says it is not.

## One prerequisite that had to be fixed first, and would have invalidated all of this

`build.rs` emitted `cargo:rerun-if-changed=../../.git/HEAD`. In a git worktree `.git` is a **file**,
so that path does not exist, and Cargo treats a missing `rerun-if-changed` target as permanently
stale — every no-op build recompiled everything downstream (18.65 s → 0.21 s once fixed, #87/#94).

**Any build measurement taken in a git worktree before that fix was contaminated**, and parallel
work happens in worktrees. Take measurements in a normal clone, or confirm the fingerprint resolves
before trusting a number.

# Gating the two benches behind a feature (#97)

`crates/lldb-qe-core/Cargo.toml` declares a `benches` feature and both `[[bench]]` targets carry
`required-features = ["benches"]`. **CI's clippy step passes `--features lldb-qe-core/benches`**, so
every CI run still compiles them and a bench that stops building still fails the build.

**This reverses the decision recorded above, and not by finding that decision wrong.** #44 priced
the change in *time*, and its answer — ~0.5 s on a 37 s gate, a quarter of the gate's own spread —
was right about time and reproduces here. What #72 established afterwards is that the constraint
that started the whole line of work, a 4-core / 30 GB VM, was never bound by time: it was bound by
**disk**, the crate split moved disk not at all (5.4 GB before, 5.4 GB after, three measurements),
and disk is the axis #44 never argued. Read the two sections together. The time argument stands; it
is simply not the argument that decides this.

## Read this before comparing to anything above

**Measured on a 14-core box (10 performance cores), 36 GB, macOS, toolchain 1.97.1** — the same
shape of machine as the #72 section immediately above, and **not** the 4-core box every figure
before that came from. Nothing here may be compared with a number in the baseline or consolidation
tables.

Protocol unchanged: `cargo clean`, then the build; cargo run plainly, no profile environment
variables. Each warm clippy run is preceded by `touch crates/lldb-qe-core/src/lib.rs`.

The worktree fingerprint check the top of this file demands was run, and passes: two consecutive
no-op `cargo build --workspace --all-targets` cost 28.91 s (genuinely stale — a touched `lib.rs`
preceded it) and then **0.26 s**; clippy the same, 20.65 s then **0.28 s**. The #87 fix holds here,
so the warm rows below are real work and not a permanently-stale rebuild.

## Disk, which is the point

| Cold `cargo build --workspace --all-targets` | benches built (`--features lldb-qe-core/benches`) | benches gated out (the new default) |
| - | - | - |
| `du -sk target`, two samples each | 5,710,536 / 5,683,276 KB | **4,963,508 / 5,007,284 KB** |
| `du -sh target` | 5.4 GB | **4.7–4.8 GB** |
| `distributed` in `target/debug/deps` | 339,142,736 B (323 MiB) | **absent** |
| `tpch` in `target/debug/deps` | 320,911,040 B (306 MiB) | **absent** |
| executables over 100 MB | 7 | **5** |

**Between −676,000 and −747,000 KB: 660–730 MiB, 11.9–13.0% of the target directory**, from a change
that removes no code. Two samples per column, because `du` on two cold builds of the *same*
configuration is not identical — 0.5% apart with the benches, 0.9% without — so a single-sample
delta would have overstated its own precision.

The two bench binaries are 660,053,776 bytes (629 MiB), which is the whole of it once that spread is
allowed for. In particular **cargo does not stop compiling `criterion`**: it is a dev-dependency of
the package, so every test target links it and it stays in the unit graph in all three
configurations — 7 criterion/plotters artifacts under `--all-targets`, under `--all-targets
--features lldb-qe-core/benches`, and under `cargo test --no-run` alike. Nor could it be avoided:
cargo **rejects** an `optional` dev-dependency, so criterion is declared unconditionally and the
saving is entirely the two linked binaries.

The two bench binaries are present again, at the same sizes, under
`cargo build --workspace --all-targets --features lldb-qe-core/benches` — which is what CI runs, so
CI keeps paying the 629 MiB and the runner is not the machine with the disk ceiling.

## Time: unchanged, exactly as #44 predicted

| Warm clippy gate, after `touch crates/lldb-qe-core/src/lib.rs` | Runs |
| - | - |
| `--workspace --all-targets` (benches now excluded) | 23.24, 20.80, 20.71 s |
| `--workspace --all-targets --features lldb-qe-core/benches` (what CI runs) | 20.80, 20.71, 20.71 s |

Medians 20.80 s and 20.71 s. The first run of the first row is a warm-up artifact; discount it and
the two configurations are indistinguishable. #44's 0.5 s does not so much *still hold* as shrink
further, for the reason #44 already gave: the two bench units check **in parallel** with the
`lldb-qe-core` lib unit-test unit, which is the critical path and finishes after them either way —
and a 14-core box has even more room to hide two units in than the 4-core box that measured 0.5 s.

The useful corollary is about CI rather than about laptops: **adding `--features
lldb-qe-core/benches` to the clippy step costs CI nothing measurable**, which is what makes the
anti-rot pairing free rather than a trade.

Cold build is likewise unmoved — 101.3, 114.4, 99.7 s with the benches; 122.0, 99.3, 104.0 s
without. Cold-build run-to-run spread was measured at ~11% earlier in this file and these ranges
overlap completely; the *slowest* of all six runs is a build that omits the benches. Do not read the
disk win as a build-time win. It is not one.

## `cargo test` never paid for the benches — verified, not assumed

`cargo test --workspace --no-run --message-format=json`, filtered to the workspace's own packages
and diffed before against after, is **identical**: the same 16 `compiler-artifact` records, not one
of kind `bench`, and the same twelve linked executables. The everyday edit-test loop is untouched by
this change in both directions — it never built the benches, and it still does not.

## The rot objection, answered rather than accepted — and proven

#44's strongest argument was that a target the default gate stops compiling starts to rot. The
answer is the CI flag, and it was verified by breaking a bench on purpose (a function returning
`&str` where it declares `u32`, appended to `benches/tpch.rs`) and running both gates against it:

| Gate, against a bench that does not compile | Exit |
| - | - |
| `cargo clippy --workspace --all-targets -- -D warnings` (local default) | **0** — the bench is not in the target set |
| `cargo clippy --workspace --all-targets --features lldb-qe-core/benches -- -D warnings` (CI) | **101**, `error[E0308]` in `benches/tpch.rs` |

That is the whole trade in two rows: a contributor who edits a bench and runs only the local default
gate will not hear about a break, and CI will, before the branch merges.

## The residual cost, stated rather than buried

- One more flag in `.github/workflows/ci.yml` that must not be dropped. It is commented there.
- **`cargo bench` on its own is now a quiet no-op.** It still spends a full *release* build of the
  crate (3m 36s cold, here), then selects neither bench target, prints no measurement, and **exits
  0**. That is cargo's documented `required-features` behaviour and it is the sharpest edge of this
  change: the failure mode is silence, not an error. The incantation is
  `cargo bench -p lldb-qe-core --features benches`, and it is now in `BENCHMARKS.md`,
  `CONTRIBUTING.md`, `CLAUDE.md`, the `lldb-commands` skill, and both bench file headers.

# Also priced, and rejected: `strip` on the test profile

`[profile.dev]` sets `debug = 0` and `incremental = false`, and `[profile.test]` inherits both.
#97 asked whether `strip` would reclaim meaningful space on top of that. **`strip = "debuginfo"`
reclaims nothing, `strip = "symbols"` reclaims 433 MiB and costs the entire panic backtrace, and
neither is implemented.** Three findings, in the order they were measured.

**1. `[profile.test]` does not affect `cargo build --all-targets` at all.** With
`strip = "symbols"` set there, a cold `--all-targets` build produced artifacts with byte-identical
names *and* sizes (`integration-ee0867e14dbf06dc`, 373 MB, both times). `cargo build` uses the
**dev** profile for every target it builds, test targets included; the `test` profile is what
`cargo test` selects. Every "du after `--all-targets`" number in this file is therefore a
dev-profile number, and no `[profile.test]` key can move it. Anyone re-running this experiment must
measure `cargo test --no-run`, not `cargo build --all-targets`, or they will measure nothing.

**2. `strip = "debuginfo"` is a no-op here by construction.** `debug = 0` already means there is no
debug info to remove: `otool -l target/debug/deps/integration-… | grep -c __DWARF` returns **0**.

**3. `strip = "symbols"` is a real 16% — and it is still the wrong trade.**

| Cold `cargo test --workspace --no-run` | no override | `[profile.test] strip = "symbols"` |
| - | - | - |
| `du -sk target` | 2,738,456 KB (2.6 GB) | **2,294,648 KB (2.2 GB)** |
| `integration` | 373 MB | **205 MB** |
| `lldb_qe_core` unit tests | 353 MB | **195 MB** |
| `lldb_qe_control` unit tests | 24 MB | 15 MB |
| symbols in `integration` (`nm \| wc -l`) | 710,820 | 236 |

443,808 KB — 433 MiB, 16.2%. Not a small number. It loses on two measured counts anyway.

**The backtrace cost is total, not partial.** Not "line numbers get worse" — the trace is *empty*.
A minimal crate with `debug = 0` in both columns and one panicking test:

```
# [profile.test] unset                        # [profile.test] strip = "symbols"
stack backtrace:                              stack backtrace:
  0: __rustc::rust_begin_unwind               note: Some details are omitted, run with
  1: core::panicking::panic_fmt                     `RUST_BACKTRACE=full` for a verbose backtrace.
  2: striptest::inner_frame_that_should…      (that is the whole output — zero frames)
  3: striptest::outer_frame_that_should…
  4: striptest::tests::it_panics
```

`debug = 0` costs line numbers and keeps function names, which is a deliberate and survivable
trade. `strip = "symbols"` takes the function names too, and on an engine whose failures surface as
panics inside async tasks on a worker that is the difference between a legible failure and an
unreadable one. (`strip = "debuginfo"` leaves the trace intact — it simply has nothing to remove.)

**And it makes disk *worse* for anyone who runs both commands.** With no overrides, `[profile.test]`
is byte-identical to `[profile.dev]`, so cargo builds each test binary **once** and `cargo build
--all-targets` and `cargo test` share it. Measured: `cargo test --workspace --no-run`
(2,738,456 KB) followed by `cargo build --workspace --all-targets` costs 3.35 s — only the bins are
missing — and lands at 5,025,468 KB, inside the 4,963,508 / 5,007,284 KB spread the build reaches on
its own. Add *any* `[profile.test]` key and the two profiles fork into separate units with separate
hashes. With `strip = "symbols"` set, `cargo build --workspace --all-targets` (5,008,532 KB) followed
by `cargo test --workspace --no-run` landed at **7,303,172 KB**, holding two copies of every test
binary — `integration` at 373 MB *and* 205 MB, `lldb_qe_core` at 353 MB *and* 195 MB. A 433 MiB
saving that costs 2.2 GB the moment someone also builds `--all-targets` is not a saving on the
machine #97 exists for.

Measured, recorded, not implemented. The lever that works on this workspace is the **number of
artifacts that statically link the whole dependency graph** — which is what #44's consolidation did,
what the crate split could not do, and what gating the benches does.

# The number every measurement above missed: worktrees (#107)

Everything above measures **one** `target/`. On the machine this work was done on, that was not
where the disk went.

Merging #104 could not delete its local branch, because a leftover git worktree still held it.
Looking at why found **eight worktrees, every one on an already-merged branch, holding 36 GB**:

| Branch | PR | Size |
| - | - | - |
| `claude/issue-97-gate-benches` | #104 | 7.3 GB |
| `claude/issue-79-encode-error-path` | #91 | 6.4 GB |
| `claude/issue-73-fargate-tls` | #81 | 5.8 GB |
| `claude/issue-87-buildrs-rerun` | #94 | 5.2 GB |
| `claude/issue-92-admin-package` | #95 | 4.7 GB |
| `claude/issue-72-control-crate` | #85 | 3.3 GB |
| `claude/issue-74-codec-version` | #75 | 2.8 GB |
| `claude/issue-32-fleet-token` | #99 | 255 MB |

All were clean. Removing them reclaimed **35.8 GiB**.

Put beside the rest of this document: **#97 won 660–730 MiB. The crate split won nothing on disk.
And the disk budget this whole effort is measured against is a 30 GB VM.** Stale worktrees alone
exceeded that budget outright, by roughly fifty times the margin gating the benches recovered.

Nothing above is wrong. Every measurement of `target/` still holds, and the lever named in the
previous section is still the right lever *for that directory*. The correction is one of scope: a
worktree is a full independent checkout **with its own `target/`** — that is precisely what the
isolation buys — so the quantity that actually fills the disk is `target/` **times the number of
worktrees nobody removed**, and only the first factor was ever being measured.

`scripts/sweep-worktrees.sh` is the recurring fix. It removes a worktree only when its branch has a
merged PR and its tree is clean, and never one that is `locked`. Two details are load-bearing:

- **Merged-ness comes from `gh pr list --state merged`, not `git branch --merged`.** Branches here
  land by *squash* merge, which gives the merge commit a different identity, so `git branch --merged`
  reports a demonstrably merged branch as unmerged — verified on `claude/issue-97-gate-benches`,
  whose PR had just merged. A sweep trusting it would skip exactly the branches it exists for.
- **It is a dry run unless given `--apply`**, and it refuses locked or dirty trees. Both signals
  were already present and correct when this was found by hand: the one live agent's worktree was
  locked, and the eight dead ones were clean.

Considered and not adopted: pointing agent worktrees at a shared `CARGO_TARGET_DIR`. It would
collapse N copies into one, but it serialises builds behind a shared lock and re-links whenever two
agents' flags differ — the contention worktree isolation exists to avoid. Worth measuring before
adopting; a sweep is the cheaper answer and it is the one implemented.

# Linker warnings are outside every gate, and that is not fixable by adding one (#105)

Every fat binary here links on macOS with:

```
ld: warning: __eh_frame section too large (max 16MB) to encode dwarf unwind offsets in
    compact unwind table, performance of exception handling might be affected
```

rustc surfaces it via `warn(linker_messages)`. Correctness is unaffected; debug-build unwinding takes
the slower path.

## Why no gate sees it

**`cargo clippy` never links.** That is stated above as the reason the clippy gate is fast, and it is
exactly why `-D warnings` there cannot catch this. Verified rather than assumed — same file touched
both times, `cargo build -p lldb-qe-worker` emits the warning once, `cargo clippy -p lldb-qe-worker`
emits it **zero** times.

So this workspace has a class of warning that no check observes. "Clippy is green" does not mean "the
build is warning-clean", and it never can.

## Why adding a linking gate would make it worse, not better

`cargo test --workspace` *does* link, and CI runs it — so a warning check there is technically
available. It would not help, and would actively mislead:

**CI runs on `ubuntu-latest`. This warning is macOS's linker.** A `-D linker_messages` on the test
step would be green on every CI run and red on a contributor's Mac — the worst shape of build
failure, because it looks like the contributor's machine is broken. It would also advertise coverage
("linker warnings are gated now") that does not extend to the platform where the warning actually
fires.

**If `RUSTFLAGS=-Dwarnings` is ever added to any build step, resolve this first or macOS
contributors are locked out.**

## Why it is not silenced

Silencing needs `RUSTFLAGS`, and the section above on `.cargo/config.toml` applies unchanged: any
`rustflags` entry invalidates every contributor's target directory once, and then makes the build
depend on a file that must agree everywhere. A crate-level `#![allow(linker_messages)]` avoids that
but is far too broad — it would silence *every* future linker message, including ones worth reading.

The better reason to leave it: **the warning is proportional to the problem this document is about.**
`__eh_frame` is too large because the binaries are enormous, which is #97's and #72's subject. It is
a free indicator that costs nothing and would disappear on its own if the binaries ever got small
enough. Silencing it would remove a signal that tracks the thing being measured.

**Decision: leave it, documented as expected on macOS.** Nothing to fix, and the honest statement is
that linker warnings are unobserved rather than that they are clean.
