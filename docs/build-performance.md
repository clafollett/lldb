# Build performance — the baseline, and what has already been tried

This file exists so nobody re-derives the same numbers, and so the next change to the build
configuration can be measured against something instead of asserted. Issue #44 is the parent.

Every figure below was taken on the machine the issue was filed from, with nothing else running:

| | |
| - | - |
| cores | 4 |
| RAM | 15 GB |
| toolchain | 1.97.1 (pinned in `rust-toolchain.toml`) |
| profile | `[profile.dev] debug = 0, incremental = false`; `[profile.test]` inherits |
| workspace | 439 crates compiled cold; 24 integration tests + 2 benches in `lldb-qe-core` |

**Run cargo plainly when reproducing these.** Do not export `CARGO_PROFILE_DEV_DEBUG` or
`CARGO_INCREMENTAL` — they are the committed profile by another route, and a build whose profile
differs from the previous one invalidates the whole target directory. See CLAUDE.md.

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
file compiled plus one 294 MB binary linked.

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

## What is left

Linking is already about as cheap per binary as a stable toolchain can make it. The remaining lever
is doing it fewer times — 47% of the warm cycle is 24 separate integration test binaries — which is
issue #44's second proposal, and the benches' effect on the clippy gate is its third. Both must be
measured against the baseline table above, or neither result means anything.
