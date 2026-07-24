# Benchmarks

Reproducible with `cargo bench` after `./scripts/bootstrap.sh`. Numbers below are from an
Apple Silicon (`arm64`) laptop — the same ISA family as the Graviton production target — on
Rust 1.97.1, TPC-H scale factor 1, criterion's optimized `bench` profile.

Treat the absolute numbers as illustrative; the **ratios and the shape of the curve** are the
point.

## Single-node TPC-H baseline

The pure DataFusion baseline every distributed result is measured against
(`cargo bench --bench tpch`).

| Query | Time | Shape |
| - | - | - |
| Q1 | ~37.5 ms | full 6M-row `lineitem` scan → grouped aggregation → hash-repartition |
| Q6 | ~13.8 ms | scan → filter → single sum (row-group pruning + column pushdown) |

## Distributed vs single-node

Grouped `COUNT(*)` over `orders` (1.5M rows), single-node vs the map → hash-shuffle → reduce
path across two workers (`cargo bench --bench distributed`).

| Path | Time | vs single-node |
| - | - | - |
| Single-node | ~2.29 ms | 1.0× |
| Distributed, 2 workers | ~7.87 ms | **~3.4× slower** |

### Why distributed is slower here (and when it stops being)

At SF1 the `orders` file fits comfortably in memory/cache, so distribution is all cost and no
benefit:

- **Redundant IO** — in this POC each worker scans the whole file and filters to its slice, so
  two workers do ~2× the scan work. (A production engine slices the scan itself.)
- **Serialization** — the physical plan is encoded to protobuf per request.
- **Network** — Arrow batches cross a gRPC boundary instead of staying in-process.
- **Co-located reduce** — the shuffle and reduce run on the coordinator (worker-to-worker
  `do_exchange` is the documented next step), adding a gather hop.

Distribution earns its keep only when the data no longer fits on one machine — when a single
node would spill to disk, thrash cache, or simply run out of RAM. Then N machines scanning
1/N of the data in parallel beats one machine grinding through all of it, and the fixed
network/serialization tax becomes a rounding error. The crossover is a function of data size
vs per-node memory; SF1 is far below it by design.

This is why a real engine's scorecard is TPC-H at SF100+/SF1000 against a commercial baseline,
not SF1 on a laptop.

## Profiling (next step)

To see *where* the distributed time goes (serialization vs network vs execution), profile a
single distributed run with [`samply`](https://github.com/mstange/samply):

```bash
cargo build --release -p lldb-qe-worker -p lldb-qe-coordinator
# start a worker, then:
samply record ./target/release/lldb-qe-coordinator http://127.0.0.1:50051 data "SELECT ..."
```
