# lldb

A distributed analytical query engine in Rust — a thin SQL coordinator in front of a fleet
of stateless workers that shuffle Apache Arrow batches over Arrow Flight, reading Apache
Iceberg tables on object storage.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Status:** early / experimental. Built in the open as a learning-grade reimplementation
> of a production architecture. Not production-ready.

## Why

Commodity ARM hardware plus open formats can get within striking distance of a commercial
cloud warehouse at a fraction of the cost. lldb explores that space: Apache DataFusion for
planning and execution, Arrow Flight for worker-to-worker exchange, and Apache Iceberg for
transactional table storage.

## Architecture

A query engine is a four-stage translator. "Distributed" means cutting the physical plan at
its expensive boundaries and running the pieces on separate workers, streaming Arrow between
them:

```text
SQL ──parse──▶ Logical Plan ──optimize──▶ Physical Plan ──cut at shuffle──▶ workers ──Arrow/Flight──▶ merge
```

| Layer | Component | Crate |
| - | - | - |
| Plan + optimize + execute | Apache DataFusion | `datafusion` |
| In-memory columnar data | Apache Arrow | `arrow` |
| On-disk columnar files | Apache Parquet | `parquet` |
| Transactional table format | Apache Iceberg | `iceberg` |
| Object storage (local / S3) | object_store | `object_store` |
| Worker data exchange | Arrow Flight (gRPC) | `arrow-flight` |

## Workspace

| Crate | Role |
| - | - |
| `lldb-qe-core` | Storage abstraction, session/catalog setup, execution nodes, plan codec |
| `lldb-qe-coordinator` | SQL entry point; cuts plans into stages and dispatches to workers *(coming)* |
| `lldb-qe-worker` | Stateless Flight server that executes sub-plans *(coming)* |

## Quickstart

**Easy path:** run `./scripts/bootstrap.sh` — it checks your toolchain, installs the
TPC-H generator, and generates the data for you (honors `SCALE_FACTOR`, default `1`).
Then run `cargo test`. The manual steps below are the explicit alternative.

Prerequisites: Rust 1.97+ (via `rustup`) and the pure-Rust TPC-H generator:

```bash
cargo install tpchgen-cli
```

Generate scale-factor-1 data and run the test suite:

```bash
tpchgen-cli -s 1 --format=parquet --output-dir data/sf1
cargo test
```

## Roadmap

- [x] **Phase 0** — Foundation: workspace, storage abstraction, first query over Parquet
- [x] **Phase 1** — Iceberg: local catalog, tables, snapshots
- [x] **Phase 2** — Physical plans + single-node TPC-H baseline
- [x] **Phase 3** — Arrow Flight coordinator/worker transport
- [x] **Phase 4** — Distributed hash aggregation (the shuffle)
- [x] **Phase 5** — Benchmark distributed vs single-node ([BENCHMARKS.md](BENCHMARKS.md))

## Versioning

DataFusion is pinned to **53.1** because `iceberg-datafusion` 0.10 caps it there; the Arrow
family (`arrow` / `arrow-flight` / `parquet`) and `object_store` are pinned to match
DataFusion exactly. See [`Cargo.toml`](Cargo.toml) for the rationale — these move together,
not independently.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This project practices trunk-based development and is
licensed under Apache-2.0.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
