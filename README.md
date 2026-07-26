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
| `lldb-qe-core` | Storage abstraction, config-as-data catalog, session setup, Flight transport, plan codec, services-DB connection layer |
| `lldb-qe-coordinator` | SQL entry point; builds a plan and dispatches it to workers over Flight. Also builds `lldb-qe-migrate` |
| `lldb-qe-worker` | Stateless Flight server that executes shipped sub-plans |

## Services database (control plane)

Query execution is stateless; the *control plane* is not. Accounts, the SQL catalog, virtual
warehouses and query history are facts the whole fleet has to agree on while it is running, so
they live in a shared PostgreSQL database rather than in any one process's memory.

Schema changes are migrations checked into
[`crates/lldb-qe-core/migrations/`](crates/lldb-qe-core/migrations/), embedded into the binaries
at compile time and applied by an explicit one-shot — `lldb-qe-migrate` — never on startup. A
rolling fleet must not race to apply the same DDL.

```bash
# Apply migrations and make sure an account exists (idempotent; safe on every deploy).
cargo run -p lldb-qe-coordinator --bin lldb-qe-migrate -- \
  --metadata-url postgres://lldb:lldb@localhost:5432/lldb --seed-account default
```

Connection settings come from `LLDB_METADATA_URL`, or from the discrete
`LLDB_METADATA_HOST/PORT/DATABASE/USER/PASSWORD/SSLMODE` — the second form exists because ECS
injects a Secrets Manager password as its own variable and cannot interpolate one into a URL.
**A services database is optional:** with none configured, single-node and local runs behave
exactly as before.

## Catalogs & schemas

Tables are declared as **data**, not code. A [manifest](manifests/) describes arbitrary
`catalogs → namespaces → tables` — their format (transactional Iceberg or a plain parquet
listing), source, and (optionally) schema — and `apply_manifest` materializes them into a
DataFusion session. Nothing is tied to TPC-H; it is just one example manifest
([`manifests/tpch.toml`](manifests/tpch.toml)). The storage backend (`local`, `memory`, or
`s3`/MinIO) is chosen at runtime, so the same binary runs against a laptop directory in dev
and an S3 warehouse in production.

## Running a cluster (Docker)

Bring the whole system up as containers — MinIO (the S3 warehouse), PostgreSQL 18.4 (the
services database, migrated by a one-shot `db-migrate` step), a worker fleet, and a coordinator
that ships a plan over Arrow Flight — to see the distributed path end-to-end:

```bash
docker compose up --build     # coordinator prints a result streamed back from a worker
```

The default query is a constant, so the cluster proves cross-container transport without
seeded data; point `--manifest` at S3 data to run real queries. Postgres is published on
`localhost:5432` (`psql -U lldb lldb`) so you can inspect the control plane while it runs. See
[`docker-compose.yml`](docker-compose.yml).

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
- [x] **Phase 6** — Generic config-as-data catalog (multi-schema), S3 storage backend
- [x] **Phase 7** — Containerized cluster + cross-container test + CI
- [x] **Phase 8** — AWS IaC in CDK: ECS Fargate worker fleet + S3 warehouse ([infra/](infra/))
- [ ] **Next** — Real network-shuffle exec node + worker-to-worker `do_exchange`; scan-level
  slicing (the work that makes fan-out across the fleet meaningful); persistent shared catalog
  (SQL/REST); publish versioned images to a registry from CI

## Deploying to AWS

[`infra/`](infra/) is a CDK app that stands the engine up on ECS Fargate — a service-discovered
worker fleet, an S3 Iceberg warehouse, and a one-shot coordinator task:

```bash
cd infra && npm ci
npx cdk deploy -c imageTag=0.1.0+8c6d8d6b57d8    # deploy one exact build, fleet-wide
```

The stack refuses to synth without a pinned tag — coordinator and workers must be the identical
build. See [infra/README.md](infra/README.md) for the deploy walkthrough and cost notes.

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
