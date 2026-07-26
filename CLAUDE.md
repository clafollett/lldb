# lldb — build notes for Claude

Distributed analytical query engine: DataFusion + Arrow Flight + Iceberg on object storage — a
rudimentary cloud data warehouse. It **began** as a learning-grade POC and is now maturing into a
real system: the distributed-execution core (scan slicing, a staging planner, a materialize-once
shuffle, fleet discovery) is in place, and the roadmap ahead is production-track — a Postgres
services DB (metadata/catalog/accounts), virtual warehouses, RBAC, a query scheduler, fault
tolerance, DML. Treat the code as production-track, not throwaway; the "POC" label describes where
it started, not where it is.

## Workflow — branch and PR, keep `main` releasable

Development is **not** trunk-direct anymore. Every change lands through: a feature branch
(`claude/issue-<n>-<slug>`) → a pull request → review (Copilot at minimum) → green CI →
squash-merge to `main`. `main` stays releasable at all times. Never push code straight to `main`.

## Version pins — do NOT bump independently

`iceberg-datafusion` 0.10 caps `datafusion` at `^53.1`; everything downstream follows.

| Crate | Pin | Reason |
| - | - | - |
| datafusion / datafusion-proto | 53.1 | capped by iceberg-datafusion 0.10 |
| iceberg / iceberg-datafusion | 0.10 | latest |
| arrow / arrow-flight / parquet | 58.4 | datafusion 53.1 → arrow ^58; ONE version tree-wide |
| object_store | 0.13 | datafusion 53.1 → object_store ^0.13.1 |
| sqlx | 0.8 | `iceberg-catalog-sql` 0.10 needs `^0.8.1` — ONE sqlx tree-wide. Do NOT go to 0.9 |

Coordinator and workers must run the identical build — serialized DataFusion plans are not
cross-version compatible. Bumping DataFusion to 54 waits on `iceberg-datafusion` 0.11.

This cap is a standing **strategic constraint**, not a passing note: several roadmap items live
under it. Do NOT add a dependency that pulls in a second `arrow` / `object_store` / `datafusion`
version — vet every new crate with `cargo tree -d` before committing. One version tree-wide is what
keeps the Flight plan-serialization boundary working. The path past the wall is `iceberg-datafusion`
0.11 (which unlocks DataFusion 54); until then, prefer designs that don't require it, and if a
must-have crate would force a second version, stop and treat it as its own scoped decision rather
than bumping.

Every binary is stamped with `version+git-sha` (workspace version + the commit, injected by
`lldb-qe-core/build.rs`): both binaries report it via `--version` and a startup log line, so an
operator can confirm a whole fleet is the identical build. CI builds one image, tags it by that
version, and the compose cluster runs that single tag for every role (`LLDB_IMAGE`).

## Layout

- `crates/lldb-qe-core` — storage (`storage.rs`, incl. S3), config-as-data catalog
  (`manifest.rs` + `catalog.rs`), session, Flight transport, plan codec, shared CLI/logging
  config (`config.rs`), Postgres services DB / control plane (`services.rs` + `migrations/`)
- `crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` — thin clap/env-configured binaries.
  The coordinator package also builds `lldb-qe-migrate` (`src/bin/`), the one-shot that applies
  the services-DB migrations
- `manifests/` — example catalog manifests (config-as-data); TPC-H is just one of them
- `Dockerfile` / `docker-compose.yml` — one image, all three binaries; a MinIO + Postgres 18.4 +
  worker-fleet cluster
- `infra/` — AWS CDK (TypeScript): ECS Fargate worker fleet, S3 warehouse, ECR. **CDK, not
  Terraform.** Deploys one pinned `imageTag` to every role; synth fails on `latest`
- `data/` — generated TPC-H + local Iceberg warehouse (gitignored)

## Catalogs are config, not code

Do NOT hardcode schemas. Declare tables in a `Manifest` (see `manifest.rs`) and load them with
`catalog::apply_manifest`. `tpch_manifest` / `register_tpch_parquet` are thin TPC-H seeds over
that generic path — add new schemas as manifests, not bespoke loaders.

A manifest picks its catalog backend: `memory` (per-process, dev default) or `sql` (persistent
Postgres, shared by the whole fleet — `manifests/shared-catalog.toml`). The SQL catalog's `uri`
is optional and normally omitted, because a manifest is committed config and must not carry a
password; it falls back to the fleet's `LLDB_METADATA_*`. Two consequences worth knowing:
`iceberg-catalog-sql` creates and owns `iceberg_tables` / `iceberg_namespace_properties`, so
they are **not** in `migrations/`; and applying a manifest is idempotent — a table that already
exists is not re-created and, crucially, not re-seeded. Iceberg 0.10 ships no object-store
`StorageFactory`, so a `sql` catalog requires a `file://` warehouse and **errors** on `s3://`
rather than silently writing metadata to local disk.

## Writes: append is DataFusion's, `DELETE`/`UPDATE` are ours

`INSERT` goes through `iceberg-datafusion`. **`DELETE` and `UPDATE` do not** — they live in
`dml.rs`, because iceberg-rust 0.10's `Transaction` exposes only `fast_append` (no overwrite, no
rewrite, no delete action), `TransactionAction` is `pub(crate)` and `TableCommit`'s builder is
`pub(crate)`, so there is no public way to commit a snapshot that *removes* a file. `dml.rs`
therefore assembles the snapshot by hand and commits it with the same conditional
`UPDATE iceberg_tables ... WHERE metadata_location = <what I read>` that `iceberg-catalog-sql`
uses — one row, one serialization point, so a DML commit and an `INSERT` commit race each other
correctly. The loser re-plans against the winner's snapshot (bounded retries) rather than
erroring, which is a serializable outcome.

Consequences worth knowing: DML is a **whole-table copy-on-write rewrite** (O(table) per
statement — the cheaper shapes all need a remove-files commit); it requires a **`sql` catalog**,
an unpartitioned **v2** table, and rejects `MERGE`; and writes are **not distributed** — the
coordinator answers them itself. `MERGE` is out because its cardinality-violation rule cannot be
approximated safely, not because of Iceberg. Do not "fix" DML by dropping and recreating a table:
that loses snapshot lineage and the commit race with it.

## Control-plane state lives in Postgres

Data-plane state (bytes in object storage, Arrow in flight, the per-worker stage cache) is
immutable or deliberately per-process. **Control-plane** state — accounts, the catalog,
warehouses, query history — is neither, so it lives in the services database (`services.rs`),
where transactions and constraints arbitrate instead of hope. Two rules:

1. **Schema changes are migrations** in `crates/lldb-qe-core/migrations/`, embedded at compile
   time by `sqlx::migrate!` and applied only by `lldb-qe-migrate`. Coordinators and workers never
   migrate — a rolling fleet racing the same DDL is a production footgun.
2. **An unconfigured services DB is legal.** `ServicesArgs::connect` returns `None`, and
   single-node/local paths must keep working without Postgres. Never make `cargo run` need a
   database.

Passwords are never logged: `ServicesArgs` has a hand-written redacting `Debug`, and every
message naming a connection URL goes through `services::redact_url` first.

## Commands

```
tpchgen-cli -s 1 --format=parquet --output-dir data/sf1   # test data
cargo test                                                # unit + integration (data-absent tests skip)
cargo fmt --all && cargo clippy --all-targets
docker compose up --build                                 # full cluster (MinIO + Postgres 18.4 + fleet)
LLDB_DOCKER=1 cargo test --test distributed_cluster       # cross-container smoke test (needs a daemon)

# Services DB (control plane). Migrations are an explicit one-shot step, NEVER startup magic —
# a rolling fleet must not race to apply DDL. Compose runs it as the `db-migrate` service.
cargo run -p lldb-qe-coordinator --bin lldb-qe-migrate -- \
  --metadata-url postgres://lldb@localhost/lldb --seed-account default
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test services_db  # or LLDB_DOCKER=1

# Shared Iceberg catalog (`backend = { kind = "sql" }`): proves two independently-built
# lakehouses on one Postgres see the same tables and the same snapshot. Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test shared_sql_catalog

# DML (`DELETE`/`UPDATE`) + the concurrent-writer race. Same three-way gating. The race test
# asserts on the *data* (four writers, `qty = qty + 1`, final value must be exactly 4), so a lost
# or double-applied commit fails it as a wrong number rather than as an error.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test dml_snapshots
cd infra && npm ci && npm test                            # CDK assertion tests
cd infra && npx cdk synth -c imageTag=<version+sha>       # emit CloudFormation
```

## Testing bar

Every module carries a `#[cfg(test)] mod tests`; end-to-end paths get a `tests/` integration
test. No milestone lands without green tests.
