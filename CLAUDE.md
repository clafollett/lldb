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
  config (`config.rs`), Postgres services DB / control plane (`services.rs` + `migrations/`),
  virtual warehouses (`warehouse.rs`) and the discovery that routes to them (`discovery.rs`)
- `crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` — thin clap/env-configured binaries.
  The coordinator package also builds the control-plane one-shots (`src/bin/`):
  `lldb-qe-migrate` (applies the services-DB migrations) and `lldb-qe-warehouse`
  (create/list/resize/suspend/resume a warehouse)
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
   database. Warehouse routing follows the same rule: `--warehouse` is opt-in, and without it
   `--workers` behaves exactly as it always has.
3. **The control plane is desired state; it does not actuate.** A warehouse row says how much
   compute *should* exist; ECS/compose makes it so. Do NOT add an orchestrator SDK to the engine
   binaries — it would breach the one-version dependency wall above and hard-code one cloud into
   the control plane.

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
# Virtual warehouses (elastic compute). The CLI writes DESIRED state; an actuator applies it —
# `aws ecs update-service --desired-count`, a CDK deploy (-c warehouses=analytics:4,etl:1), or
# `docker compose up -d --scale`. The engine carries no orchestrator SDK, on purpose.
cargo run -p lldb-qe-coordinator --bin lldb-qe-warehouse -- \
  --metadata-url postgres://lldb@localhost/lldb create --name analytics --size 4
cargo run -p lldb-qe-coordinator -- --warehouse analytics --sql "SELECT ..."   # route a query
cd infra && npm ci && npm test                            # CDK assertion tests
cd infra && npx cdk synth -c imageTag=<version+sha>       # emit CloudFormation
```

## Testing bar

Every module carries a `#[cfg(test)] mod tests`; end-to-end paths get a `tests/` integration
test. No milestone lands without green tests.
