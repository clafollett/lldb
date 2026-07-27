# lldb — build notes for Claude

Distributed analytical query engine: DataFusion + Arrow Flight + Iceberg on object storage — a
rudimentary cloud data warehouse. It **began** as a learning-grade POC and is now maturing into a
real system: the distributed-execution core (scan slicing, a staging planner, a materialize-once
shuffle, fleet discovery, and Iceberg scans resolved to their snapshot's files so an Iceberg query
can be distributed at all) is in place, and the roadmap ahead is production-track — a Postgres
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
  (`manifest.rs` + `catalog.rs`), session, Flight transport, plan codec, the coordinator-side
  Iceberg-scan resolver that makes an Iceberg plan shippable and sliceable (`iceberg_scan.rs`),
  shared CLI/logging config (`config.rs`), Postgres services DB / control plane (`services.rs` +
  `migrations/`), virtual warehouses (`warehouse.rs`) and the discovery that routes to them
  (`discovery.rs`), the cross-query result cache (`result_cache.rs`), the one-query pipeline both
  front ends share (`engine.rs`), admission control (`scheduler.rs`), query history
  (`query_log.rs`), the long-running coordinator (`server.rs`), and access control — identity and
  credentials (`auth.rs`) plus grants and the plan-time check (`rbac.rs`)
- `crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` — thin clap/env-configured binaries.
  The coordinator package also builds (`src/bin/`): `lldb-qe-migrate` (applies the services-DB
  migrations), `lldb-qe-warehouse` (create/list/resize/suspend/resume a warehouse),
  `lldb-qe-auth` (users, API keys, roles, grants) and `lldb-qe-server` (the long-running query
  scheduler — the *only* binary here that authenticates)
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

## Iceberg reads: resolved to files on the coordinator, then distributed

`iceberg-datafusion` plans a table read as an `IcebergTableScan` — a DataFusion *extension* node
holding a live `iceberg::table::Table` (catalog handle, FileIO, metadata) that resolves its own
files when it executes. That node can be neither **serialized** (`LldbCodec` encodes exactly one
node, `FlightReaderExec`) nor **byte-range sliced** (it is not a `DataSourceExec` over a
`FileScanConfig`). Until `iceberg_scan.rs` existed, that meant **no Iceberg query could be
distributed at all** — the fleet only ever worked on plain-parquet listing tables, which is the
opposite of what this engine is for.

`iceberg_scan::resolve_iceberg_scans` rewrites every such node — **on the coordinator, before the
plan is staged, sliced or serialized** — into a plain parquet `DataSourceExec` over the concrete
data files of the snapshot the scan was planned against. `engine::run_on_fleet` calls it at the
top, so the one-shot coordinator and `lldb-qe-server` inherit it identically. Three consequences,
in order of importance:

1. **The snapshot is pinned by construction.** The file list *is* the snapshot, and it travels
   inside the plan bytes. The tempting alternative — teach the codec to encode a table identifier
   plus a snapshot id and let each worker load the table and re-plan its files — is the design to
   reject, and not merely because it puts a catalog credential in every worker and re-does manifest
   planning `n` times. Each worker would resolve *which files* independently, so a commit landing
   mid-query could leave two workers of the same query reading two different tables: the exact
   split-brain #8 closed by putting one shared catalog behind the fleet, reopened one layer down.
   Naming the snapshot id narrows that window; it does not close it, because the file list is still
   recomputed remotely from state the coordinator does not control.
2. **A worker needs no catalog access of any kind** — no connection, no credential, no manifest, no
   warehouse path. `tests/integration/distributed_iceberg.rs` proves that rather than asserting it:
   its catalog is a per-process `MemoryCatalog`, so the workers *cannot* see it even in principle,
   and the query still answers correctly.
3. **Iceberg inherits scan slicing for free.** A `FileScanConfig` is precisely what
   `scan_split::split_scan` cuts into byte ranges, so the fleet reads the snapshot once *between*
   the workers instead of once *each*.

**Deployment requirement this implies:** every worker must be able to read the warehouse's object
store, because the plan names data files directly (`s3://bucket/…`, `file:///wh/…`). A coordinator
that can reach the bucket and workers that cannot is now a broken deployment rather than a slow one.
The rewrite checks the store resolves on the coordinator's session and errors there, naming the
scheme, instead of failing deep inside a remote stage on some worker.

Two details worth keeping straight. The rewrite lands *after* DataFusion's physical optimizer, so
nothing is pushed into the replacement — the `FilterExec` above it still filters the rows
(`IcebergTableProvider` reports every pushdown as `Inexact`, so that node was always going to be
there), and what is lost is parquet row-group pruning, an IO optimization rather than a correctness
property. And the replacement uses **exactly one** `FileGroup` however many files the snapshot has,
because `IcebergTableScan` reports one partition and every parent's `PlanProperties` were computed
against that; re-slicing across the fleet is `split_scan`'s job, later.

### The refusals — each an error naming its reason, never an approximation

A plain parquet read is not a complete Iceberg reader, and the gap between them is where silent
wrong answers live:

| Refused | Why |
| - | - |
| row-level deletes attached to a data file | position/equality deletes are applied by Iceberg's own reader and by nothing else, so reading the data files alone resurrects deleted rows |
| a non-parquet data file (Avro/ORC/Puffin) | `ParquetSource` cannot read it |
| a partitioned table | identity-partition values live in manifest metadata, not in the data file, so a bare parquet read can be missing columns. Checked against the table metadata's partition specs, because iceberg-rust 0.10 hardcodes `FileScanTask::partition_spec` to `None` and the per-file check would never fire |
| data files spanning two object stores | a `FileScanConfig` names exactly one `ObjectStoreUrl` |
| a scan task covering a partial byte range | iceberg 0.10 always plans whole files, so anything else means the library changed under us — and reading the whole file regardless would duplicate rows |

Be honest about what a refusal costs: the binaries have **no single-node fallback**, so a refused
table is a refused query, not a slower one. That is deliberate — quietly running a shape we cannot
distribute correctly on the coordinator would hide the problem — and the errors say what to do about
the table (compact it, rewrite it as parquet). Do not "fix" one of these by widening the rewrite;
widen the *check*, or add the missing reader.

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
   database. Warehouse routing follows the same rule: `--warehouse` is opt-in, and without it
   `--workers` behaves exactly as it always has.
3. **The control plane is desired state; it does not actuate.** A warehouse row says how much
   compute *should* exist; ECS/compose makes it so. Do NOT add an orchestrator SDK to the engine
   binaries — it would breach the one-version dependency wall above and hard-code one cloud into
   the control plane.

Passwords are never logged: `ServicesArgs` has a hand-written redacting `Debug`, and every
message naming a connection URL goes through `services::redact_url` first.

## Access control: proven identity, checked at plan time

`lldb-qe-server` is the front door and the only binary that authenticates. A request carries
`authorization: Bearer <token>` in the **gRPC metadata, never in the ticket** (a ticket is logged,
hashed and cached; a secret must be in none of those). The token names a user, the user's roles
carry grants, and every object the query's *logical* plan touches is checked before it is staged or
dispatched — and, critically, **before the result cache is consulted**, since a cached row is still
tenant data. Four rules follow:

1. **The account is derived from the credential, never claimed.** The ticket's `account` field
   survives only as an assertion: if it disagrees with the token's, the request is denied.
2. **Auth follows the services DB.** No `--metadata-*` → no accounts, no keys, no grants, so
   nothing is enforced; that is the supported single-node mode and `cargo run` must never need
   Postgres. With one, a credential is required unless `--allow-anonymous` is set (it warns on
   every startup, and it never upgrades a *bad* token to a good one).
3. **Fail closed on anything we cannot name.** DDL, `COPY TO`, `DESCRIBE` and unknown plan
   extensions are refused rather than allowed — there is no privilege that describes what
   `CREATE EXTERNAL TABLE '/etc/'` touches.
4. **Tokens are SHA-256, deliberately not argon2/bcrypt.** A 256-bit CSPRNG token has nothing to
   guess, so a KDF buys no security and costs a dependency plus per-request CPU. Constant-time
   compare, prefix lookup, and the token is stored nowhere — printed once, at creation.

Two boundaries, two claims. The **coordinator** proves "you are user X of tenant Y". A **worker**
only proves membership of the deployment: `LLDB_FLEET_TOKEN`, constant-time compared, required if
set and open-with-a-loud-warning if not. Per-request identity at the worker boundary is a follow-on,
not something this already does.

Be honest about the limit: `iceberg_tables` is owned by `iceberg-catalog-sql` and is **not**
partitioned by account, so tenant isolation over table data is what the grant check enforces and
nothing else. Physically separate per-account catalogs are their own issue.

The other operator binaries — `lldb-qe-coordinator`, `lldb-qe-warehouse`, `lldb-qe-auth`,
`lldb-qe-migrate` — are **not** access-controlled and cannot usefully be: their credential is the
services database's own, and whoever holds that can grant themselves anything. Do not expose them
as a multi-tenant entry point; expose `lldb-qe-server`.

## The result cache is keyed, never invalidated

`result_cache.rs` answers a repeat query from Postgres instead of the fleet. Its key is
`(account, build version, default catalog+schema, statement + plan rendering, every referenced
table @ its Iceberg snapshot id)`. Nothing ever *invalidates* an entry: a commit moves a snapshot,
the next run composes a different key, and the stale row is simply unreachable. Two rules follow —
**an input we cannot version means the query is not cached at all** (a listing table, a table
function, `information_schema`), and **not caching is always a legal answer**, so every refusal and
every services-DB failure falls through to ordinary execution.

## Debug builds are big — the profile handles it, don't re-derive it

A default `--all-targets` debug build of this workspace is ~30 GB, almost all of it debuginfo for
dependencies. That is the version wall's other face: one arrow / object_store / datafusion tree-wide
means nothing dedupes away. `[profile.dev]` in the root `Cargo.toml` sets `debug = 0` and
`incremental = false`, which takes it to ~9.8 GB, and `[profile.test]` inherits both. Consolidating
the integration tests into one binary took it the rest of the way, to **4.4 GB** — the same
one-version-tree-wide fact, seen from the other end: 24 test targets each statically linked the
whole graph, so the target directory held 24 near-identical copies of it.

This is **committed config, not a flag to remember**. Do not pass `CARGO_PROFILE_DEV_DEBUG=0` /
`CARGO_INCREMENTAL=0` on the command line — they are the same settings by another route, and a build
run with different values than the last one invalidates the whole target directory and rebuilds it,
which is the disk exhaustion the profile exists to prevent. If you need line numbers in a backtrace,
change `debug` to `"line-tables-only"` in `Cargo.toml` rather than overriding it per invocation.

There is deliberately **no `.cargo/config.toml`, and in particular no `rustflags` selecting a
linker.** rustc 1.97.1 already links `x86_64-unknown-linux-gnu` with the `rust-lld` that ships
inside the toolchain — `rustc --print link-args` shows it passing `-B .../bin/gcc-ld -fuse-ld=lld`
itself. Setting `-C link-arg=-fuse-ld=lld` was measured (issue #44) and produces a **byte-identical
binary**; all it would buy is one target-directory invalidation per contributor plus a soft
dependency on whatever `lld` is on `PATH`, which on this box is *older* than the bundled one. lld is
worth roughly 8x over GNU `bfd` here — we just already have it. `docs/build-performance.md` holds
the numbers, the method, and the baseline that any future build-time change is measured against;
read it before re-running this experiment.

## Commands

```
tpchgen-cli -s 1 --format=parquet --output-dir data/sf1   # test data
cargo test                                                # unit + integration (data-absent tests skip)
cargo fmt --all && cargo clippy --all-targets
docker compose up --build                                 # full cluster (MinIO + Postgres 18.4 + fleet)
LLDB_DOCKER=1 cargo test --test distributed_cluster       # cross-container smoke test (needs a daemon)

# `crates/lldb-qe-core/tests/` is ONE test target, `integration` (plus `distributed_cluster`,
# which stays separate — see `tests/integration/main.rs` for why). Each former file is a module
# of it, so selecting one is a filter, not a target: `--test integration <module>`, as below.

# Services DB (control plane). Migrations are an explicit one-shot step, NEVER startup magic —
# a rolling fleet must not race to apply DDL. Compose runs it as the `db-migrate` service.
cargo run -p lldb-qe-coordinator --bin lldb-qe-migrate -- \
  --metadata-url postgres://lldb@localhost/lldb --seed-account default
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration services_db  # or LLDB_DOCKER=1

# Shared Iceberg catalog (`backend = { kind = "sql" }`): proves two independently-built
# lakehouses on one Postgres see the same tables and the same snapshot. Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration shared_sql_catalog
# Virtual warehouses (elastic compute). The CLI writes DESIRED state; an actuator applies it —
# `aws ecs update-service --desired-count`, a CDK deploy (-c warehouses=analytics:4,etl:1), or
# `docker compose up -d --scale`. The engine carries no orchestrator SDK, on purpose.
cargo run -p lldb-qe-coordinator --bin lldb-qe-warehouse -- \
  --metadata-url postgres://lldb@localhost/lldb create --name analytics --size 4
cargo run -p lldb-qe-coordinator -- --warehouse analytics --sql "SELECT ..."   # route a query

# Result cache (`result_cache.rs`): proves a repeat query over unchanged tables executes nothing —
# asserted on a WORKER's `StageCache::execution_count`, with a fresh in-process fleet per query so
# "flat" cannot mean "the worker's own stage cache absorbed it" — and that an Iceberg commit
# invalidates it. Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration result_cache_db

# DML (`DELETE`/`UPDATE`) + the concurrent-writer race. Same three-way gating. The race test
# asserts on the *data* (four writers, `qty = qty + 1`, final value must be exactly 4), so a lost
# or double-applied commit fails it as a wrong number rather than as an error.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration dml_snapshots

# Query scheduler. `lldb-qe-coordinator` runs ONE query and exits (compose and the cluster smoke
# test depend on that, unchanged). `lldb-qe-server` is the long-running shape: concurrent
# submissions over Arrow Flight, a bounded number running per warehouse, the rest queued, every
# query recorded in `queries`. Admission control is per COORDINATOR PROCESS, not fleet-wide —
# two servers on one warehouse each enforce their own limit (crates/lldb-qe-core/src/scheduler.rs).
cargo run -p lldb-qe-coordinator --bin lldb-qe-server -- \
  --workers http://127.0.0.1:50051 --metadata-url postgres://lldb@localhost/lldb
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration query_scheduler
# Accounts, API keys, roles, grants. Same operator-tool posture as `lldb-qe-warehouse`: its
# credential IS the Postgres password, so treat that as the deployment's root credential. The token
# `key create` prints is shown exactly once and stored nowhere.
cargo run -p lldb-qe-coordinator --bin lldb-qe-auth -- \
  --metadata-url postgres://lldb@localhost/lldb user create --name alice
cargo run -p lldb-qe-coordinator --bin lldb-qe-auth -- \
  --metadata-url postgres://lldb@localhost/lldb key create --user alice --name cli
cargo run -p lldb-qe-coordinator --bin lldb-qe-auth -- \
  --metadata-url postgres://lldb@localhost/lldb \
  grant --role analyst --privilege SELECT --object-type namespace --object-name lldb.sales
cargo run -p lldb-qe-coordinator --bin lldb-qe-auth -- \
  --metadata-url postgres://lldb@localhost/lldb show

# Authentication, RBAC and tenant isolation end-to-end: the issue's three "done when" bullets as
# actual assertions, plus the worker fleet-secret boundary. Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration auth_rbac

# The grant check dominates the result-cache lookup: a caller whose SELECT is revoked is refused
# even though the answer is still cached and still reachable (the entry is asserted to be live,
# and re-granting turns the next run straight back into a hit). Same three-way gating. Confirm the
# fix's teeth by moving the check below the lookup in `result_cache.rs` — this must then fail.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration cache_grant_ordering
cd infra && npm ci && npm test                            # CDK assertion tests
cd infra && npx cdk synth -c imageTag=<version+sha>       # emit CloudFormation
```

## Testing bar

Every module carries a `#[cfg(test)] mod tests`; end-to-end paths get a `tests/` integration
test. No milestone lands without green tests.

A new integration test is a **module of the `integration` binary** (`tests/integration/`, declared
in its `main.rs`), not a new `tests/*.rs` file — a new file is a new target that statically links
the entire dependency graph again, which is what Story 2 of issue #44 spent itself undoing. The one
reason to add a separate target is process-global state: one binary is one process, so a test that
mutates the environment, or asserts on a process-wide counter, must not share it. `main.rs` names
the three pieces of such state that exist today and why each is safe; add to that list or add a
target, but do not add a file and hope.
