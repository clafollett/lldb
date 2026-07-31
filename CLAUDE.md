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
`lldb-qe-control/build.rs` and re-exported from `lldb-qe-core` as `BUILD_VERSION`, because
`liveness.rs` records it on every coordinator registration and is the lowest crate that reads
it): both binaries report it via `--version` and a startup log line, so an
operator can confirm a whole fleet is the identical build. CI builds one image, tags it by that
version, and the compose cluster runs that single tag for every role (`LLDB_IMAGE`).

## Layout

- `crates/lldb-qe-types` — the vocabulary, and the crate the version wall does not apply to: RBAC
  (`rbac.rs`) and the storage *declaration* (`storage.rs`). Depends on **none** of
  datafusion / arrow / iceberg / parquet / object_store / sqlx, and
  `cargo tree -p lldb-qe-types` piped through that pattern must stay empty
- `crates/lldb-qe-control` — the control plane, and the second crate the version wall does not
  apply to: Postgres services DB (`services.rs` + `migrations/`), identity and credentials
  (`auth.rs`), virtual warehouses (`warehouse.rs`) and the discovery that routes to them
  (`discovery.rs`), admission control (`scheduler.rs`) with the fleet-wide bound behind it
  (`fleet_admission.rs`) and the cancellation that hands a slot back (`cancel.rs`), query history
  (`query_log.rs`), coordinator liveness (`liveness.rs`) and the sweep that acts on it
  (`reaper.rs`), the transport those credentials travel over (`tls.rs`), the per-account
  catalog/warehouse partitioning auth and rbac rest on (`tenancy.rs`), and the shared CLI/logging
  config (`config.rs`). Depends on **none** of datafusion / arrow / iceberg / parquet, and
  `cargo tree -p lldb-qe-control` piped through that pattern must stay empty. `sqlx` is expected
  here — the control plane *is* a database
- `crates/lldb-qe-core` — the query engine: storage (`storage.rs`, incl. S3), config-as-data
  catalog (`manifest.rs` + `catalog.rs`), session, Flight transport, plan codec, the
  coordinator-side Iceberg-scan resolver that makes an Iceberg plan shippable and sliceable
  (`iceberg_scan.rs`), the cross-query result cache (`result_cache.rs`), the one-query pipeline
  both front ends share (`engine.rs`), grants and the plan-time check (`rbac.rs`), the signed,
  short-lived assertion that carries that check's answer to a worker (`plan_assertion.rs`), and
  the long-running coordinator (`server.rs`) — which is the **composition root**, wiring the
  control plane to the engine, and is why it lives here rather than in `lldb-qe-control`. Every
  control-plane module is re-exported from `lib.rs`, so `lldb_qe_core::auth`, `::services`,
  `::tls` and the rest still resolve exactly as before
- `crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` — thin clap/env-configured binaries.
  The coordinator package also builds `lldb-qe-server` (`src/bin/`), the long-running query
  scheduler and the *only* binary that authenticates. Both targets here run queries, and that is
  the entry rule: cargo resolves dependencies **per package, not per binary**, so anything in this
  package compiles the whole DataFusion graph whether it imports it or not
- `crates/lldb-qe-admin` — the operator one-shots: `lldb-qe-migrate` (applies the services-DB
  migrations), `lldb-qe-warehouse` (create/list/resize/suspend/resume a warehouse), `lldb-qe-auth`
  (users, API keys, roles, grants) and `lldb-qe-reap` (resolve query-history rows stranded by a
  coordinator that died). The third crate the version wall does not apply to: it depends on
  `lldb-qe-control` and `lldb-qe-types` and on **none** of datafusion / arrow / iceberg / parquet,
  and `cargo tree -p lldb-qe-admin` piped through that pattern must stay empty. A binary that needs
  the engine does not belong here; a binary that does not need it does not belong in
  `lldb-qe-coordinator`
- `manifests/` — example catalog manifests (config-as-data); TPC-H is just one of them
- `Dockerfile` / `docker-compose.yml` — one image, every binary (it builds `-p lldb-qe-coordinator
  -p lldb-qe-worker -p lldb-qe-admin`; drop a `-p` and the services that invoke those binaries lose
  their entrypoint); a MinIO + Postgres 18.4 + worker-fleet cluster
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

## Where the rest of the design rationale lives

The per-subsystem arguments — Iceberg scan resolution, DML, the control plane, access control, the
worker plan assertion, TLS, per-tenant catalogs, coordinator liveness, cancellation, the reaper,
fleet-wide admission and the result cache — moved to **`crates/lldb-qe-core/CLAUDE.md`**, which
loads automatically whenever a session works under that crate. They are long, and they were costing
every session that never touched the crate about 8.8k tokens.

Read that file before changing any of those subsystems: most of its sections exist to name a
tempting alternative and say why it is wrong, and the module docs alone will not tell you that.

**If you are working in `crates/lldb-qe-admin`, `crates/lldb-qe-control`,
`crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` or `infra/` and the
change touches one of those subsystems, read it explicitly** — it will not have loaded for you.

## Non-negotiables — these apply everywhere, including outside `lldb-qe-core`

Kept resident deliberately. The reasoning behind each is in `crates/lldb-qe-core/CLAUDE.md`; the
rule itself must not depend on that file having been loaded.

- **Never push straight to `main`.** Feature branch → PR → review → green CI → squash-merge.
- **Do NOT bump `datafusion`, `arrow`, `object_store`, `iceberg` or `sqlx` independently**, and do
  not add a dependency that pulls in a second `arrow` / `object_store` / `datafusion` version. Vet
  every new crate with `cargo tree -d` before committing.
- **Schema changes are migrations**, in `crates/lldb-qe-control/migrations/`, applied only by
  `lldb-qe-migrate`. Coordinators and workers never migrate — a rolling fleet racing the same DDL
  is a production footgun.
- **An unconfigured services DB is legal.** `cargo run` must never need Postgres, certificates, or
  a fleet secret. Every control-plane feature degrades to today's single-node behaviour.
- **Fail closed on anything we cannot name** at the auth boundary — DDL, `COPY TO`, `DESCRIBE` and
  unknown plan extensions are refused rather than allowed.
- **Do not widen an Iceberg-scan refusal to make a query run.** Widen the *check*, or add the
  missing reader; quietly running a shape we cannot distribute correctly hides the problem.
- **Do not "fix" DML by dropping and recreating a table** — that loses snapshot lineage and the
  commit race with it.
- **Passwords are never logged.** Every message naming a connection URL goes through
  `services::redact_url` first.
- **Do NOT pass `CARGO_PROFILE_DEV_DEBUG=0` / `CARGO_INCREMENTAL=0` on the command line** — see the
  build-profile section below.

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

The full set — data generation, every gated integration suite, the operator binaries, the compose
cluster and CDK — is the **`lldb-commands` skill** (`.claude/skills/lldb-commands/SKILL.md`).
Invoke it when running or selecting tests, driving an operator binary, or bringing the cluster up.

The two that come up constantly:

```
cargo test                                                # unit + integration (gated suites skip)
cargo fmt --all && cargo clippy --all-targets
```

A gated suite **skips silently** without its prerequisite, so a green `cargo test` does not mean
the database-gated tests ran. The tell is timing: the `integration` binary takes ~20s with a
database and under 2s without.

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
