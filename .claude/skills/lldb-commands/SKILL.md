---
name: lldb-commands
description: Runnable commands for the lldb query engine — generating TPC-H data, the gated integration suites (services DB, shared catalog, result cache, DML, scheduler, cancellation, liveness, reaper, auth/RBAC, tenancy, TLS, plan assertions), the operator binaries (migrate, warehouse, auth, reap, server), the compose cluster, and CDK. Use when running or selecting tests, invoking an operator binary, or bringing up the cluster.
---

# lldb — commands

Every gated suite below skips silently without its prerequisite, so a green run does not by itself
mean a suite ran. The integration binary takes roughly 20s with a database versus under 2s without
— that timing is the tell for whether the database-gated tests actually executed.


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
# query recorded in `queries`. Admission control is FLEET-WIDE with a services DB: two servers on
# one warehouse admit K between them, not K each (crates/lldb-qe-core/src/fleet_admission.rs).
# The `query_scheduler` module carries both acceptance tests — the two-coordinator K-not-2K one
# (asserted on `peak_concurrency` across every `queries.coordinator` value, which is exactly where
# a per-process counter is self-consistently wrong), and the one that kills a slot-holder and
# demands its slots come back.
cargo run -p lldb-qe-coordinator --bin lldb-qe-server -- \
  --workers http://127.0.0.1:50051 --metadata-url postgres://lldb@localhost/lldb
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration query_scheduler

# Cancelling a running query (`cancel.rs`). The acceptance assertion is that the QUEUE ADVANCES:
# a warehouse of size 1, one query held on a gated (but otherwise real) worker, one queued behind
# it, and after the cancel the queued one must start AND return its answer. Also: the cross-account
# refusal is indistinguishable from an unknown id, USAGE is not permission to cancel, and a
# cancelled row is never taken by the reaper. Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration query_cancel

# Coordinator liveness (`liveness.rs`): registers, renews, is promptly not-live on a clean exit and
# not-live within MISSED_RENEWALS_BEFORE_DEAD intervals after a kill — and, the assertion that makes
# a reaper safe to build, a LIVE coordinator running a query that outlives the threshold is never
# concluded dead. Same three-way gating. Slower than the rest of the binary on purpose:
# `renew_interval_secs` is whole seconds, so the shortest threshold that can exist is 3s and a test
# that outlives it must really take that long.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration coordinator_liveness
# Reaping stranded query rows (`reaper.rs`). A one-shot, like `lldb-qe-migrate`: schedule it
# (cron / an ECS scheduled task) at whatever interval you want stranded rows resolved within.
# Idempotent, bounded by --limit, and --dry-run shows what it would take without writing.
cargo run -p lldb-qe-coordinator --bin lldb-qe-reap -- \
  --metadata-url postgres://lldb@localhost/lldb --dry-run
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration query_reaper

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
# Privileges: SELECT | INSERT | DELETE | UPDATE | USAGE | CANCEL | ALL. CANCEL is held on a
# warehouse and permits stopping any query running on it; USAGE (which every submitter needs)
# deliberately does not imply it, and ALL does.
cargo run -p lldb-qe-coordinator --bin lldb-qe-auth -- \
  --metadata-url postgres://lldb@localhost/lldb \
  grant --role oncall --privilege CANCEL --object-type warehouse --object-name analytics
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

# A catalog per tenant: two accounts own the SAME qualified table name from ONE manifest, and a
# catalog-wide grant — held by both — still reaches only its own account's rows. Asserted against
# the live `iceberg_tables` (two rows, distinct `catalog_name`, distinct `metadata_location`), so
# scoping the name without the warehouse root fails it on the file paths rather than passing.
# Same three-way gating.
LLDB_TEST_POSTGRES_URL=postgres://… cargo test -p lldb-qe-core --test integration tenant_catalogs

# TLS on both Flight boundaries (`tls.rs`). Needs NO database and no fixtures: the suite mints its
# own CA with `rcgen` at run time, so nothing key-shaped is ever committed. One query crosses an
# encrypted client→coordinator hop AND an encrypted coordinator→worker hop and is asserted on the
# rows; a plaintext client is refused by a TLS worker *within a timeout*, so "legible, not a hang"
# is an assertion; and the no-certificate path is proven still to work in the same process, which
# is the inertness claim for the ambient client trust. The refuse-to-start rule is a unit test in
# `tls.rs` — it is a pure function of flags and needs no socket.
cargo test -p lldb-qe-core --test integration flight_tls
cargo test -p lldb-qe-core tls::tests

# Per-request identity at the worker boundary (`plan_assertion.rs`). Needs NO database: the fleet
# secret and the assertion are both passed explicitly, because `LLDB_FLEET_TOKEN` is read once per
# process and `set_var` is `unsafe`. Four properties — a plan with no assertion (or one covering
# another table) is REFUSED; stage caching still HITS across requests carrying different assertions
# (the trap: an assertion inside the plan bytes would change the content-addressed stage id and make
# every request a miss); a worker forwards the assertion to the worker it pulls from; and a
# reassigned stage carries it to whichever worker serves it.
cargo test -p lldb-qe-core --test integration worker_plan_assertion
cargo test -p lldb-qe-core plan_assertion::tests
# Serve TLS. Certificates are supplied by the deployment; the engine issues none. Files, or —
# where a file cannot be mounted (ECS Fargate) — LLDB_TLS_CERT_PEM/_KEY_PEM/_CA_PEM inline.
cargo run -p lldb-qe-coordinator --bin lldb-qe-server -- \
  --tls-cert /certs/server.crt --tls-key /certs/server.key --tls-ca /certs/ca.crt \
  --workers https://worker-1.lldb.local:50051 --metadata-url postgres://lldb@localhost/lldb
cd infra && npm ci && npm test                            # CDK assertion tests
cd infra && npx cdk synth -c imageTag=<version+sha>       # emit CloudFormation
./scripts/mint-fleet-tls.sh                               # fleet CA + cert -> Secrets Manager
cd infra && npx cdk deploy -c imageTag=<version+sha> -c tls=fleet   # TLS fleet (mint first)
```

