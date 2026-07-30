# Roadmap gaps — what is not yet a story, and why

**Snapshot: 2026-07-30.** A register of work that is *known to be missing* but is deliberately not
filed as a GitHub issue yet. Its purpose is to stop a gap being rediscovered from scratch in six
months, and — more importantly — to record **why** each one is not a story, because "nobody thought
of it" and "we thought about it and it is not ready" are very different states and look identical
in an empty backlog.

## How to use this

- **Filed work lives in GitHub issues, not here.** This file indexes what is *not* filed. When an
  item graduates to an issue, replace its entry with a one-line pointer rather than deleting it —
  the reasoning about *why it waited* is usually still the most useful part.
- **An entry here is not a commitment.** Several items below should probably never be built.
- **Re-read the blockers before picking anything up.** Most entries are gated on something
  external, and the gate is the point.

## The honest position

A single "percent of Snowflake" number flatters the project, because Snowflake's engineering mass
is concentrated in exactly the areas this system has not started. Split by dimension, assessed
2026-07-30:

| Dimension | Where we are | What is missing |
| - | - | - |
| Distributed execution core | ~35% | Scan slicing, staging, materialize-once shuffle, stage reassignment are real. No spilling, no memory-pressure handling, no skew handling, no adaptive re-planning. |
| Table format / storage | ~50% | Iceberg gives snapshot isolation, schema evolution and lineage nearly free. Time travel exists in the format but is unreachable; no clone, no fail-safe, no retention policy. |
| Control plane | ~30% | Accounts, warehouses, history, liveness, reaper, fleet-wide admission. No auto-suspend/resume, no multi-cluster scale-out, no resource monitors. |
| Security | ~40% | TLS on both hops, plan-time RBAC, per-request worker identity, per-tenant catalogs. No column/row-level security, no masking, no network policies, no key management. |
| Optimizer / performance | ~10% | DataFusion's optimizer as shipped. No custom statistics, no clustering, no pruning beyond what Iceberg manifests give for free. Snowflake's deepest moat; barely started. |
| SQL surface | ~15% | No multi-statement transactions, no UDFs or stored procedures, no semi-structured types, no time-travel syntax. |
| Ecosystem | ~5% | Arrow Flight only. No JDBC/ODBC, no web UI, no drivers. |
| Observability | ~15% | Query history and admission counters. No query profile, no usage views. |

**The skeleton is maybe 35%. The product is closer to 12%.**

**The heaviest caveat, which applies to every number above: none of this has been run at scale or
measured.** Every claim in this repository is test-backed, and tests are not measurements. #9 (a
real deploy) is blocked, there is no AWS account, and the largest workload ever executed is TPC-H on
a laptop-sized fleet. Snowflake's hard problems mostly appear at a scale this system has never seen.
A distributed engine that is correct on four in-process workers and one that is correct on four
hundred are different systems, and we only know we are the first one.

## Filed — for orientation

Open issues as of this snapshot. Detail lives in the issues; this is an index so the gaps below can
be read in context.

| # | Subject |
| - | - |
| #7 | Publish versioned images to a registry |
| #9 | Validate the CDK stack with a real deploy |
| #32 | Fleet token into the ECS task definitions (gated on #33's posture decision) |
| #39 | DML is a whole-table rewrite; `MERGE` unimplemented (gated upstream) |
| #40 | The `iceberg-datafusion` 0.10 version wall — tracking DataFusion 54 |
| #41 | Nothing makes a warehouse's desired size real (the actuator gap) |
| #59 | Iceberg time travel is paid for and unreachable |
| #60 | Query history cannot say who ran a query |
| #61 | No query profile — no execution metrics collected anywhere |
| #62 | Nothing bounds tenant spend, and there is no unit of consumption |
| #63 | The Iceberg rewrite discards every manifest statistic — and runs too late to use one |
| #64 | No `MemoryPool` is configured, and the largest buffer is one no pool can see |
| #65 | `DELETE`/`UPDATE` are unreachable from `lldb-qe-server`, the only binary that authenticates |
| #66 | Multi-statement transactions — settle the session question first |
| #67 | Auto-suspend and auto-resume (blocked on #41) |
| #68 | Multi-cluster warehouses — scale out on concurrency |
| #69 | Column-level, row-level and masking — RBAC is whole-table allow/deny |

## Not yet filed

### A. Awaiting a research spike — *all graduated 2026-07-30*

This bucket is empty. The three items in it were researched against source and filed as #63
(statistics), #64 (memory pressure) and #66 (transactions). Each is filed **spike-first or with its
governing decision stated up front** rather than as an implementation ticket, which is the outcome
this bucket exists to produce — the research did not make them ready to build, it made them ready to
*decide*.

Three findings from that research are worth keeping here, because they change how neighbouring work
should be read:

- **Statistics cannot influence a plan under the current architecture.** `resolve_iceberg_scans`
  runs *after* `create_physical_plan` (`engine.rs:471` → `:493`), so every statistics consumer has
  already run against `Statistics::new_unknown`. Populating statistics in the rewrite would change
  `EXPLAIN` and nothing else. #63 is therefore a decision about *where scan resolution happens*
  before it is anything else.
- **The largest per-worker allocation is invisible to any memory pool.** `MaterializedStage` holds
  every output partition of a producer stage and takes no `MemoryReservation`, so configuring a pool
  squeezes the operators *underneath* the untracked buffer that is actually consuming the memory.
  See #64 — the two halves must land together.
- **A whole write path was found missing.** `DELETE`/`UPDATE` are unreachable from `lldb-qe-server`
  entirely; they exist only in the one-shot `lldb-qe-coordinator`, which is deliberately *not*
  access-controlled. Filed as #65. It blocks both #66 and the write half of #69, and nothing
  documented it.

### B. Blocked on measurement

Design decisions that cannot be made honestly without numbers from a real deployment. Filing these
now would bake in guesses.

- **Skew handling.** Slicing is currently size-unaware or count-based; whether that produces real
  skew depends on data we do not have.
- **Adaptive re-planning.** Worth nothing until there is evidence the static plan is wrong.
- **Clustering and data-layout optimization.** Snowflake's automatic clustering is a response to
  measured access patterns. We have no access patterns.
- **A worker-local cache tier.** Caching parquet on worker disk is an obvious idea whose value is
  entirely a function of hit rate, which is unknown.

**This whole bucket is downstream of #9.** A real deploy is the single highest-leverage unblock in
the project, because it converts an entire category of speculation into measurement.

### C. Blocked upstream

Tracked by **#40**, which is the standing record of the version wall. Nothing here should be
attempted before `iceberg-datafusion` 0.11.

- **DataFusion 54** and whatever it changes about `datafusion-proto`'s wire format.
- **Cheaper DML shapes** — needs a public remove-files/overwrite action. This is #39.
- **An object-store `StorageFactory`.** Iceberg 0.10 ships only local-fs and memory, which is why a
  `sql` catalog requires a `file://` warehouse and errors on `s3://`. This constrains deployment
  topology and makes a per-tenant warehouse root awkward to place.
- **Public `metadata_table()`.** `IcebergMetadataTableProvider` covers snapshots and manifests but
  its constructor is `pub(crate)` in 0.10, so snapshot discovery has to be built by hand (see #59).

### D. Blocked on environment

- Everything requiring AWS: **#9** (deploy validation), **#32** (verifiable only on a real deploy),
  **#41** (the actuator), and certificate provisioning on Fargate — which cannot mount a Secrets
  Manager value as a file, the `KNOWN GAP` recorded in `infra/`.
- **#7** (registry publishing) gates #9's step 2 on a manual docker push.

### E. Deferred by decision

Real gaps where the decision is "not now" rather than "not ready". Grouped by what they are.

**Storage and table management**
- **Snapshot retention / `expire_snapshots`.** The natural companion to #59: we retain every
  snapshot forever and have no way to reclaim them. This is also the compliance answer to time
  travel reading around a `DELETE`, which makes it more than housekeeping.
- **Zero-copy clone.** Iceberg makes this cheap in principle. No demand yet.
- **Fail-safe / undrop.** Depends on retention above.
- **Materialized views.** Large, and the result cache covers some of the same ground for free.
- **Streaming ingest.** No continuous-ingest path exists; everything is batch DML.

**SQL surface**
- **UDFs and stored procedures.** Both are large, and both are a sandboxing problem before they are
  a language problem — worth stating plainly, because "add UDFs" reads much smaller than it is.
- **Semi-structured types (VARIANT/JSON).** A significant Snowflake differentiator, entirely absent.
- **`MERGE`.** Tracked under #39, and refused today for a *correctness* reason worth preserving: its
  cardinality-violation rule cannot be approximated safely.

**Security**
- **Column-level and row-level security, and masking** — now **#69**. Kept here for the finding: the
  natural implementation (a DataFusion `AnalyzerRule`) fires *after* the result-cache key is composed
  and after the lookup, so an unmasked result stored by one role would be served to another. The
  current allow/deny model is safe only because denial is all-or-nothing; RLS and masking break that
  premise, because both users are *allowed* and must get different answers.
- **Network policies / IP allowlists.** Not started.
- **Key management, customer-managed keys, encryption at rest.** Not started.
- **Asymmetric plan-assertion keys.** Named as the intended end state in `plan_assertion.rs`: the
  current HMAC key is symmetric, so a compromised worker can forge an assertion. The payload's
  version byte is where a key id would go.
- **mTLS at the fleet boundary.** The open posture decision behind #33/#32. Until it is settled, #32
  may be unnecessary rather than merely later.
- **Hitless credential rotation.** Rotating `LLDB_FLEET_TOKEN` currently requires a fleet restart,
  with a window during a rolling restart where the halves disagree. Needs a *set* of accepted keys.

**Control plane and elasticity**
- **Auto-suspend / auto-resume** — now **#67**. **Multi-cluster scale-out** — now **#68**, and
  gated on #67 rather than parallel to it. Both remain hard-blocked on #41: without an actuator,
  auto-suspend stops routing while ECS keeps billing, and auto-resume replaces an actionable "resume
  it first" error with a worse one. Note #68's honest caveat — no measurement shows concurrency is
  the binding constraint, so it may be a feature wanted because Snowflake has it.
- **Fleet-wide FIFO fairness.** `fleet_admission.rs` polls, so FIFO holds within a coordinator and
  not between them. `LISTEN`/`NOTIFY` is the identified next step.
- **Per-tenant admission queues.** The process-local gate is keyed by warehouse *name*, so two
  tenants can share a queue. A known fairness wart, deliberately untouched.
- **Multi-region and replication.** Not started.

**Ecosystem** — the largest *surface* gap in the project and the least interesting engineering.
- JDBC and ODBC drivers, a web UI, Python/JS clients, BI-tool connectors. Arrow Flight is the only
  way in today. Filing these would create backlog nobody will action; they need a product decision
  about who the client is, first.
- **Data sharing / cross-account access.** Depends on tenancy decisions not yet made.

## Known limitations already named in code

Documented honestly at the site, easy to lose track of because they are not in any backlog. Listed
so they can be found; each is a deliberate, argued trade-off rather than an oversight.

| Limitation | Where |
| - | - |
| Worker-side work is not cancelled — a stage already materializing runs to completion | `cancel.rs` |
| A row with a NULL `coordinator_incarnation` is never reaped | `reaper.rs` |
| A row stranded by the insert-to-guard window waits for its incarnation to disappear | `reaper.rs` |
| A slot leaked by a live coordinator that never claims again waits for its registration | `fleet_admission.rs` |
| Plan assertions are symmetric — a worker can forge one; rotation needs a restart | `plan_assertion.rs` |
| Assertion coverage is by directory, not by exact file list | `plan_assertion.rs` |
| Iceberg scans refuse row-level deletes, non-parquet files, partitioned tables, multi-store snapshots | `iceberg_scan.rs` |
| A refused scan is a refused query — there is no single-node fallback | `iceberg_scan.rs` |
| DML is O(table) copy-on-write, coordinator-local, `sql`-catalog and unpartitioned-v2 only | `dml.rs` |
| A `sql` catalog errors on `s3://` — `file://` warehouses only | `lakehouse.rs` |
| Result-cache inputs are versioned by *current* snapshot, which time travel would separate | `result_cache.rs` |

## Deliberate non-goals

Not gaps. Recorded so they are not "fixed" by someone reading the list above as a to-do.

- **Submit-then-poll, and streaming results.** Named in `server.rs`'s "what this does NOT do" list as
  deliberate rather than missing.
- **An orchestrator SDK in the engine binaries.** The control plane is desired state and does not
  actuate. Adding an AWS SDK to a coordinator would breach the one-version dependency wall and
  hard-code one cloud into the control plane. This is why #41 is a separate component.
- **A second `arrow` / `object_store` / `datafusion` version, ever.** The Flight plan-serialization
  boundary depends on one version tree-wide. `cargo tree -d` must stay at **48**.
- **Migrations applied by coordinators or workers.** A rolling fleet racing the same DDL.
- **Requiring Postgres, certificates or a fleet secret for `cargo run`.** Every control-plane feature
  degrades to single-node behaviour.

## What would move the number most

In order, and the order matters more than the list:

1. **A real deploy with real data volume (#9).** Converts an entire category of speculation into
   measurement, and unblocks bucket B wholesale. Nothing else on this page has comparable leverage.
2. **Statistics and pruning.** The widest capability gap, and it compounds — every other performance
   improvement is measured against a plan that could have been better.
3. **Auto-suspend / auto-resume.** What users actually experience as "elastic", and cheap relative to
   its perceived value — but gated on #41.

**What is genuinely ahead of a typical project at this stage** is the discipline around refusals:
Iceberg scan shapes that cannot be distributed correctly are refused rather than silently run,
unknown plan extensions fail closed, and the version wall is treated as a design constraint rather
than a chore. None of that is a Snowflake feature. It is why the 35% is real rather than a demo.
