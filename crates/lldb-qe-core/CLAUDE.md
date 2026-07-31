# `lldb-qe-core` / `lldb-qe-control` — the subsystems, and why each is shaped the way it is

Loaded when a session works under `crates/lldb-qe-core/`. It holds the per-subsystem design
rationale that used to sit in the root `CLAUDE.md` and cost every session ~8.8k tokens whether or
not it was touching this crate. The root file keeps what is genuinely cross-cutting — the version
wall, the workflow, the layout, the build profile, the testing bar, and the non-negotiables digest.

**Half of what follows now lives in `crates/lldb-qe-control/`** — the control plane, the services
DB, access control, TLS, tenancy, liveness, cancellation, the reaper and fleet-wide admission — and
this file does **not** load automatically for a session working there. Read it explicitly before
changing any of those. The arguments are unchanged by the move; only the crate boundary is new.

**These sections argue for the shapes the code already has.** They are not a tour of the code; the
module docs are that, in more detail. Read a section before changing the subsystem it describes,
because most of them exist to name a tempting alternative and say why it is wrong.

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

1. **Schema changes are migrations** in `crates/lldb-qe-control/migrations/`, embedded at compile
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

Two boundaries, two claims — and the worker's boundary now makes two of its own (see the next
section). The **coordinator** proves "you are user X of tenant Y". A **worker** proves membership of
the deployment (`LLDB_FLEET_TOKEN`, constant-time compared, required if set and
open-with-a-loud-warning if not) *and* checks a per-request assertion of what the coordinator
authorized that particular plan to read.

## The worker boundary carries the request's identity, not just the fleet's

`plan_assertion.rs` closes the hole `#28` made concrete: a resolved plan names warehouse data files
by absolute URI, so a worker executing one is reading data at rest, and `FleetAuth` alone made every
plan **self-authorizing** — anything holding the fleet secret could have any plan executed. The
coordinator now mints a short-lived, HMAC'd assertion naming the account, the user, the objects the
grant check passed and the object-store **locations** the plan may read; a worker verifies it and
refuses a plan whose file scans fall outside them. Six things to keep straight.

1. **It travels BESIDE the plan, in gRPC metadata (`lldb-plan-assertion`), never inside it.** That is
   arithmetic, not hygiene: `stage_id_of` is a content hash of the plan bytes and it *is* the
   `StageCache` key, so a per-request value inside them would make every request a cache miss and
   silently destroy the materialize-once shuffle. `AUTHORIZATION_HEADER` still carries the fleet
   token on the same call; both are present and both are checked.
2. **A worker forwards it through the `TaskContext`.** A `FlightReaderExec` deserialized on worker A
   dials worker B, and the plan cannot carry the assertion (1) while a `tokio::task_local!` cannot
   either — `collect_partitioned` drives partitions in a `JoinSet::spawn`. The `TaskContext` is the
   one per-request channel every operator gets by construction. This is why the fleet token's
   ambience is *right* and the assertion's would be wrong: one is per process, the other per request.
3. **The covering check is the point.** A worker that verified a MAC and ignored the plan would have
   built a second fleet token. Coverage is by **directory** (each read file's parent), because exact
   file lists do not fit in a gRPC header — so a sibling file of the same table is covered, and that
   is stated rather than hidden. What is *not* checkable is everything a physical plan lost:
   `SELECT on table lldb.sales.orders` and the user name ride along for audit, and a worker has no
   catalog to check them against.
4. **The gate is the fleet secret, exactly like TLS's plaintext rule.** No `LLDB_FLEET_TOKEN` → no
   key → nothing minted and nothing checked, so `cargo run` and every single-node path are untouched.
   `PlanAuth` is *derived* from `FleetAuth` so the two postures cannot be configured apart
   (`new_with_postures` can express it, is documented as a test seam, and exists only because two
   closed workers in one test process cannot authenticate to each other).
5. **The key is symmetric, so do not overclaim.** It is HMAC-SHA256 keyed from the fleet secret: a
   worker can mint as well as verify, so an assertion proves *"someone in this fleet authorized
   this"*, not *"the coordinator did"*. A compromised worker can still forge one. That is a large
   improvement on a plan needing no authorization at all, and it is not the end state; asymmetric
   keys are, and the payload's version byte is where a key id would go.
6. **Rotation needs a restart, and that is a limitation, not a feature.** One accepted key, derived
   from a secret read once per process, so rotating it means rotating `LLDB_FLEET_TOKEN` and
   restarting the fleet — with a window during a rolling restart where the halves disagree. Hitless
   rotation needs a *set* of accepted keys; it is deliberately not invented here.

Expiry is `DEFAULT_TTL` = 15 minutes with a minute of clock skew, which bounds replay and also
bounds a query: nothing re-mints mid-query, so a query still pulling stages after that fails at the
worker.

## TLS on both boundaries, and a plaintext port you have to ask for

Both credentials above used to cross the wire in the clear. `tls.rs` puts
`tonic::transport::{ServerTlsConfig, ClientTlsConfig}` on **both** Flight boundaries — in process,
not behind a terminating proxy, because workers pull from each other directly and a single front
door would encrypt one hop of a mesh. Certificates are **supplied, never minted**: as files
(`--tls-cert`/`--tls-key` to serve, `--tls-ca`/`--tls-domain` to dial) or as the PEM itself
(`--tls-cert-pem`/`--tls-key-pem`/`--tls-ca-pem`). Six things to keep straight.

1. **The rule is not "TLS unless you said otherwise".** That would make `cargo run` need
   certificates, which is the same rule that forbids it needing Postgres. The rule is: **binding a
   plaintext port while a credential is actually being checked on it requires `--allow-plaintext`**
   — because that, and only that, is where a real secret crosses a real network in the clear. It is
   the `--allow-anonymous` idiom (default secure, explicit opt-in, a warning on every startup) with
   the condition it actually applies to.
2. **Each binary answers "is a credential checked here?" from what it already knows.**
   `lldb-qe-server`: a services database is configured. `lldb-qe-worker`: `LLDB_FLEET_TOKEN` is set.
   `lldb-qe-coordinator`: never — it binds no port, so it takes `TlsClientArgs` and not `TlsArgs`.
   A single-node checkout has no credential to leak and needs no flag and no certificate.
3. **`LLDB_FLEET_TOKEN` is untouched, and TLS does not make it redundant.** This is *server*
   authentication: a client verifies the server, never the reverse. Which fleet member is calling is
   still the shared secret's claim alone, and mTLS at the worker boundary is a separate decision
   (#106, still open). Say so rather than letting "we have TLS now" imply more than it does. What a
   request *is* — the per-request half — is `plan_assertion.rs`'s job, not TLS's; the two are
   complementary and the next section covers it.
4. **The scheme is the switch, and there is no fallback.** `https://` dials TLS, `http://` does not,
   and a TLS server refuses a plaintext client rather than obliging it — so turning certificates on
   means changing the `--workers` URLs too, and a half-converted fleet fails loudly. Note the trap
   `discovery.rs` creates: a DNS endpoint is expanded to one URL *per task IP*, so the name verified
   is an IP unless the certificate carries IP SANs or `--tls-domain` names the certificate's host.
   On ECS the first is unavailable — a Fargate task's IP is allocated at task start and changes on
   every replacement and scale event, which is the elasticity `discovery.rs` exists to deliver — so
   `infra/` mints **one** fleet leaf carrying `DNS:fleet.lldb.local` and sets `--tls-domain` to it.
   One name and not one per warehouse, because the dialing trust is process-global (5) while a
   coordinator dials several warehouses: there is exactly one name available to verify against.
5. **Two process-globals, both argued rather than assumed.** rustls's crypto provider is *installed*
   (`ring`, once, idempotently) rather than inferred from crate features — inference panics if a
   future dependency adds `aws-lc-rs`. And the dialing trust is ambient
   (`tls::install_client_trust`) for the same reason the fleet token is: a `FlightReaderExec` is
   serialized into a plan, so nothing per-call can travel with it. It is consulted **only for
   `https://`**, which is what makes installing one inert for every plaintext caller — and is what
   lets the TLS tests live in the shared `integration` binary at all.

6. **The inline spelling exists for one platform, and does not replace the file one.** ECS Fargate
   resolves a Secrets Manager value into an **environment variable and by no other means**, so a
   fleet there cannot be given a certificate file at all without an entrypoint writing the key to
   disk, an EFS volume, or a $400/month private CA — all priced in #73, all worse. Hence
   `--tls-*-pem`. Two rules keep it honest: a path and its `-pem` twin together are an **error**
   (there is no way to tell which was meant, and the thing being guessed about is a private key),
   and inline material is checked for a `-----BEGIN` line where a file is not — an env var has no
   filename to name in the error, and its realistic failures (an unfilled secret, a shell-mangled
   value) would otherwise surface as an opaque rustls parse failure. `TlsArgs`/`TlsClientArgs`
   carry a hand-written `Debug` rendering PEM as presence, `ServicesArgs`-style.

`docker-compose.yml` is the plaintext path and says so out loud: `LLDB_ALLOW_PLAINTEXT` is set on
every service that binds a port, with a comment explaining that deleting it stops the cluster coming
up — which is the guard working. `infra/` is no longer a gap: `-c tls=fleet` injects the PEM from
three Secrets Manager secrets that `scripts/mint-fleet-tls.sh` fills, and the stack **imports** them
rather than creating them so that a private key is never something CDK holds. It still does not set
`LLDB_ALLOW_PLAINTEXT`, in either mode, and a CDK test asserts that in both.

## A catalog per tenant — the boundary the grant check no longer carries alone

`iceberg_tables` is owned by `iceberg-catalog-sql`, so no migration of ours can add an `account_id`
column to it. `tenancy.rs` partitions it anyway, with the column that is already there: **an account
gets its own catalog name and its own warehouse root** (`TenantScope`), and `catalog_name` is the
leading primary-key column of both of that crate's tables and appears in the `WHERE` clause of every
statement it issues. Three rules follow, and the second is the one that is easy to get wrong.

1. **The two knobs move together, always.** A table's location is `{warehouse}/{namespace}/{table}`
   — the catalog name is *not* in it. Scoping the name without the root gives two tenants clean
   separation in Postgres and the same directory on disk, which is worse than scoping neither
   because it looks separated.
2. **A lakehouse has two catalog names and they are not interchangeable.** `catalog_name()` is what
   SQL says (the manifest's declared name, the same for every tenant, what a grant is written
   against); `iceberg_catalog_name()` is what storage says (`acct_<id>__<declared>`, the row
   discriminator). `dml.rs`'s pointer swap uses the storage one; `result_cache.rs`'s input
   versioning uses the SQL one. Swapping either does not fail — DML would target one shared row, and
   the cache would silently stop firing forever, which is why `ResultCache::catalog_mismatch_count`
   exists and is asserted to be zero.
3. **A session is per account, not per process** (`engine::TenantSessions`). `register_catalog` is
   global to a `SessionContext`, so one context holding every tenant's catalog would make every
   tenant's catalog *name* visible to every other one — isolation back to a check rather than a
   structure. `engine.rs`'s "one context, shared across concurrent queries" claim is about
   concurrency and still holds; the unit changed, not the property. `Coordinator` therefore holds a
   lazily-built memo of sessions, which is per-*tenant* state and still not per-*query* state.

**Tenancy follows the services DB**, like auth: no `--metadata-*` → no accounts → `TenantScope::
untenanted()` → the catalog names and warehouse paths the manifest literally declares. `cargo run`
still needs no Postgres.

Be honest about what this buys: **layout, not access.** Since #28 a resolved plan names data files
by absolute path and a worker reads them with its own credentials. `plan_assertion.rs` is the other
half and it landed (#34): a worker now refuses a plan whose files fall outside the locations the
coordinator's assertion covers, so handing a worker tenant B's plan no longer gets tenant B's files
read under tenant A's request — *given* the fleet has a secret to key that assertion from, and given
whoever forges one does not already hold that secret (it is symmetric). Listing tables are also outside the boundary
entirely — they have no catalog row and no warehouse — so a multi-tenant manifest should declare
`format = "iceberg"`.

The other operator binaries — `lldb-qe-coordinator`, `lldb-qe-warehouse`, `lldb-qe-auth`,
`lldb-qe-migrate` — are **not** access-controlled and cannot usefully be: their credential is the
services database's own, and whoever holds that can grant themselves anything. Do not expose them
as a multi-tenant entry point; expose `lldb-qe-server`.

## Coordinator liveness: a lease, and deliberately nothing that acts on it

Nothing could tell a dead coordinator from a slow one — no lease, no heartbeat, no `last_seen`.
`liveness.rs` is that mechanism, and it exists *once* because two later issues (reaping stranded
query rows, fleet-wide admission) each need the answer and would otherwise each build half of it.
Both landed and both splice `LIVE_PREDICATE` in verbatim — `reaper.rs` and `fleet_admission.rs`.
Four decisions, all argued in the module docs:

1. **Identity is a pair.** `--coordinator-id` is ambiguous in both directions: a coordinator that
   restarts on a new port looks like a different coordinator, and one that restarts onto the *same*
   address inherits the previous process's rows without having run them. So a registration is a
   stable `slot` (the configured id) plus an `incarnation` (128 CSPRNG bits minted at startup), and
   `queries` records **both** — a row whose slot is live but whose incarnation is gone belongs to a
   coordinator that died and was replaced. A warehouse is deliberately *not* registered: this
   coordinator serves whatever a request names, so it has a set of them, not one.
2. **A failed renewal does not stop the process.** It is counted, logged and retried at the same
   interval; past the threshold the log moves to `error` and `is_stale()` goes true. A
   control-plane hiccup must not become a data-plane outage — the same rule history writes and the
   result cache already follow. A *stolen slot* (another process registered the same id) is the
   other branch and is conceded rather than fought, because two processes trading one lease forever
   is worse than one being visibly wrong.
3. **The threshold is `MISSED_RENEWALS_BEFORE_DEAD` × the renewal interval, and there is no second
   knob.** Two settings could be configured inconsistently and the failure mode of that is reaping a
   working coordinator. The interval is stored per row, so each coordinator is judged by its own
   cadence. A clean exit stamps `shutdown_at` and is not-live at once rather than after the
   threshold.
4. **Nobody in-process evaluates it.** This ships the predicate only (`is_coordinator_live`,
   `live_coordinators`). A coordinator sweeping for dead peers at startup is the dangerous shape — a
   fleet restarting together would have every member judging the others through a lease none of them
   had renewed. Whatever acts on the answer is a one-shot out of process, in `lldb-qe-migrate`'s
   style; nothing here writes to `queries`.

**`scheduler.rs` still does not know this module exists** — it takes a `FleetGate` trait, which is
what keeps its bound, fairness, release-on-failure *and* the two-coordinator property provable with
no Postgres, no workers and no Flight. What `fleet_admission.rs` changed is that liveness is now
consulted on the admit path, by the gate rather than by the scheduler, and it can only ever answer
granted / full / unreachable-so-admit-locally. Liveness is still never a precondition for
scheduling: there is no answer it can give that refuses a query. With no services DB there is no
row, no background task, no fleet gate and no per-query anything —
`CoordinatorRegistration::start_if_configured` and `FleetAdmission::start_if_registered` are that
rule as two functions, so both are testable without a database.

## Cancellation returns the slot, and that is the whole point

`cancel.rs` + `do_action("cancel", <query id>)` stop a query that already holds an admission slot.
Hanging up only ever removed a *queued* one; a running query held its slot for its full duration and
the queue behind it waited. Five things to keep straight.

1. **A slot comes back by dropping a future, not by calling anything.** There is still no
   `QuerySlot::release()`. `server.rs` runs the admit-and-execute future in a `tokio::select!`
   against the cancellation signal, so a cancel drops that future and `QuerySlot`'s `Drop` hands the
   permit to the next waiter — the same mechanism that already survived failures and panics.
   Aborting a `JoinHandle` is the shape to reject: it kills the task from outside and leaves nobody
   to write the history row.
2. **There is no third writer to a `queries` row.** The `do_action` handler only *signals*; the task
   that already owns the row is what writes `cancelled`, from the same `run_query` frame that writes
   every other terminal state. So `reaper.rs`'s asymmetry is unchanged — coordinator unconditional,
   reaper CAS — and cancellation composes with it by landing on the other side of the reaper's
   predicate: `cancelled` is terminal, therefore outside `state IN ('queued','running')`, in both
   the scan and the recheck. `query_log::active_states_sql` is now the single spelling of that set,
   spliced into the reaper's predicate so a future state cannot drift out of it.
3. **Worker-side work is NOT cancelled.** No cancel crosses the Flight boundary. Dropping the
   coordinator's streams resets them, so a worker still *streaming* stops when its send fails, but a
   stage already materializing into the `StageCache` runs to completion — the cache decouples
   producing from consuming on purpose. The honest claim is: the coordinator's slot comes back
   immediately and deterministically; worker CPU drains on its own, promptly and unboundedly.
4. **Cancelling is its own privilege**, `CANCEL` on the warehouse whose slot it frees — not `USAGE`,
   which every submitter holds and which would make the grant decorative. A query belonging to
   another account is refused as `NOT_FOUND`, identical to an unknown id, because query ids are
   consecutive integers from one sequence shared by every tenant and a distinguishable denial would
   let anyone map the id space. Within the account, a missing grant *is* `PERMISSION_DENIED`.
5. **The handle is the history row's id, so cancellation follows the services DB.** No
   `--metadata-*` → no id → nothing to name a query by, and the registry stays empty. `cargo run`
   still needs no Postgres, and a single-node user hangs up.

Migration `0007` is what makes a fifth state storable, and it widens **three** constraints:
`queries_state_check`, `queries_error_only_when_failed` (renamed
`queries_error_only_when_unsuccessful` — a cancelled row must be able to say *who* cancelled it, and
only `failed` could carry prose before) and `grants_privilege_check`. Note what adding a privilege
under an existing wildcard does: every pre-existing `ALL ON WAREHOUSE …` grant now also confers
`CANCEL`.

## The reaper acts on that lease, and only on it

`reaper.rs` + `lldb-qe-reap` are liveness's first consumer: they resolve `queries` rows left in
`queued`/`running` by a coordinator that died, marking them `failed` with a reason that
distinguishes **never started** (`started_at IS NULL`) from **died mid-flight**. Four things to
keep straight.

1. **Eligibility is the `(slot, incarnation)` pair, never age.** A legitimately long-running query
   is indistinguishable from an abandoned one by age, so a rule containing "…and it has been
   running for more than N minutes" kills live work. A row is reapable only when no *live*
   `coordinators` row matches **both** its slot and its incarnation — which is exactly the case a
   slot-only rule gets wrong: a coordinator that died and restarted onto the same address has a
   live slot, and its stranded rows would be judged live forever.
   `query_reaper::a_live_coordinators_long_running_query_is_never_reaped` sweeps repeatedly for
   several multiples of the threshold against a running query, then kills the coordinator and
   demands the same row *is* taken — so "never reaped" cannot mean "reaps nothing".
2. **There are now two writers to a query row, and they are asymmetric.** `query_log.rs`'s
   single-writer justification is gone: the owning coordinator writes **unconditionally** (it is
   the authority on its own query), and the reaper writes a **CAS** — its `UPDATE` repeats the whole
   reapable predicate in its own `WHERE`, so a row that moved under it is skipped. Both interleavings
   of "reap" and "succeeded" end at `succeeded`. Do not "simplify" that repeated predicate away;
   `a_reaper_never_clobbers_a_terminal_state` is what fails if you do.
3. **`finished_at` is the writer's last renewal, not `now()`.** `now()` would claim the query ran
   right up to the sweep, which re-creates the `peak_concurrency` bias the reaper exists to remove.
   Where the registration is gone or the slot has been taken over it falls back to `now()` — an
   over-estimate, stated rather than hidden.
4. **The honest limits.** A NULL `coordinator_incarnation` (history predating the column, a writer
   that never registered) is never reaped — liveness *says nothing* about that row, which is not
   "dead". And a row stranded by the insert-to-guard window on a coordinator that is *still alive*
   waits until that incarnation goes away; closing that gap belongs on the submit path, not here.

It runs **out of process**, on purpose — a coordinator sweeping at startup would have a fleet
restarting together reaping each other's live queries. The sweep is bounded by a `LIMIT`
(result_cache's rule), idempotent, and safe to run concurrently with itself.

## Admission is fleet-wide, and the lease it expires on is the coordinator's

`Admission` was a `tokio::sync::Semaphore` in one process's memory, so two coordinators each
configured `K = 4` ran up to **8** queries on one warehouse — a limit that was a property of a
process multiplied by however many were running, which is the number an operator scales for
*availability*. `fleet_admission.rs` + migration `0008` make it a property of the warehouse. Six
things to keep straight.

1. **`K` rows, not a counter.** A warehouse of size `K` has slots `0..K-1` in `admission_slots`, and
   a claim is a row, so over-admission is impossible by construction rather than by argument. The
   counter shape needs an advisory lock or `SELECT ... FOR UPDATE` — a transaction, three round
   trips — to arrive somewhere a unique index already is. A claim is **one statement**: pick a
   claimable slot number, `INSERT ... ON CONFLICT DO UPDATE`, see whether a row comes back. The
   `ON CONFLICT`'s `WHERE` repeats the whole claimable predicate, which is `reaper.rs`'s
   compare-and-swap idiom; do not "simplify" it away, it is what makes a lost race a no-op.
2. **There is no second lease and no expiry column.** A slot is held by a *process*, so its expiry is
   that process's `coordinators` row — `liveness::LIVE_PREDICATE`, spliced verbatim, matched on the
   `(slot, incarnation)` **pair** for exactly `reaper.rs`'s reason. That buys one heartbeat per
   coordinator instead of one per running query, one spelling of "alive" tree-wide, and **no sweep**:
   a dead coordinator's slots are reclaimed by the next coordinator that wants one. It also means a
   holder must be *registered*, so `FleetAdmission` cannot be constructed without a
   `CoordinatorRegistration`.
3. **The local semaphore stays, as fast path and backstop.** A failed claim admits on the local bound
   and counts `fleet_degraded`; it never refuses. So a control-plane outage degrades to `N × K` —
   exactly the old bug, never worse than it — and that is also the answer `liveness.rs`'s decision 2
   asked a future issue to give: a coordinator that cannot renew also cannot claim, so both sides
   fall back at the same moment. Local gate first, fleet second, always: nothing holds a fleet lease
   while waiting for a permit, so there is a strict order and no cycle.
4. **Both halves come back by dropping the guard.** `QuerySlot` carries the permit *and* the lease,
   and there is still no `release()`. The permit returns synchronously; the lease is a `DELETE` a
   destructor cannot await, so it is spawned — best effort, like every `Drop`-issued write here. A
   leaked lease would be *worse* than the bug this fixed, so two things close it: the `DELETE` is a
   CAS on `(holder_incarnation, holder_token)` (a coordinator whose slot was reclaimed must not free
   its successor's), and a coordinator's own rows carrying a token it no longer holds are claimable
   by its own next claim. The honest residue: a slot leaked by a coordinator that stays alive and
   never claims on that warehouse again waits for its registration to go away — `reaper.rs`'s bound,
   argued the same way.
5. **The costs, stated.** One round trip per admit, on the hottest path. Fleet-wide waiting is
   *polled* (`FLEET_POLL_INTERVAL`, jittered), so FIFO holds within a coordinator and not between
   them — `LISTEN`/`NOTIFY` is the next step, not this one. Hand-off across coordinators costs one
   poll interval. Coordinators that disagree about `K` bound the warehouse by `max(K)`, not by a sum.
   And a raw `--workers` fleet has no warehouse row, so it keeps per-process admission — there is
   nothing for two coordinators to agree they are bounding.
6. **The fleet key is `warehouses.id`, never the name.** Names are unique per *account*, so shared
   state keyed by one would merge two tenants' concurrency budgets. (The process-local gate is still
   keyed by name and so two tenants can share a queue — a pre-existing fairness wart, untouched, and
   the reason the fleet half deliberately does not inherit it.)

`scheduler.rs` takes a `FleetGate` **trait**, not a `ServicesDb`. That is what keeps its bound, its
fairness, its release-on-failure behaviour and now "two coordinators admit `K`, not `2K`" all
provable by unit tests with no Postgres, no workers and no Flight.

## The result cache is keyed, never invalidated

`result_cache.rs` answers a repeat query from Postgres instead of the fleet. Its key is
`(account, build version, default catalog+schema, statement + plan rendering, every referenced
table @ its Iceberg snapshot id)`. Nothing ever *invalidates* an entry: a commit moves a snapshot,
the next run composes a different key, and the stale row is simply unreachable. Two rules follow —
**an input we cannot version means the query is not cached at all** (a listing table, a table
function, `information_schema`), and **not caching is always a legal answer**, so every refusal and
every services-DB failure falls through to ordinary execution.

