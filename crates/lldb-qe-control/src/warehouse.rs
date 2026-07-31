//! **Virtual warehouses** — named, resizable, suspendable pools of compute.
//!
//! # What a warehouse buys you
//!
//! Before this module the fleet was a fleet: one ECS service, one `worker.lldb.local`, one size,
//! changed by editing infrastructure. Every query in the system shared it. That is a cluster, not
//! a warehouse.
//!
//! A *warehouse* is the elastic-compute abstraction that separates "what compute am I using" from
//! "what data am I reading". Storage and the catalog stay shared and untouched; compute becomes a
//! named pool you can size for the workload pointed at it, suspend when nobody is asking, and
//! resume without a deploy. Two teams can hold a small warehouse and a large one against the same
//! tables and neither can slow the other down, because the only thing they share is bytes at rest.
//!
//! # The model
//!
//! A warehouse is four facts, and the database enforces all four:
//!
//! - **who owns it** — `account_id`, `ON DELETE CASCADE`. Warehouses are per-tenant, and
//!   `UNIQUE (account_id, name)` says so precisely: two *accounts* may each own a warehouse named
//!   `analytics`; one account may not own two. Every lookup here therefore takes an `account_id`.
//!   Passing the wrong one finds nothing rather than someone else's compute.
//! - **its name** — the human handle, and also a **DNS label**: routing renders it into
//!   `<warehouse>.lldb.local`. That is why [`validate_warehouse_name`] is strict about the
//!   character set and insists on lowercase; `Analytics` and `analytics` are two distinct rows
//!   under a case-sensitive `UNIQUE` but one hostname, and that collision would be discovered as
//!   a query mysteriously running on the wrong pool.
//! - **its size** — the desired worker count, retained while suspended so resume has something to
//!   scale back *to*.
//! - **its state** — [`WarehouseState`], `running` or `suspended`, moved only by the transitions
//!   in [`WarehouseState::apply`].
//!
//! # The database is desired state; something else actuates it
//!
//! This is the design decision worth being loud about. `resize`/`suspend`/`resume` here write
//! **rows**, not AWS API calls. The row is the desired state of a warehouse's compute; an
//! actuator — the CDK stack at deploy time, or an operator running `aws ecs update-service`, or
//! `docker compose up --scale` locally — makes the world match it.
//!
//! The alternative (the engine binaries calling the ECS API themselves) was rejected on purpose:
//! it drags the AWS SDK into a workspace whose entire dependency story is "one `arrow`, one
//! `object_store`, one `datafusion` tree-wide" (see CLAUDE.md), it makes the coordinator's
//! blast radius include an IAM role that can scale services, and it hard-codes one cloud into
//! the control plane. Keeping the engine declarative means the same rows drive ECS, compose, or
//! whatever runs the workers next — and the CLI prints the command a human still has to run.
//!
//! # What this does NOT do
//!
//! - **No auto-suspend, no auto-resume, no queueing.** A warehouse changes state because someone
//!   said so. Idle-timeout suspension needs query history (#18) to know what "idle" means.
//! - **No actuation.** See above. `suspend` does not free a single container by itself; it
//!   records that it should be freed and refuses to route queries there from that moment on.
//! - **No idempotent shortcuts.** Suspending an already-suspended warehouse is an *error*, not a
//!   no-op. Two operators fighting over one warehouse is a thing that happens, and a silent
//!   success would hide it — so the transition table rejects it and says what the state is.
//! - **No cost model.** Size is a worker count, not a credit rate. Metering is out of scope.
//! - **No enforcement of tenancy.** Callers pass the `account_id` they resolved; nothing here
//!   checks that the *caller* is entitled to it. That is #19's job (accounts & RBAC).
//!
//! # Conventions
//!
//! Same as [`crate::services`], for the same reasons: runtime `sqlx::query`/`query_as` with bind
//! parameters (never the `query!` macros, which would need a live database at build time), and
//! [`anyhow::Result`] with `.context(...)` naming the warehouse an operator was reaching for.

use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};

use crate::discovery::render_warehouse_endpoint;
use crate::services::ServicesDb;

/// The longest a warehouse name may be: one DNS label. Names become hostnames.
pub const MAX_WAREHOUSE_NAME_LEN: usize = 63;

/// Whether a warehouse's compute is provisioned.
///
/// Two states, not three. "starting"/"stopping" are properties of the *actuator* (an ECS
/// deployment in progress), not of the desired state this table holds — modelling them here would
/// mean the control plane owns a fact only the orchestrator can observe, and it would go stale the
/// first time a deploy failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarehouseState {
    /// Compute is provisioned at the warehouse's size; queries may route here.
    Running,
    /// Compute is scaled to zero. The size is remembered; routing refuses the warehouse.
    Suspended,
}

/// Every legal state, in the order the CLI's help text lists them. Kept next to the enum so a
/// third state cannot be added without this (and the migration's `CHECK`) being updated.
pub const WAREHOUSE_STATES: [WarehouseState; 2] =
    [WarehouseState::Running, WarehouseState::Suspended];

impl WarehouseState {
    /// The spelling stored in the `state` column and accepted by the migration's `CHECK`.
    pub fn as_str(self) -> &'static str {
        match self {
            WarehouseState::Running => "running",
            WarehouseState::Suspended => "suspended",
        }
    }

    /// Apply a lifecycle operation, or explain why it is illegal.
    ///
    /// This is the whole state machine, and it is deliberately a pure function of
    /// `(state, op)` — no database, no clock, no I/O — so the transition table is unit-testable
    /// and the SQL layer's only job is to hold a row still while it is consulted.
    ///
    /// The table:
    ///
    /// | from        | `Suspend`   | `Resume`  |
    /// | -           | -           | -         |
    /// | `Running`   | `Suspended` | **error** |
    /// | `Suspended` | **error**   | `Running` |
    ///
    /// The errors are the point. A no-op "success" for `suspend` on an already-suspended
    /// warehouse reads as "I freed that compute" to whoever ran it, when in fact someone else
    /// already did — and the same reasoning applies to a resume racing a resume.
    pub fn apply(self, op: WarehouseOp) -> Result<Self> {
        match (self, op) {
            (WarehouseState::Running, WarehouseOp::Suspend) => Ok(WarehouseState::Suspended),
            (WarehouseState::Suspended, WarehouseOp::Resume) => Ok(WarehouseState::Running),
            (state, op) => bail!(
                "cannot {} a warehouse that is already {}",
                op.as_str(),
                state.as_str()
            ),
        }
    }
}

impl fmt::Display for WarehouseState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WarehouseState {
    type Err = anyhow::Error;

    /// Parse a stored (or CLI-supplied) state. An unknown value is an error naming the legal set
    /// rather than a silent fallback — a row the database somehow holds with an uninterpretable
    /// state must stop the query, not quietly become "suspended".
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "running" => Ok(WarehouseState::Running),
            "suspended" => Ok(WarehouseState::Suspended),
            other => bail!(
                "unknown warehouse state `{other}` (expected one of: {})",
                WAREHOUSE_STATES
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// The operations that move a warehouse between states. Resizing is *not* one of them: a resize
/// is legal from either state and changes no state, which is exactly why it is not modelled here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarehouseOp {
    /// Scale to zero: free the compute, keep the definition.
    Suspend,
    /// Scale back up to `size`.
    Resume,
}

impl WarehouseOp {
    /// The verb, for error messages that read like the command that produced them.
    pub fn as_str(self) -> &'static str {
        match self {
            WarehouseOp::Suspend => "suspend",
            WarehouseOp::Resume => "resume",
        }
    }
}

/// One virtual warehouse, as stored.
///
/// Fields are public because this is a record, not an invariant-holding type: the invariants
/// (name shape, positive size, legal state) are enforced by the constructors on [`ServicesDb`]
/// and by the database's own constraints, which is where they survive a second writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warehouse {
    pub id: i64,
    /// The owning tenant. Warehouses are never global.
    pub account_id: i64,
    pub name: String,
    /// Desired worker count. Retained across a suspend.
    pub size: i32,
    pub state: WarehouseState,
    pub created_at: DateTime<Utc>,
    /// Last lifecycle change — create, resize, suspend or resume.
    pub updated_at: DateTime<Utc>,
}

impl Warehouse {
    /// The Flight endpoint queries for this warehouse should be discovered behind, or an error
    /// explaining why this warehouse cannot serve a query.
    ///
    /// This is the routing guard, and it lives here rather than in the coordinator so that
    /// "a suspended warehouse refuses queries" is a property of the *type* — testable without a
    /// database, a network, or a fleet. The error names the exact command that fixes it, because
    /// the person who hits it is usually the person who suspended it last week.
    ///
    /// `template` is a `scheme://host:port` string containing `{warehouse}`; see
    /// [`render_warehouse_endpoint`]. The rendered endpoint is a *fan-out point*, not a worker:
    /// discovery resolves it to every task standing behind it.
    pub fn endpoint(&self, template: &str) -> Result<String> {
        if self.state != WarehouseState::Running {
            bail!(
                "warehouse `{}` is {} — resume it first: \
                 `lldb-qe-warehouse resume --name {}` (then apply the change to the fleet)",
                self.name,
                self.state,
                self.name
            );
        }
        render_warehouse_endpoint(template, &self.name)
            .with_context(|| format!("routing to warehouse `{}`", self.name))
    }
}

/// Reject a name that cannot be a DNS label, before it becomes a hostname nobody can resolve.
///
/// The rules are RFC 1123's, minus uppercase: 1–63 characters of `[a-z0-9-]`, not starting or
/// ending with `-`. Lowercase is enforced rather than folded because the `UNIQUE (account_id,
/// name)` index is case-*sensitive* while DNS is case-*insensitive*: accepting `Analytics`
/// alongside `analytics` would let one account hold two warehouse rows that resolve to a single
/// pool of workers, and the symptom would be a query running on compute it was not routed to.
pub fn validate_warehouse_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("a warehouse name must not be empty");
    }
    if name.len() > MAX_WAREHOUSE_NAME_LEN {
        bail!(
            "warehouse name `{name}` is {} characters; the limit is {MAX_WAREHOUSE_NAME_LEN} \
             (a name becomes a DNS label)",
            name.len()
        );
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        bail!(
            "warehouse name `{name}` may contain only lowercase letters, digits and `-` \
             (it becomes the DNS label `{name}.lldb.local`)"
        );
    }
    if name.starts_with('-') || name.ends_with('-') {
        bail!("warehouse name `{name}` must not start or end with `-` (it is a DNS label)");
    }
    Ok(())
}

/// Reject a size that cannot describe a pool of workers. Mirrors the `CHECK (size > 0)` in the
/// schema so the message names the flag rather than the constraint.
pub fn validate_warehouse_size(size: i32) -> Result<()> {
    if size < 1 {
        bail!("warehouse size must be at least 1 worker, got {size}");
    }
    Ok(())
}

/// The column list every warehouse query returns, in the order [`WarehouseRow`] expects.
const WAREHOUSE_COLUMNS: &str = "id, account_id, name, size, state, created_at, updated_at";

/// The raw row shape. Named so the several `query_as` turbofishes below stay readable.
type WarehouseRow = (i64, i64, String, i32, String, DateTime<Utc>, DateTime<Utc>);

/// Turn a row into a [`Warehouse`], failing loudly on a state the schema should have made
/// impossible. Reachable only if someone edits `state` by hand or a future migration adds a value
/// this build does not know — both of which are worth an error rather than a guess.
fn warehouse_from_row(row: WarehouseRow) -> Result<Warehouse> {
    let (id, account_id, name, size, state, created_at, updated_at) = row;
    let state = state
        .parse::<WarehouseState>()
        .with_context(|| format!("reading warehouse `{name}` (id {id})"))?;
    Ok(Warehouse {
        id,
        account_id,
        name,
        size,
        state,
        created_at,
        updated_at,
    })
}

/// The "you asked for a warehouse that isn't there" message, in one place so every path says the
/// same thing and names the tool that creates one.
fn missing_warehouse(account_id: i64, name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "no warehouse named `{name}` for account {account_id} \
         (create one with `lldb-qe-warehouse create --name {name} --size 2`)"
    )
}

impl ServicesDb {
    /// Create a warehouse. Fails if the account already owns one by that name — that is
    /// `UNIQUE (account_id, name)` doing its job, and it is what makes a name a usable handle.
    ///
    /// `state` is explicit rather than defaulted because "does creating compute start billing it"
    /// is a decision the caller should have to make out loud. The CLI defaults it to `running`
    /// (you almost always want to query what you just made) and offers `--suspended`.
    pub async fn create_warehouse(
        &self,
        account_id: i64,
        name: &str,
        size: i32,
        state: WarehouseState,
    ) -> Result<Warehouse> {
        validate_warehouse_name(name)?;
        validate_warehouse_size(size)?;
        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "INSERT INTO warehouses (account_id, name, size, state) VALUES ($1, $2, $3, $4) \
             RETURNING {WAREHOUSE_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .bind(size)
        .bind(state.as_str())
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("creating warehouse `{name}` for account {account_id}"))?;
        warehouse_from_row(row)
    }

    /// Create the warehouse if this account does not already have one by that name, and return it
    /// either way.
    ///
    /// The `DO UPDATE SET name = EXCLUDED.name` is the same no-op trick [`ServicesDb::ensure_account`]
    /// uses and for the same reason: a bare `DO NOTHING` returns no row on conflict, forcing a
    /// second round trip with a race in between, while writing the name back to itself makes the
    /// conflicting row part of this statement's result.
    ///
    /// Note what it deliberately does **not** update: `size` and `state`. An idempotent create is
    /// for "make sure this exists" in a bootstrap script that runs on every deploy — if it also
    /// reset the size, it would silently undo an operator's resize, and if it reset the state it
    /// would resume a warehouse someone suspended to stop paying for it. Changing those is what
    /// `resize`/`resume` are for.
    pub async fn ensure_warehouse(
        &self,
        account_id: i64,
        name: &str,
        size: i32,
        state: WarehouseState,
    ) -> Result<Warehouse> {
        validate_warehouse_name(name)?;
        validate_warehouse_size(size)?;
        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "INSERT INTO warehouses (account_id, name, size, state) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (account_id, name) DO UPDATE SET name = EXCLUDED.name \
             RETURNING {WAREHOUSE_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .bind(size)
        .bind(state.as_str())
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("ensuring warehouse `{name}` exists for account {account_id}"))?;
        warehouse_from_row(row)
    }

    /// Look a warehouse up by its handle *within an account* — how `--warehouse analytics`
    /// becomes a row. Another tenant's identically named warehouse is invisible here, which is
    /// the entire reason the account id is a parameter and not an afterthought.
    pub async fn warehouse_by_name(
        &self,
        account_id: i64,
        name: &str,
    ) -> Result<Option<Warehouse>> {
        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "SELECT {WAREHOUSE_COLUMNS} FROM warehouses WHERE account_id = $1 AND name = $2"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up warehouse `{name}` for account {account_id}"))?;
        row.map(warehouse_from_row).transpose()
    }

    /// Look a warehouse up by id. Ids are already account-scoped by construction, so this needs
    /// no `account_id` — it is for callers holding an id from a previous call (query history's
    /// `warehouse_id`, say), not for resolving user input.
    pub async fn warehouse_by_id(&self, id: i64) -> Result<Option<Warehouse>> {
        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "SELECT {WAREHOUSE_COLUMNS} FROM warehouses WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up warehouse {id}"))?;
        row.map(warehouse_from_row).transpose()
    }

    /// Every warehouse this account owns, by name — the order a human reads a list in, and stable
    /// across calls so a diff of two `list` runs shows only what changed.
    pub async fn list_warehouses(&self, account_id: i64) -> Result<Vec<Warehouse>> {
        let rows = sqlx::query_as::<_, WarehouseRow>(&format!(
            "SELECT {WAREHOUSE_COLUMNS} FROM warehouses WHERE account_id = $1 ORDER BY name"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing warehouses for account {account_id}"))?;
        rows.into_iter().map(warehouse_from_row).collect()
    }

    /// Change a warehouse's desired worker count.
    ///
    /// Legal in **either** state, and that is deliberate: resizing a suspended warehouse sets what
    /// it will resume to, which is how you grow a pool before the workload that needs it arrives
    /// without paying for the interval. On a running warehouse the row changes immediately and the
    /// fleet follows when the change is applied — the next coordinator run against the resized
    /// warehouse discovers the new task count and fans out accordingly, with no redeploy of the
    /// engine.
    pub async fn resize_warehouse(
        &self,
        account_id: i64,
        name: &str,
        size: i32,
    ) -> Result<Warehouse> {
        validate_warehouse_size(size)?;
        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "UPDATE warehouses SET size = $3, updated_at = now() \
             WHERE account_id = $1 AND name = $2 \
             RETURNING {WAREHOUSE_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .bind(size)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("resizing warehouse `{name}` to {size}"))?
        .ok_or_else(|| missing_warehouse(account_id, name))?;
        warehouse_from_row(row)
    }

    /// Suspend: record that this warehouse's compute should be released. Errors if it is already
    /// suspended (see [`WarehouseState::apply`] for why that is not a no-op).
    pub async fn suspend_warehouse(&self, account_id: i64, name: &str) -> Result<Warehouse> {
        self.transition_warehouse(account_id, name, WarehouseOp::Suspend)
            .await
    }

    /// Resume: record that this warehouse should be back at `size` workers. Errors if it is
    /// already running.
    pub async fn resume_warehouse(&self, account_id: i64, name: &str) -> Result<Warehouse> {
        self.transition_warehouse(account_id, name, WarehouseOp::Resume)
            .await
    }

    /// Read-check-write a state change **inside a transaction**, with the row locked.
    ///
    /// The lock is the whole reason this is not a single conditional `UPDATE ... WHERE state =
    /// 'running'`. That statement can express the transition, but it cannot tell "the warehouse
    /// does not exist" apart from "it was already suspended" — both update zero rows — and
    /// re-reading afterwards to find out is a race that reports the wrong error under exactly the
    /// concurrency it exists to handle. `SELECT ... FOR UPDATE` holds the row still, the pure
    /// state machine in [`WarehouseState::apply`] decides, and a concurrent transition waits its
    /// turn and then legitimately fails.
    async fn transition_warehouse(
        &self,
        account_id: i64,
        name: &str,
        op: WarehouseOp,
    ) -> Result<Warehouse> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .with_context(|| format!("beginning a transaction to {} `{name}`", op.as_str()))?;

        let current = sqlx::query_as::<_, WarehouseRow>(&format!(
            "SELECT {WAREHOUSE_COLUMNS} FROM warehouses \
             WHERE account_id = $1 AND name = $2 FOR UPDATE"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await
        .with_context(|| format!("locking warehouse `{name}` to {}", op.as_str()))?
        .ok_or_else(|| missing_warehouse(account_id, name))?;
        let current = warehouse_from_row(current)?;

        // `map_err` rather than `.context()`: the reason ("already suspended") is the useful half,
        // so it belongs in the top-level message an operator sees, not in a `Caused by:` line
        // under a context that only repeats the name they just typed.
        let next = current
            .state
            .apply(op)
            .map_err(|e| anyhow::anyhow!("warehouse `{name}`: {e}"))?;

        let row = sqlx::query_as::<_, WarehouseRow>(&format!(
            "UPDATE warehouses SET state = $2, updated_at = now() WHERE id = $1 \
             RETURNING {WAREHOUSE_COLUMNS}"
        ))
        .bind(current.id)
        .bind(next.as_str())
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("setting warehouse `{name}` to {next}"))?;

        tx.commit()
            .await
            .with_context(|| format!("committing the {} of `{name}`", op.as_str()))?;
        warehouse_from_row(row)
    }

    /// Drop a warehouse definition. Returns whether a row was removed, so a caller can tell
    /// "deleted" from "there was nothing to delete" without a second query.
    ///
    /// Deliberately allowed while running: the row is desired state, and the actuator scaling the
    /// service to zero is a consequence of the row's absence, not a precondition for it. Query
    /// history survives (`queries.warehouse_id` is `ON DELETE SET NULL`) — dropping a warehouse
    /// must not erase the record of what it ran.
    pub async fn delete_warehouse(&self, account_id: i64, name: &str) -> Result<bool> {
        let deleted = sqlx::query("DELETE FROM warehouses WHERE account_id = $1 AND name = $2")
            .bind(account_id)
            .bind(name)
            .execute(self.pool())
            .await
            .with_context(|| format!("deleting warehouse `{name}` for account {account_id}"))?
            .rows_affected();
        Ok(deleted > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::DEFAULT_WAREHOUSE_ENDPOINT;

    fn warehouse(name: &str, size: i32, state: WarehouseState) -> Warehouse {
        let now = Utc::now();
        Warehouse {
            id: 1,
            account_id: 7,
            name: name.to_string(),
            size,
            state,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn the_legal_transitions_are_the_only_transitions() {
        assert_eq!(
            WarehouseState::Running.apply(WarehouseOp::Suspend).unwrap(),
            WarehouseState::Suspended
        );
        assert_eq!(
            WarehouseState::Suspended
                .apply(WarehouseOp::Resume)
                .unwrap(),
            WarehouseState::Running
        );
    }

    #[test]
    fn a_redundant_transition_is_an_error_naming_the_state() {
        // Not a no-op: a silent success would tell an operator they freed compute someone else
        // had already freed, or started compute that was already running.
        let err = WarehouseState::Suspended
            .apply(WarehouseOp::Suspend)
            .expect_err("suspending a suspended warehouse is illegal");
        let msg = err.to_string();
        assert!(msg.contains("suspend"), "{msg}");
        assert!(msg.contains("already suspended"), "{msg}");

        let err = WarehouseState::Running
            .apply(WarehouseOp::Resume)
            .expect_err("resuming a running warehouse is illegal");
        assert!(err.to_string().contains("already running"), "{err}");
    }

    #[test]
    fn states_round_trip_through_their_stored_spelling() {
        for state in WAREHOUSE_STATES {
            assert_eq!(state.as_str().parse::<WarehouseState>().unwrap(), state);
            assert_eq!(state.to_string(), state.as_str());
        }
    }

    #[test]
    fn an_unknown_state_is_refused_not_guessed() {
        // The migration's CHECK should make this unreachable; if it ever is reached, a query must
        // stop rather than silently treat an unreadable warehouse as suspended.
        let err = "runing".parse::<WarehouseState>().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("runing"), "{msg}");
        assert!(msg.contains("running"), "must list the legal set: {msg}");
        assert!(msg.contains("suspended"), "{msg}");
    }

    #[test]
    fn names_must_be_dns_labels() {
        for good in ["analytics", "wh-1", "a", "etl-nightly-2"] {
            validate_warehouse_name(good).unwrap_or_else(|e| panic!("`{good}` rejected: {e}"));
        }
        for bad in [
            "",
            "-leading",
            "trailing-",
            "Analytics", // uppercase: two rows, one hostname
            "wh_1",      // underscore is not a DNS label character
            "wh.analytics",
            "wh 1",
        ] {
            assert!(
                validate_warehouse_name(bad).is_err(),
                "`{bad}` should be rejected"
            );
        }
        let too_long = "a".repeat(MAX_WAREHOUSE_NAME_LEN + 1);
        assert!(validate_warehouse_name(&too_long).is_err());
        assert!(validate_warehouse_name(&"a".repeat(MAX_WAREHOUSE_NAME_LEN)).is_ok());
    }

    #[test]
    fn sizes_must_describe_at_least_one_worker() {
        assert!(validate_warehouse_size(1).is_ok());
        assert!(validate_warehouse_size(64).is_ok());
        for bad in [0, -1, i32::MIN] {
            assert!(validate_warehouse_size(bad).is_err(), "size {bad}");
        }
    }

    #[test]
    fn a_running_warehouse_routes_to_its_own_dns_name() {
        let wh = warehouse("analytics", 4, WarehouseState::Running);
        assert_eq!(
            wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT).unwrap(),
            "http://analytics.lldb.local:50051"
        );
        // Two warehouses are two endpoints — the routing story in one assertion.
        let other = warehouse("etl", 1, WarehouseState::Running);
        assert_ne!(
            other.endpoint(DEFAULT_WAREHOUSE_ENDPOINT).unwrap(),
            wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT).unwrap()
        );
    }

    #[test]
    fn a_suspended_warehouse_refuses_to_route_and_says_how_to_fix_it() {
        let wh = warehouse("analytics", 4, WarehouseState::Suspended);
        let err = wh
            .endpoint(DEFAULT_WAREHOUSE_ENDPOINT)
            .expect_err("a suspended warehouse has no compute to route to");
        let msg = err.to_string();
        assert!(msg.contains("analytics"), "{msg}");
        assert!(msg.contains("suspended"), "{msg}");
        assert!(
            msg.contains("lldb-qe-warehouse resume"),
            "the error must name the command that fixes it: {msg}"
        );
    }

    #[test]
    fn a_bad_template_is_reported_against_the_warehouse() {
        let wh = warehouse("analytics", 1, WarehouseState::Running);
        let err = wh
            .endpoint("http://workers.lldb.local:50051")
            .expect_err("a template with no placeholder routes every warehouse to one fleet");
        let chain = format!("{err:#}");
        assert!(chain.contains("analytics"), "{chain}");
        assert!(chain.contains("{warehouse}"), "{chain}");
    }
}
