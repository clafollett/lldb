//! # lldb-qe-control — the control plane, without the query engine
//!
//! Everything the fleet has to *agree* on rather than *compute*: who is asking (`auth`), what
//! they may touch, which warehouse answers them (`warehouse`, `discovery`), how many queries it
//! runs at once (`scheduler`, `fleet_admission`), how one is stopped (`cancel`), what ran
//! (`query_log`), which coordinators are alive (`liveness`) and what to do about the rows a dead
//! one left behind (`reaper`) — over the Postgres services database (`services` + `migrations/`)
//! and the transport those credentials cross (`tls`), scoped per tenant (`tenancy`) and
//! configured by the same clap groups every binary shares (`config`).
//!
//! ## The crate exists for what it does not depend on
//!
//! `lldb-qe-core` was 33 modules in one crate, and one crate is one compilation unit twice over —
//! the lib and its `#[cfg(test)]` binary. Because DataFusion, Arrow, Iceberg and parquet all sat
//! in it, editing `scheduler.rs` rebuilt `staging.rs`, and the control plane's own unit tests
//! could not run until the whole query engine had compiled.
//!
//! None of the modules here need any of that vocabulary. **This crate must never depend on
//! `datafusion`, `arrow`, `iceberg`, `parquet` or `object_store`**, and the acceptance test is
//! literally that:
//!
//! ```text
//! cargo tree -p lldb-qe-control | grep -E 'datafusion|arrow|iceberg|parquet'   # empty
//! ```
//!
//! `sqlx` is the deliberate exception and is not a leak: the control plane *is* a database. What
//! that buys is `lldb-qe-admin` — the operator one-shots `lldb-qe-migrate`, `lldb-qe-warehouse`,
//! `lldb-qe-auth` and `lldb-qe-reap` — linking this crate *instead of* the query engine, and a
//! control-plane test binary that does not wait on DataFusion to build. That is a package boundary
//! and has to be: cargo resolves dependencies per package, not per binary, so while those four were
//! `src/bin/` targets of `lldb-qe-coordinator` they compiled DataFusion regardless of what they
//! imported.
//!
//! [`services::MIGRATOR`] therefore stays *here*. `sqlx::migrate!` resolves its directory relative
//! to the crate manifest at compile time, and `migrations/` is this crate's — moving the macro
//! invocation into `lldb-qe-admin` would silently embed nothing.
//!
//! ## What is deliberately *not* here
//!
//! `server.rs` stays in `lldb-qe-core`. It imports eight of these modules because it is the
//! **composition root** — the thing that wires a control plane to an execution engine — and a
//! composition root belongs above both halves, not inside one of them.
//!
//! The vocabulary these modules share with the query engine — privileges, grants, the storage
//! *declaration* — is one layer further down, in [`lldb_qe_types`], for the same reason.
//!
//! ## Where the design rationale lives
//!
//! Per-subsystem arguments (the control plane, access control, TLS, per-tenant catalogs,
//! liveness, cancellation, the reaper, fleet-wide admission) are in
//! `crates/lldb-qe-core/CLAUDE.md`. Read the section before changing the subsystem it describes:
//! most of them exist to name a tempting alternative and say why it is wrong.

/// This crate's semantic version (from `Cargo.toml`, unified across the workspace).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The git commit this build was cut from — a 12-char short SHA, or `unknown`. Injected by
/// `build.rs` from `$LLDB_GIT_SHA` (Docker) or `git` (local).
pub const GIT_SHA: &str = env!("LLDB_GIT_SHA");
/// Full build identifier `"<version>+<sha>"`. This is what a running coordinator/worker reports
/// (via `--version` and a startup log line) so an operator can confirm the whole fleet is the
/// identical build — the precondition for shipping serialized DataFusion plans between them.
///
/// The stamp lives here rather than in `lldb-qe-core` because [`liveness`] records it on every
/// coordinator registration, and this is the lowest crate that reads it. `lldb-qe-core`
/// re-exports all three, so `lldb_qe_core::BUILD_VERSION` still resolves for every binary.
pub const BUILD_VERSION: &str = env!("LLDB_BUILD_VERSION");

pub mod auth;
pub mod cancel;
pub mod config;
pub mod discovery;
pub mod fleet_admission;
pub mod liveness;
pub mod query_log;
pub mod reaper;
pub mod scheduler;
pub mod services;
pub mod tenancy;
pub mod tls;
pub mod warehouse;

pub use auth::{ApiKey, AuthError, FleetAuth, NewToken, Principal, Role, User};
pub use cancel::{CANCEL_ACTION, Cancellation, QueryRegistry, RunningQuery};
pub use config::{StorageArgs, init_tracing};
pub use discovery::{
    DEFAULT_WAREHOUSE_ENDPOINT, WAREHOUSE_PLACEHOLDER, discover_workers, discover_workers_with,
    render_warehouse_endpoint,
};
pub use liveness::{
    CoordinatorIdentity, CoordinatorRegistration, CoordinatorRow, DEFAULT_RENEW_INTERVAL,
    MISSED_RENEWALS_BEFORE_DEAD, death_threshold,
};
pub use query_log::{QueryRecord, QueryState};
pub use reaper::{DEFAULT_REAP_BATCH, ReapReason, ReapedQuery};
pub use scheduler::{Admission, AdmissionError, AdmissionLimits, QuerySlot, Scheduler};
pub use services::{Account, ServicesArgs, ServicesDb, redact_url};
pub use tenancy::TenantScope;
pub use tls::{
    ClientTrust, CredentialCheck, ServerTls, TlsArgs, TlsClientArgs, install_client_trust,
};
pub use warehouse::{Warehouse, WarehouseOp, WarehouseState};
