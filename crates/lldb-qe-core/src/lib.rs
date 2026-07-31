//! # lldb-qe-core
//!
//! Core building blocks for the **lldb** distributed analytical query engine.
//!
//! ## Mental model: a query engine is a four-stage translator
//!
//! ```text
//! SQL text ──parse──▶ Logical Plan ──optimize──▶ Physical Plan ──execute──▶ RecordBatch stream
//! ```
//!
//! DataFusion hands us all four stages as a Rust *library*. This crate wraps it with the
//! pieces John's architecture needs. We build outward from the bottom of the stack:
//!
//! | Layer            | Concept                                   | Phase |
//! |------------------|-------------------------------------------|-------|
//! | [`storage`]      | Where bytes live — abstracted so local dev today, S3 later | 0 |
//! | [`session`]      | A configured DataFusion context + registered tables        | 0 |
//! | [`lakehouse`]    | Iceberg: files → transactional, versioned tables           | 1 |
//! | [`iceberg_scan`] | Pin an Iceberg scan to its snapshot's files, so it can be shipped and sliced | 3 |
//! | Flight transport | Ship sub-plans to workers, stream Arrow back | 3 |
//! | Distributed shuffle | Hash-partition a join across workers   | 4 |
//! | [`services`]     | Postgres control plane: tenants, and the schema later issues fill in | 5 |
//! | [`auth`]         | Who is asking: API keys, users, principals — and the fleet secret     | 5 |
//! | [`rbac`]         | What they may touch: grants, checked on the logical plan before dispatch | 5 |
//! | [`plan_assertion`] | Carrying that answer to a worker: a signed, short-lived statement of who authorized a plan and which locations it may read | 5 |
//! | [`warehouse`]    | Virtual warehouses: named, resizable, suspend/resume compute pools    | 5 |
//! | [`engine`]       | Running one query: plan → distribute → collect, shared by both front ends | 5 |
//! | [`scheduler`]    | Admission control: how many queries a warehouse runs at once          | 5 |
//! | [`cancel`]       | Stopping a running query, so its admission slot goes back to the warehouse | 5 |
//! | [`query_log`]    | Query history: what ran, when, and how it ended                       | 5 |
//! | [`liveness`]     | Coordinator registration and renewal: telling a dead coordinator from a slow one | 5 |
//! | [`reaper`]       | Resolving history rows whose coordinator is gone — the first consumer of that answer | 5 |
//! | [`server`]       | The long-running coordinator: concurrent queries over one Flight port  | 5 |
//! | [`tls`]          | Transport security on both Flight boundaries, and the refusal to bind a plaintext port that carries a credential | 5 |
//!
//! The guiding principle of the storage layer: **the engine is written against the
//! `object_store::ObjectStore` trait, never a concrete backend.** Swapping a laptop's disk
//! for S3 is a one-line change in [`StorageConfig`], not a rewrite — the same trick that
//! lets stateless workers read the same data from anywhere.
//!
//! Some of what this crate exposes is *vocabulary* rather than machinery — an access-control
//! privilege, the name of a storage backend — and needs none of DataFusion, Arrow, Iceberg or
//! `object_store` to be defined. That lives in [`lldb_qe_types`] and is re-exported from
//! [`rbac`] and [`storage`] here, so every path that already worked still does.
//!
//! ## The control plane is a crate, not a set of modules
//!
//! Thirteen of the rows in that table — [`auth`], [`cancel`], [`config`], [`discovery`],
//! [`fleet_admission`], [`liveness`], [`query_log`], [`reaper`], [`scheduler`], [`services`],
//! [`tenancy`], [`tls`] and [`warehouse`] — live in [`lldb_qe_control`] and are **re-exported
//! here unchanged**, so `lldb_qe_core::auth::…` and every other existing path still resolves.
//!
//! The split is a build fact rather than a taste one: none of those modules needs DataFusion,
//! Arrow, Iceberg or parquet, and while they sat in this crate, editing `scheduler.rs` recompiled
//! `staging.rs` — twice, because a `#[cfg(test)]` build is a second compilation unit. What it buys
//! is the operator one-shots (`lldb-qe-migrate`, `lldb-qe-warehouse`, `lldb-qe-auth`,
//! `lldb-qe-reap`) linking a DataFusion-free crate, and a control-plane test binary that runs
//! without waiting on a query engine to build.
//!
//! [`server`] is the exception that proves the boundary. It imports eight control-plane modules
//! because it is the **composition root** — the thing that wires a control plane to an execution
//! engine — and that belongs above both halves, which is here.
//!
//! Its counterpart above the storage layer, and the reason a worker can be as thin as it is:
//! **a plan is self-contained.** Everything a worker needs to answer its stage travels inside the
//! plan bytes — file paths, byte ranges, and, since [`iceberg_scan`], the data files of the exact
//! Iceberg snapshot the coordinator planned against. A worker registers no tables, loads no
//! manifest and holds no catalog credential; what it must have is read access to the object store
//! the plan names.
//!
//! Self-contained is not the same as self-*authorizing*, and the difference is [`plan_assertion`]:
//! a worker with a fleet secret executes a plan only when the request beside it carries a live,
//! MAC'd statement of who authorized it and which locations it may read, and only when the plan's
//! own file scans sit inside those locations.

/// This crate's semantic version (from `Cargo.toml`, unified across the workspace).
///
/// Stamped by `lldb-qe-control`'s build script and re-exported, so every existing
/// `lldb_qe_core::VERSION` / `GIT_SHA` / `BUILD_VERSION` path still resolves.
pub use lldb_qe_control::{BUILD_VERSION, GIT_SHA, VERSION};

// The control plane, one crate down: it needs none of DataFusion, Arrow, Iceberg or parquet, and
// the whole point of the split is that it does not link them. Re-exported as modules rather than
// re-listed item by item so `lldb_qe_core::auth::…` and `crate::auth::…` both keep working
// unchanged — the binaries, the benches and the integration suite were not touched by the move.
pub use lldb_qe_control::{
    auth, cancel, config, discovery, fleet_admission, liveness, query_log, reaper, scheduler,
    services, tenancy, tls, warehouse,
};

pub mod catalog;
pub mod distributed;
pub mod dml;
pub mod engine;
pub mod flight;
pub mod iceberg_scan;
pub mod lakehouse;
pub mod manifest;
pub mod plan_assertion;
pub mod rbac;
pub mod remote;
pub mod result_cache;
pub mod retry;
pub mod scan_split;
pub mod server;
pub mod session;
pub mod stage_cache;
pub mod staging;
pub mod storage;
pub mod tpch;

pub use auth::{ApiKey, AuthError, FleetAuth, NewToken, Principal, Role, User};
pub use cancel::{CANCEL_ACTION, Cancellation, QueryRegistry, RunningQuery};
pub use catalog::{apply_manifest, register_listing_tables};
pub use config::{StorageArgs, init_tracing};
pub use discovery::{
    DEFAULT_WAREHOUSE_ENDPOINT, WAREHOUSE_PLACEHOLDER, discover_workers, discover_workers_with,
    render_warehouse_endpoint,
};
pub use dml::{DmlKind, DmlOutcome, DmlStatement};
pub use engine::{
    CatalogSource, TenantSession, TenantSessions, build_query_session, contains_flight_reader,
    execute_query, execute_query_cached, reject_inmemory_storage, resolve_fleet,
};
pub use flight::{
    fetch, fetch_with_failover, serve_worker, serve_worker_with, serve_worker_with_auth,
    serve_worker_with_cache,
};
pub use iceberg_scan::{resolve_iceberg_scans, scanned_data_files};
pub use lakehouse::Lakehouse;
pub use liveness::{
    CoordinatorIdentity, CoordinatorRegistration, CoordinatorRow, DEFAULT_RENEW_INTERVAL,
    MISSED_RENEWALS_BEFORE_DEAD, death_threshold,
};
pub use manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableFormat,
    TableSource,
};
pub use plan_assertion::{
    AssertionError, AssertionKey, PLAN_ASSERTION_HEADER, PlanAssertion, PlanAuth, QueryIdentity,
    SignedAssertion, plan_reads,
};
pub use query_log::{QueryRecord, QueryState};
pub use rbac::{Grant, ObjectRef, ObjectType, Privilege, QueryAuthorization, Requirement};
pub use reaper::{DEFAULT_REAP_BATCH, ReapReason, ReapedQuery};
pub use remote::{FlightReaderExec, LldbCodec};
pub use result_cache::{
    ResultCache, ResultCacheArgs, ResultCacheConfig, ResultCacheKey, TableInput, execute_cached,
};
pub use retry::{Retriability, RetryPolicy};
pub use scan_split::split_scan;
pub use scheduler::{Admission, AdmissionError, AdmissionLimits, QuerySlot, Scheduler};
pub use server::{
    Coordinator, CoordinatorConfig, QueryRequest, cancel_query, serve_coordinator,
    serve_coordinator_with_tls, submit_query, submit_query_as,
};
pub use services::{Account, ServicesArgs, ServicesDb, redact_url};
pub use session::{build_session, register_tpch_parquet};
pub use stage_cache::{MaterializedStage, StageCache, stage_id_of};
pub use staging::plan_distributed;
pub use storage::{Storage, StorageConfig};
pub use tenancy::TenantScope;
pub use tls::{
    ClientTrust, CredentialCheck, ServerTls, TlsArgs, TlsClientArgs, install_client_trust,
};
pub use tpch::{TPCH_TABLES, tpch_manifest};
pub use warehouse::{Warehouse, WarehouseOp, WarehouseState};
