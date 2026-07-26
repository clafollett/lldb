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
//! | Flight transport | Ship sub-plans to workers, stream Arrow back | 3 |
//! | Distributed shuffle | Hash-partition a join across workers   | 4 |
//! | [`services`]     | Postgres control plane: tenants, and the schema later issues fill in | 5 |
//! | [`warehouse`]    | Virtual warehouses: named, resizable, suspend/resume compute pools    | 5 |
//!
//! The guiding principle of the storage layer: **the engine is written against the
//! `object_store::ObjectStore` trait, never a concrete backend.** Swapping a laptop's disk
//! for S3 is a one-line change in [`StorageConfig`], not a rewrite — the same trick that
//! lets stateless workers read the same data from anywhere.

/// This crate's semantic version (from `Cargo.toml`, unified across the workspace).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The git commit this build was cut from — a 12-char short SHA, or `unknown`. Injected by
/// `build.rs` from `$LLDB_GIT_SHA` (Docker) or `git` (local).
pub const GIT_SHA: &str = env!("LLDB_GIT_SHA");
/// Full build identifier `"<version>+<sha>"`. This is what a running coordinator/worker reports
/// (via `--version` and a startup log line) so an operator can confirm the whole fleet is the
/// identical build — the precondition for shipping serialized DataFusion plans between them.
pub const BUILD_VERSION: &str = env!("LLDB_BUILD_VERSION");

pub mod catalog;
pub mod config;
pub mod discovery;
pub mod distributed;
pub mod flight;
pub mod lakehouse;
pub mod manifest;
pub mod remote;
pub mod result_cache;
pub mod retry;
pub mod scan_split;
pub mod services;
pub mod session;
pub mod stage_cache;
pub mod staging;
pub mod storage;
pub mod tpch;
pub mod warehouse;

pub use catalog::{apply_manifest, register_listing_tables};
pub use config::{StorageArgs, init_tracing};
pub use discovery::{
    DEFAULT_WAREHOUSE_ENDPOINT, WAREHOUSE_PLACEHOLDER, discover_workers, discover_workers_with,
    render_warehouse_endpoint,
};
pub use flight::{fetch, fetch_with_failover, serve_worker, serve_worker_with_cache};
pub use lakehouse::Lakehouse;
pub use manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableFormat,
    TableSource,
};
pub use remote::{FlightReaderExec, LldbCodec};
pub use result_cache::{
    ResultCache, ResultCacheArgs, ResultCacheConfig, ResultCacheKey, TableInput, execute_cached,
};
pub use retry::{Retriability, RetryPolicy};
pub use scan_split::split_scan;
pub use services::{Account, ServicesArgs, ServicesDb, redact_url};
pub use session::{build_session, register_tpch_parquet};
pub use stage_cache::{MaterializedStage, StageCache, stage_id_of};
pub use staging::plan_distributed;
pub use storage::{Storage, StorageConfig};
pub use tpch::{TPCH_TABLES, tpch_manifest};
pub use warehouse::{Warehouse, WarehouseOp, WarehouseState};
