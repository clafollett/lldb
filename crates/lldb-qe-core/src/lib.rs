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
//!
//! The guiding principle of the storage layer: **the engine is written against the
//! `object_store::ObjectStore` trait, never a concrete backend.** Swapping a laptop's disk
//! for S3 is a one-line change in [`StorageConfig`], not a rewrite — the same trick that
//! lets stateless workers read the same data from anywhere.

pub mod catalog;
pub mod config;
pub mod distributed;
pub mod flight;
pub mod lakehouse;
pub mod manifest;
pub mod session;
pub mod storage;
pub mod tpch;

pub use catalog::{apply_manifest, register_listing_tables};
pub use config::{StorageArgs, init_tracing};
pub use distributed::distributed_group_count;
pub use flight::{fetch, serve_worker};
pub use lakehouse::Lakehouse;
pub use manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef, TableFormat,
    TableSource,
};
pub use session::{build_session, register_tpch_parquet};
pub use storage::{Storage, StorageConfig};
pub use tpch::{TPCH_TABLES, tpch_manifest};
