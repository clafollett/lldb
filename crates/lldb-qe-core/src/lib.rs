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

pub mod lakehouse;
pub mod session;
pub mod storage;

pub use lakehouse::Lakehouse;
pub use session::{TPCH_TABLES, build_session, register_tpch_parquet};
pub use storage::{Storage, StorageConfig};
