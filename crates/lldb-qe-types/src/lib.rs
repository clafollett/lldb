//! # lldb-qe-types
//!
//! The **vocabulary** of the lldb query engine: the types that describe what a caller may touch
//! and where bytes live, with none of the machinery that acts on them.
//!
//! ## The rule this crate exists to enforce
//!
//! **Nothing here may depend on `datafusion`, `arrow`, `iceberg`, `parquet`, `object_store` or
//! `sqlx`.** That is checked, not hoped for:
//!
//! ```text
//! cargo tree -p lldb-qe-types | grep -E 'datafusion|arrow|iceberg|parquet|object_store|sqlx'
//! ```
//!
//! must print nothing. A grant check is a pure function of a grant set and a
//! `StorageConfig` is four scalars, so anything that drags the query engine in to define them is
//! an accident of where the code was first written — and one that every dependent pays for in
//! compile time.
//!
//! ## What is here, and what stayed behind
//!
//! | Here | In `lldb_qe_core` |
//! |------|-------------------|
//! | [`rbac`]: [`Privilege`], [`ObjectRef`], [`Grant`], [`Requirement`], [`QueryAuthorization`] | `required_privileges` and `check_plan`, which read a DataFusion `LogicalPlan` |
//! | [`storage`]: [`StorageConfig`] | `Storage`, which builds a live `object_store::ObjectStore` from one |
//!
//! `lldb_qe_core` re-exports everything below from its own `rbac` and `storage` modules, so a
//! caller that already says `lldb_qe_core::rbac::Privilege` needs no change. Importing from here
//! directly is what buys the shorter dependency edge.

pub mod rbac;
pub mod storage;

pub use rbac::{
    Denied, Grant, OBJECT_TYPES, ObjectRef, ObjectType, PRIVILEGES, Privilege, QueryAuthorization,
    Requirement, is_denial, validate_object_name,
};
pub use storage::StorageConfig;
