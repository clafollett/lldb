//! Where table data physically lives — the *declaration*, not the connection.
//!
//! `lldb_qe_core::storage::Storage` is what turns one of these into a live
//! `object_store::ObjectStore` and registers it on a session. The split is the point: choosing a
//! backend is four scalars and belongs in a config file, a clap arg group and a manifest — none of
//! which should have to link the query engine to say "local, rooted at `data`".

use std::path::PathBuf;

/// Where table data physically lives.
///
/// The engine speaks `object_store::ObjectStore`, so adding a backend is adding an arm here plus
/// an arm in `lldb_qe_core::storage::Storage::from_config` — nothing else in the engine changes.
/// Local + InMemory cover dev and tests; S3 covers real object-storage warehouses (and any
/// S3-compatible service like MinIO via `endpoint`).
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// Local filesystem rooted at a directory. The default for development.
    Local(PathBuf),
    /// Ephemeral in-memory store. Fast and isolated — ideal for tests.
    InMemory,
    /// S3 (or S3-compatible) bucket. Credentials come from the environment
    /// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, or an instance
    /// role) — never from config, so warehouse definitions carry no secrets.
    S3 {
        /// Bucket name; addresses resolve as `s3://{bucket}/...`.
        bucket: String,
        /// Region, e.g. `us-east-1`. `None` lets the SDK resolve it from the environment.
        region: Option<String>,
        /// Custom endpoint URL for S3-compatible stores (e.g. MinIO `http://minio:9000`).
        endpoint: Option<String>,
        /// Allow plaintext HTTP — needed for a local MinIO endpoint, off for real S3.
        allow_http: bool,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Local(PathBuf::from("data"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_local_data_dir() {
        match StorageConfig::default() {
            StorageConfig::Local(p) => assert_eq!(p, PathBuf::from("data")),
            other => panic!("expected Local, got {other:?}"),
        }
    }
}
