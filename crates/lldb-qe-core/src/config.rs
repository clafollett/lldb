//! Shared configuration surface for the binaries.
//!
//! The coordinator and worker used to parse positional argv by hand and print with
//! `println!`. That does not survive containerization: a compose/ECS deployment sets
//! addresses, worker fleets, and S3 credentials through flags and environment variables, and
//! expects structured logs. This module centralizes both — a reusable clap [`StorageArgs`]
//! group and a `tracing` initializer — so both binaries configure storage and logging the
//! same way.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;

// Straight from `lldb_qe_types`, deliberately NOT via `crate::storage` — that module re-exports
// this type but also builds live `object_store` backends, and parsing four clap args must not
// need one. Same rule as `crate::auth`'s `rbac` imports.
use lldb_qe_types::storage::StorageConfig;

/// Storage backend selection, shared by both binaries. Every field has an env-var fallback so
/// a container can be configured entirely through the environment.
#[derive(Debug, Clone, Args)]
pub struct StorageArgs {
    /// Storage backend: `local`, `memory`, or `s3`.
    #[arg(long, env = "LLDB_STORAGE", default_value = "local")]
    pub storage: String,

    /// Local data root (used when `--storage local`).
    #[arg(long, env = "LLDB_DATA_DIR", default_value = "data")]
    pub data_dir: PathBuf,

    /// S3 bucket (required when `--storage s3`).
    #[arg(long, env = "LLDB_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 region (optional; resolved from the environment if unset).
    #[arg(long, env = "LLDB_S3_REGION")]
    pub s3_region: Option<String>,

    /// Custom S3 endpoint for S3-compatible stores (e.g. MinIO `http://minio:9000`).
    #[arg(long, env = "LLDB_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// Allow plaintext HTTP to the S3 endpoint (needed for local MinIO).
    #[arg(long, env = "LLDB_S3_ALLOW_HTTP", default_value_t = false)]
    pub s3_allow_http: bool,
}

impl StorageArgs {
    /// Resolve these args into a [`StorageConfig`].
    pub fn to_config(&self) -> Result<StorageConfig> {
        match self.storage.as_str() {
            "local" => Ok(StorageConfig::Local(self.data_dir.clone())),
            "memory" => Ok(StorageConfig::InMemory),
            "s3" => Ok(StorageConfig::S3 {
                bucket: self
                    .s3_bucket
                    .clone()
                    .context("--s3-bucket (LLDB_S3_BUCKET) is required for --storage s3")?,
                region: self.s3_region.clone(),
                endpoint: self.s3_endpoint.clone(),
                allow_http: self.s3_allow_http,
            }),
            other => bail!("unknown --storage backend `{other}` (expected local|memory|s3)"),
        }
    }
}

/// Initialize `tracing` from the `RUST_LOG` env var (default `info`). Idempotent-ish: safe to
/// call once at the top of a binary; a second call is ignored rather than panicking.
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(storage: &str) -> StorageArgs {
        StorageArgs {
            storage: storage.to_string(),
            data_dir: PathBuf::from("data"),
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_allow_http: false,
        }
    }

    #[test]
    fn local_and_memory_resolve() -> Result<()> {
        assert!(matches!(
            args("local").to_config()?,
            StorageConfig::Local(_)
        ));
        assert!(matches!(
            args("memory").to_config()?,
            StorageConfig::InMemory
        ));
        Ok(())
    }

    #[test]
    fn s3_requires_bucket() {
        assert!(args("s3").to_config().is_err());
        let mut a = args("s3");
        a.s3_bucket = Some("b".to_string());
        assert!(matches!(a.to_config().unwrap(), StorageConfig::S3 { .. }));
    }

    #[test]
    fn unknown_backend_errors() {
        assert!(args("floppy").to_config().is_err());
    }
}
