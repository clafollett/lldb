//! Storage abstraction.
//!
//! Everything above this module speaks [`object_store::ObjectStore`] — a trait with backends
//! for local disk, memory, S3, GCS, and Azure. Picking a backend is a runtime decision, so
//! the *same binary* runs against a laptop directory in dev and an S3 bucket in production.
//!
//! DataFusion resolves a table path like `s3://bucket/lineitem.parquet` by looking up a store
//! registered for that URL scheme+authority in its `RuntimeEnv`. We lean on that: [`Storage`]
//! knows how to (a) build the right `ObjectStore` and (b) register it on a session so table
//! paths just resolve.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use url::Url;

/// Where table data physically lives.
///
/// The engine speaks `object_store::ObjectStore`, so adding a backend is adding an arm here —
/// nothing else in the engine changes. Local + InMemory cover dev and tests; S3 covers real
/// object-storage warehouses (and any S3-compatible service like MinIO via `endpoint`).
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

/// A live object store plus the knowledge of how DataFusion should address it.
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    kind: Kind,
}

#[derive(Debug, Clone)]
enum Kind {
    /// Absolute, canonicalized root on the local filesystem.
    Local { root: PathBuf },
    /// In-memory; addressed via the `memory://` scheme.
    Memory,
    /// S3 bucket; addressed via the `s3://{bucket}/...` scheme.
    S3 { bucket: String },
}

impl StorageConfig {
    /// Construct the backing [`ObjectStore`] for this configuration.
    pub fn build(&self) -> Result<Storage> {
        match self {
            StorageConfig::Local(root) => {
                let root = std::fs::canonicalize(root).with_context(|| {
                    format!("local storage root does not exist: {}", root.display())
                })?;
                // A bare LocalFileSystem addresses the whole filesystem, so we can hand
                // DataFusion absolute paths and it resolves them via the default `file://`
                // store — no extra registration needed for local dev.
                let store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
                Ok(Storage {
                    store,
                    kind: Kind::Local { root },
                })
            }
            StorageConfig::InMemory => {
                let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
                Ok(Storage {
                    store,
                    kind: Kind::Memory,
                })
            }
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                allow_http,
            } => {
                // Credentials are read from the environment/instance role by `from_env`;
                // we only wire up addressing (bucket/region/endpoint) here.
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(*allow_http);
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let store = Arc::new(
                    builder
                        .build()
                        .with_context(|| format!("building S3 store for bucket {bucket}"))?,
                ) as Arc<dyn ObjectStore>;
                Ok(Storage {
                    store,
                    kind: Kind::S3 {
                        bucket: bucket.clone(),
                    },
                })
            }
        }
    }
}

impl Storage {
    /// The raw object store, e.g. to seed the in-memory backend in a test.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Register this store on a session so table paths using its scheme resolve.
    ///
    /// Local storage rides DataFusion's built-in `file://` store, so this is a no-op there;
    /// the memory and S3 backends must be registered under `memory://` / `s3://{bucket}`.
    pub fn register_on(&self, ctx: &SessionContext) -> Result<()> {
        match &self.kind {
            Kind::Memory => {
                let url = Url::parse("memory://").unwrap();
                ctx.runtime_env()
                    .register_object_store(&url, self.store.clone());
            }
            Kind::S3 { bucket } => {
                // DataFusion resolves `s3://bucket/...` paths by looking up a store registered
                // for that scheme+authority, so register under the bucket's URL.
                let url = Url::parse(&format!("s3://{bucket}"))
                    .with_context(|| format!("invalid s3 url for bucket {bucket}"))?;
                ctx.runtime_env()
                    .register_object_store(&url, self.store.clone());
            }
            // Local rides DataFusion's built-in `file://` store — nothing to register.
            Kind::Local { .. } => {}
        }
        Ok(())
    }

    /// Full path/URL DataFusion should use to load a table stored at `relative`
    /// (e.g. `"sf1/lineitem.parquet"`).
    pub fn table_path(&self, relative: &str) -> Result<String> {
        match &self.kind {
            Kind::Local { root } => {
                let p = root.join(relative);
                Ok(p.to_str()
                    .ok_or_else(|| anyhow!("non-UTF8 path: {}", p.display()))?
                    .to_string())
            }
            Kind::Memory => Ok(format!("memory:///{relative}")),
            Kind::S3 { bucket } => Ok(format!("s3://{bucket}/{relative}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::path::Path as ObjPath;
    // In object_store 0.13 the async `put`/`get` are dyn-callable via this extension trait.
    use object_store::{ObjectStoreExt, PutPayload};

    #[test]
    fn default_config_is_local_data_dir() {
        match StorageConfig::default() {
            StorageConfig::Local(p) => assert_eq!(p, PathBuf::from("data")),
            other => panic!("expected Local, got {other:?}"),
        }
    }

    #[test]
    fn local_build_errors_on_missing_root() {
        let cfg = StorageConfig::Local(PathBuf::from("/definitely/not/real/xyzzy-lldb"));
        assert!(
            cfg.build().is_err(),
            "a missing root must fail to canonicalize"
        );
    }

    #[test]
    fn local_table_path_is_absolute_join() -> Result<()> {
        // CARGO_MANIFEST_DIR is guaranteed to exist while tests run.
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let storage = StorageConfig::Local(root).build()?;
        let path = storage.table_path("sf1/lineitem.parquet")?;
        assert!(path.ends_with("sf1/lineitem.parquet"), "got {path}");
        assert!(
            PathBuf::from(&path).is_absolute(),
            "expected absolute, got {path}"
        );
        Ok(())
    }

    #[test]
    fn memory_table_path_uses_memory_scheme() -> Result<()> {
        let storage = StorageConfig::InMemory.build()?;
        assert_eq!(
            storage.table_path("lineitem.parquet")?,
            "memory:///lineitem.parquet"
        );
        Ok(())
    }

    #[test]
    fn s3_build_wires_addressing_without_network() -> Result<()> {
        // Building an S3 store only configures addressing/credentials-from-env; it makes no
        // network call, so this is safe offline. A MinIO-style endpoint with http allowed
        // exercises the S3-compatible path.
        let cfg = StorageConfig::S3 {
            bucket: "lldb-warehouse".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            allow_http: true,
        };
        let storage = cfg.build()?;
        assert!(matches!(storage.kind, Kind::S3 { .. }));
        Ok(())
    }

    #[test]
    fn s3_table_path_uses_s3_scheme() -> Result<()> {
        let cfg = StorageConfig::S3 {
            bucket: "lldb-warehouse".to_string(),
            region: None,
            endpoint: None,
            allow_http: false,
        };
        let storage = cfg.build()?;
        assert_eq!(
            storage.table_path("sf1/lineitem.parquet")?,
            "s3://lldb-warehouse/sf1/lineitem.parquet"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_store_roundtrips_bytes() -> Result<()> {
        // Proves the abstraction is a real object store, not a stub: write then read back.
        let storage = StorageConfig::InMemory.build()?;
        let store = storage.object_store();
        let path = ObjPath::from("greeting.txt");
        store
            .put(&path, PutPayload::from(b"hello lldb".to_vec()))
            .await?;
        let got = store.get(&path).await?.bytes().await?;
        assert_eq!(got.as_ref(), b"hello lldb");
        Ok(())
    }
}
