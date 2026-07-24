//! Iceberg lakehouse: turn a pile of Parquet files into transactional, versioned tables.
//!
//! Phase 0 pointed DataFusion straight at `lineitem.parquet` — a *file*. This module slides
//! Apache Iceberg in between: a **catalog** (the `sys.tables` + transaction-log analog) that
//! tracks each table's schema and snapshots. The same SQL then runs against a *table*, not a
//! file — with transactions, snapshots, and schema evolution underneath.
//!
//! A [`Lakehouse`] is now **generic over catalog name and namespace** — nothing here is tied
//! to TPC-H. It exposes small primitives (`ensure_namespace`, `create_table_from_arrow`,
//! `register_with`, snapshot inspection) that [`crate::catalog::apply_manifest`] composes to
//! materialize an arbitrary [`crate::manifest::Manifest`].
//!
//! For local dev we use an in-process [`MemoryCatalog`](iceberg::memory) (catalog state in
//! RAM) backed by `LocalFsStorageFactory` (data files on disk). A persistent SQL catalog is
//! the upgrade path when multiple processes must share one catalog — see
//! [`crate::manifest::CatalogBackend`].
//!
//! Note: Iceberg 0.10 does its own file IO (native `std::fs`), independent of the
//! [`crate::storage`] `object_store` layer — so there is no version coupling between them.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::prelude::SessionContext;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergCatalogProvider;

use crate::manifest::CatalogBackend;

/// An Iceberg catalog plus the glue to create tables and expose them to DataFusion.
///
/// Generic over the DataFusion catalog name; namespaces and tables are passed per call, so a
/// single `Lakehouse` can hold many namespaces.
pub struct Lakehouse {
    catalog: Arc<dyn Catalog>,
    catalog_name: String,
}

impl Lakehouse {
    /// Open an in-process MemoryCatalog whose data files live under `warehouse` on the local
    /// filesystem. `warehouse` must be an absolute path.
    pub async fn open_memory(catalog_name: &str, warehouse: &Path) -> Result<Self> {
        let warehouse_uri = format!("file://{}", warehouse.display());
        Self::open_memory_uri(catalog_name, &warehouse_uri).await
    }

    /// Open a catalog for `backend`. Memory backends need a `warehouse` (path or URI); the
    /// persistent `Sql`/`Rest` backends are gated behind build features and error until then.
    pub async fn open(
        catalog_name: &str,
        backend: &CatalogBackend,
        warehouse: Option<&str>,
    ) -> Result<Self> {
        match backend {
            CatalogBackend::Memory => {
                let warehouse = warehouse
                    .context("memory catalog requires a `warehouse` (path or file:// URI)")?;
                Self::open_memory_uri(catalog_name, &normalize_warehouse_uri(warehouse)).await
            }
            CatalogBackend::Sql { .. } => bail!(
                "sql catalog backend requires the `sql-catalog` build feature (not yet enabled)"
            ),
            CatalogBackend::Rest { .. } => bail!(
                "rest catalog backend requires the `rest-catalog` build feature (not yet enabled)"
            ),
        }
    }

    async fn open_memory_uri(catalog_name: &str, warehouse_uri: &str) -> Result<Self> {
        let catalog = MemoryCatalogBuilder::default()
            // Without a storage factory the "files" would live in RAM; we want them on disk.
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                catalog_name,
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_uri.to_string(),
                )]),
            )
            .await
            .context("building MemoryCatalog")?;
        Ok(Self {
            catalog: Arc::new(catalog),
            catalog_name: catalog_name.to_string(),
        })
    }

    /// The DataFusion catalog name this lakehouse registers under.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Create the namespace if it does not already exist.
    pub async fn ensure_namespace(&self, ns: &NamespaceIdent) -> Result<()> {
        if !self.catalog.namespace_exists(ns).await? {
            self.catalog.create_namespace(ns, HashMap::new()).await?;
        }
        Ok(())
    }

    /// Create an Iceberg table in `ns` from an Arrow schema. Field IDs are auto-assigned
    /// (Arrow schemas from parquet carry none), matching how the loader has always worked.
    pub async fn create_table_from_arrow(
        &self,
        ns: &NamespaceIdent,
        name: &str,
        arrow_schema: &ArrowSchema,
    ) -> Result<()> {
        let ice_schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema)
            .with_context(|| format!("converting arrow schema for {name}"))?;
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(ice_schema)
            .properties(HashMap::new())
            .build();
        self.catalog
            .create_table(ns, creation)
            .await
            .with_context(|| format!("creating iceberg table {name}"))?;
        Ok(())
    }

    /// Register this catalog with `ctx` so `SELECT ... FROM {catalog}.{ns}.{table}` resolves.
    /// Call after the tables exist so DataFusion sees them.
    pub async fn register_with(&self, ctx: &SessionContext) -> Result<()> {
        let provider = IcebergCatalogProvider::try_new(self.catalog.clone())
            .await
            .context("building IcebergCatalogProvider")?;
        ctx.register_catalog(&self.catalog_name, Arc::new(provider));
        Ok(())
    }

    /// The current snapshot id of a loaded table (`None` before any write).
    pub async fn current_snapshot_id(&self, ns: &str, table: &str) -> Result<Option<i64>> {
        let t = self.load_table(ns, table).await?;
        Ok(t.metadata().current_snapshot_id())
    }

    /// The current snapshot's summary (e.g. `total-records`, `added-data-files`) for inspection.
    pub async fn snapshot_summary(&self, ns: &str, table: &str) -> Result<HashMap<String, String>> {
        let t = self.load_table(ns, table).await?;
        match t.metadata().current_snapshot() {
            Some(snap) => Ok(snap.summary().additional_properties.clone()),
            None => Ok(HashMap::new()),
        }
    }

    /// Load an Iceberg table by namespace + name.
    pub async fn load_table(&self, ns: &str, table: &str) -> Result<iceberg::table::Table> {
        let tid = TableIdent::new(NamespaceIdent::new(ns.to_string()), table.to_string());
        self.catalog
            .load_table(&tid)
            .await
            .with_context(|| format!("loading iceberg table {ns}.{table}"))
    }
}

/// Treat a bare filesystem path as a `file://` URI; pass through anything already schemed.
fn normalize_warehouse_uri(warehouse: &str) -> String {
    if warehouse.contains("://") {
        warehouse.to_string()
    } else {
        format!("file://{warehouse}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_memory_catalog_supports_namespaces() -> Result<()> {
        let warehouse = tempfile::tempdir()?;
        let lake = Lakehouse::open_memory("lldb", warehouse.path()).await?;

        let ns = NamespaceIdent::new("tpch".to_string());
        assert!(!lake.catalog.namespace_exists(&ns).await?);
        lake.ensure_namespace(&ns).await?;
        assert!(lake.catalog.namespace_exists(&ns).await?);
        // Idempotent: a second ensure on an existing namespace is fine.
        lake.ensure_namespace(&ns).await?;
        assert_eq!(lake.catalog_name(), "lldb");
        Ok(())
    }

    #[test]
    fn warehouse_uri_normalization() {
        assert_eq!(normalize_warehouse_uri("/tmp/wh"), "file:///tmp/wh");
        assert_eq!(normalize_warehouse_uri("file:///tmp/wh"), "file:///tmp/wh");
        assert_eq!(normalize_warehouse_uri("s3://bucket/wh"), "s3://bucket/wh");
    }

    #[tokio::test]
    async fn open_rejects_unbuilt_backends() {
        let sql = CatalogBackend::Sql {
            uri: "sqlite://x".to_string(),
        };
        assert!(Lakehouse::open("c", &sql, None).await.is_err());
    }
}
