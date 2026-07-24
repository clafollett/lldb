//! Iceberg lakehouse: turn a pile of Parquet files into transactional, versioned tables.
//!
//! Phase 0 pointed DataFusion straight at `lineitem.parquet` — a *file*. This module slides
//! Apache Iceberg in between: a **catalog** (the `sys.tables` + transaction-log analog) that
//! tracks each table's schema and snapshots. The same SQL then runs against a *table*, not a
//! file — with transactions, snapshots, and schema evolution underneath.
//!
//! For local dev we use an in-process [`MemoryCatalog`](iceberg::memory) (catalog state in
//! RAM) backed by `LocalFsStorageFactory` (data files on disk). A persistent SQLite
//! `SqlCatalog` is the upgrade path when multiple workers must share one catalog (Phase 3+).
//!
//! Note: Iceberg 0.10 does its own file IO (native `std::fs`), independent of the
//! [`crate::storage`] `object_store` layer — so there is no version coupling between them.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergCatalogProvider;

/// DataFusion catalog name the Iceberg catalog registers under (`SELECT ... FROM lldb.tpch.t`).
pub const CATALOG: &str = "lldb";
/// Namespace (a DataFusion schema) that holds the TPC-H tables.
pub const NAMESPACE: &str = "tpch";

/// An Iceberg catalog plus the glue to load tables and expose them to DataFusion.
pub struct Lakehouse {
    catalog: Arc<dyn Catalog>,
}

impl Lakehouse {
    /// Open an in-process MemoryCatalog whose data files live under `warehouse` on the local
    /// filesystem. `warehouse` must be an absolute path.
    pub async fn open_memory(warehouse: &Path) -> Result<Self> {
        let warehouse_uri = format!("file://{}", warehouse.display());
        let catalog = MemoryCatalogBuilder::default()
            // Without a storage factory the "files" would live in RAM; we want them on disk.
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                CATALOG,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_uri)]),
            )
            .await
            .context("building MemoryCatalog")?;
        Ok(Self {
            catalog: Arc::new(catalog),
        })
    }

    /// Create each `(table, parquet_path)` as an Iceberg table, load its rows, and register
    /// the catalog with `ctx` so `SELECT ... FROM lldb.tpch.<table>` resolves.
    ///
    /// Order matters: tables are created *before* the catalog provider is registered (so
    /// DataFusion sees them), then filled via `INSERT INTO ... SELECT * FROM <parquet source>`.
    pub async fn load_tpch(&self, ctx: &SessionContext, tables: &[(&str, String)]) -> Result<()> {
        let ns = NamespaceIdent::new(NAMESPACE.to_string());
        if !self.catalog.namespace_exists(&ns).await? {
            self.catalog.create_namespace(&ns, HashMap::new()).await?;
        }

        // 1. Register each parquet as a source, then create a matching Iceberg table.
        for (table, path) in tables {
            let src = source_name(table);
            ctx.register_parquet(&src, path, ParquetReadOptions::default())
                .await
                .with_context(|| format!("registering source parquet for {table}"))?;

            let arrow_schema: ArrowSchema = ctx.table(&src).await?.schema().as_arrow().clone();
            // TPC-H parquet has no Iceberg field ids, so auto-assign them from the Arrow schema.
            let ice_schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
                .with_context(|| format!("converting arrow schema for {table}"))?;

            let creation = TableCreation::builder()
                .name((*table).to_string())
                .schema(ice_schema)
                .properties(HashMap::new())
                .build();
            self.catalog
                .create_table(&ns, creation)
                .await
                .with_context(|| format!("creating iceberg table {table}"))?;
        }

        // 2. Register the catalog with DataFusion now that every table exists.
        let provider = IcebergCatalogProvider::try_new(self.catalog.clone())
            .await
            .context("building IcebergCatalogProvider")?;
        ctx.register_catalog(CATALOG, Arc::new(provider));

        // 3. Fill each Iceberg table from its parquet source (append-only write path).
        for (table, _) in tables {
            let sql = format!(
                "INSERT INTO {CATALOG}.{NAMESPACE}.{table} SELECT * FROM {src}",
                src = source_name(table),
            );
            ctx.sql(&sql)
                .await?
                .collect()
                .await
                .with_context(|| format!("loading {table} into iceberg"))?;
        }
        Ok(())
    }

    /// The current snapshot id of a loaded table (`None` before any write).
    pub async fn current_snapshot_id(&self, table: &str) -> Result<Option<i64>> {
        let t = self.load(table).await?;
        Ok(t.metadata().current_snapshot_id())
    }

    /// The current snapshot's summary (e.g. `total-records`, `added-data-files`) for inspection.
    pub async fn snapshot_summary(&self, table: &str) -> Result<HashMap<String, String>> {
        let t = self.load(table).await?;
        match t.metadata().current_snapshot() {
            Some(snap) => Ok(snap.summary().additional_properties.clone()),
            None => Ok(HashMap::new()),
        }
    }

    async fn load(&self, table: &str) -> Result<iceberg::table::Table> {
        let tid = TableIdent::new(
            NamespaceIdent::new(NAMESPACE.to_string()),
            table.to_string(),
        );
        self.catalog
            .load_table(&tid)
            .await
            .with_context(|| format!("loading iceberg table {table}"))
    }
}

/// The DataFusion table name a raw parquet source is registered under while loading `table`.
fn source_name(table: &str) -> String {
    format!("{table}_src")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_memory_catalog_supports_namespaces() -> Result<()> {
        let warehouse = tempfile::tempdir()?;
        let lake = Lakehouse::open_memory(warehouse.path()).await?;

        let ns = NamespaceIdent::new(NAMESPACE.to_string());
        assert!(!lake.catalog.namespace_exists(&ns).await?);
        lake.catalog.create_namespace(&ns, HashMap::new()).await?;
        assert!(lake.catalog.namespace_exists(&ns).await?);
        Ok(())
    }

    #[test]
    fn source_name_is_suffixed() {
        assert_eq!(source_name("lineitem"), "lineitem_src");
    }
}
