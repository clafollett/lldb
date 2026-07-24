//! Session construction and table registration.
//!
//! A DataFusion [`SessionContext`] is the front door to the engine: you hand it SQL, it
//! runs the four-stage pipeline. Here we build one wired to a chosen storage backend and
//! teach it about the TPC-H tables.

use anyhow::{Context, Result};
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use crate::storage::{Storage, StorageConfig};

/// The eight TPC-H tables, in a stable order.
pub const TPCH_TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// Build a DataFusion [`SessionContext`] wired to `config`'s storage backend.
///
/// Returns the context plus the [`Storage`] handle — needed to resolve table paths and, for
/// the in-memory backend, to seed data.
pub async fn build_session(config: StorageConfig) -> Result<(SessionContext, Storage)> {
    let storage = config.build()?;
    let ctx = SessionContext::new();
    storage.register_on(&ctx)?;
    Ok((ctx, storage))
}

/// Register every TPC-H table stored as `<subdir>/<table>.parquet` under `storage`.
///
/// Uses DataFusion's `ListingTable` machinery, so a `<table>.parquet` file OR a directory of
/// part files both work.
pub async fn register_tpch_parquet(
    ctx: &SessionContext,
    storage: &Storage,
    subdir: &str,
) -> Result<()> {
    for table in TPCH_TABLES {
        let rel = format!("{subdir}/{table}.parquet");
        let path = storage.table_path(&rel)?;
        ctx.register_parquet(table, &path, ParquetReadOptions::default())
            .await
            .with_context(|| format!("registering `{table}` from {path}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpch_has_eight_tables_including_lineitem_and_orders() {
        assert_eq!(TPCH_TABLES.len(), 8);
        assert!(TPCH_TABLES.contains(&"lineitem"));
        assert!(TPCH_TABLES.contains(&"orders"));
    }

    #[tokio::test]
    async fn build_session_inmemory_yields_a_live_context() -> Result<()> {
        let (ctx, _storage) = build_session(StorageConfig::InMemory).await?;
        // A table-less context can still evaluate a constant query — proves it's wired up.
        let batches = ctx.sql("SELECT 1 + 41 AS answer").await?.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        Ok(())
    }
}
