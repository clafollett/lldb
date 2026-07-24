//! Session construction and table registration.
//!
//! A DataFusion [`SessionContext`] is the front door to the engine: you hand it SQL, it
//! runs the four-stage pipeline. Here we build one wired to a chosen storage backend and
//! teach it about the TPC-H tables.

use anyhow::Result;
use datafusion::prelude::SessionContext;

use crate::catalog::register_listing_tables;
use crate::storage::{Storage, StorageConfig};
use crate::tpch::TPCH_TABLES;

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
/// A thin TPC-H convenience over the generic [`register_listing_tables`]: it just supplies the
/// eight table names and their `<subdir>/<table>.parquet` paths. New schemas should use a
/// [`crate::manifest::Manifest`] + [`crate::catalog::apply_manifest`] instead of a bespoke
/// helper like this.
pub async fn register_tpch_parquet(
    ctx: &SessionContext,
    storage: &Storage,
    subdir: &str,
) -> Result<()> {
    let tables: Vec<(String, String)> = TPCH_TABLES
        .iter()
        .map(|t| (t.to_string(), format!("{subdir}/{t}.parquet")))
        .collect();
    register_listing_tables(ctx, storage, &tables).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
