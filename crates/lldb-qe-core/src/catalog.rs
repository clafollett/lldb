//! Materialize a [`Manifest`] into registered DataFusion tables.
//!
//! This is the single config-driven entry point that ties together
//! [`crate::manifest`], [`crate::storage`], and [`crate::lakehouse`]. It replaces the old
//! hardcoded `Lakehouse::load_tpch` with a generic loader that walks arbitrary
//! `catalogs → namespaces → tables` and, for each catalog, either:
//!
//! - registers **listing** tables (plain parquet) in DataFusion's default catalog, or
//! - creates **Iceberg** tables in the catalog's warehouse and seeds them from their source.
//!
//! TPC-H is now just the manifest produced by [`crate::tpch::tpch_manifest`].

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iceberg::NamespaceIdent;

use crate::lakehouse::Lakehouse;
use crate::manifest::{Manifest, TableDef, TableFormat, TableSource};
use crate::storage::Storage;

/// Register plain-parquet `ListingTable`s in the default catalog under bare names.
///
/// `tables` are `(table_name, source_path)` pairs, where `source_path` is either an absolute
/// URL/path or a path relative to `storage`. This is the generic form of the old
/// `register_tpch_parquet`; the TPC-H helper now delegates here.
pub async fn register_listing_tables(
    ctx: &SessionContext,
    storage: &Storage,
    tables: &[(String, String)],
) -> Result<()> {
    for (name, path) in tables {
        let resolved = resolve_source_path(storage, path)?;
        ctx.register_parquet(name, &resolved, ParquetReadOptions::default())
            .await
            .with_context(|| format!("registering `{name}` from {resolved}"))?;
    }
    Ok(())
}

/// Materialize every catalog in `m`. Returns the opened [`Lakehouse`] handles (one per catalog
/// that has Iceberg tables) so callers can inspect snapshots afterward.
pub async fn apply_manifest(
    ctx: &SessionContext,
    storage: &Storage,
    m: &Manifest,
) -> Result<Vec<Lakehouse>> {
    let mut lakehouses = Vec::new();

    for catalog in &m.catalogs {
        // Split this catalog's tables by how they're surfaced.
        let mut listing: Vec<(String, String)> = Vec::new();
        let mut iceberg: Vec<(&str, &TableDef)> = Vec::new(); // (namespace, table)
        for ns in &catalog.namespaces {
            for table in &ns.tables {
                match table.format {
                    TableFormat::Listing => {
                        let path = source_path(table).with_context(|| {
                            format!("listing table `{}` needs a parquet source", table.name)
                        })?;
                        listing.push((table.name.clone(), path.to_string()));
                    }
                    TableFormat::Iceberg => iceberg.push((ns.name.as_str(), table)),
                }
            }
        }

        register_listing_tables(ctx, storage, &listing).await?;

        if iceberg.is_empty() {
            continue;
        }

        // Iceberg tables: create each, register the catalog, then seed from sources.
        let lake = Lakehouse::open(
            &catalog.name,
            &catalog.backend,
            catalog.warehouse.as_deref(),
        )
        .await?;

        // 1. Create each table (registering its parquet source alongside for the seed step).
        for (ns_name, table) in &iceberg {
            let ns = NamespaceIdent::new((*ns_name).to_string());
            lake.ensure_namespace(&ns).await?;

            let arrow_schema = match source_path(table) {
                Some(path) => {
                    let src = source_name(&catalog.name, ns_name, &table.name);
                    let resolved = resolve_source_path(storage, path)?;
                    ctx.register_parquet(&src, &resolved, ParquetReadOptions::default())
                        .await
                        .with_context(|| {
                            format!("registering source parquet for {}", table.name)
                        })?;
                    ctx.table(&src).await?.schema().as_arrow().clone()
                }
                None => explicit_arrow_schema(table)?,
            };
            lake.create_table_from_arrow(&ns, &table.name, &arrow_schema)
                .await?;
        }

        // 2. Register the catalog now that every table exists.
        lake.register_with(ctx).await?;

        // 3. Seed each table from its parquet source (append-only). Empty sources stay empty.
        for (ns_name, table) in &iceberg {
            if source_path(table).is_some() {
                let src = source_name(&catalog.name, ns_name, &table.name);
                let sql = format!(
                    "INSERT INTO {cat}.{ns}.{tbl} SELECT * FROM {src}",
                    cat = quote_ident(&catalog.name),
                    ns = quote_ident(ns_name),
                    tbl = quote_ident(&table.name),
                    src = quote_ident(&src),
                );
                ctx.sql(&sql).await?.collect().await.with_context(|| {
                    format!("loading {}.{}.{}", catalog.name, ns_name, table.name)
                })?;
            }
        }

        lakehouses.push(lake);
    }

    Ok(lakehouses)
}

/// Quote a SQL identifier so names needing escaping (mixed case, `-`, reserved words) survive
/// interpolation, and a manifest can't inject SQL through a table name. Embedded `"` is doubled
/// per the SQL standard.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The parquet source path for a table, if it has one.
fn source_path(table: &TableDef) -> Option<&str> {
    match &table.source {
        TableSource::Parquet { path } => Some(path.as_str()),
        TableSource::Empty => None,
    }
}

/// Build an Arrow schema from a table's explicit column definitions (for `Empty` sources).
fn explicit_arrow_schema(table: &TableDef) -> Result<ArrowSchema> {
    let columns = table.schema.as_ref().with_context(|| {
        format!(
            "table `{}` has no source and no explicit schema",
            table.name
        )
    })?;
    let fields: Result<Vec<Field>> = columns.iter().map(|c| c.to_arrow_field()).collect();
    Ok(ArrowSchema::new(fields?))
}

/// A unique DataFusion table name for a table's raw parquet source during loading.
fn source_name(catalog: &str, ns: &str, table: &str) -> String {
    format!("{catalog}__{ns}__{table}_src")
}

/// Resolve a manifest source path: absolute URLs/paths pass through; relative paths are
/// resolved against `storage` (so `sf1/x.parquet` becomes the backend's real address).
fn resolve_source_path(storage: &Storage, path: &str) -> Result<String> {
    if path.contains("://") || path.starts_with('/') {
        Ok(path.to_string())
    } else {
        storage.table_path(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_name_is_unique_per_path() {
        assert_eq!(
            source_name("lldb", "tpch", "nation"),
            "lldb__tpch__nation_src"
        );
        assert_ne!(
            source_name("a", "n", "t"),
            source_name("b", "n", "t"),
            "different catalogs must not collide"
        );
    }

    #[test]
    fn quote_ident_wraps_and_escapes() {
        assert_eq!(quote_ident("orders"), "\"orders\"");
        assert_eq!(quote_ident("odd-name"), "\"odd-name\"");
        // an embedded quote is doubled, so a name can't break out of the identifier
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn resolve_passes_through_urls_and_absolute() -> Result<()> {
        let storage = crate::storage::StorageConfig::InMemory.build()?;
        assert_eq!(
            resolve_source_path(&storage, "s3://b/x.parquet")?,
            "s3://b/x.parquet"
        );
        assert_eq!(
            resolve_source_path(&storage, "/abs/x.parquet")?,
            "/abs/x.parquet"
        );
        // relative resolves against the memory backend's scheme
        assert_eq!(
            resolve_source_path(&storage, "rel/x.parquet")?,
            "memory:///rel/x.parquet"
        );
        Ok(())
    }
}
