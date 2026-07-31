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
//!
//! # Applying a manifest is idempotent
//!
//! Applying the same manifest twice must leave the same state, because with a persistent
//! catalog ([`crate::manifest::CatalogBackend::Sql`]) that is the *normal* case: every process
//! in the fleet applies it at startup, and every restart applies it again. So a table that
//! already exists is the manifest being satisfied, not a conflict — and, crucially, it is not
//! re-seeded. Re-running the loading `INSERT` would append a second copy of the source data and
//! commit a new snapshot, making a query's answer depend on how many times the fleet had
//! booted. Only tables this call actually created are seeded.
//!
//! # Iceberg tables are tenant-scoped; listing tables are not
//!
//! Every catalog here is opened for a [`TenantScope`], so an Iceberg table lands in a catalog and
//! a warehouse root belonging to one account (see [`crate::tenancy`]). **Listing tables are
//! different and cannot be otherwise**: a `ListingTable` is a plain path registered under a bare
//! name in DataFusion's default catalog, with no catalog row and no warehouse to partition. Two
//! tenants applying a manifest that declares one are reading the same files. That is not a
//! regression — it is the same reason the result cache refuses to version a listing table — but it
//! does mean a manifest that mixes the two only isolates half of itself, and a multi-tenant
//! deployment should declare its tables `format = "iceberg"`.

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use iceberg::NamespaceIdent;

use crate::lakehouse::Lakehouse;
use crate::manifest::{Manifest, TableDef, TableFormat, TableSource};
use crate::storage::Storage;
use crate::tenancy::TenantScope;

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

/// Materialize every catalog in `m` for one tenant. Returns the opened [`Lakehouse`] handles (one
/// per catalog that has Iceberg tables) so callers can inspect snapshots afterward.
///
/// `scope` is an explicit parameter rather than something defaulted, and that is deliberate: it
/// decides whether these tables land in a catalog of this tenant's own or in one shared with every
/// other tenant on the deployment, and there is no safe guess. [`TenantScope::untenanted`] is the
/// single-node answer — see [`crate::tenancy`]. Nothing about the *manifest* changes: the catalog
/// keeps the name it declares as far as SQL is concerned, so the same manifest materializes the
/// same-looking catalog for every tenant.
pub async fn apply_manifest(
    ctx: &SessionContext,
    storage: &Storage,
    m: &Manifest,
    scope: &TenantScope,
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
            scope,
            &catalog.backend,
            catalog.warehouse.as_deref(),
        )
        .await?;

        // 1. Create each table that is missing, registering its parquet source alongside for
        //    the seed step. Tables the catalog already has are left exactly as they are — see
        //    `seeded` below for why that distinction matters.
        let mut seeded: Vec<(&str, &TableDef)> = Vec::new();
        for (ns_name, table) in &iceberg {
            let ns = NamespaceIdent::new((*ns_name).to_string());
            lake.ensure_namespace(&ns).await?;

            if lake.table_exists(ns_name, &table.name).await? {
                continue;
            }

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
            if lake
                .ensure_table_from_arrow(&ns, &table.name, &arrow_schema)
                .await?
            {
                seeded.push((*ns_name, table));
            }
        }

        // 2. Register the catalog now that every table exists.
        lake.register_with(ctx).await?;

        // 3. Seed from parquet — but only the tables *this call* created.
        //
        //    A memory catalog is empty at every startup, so that is all of them and nothing
        //    changes. A persistent SQL catalog is not: the second process to apply the manifest,
        //    and the same process after a restart, find the tables already populated. Seeding
        //    those again would append a duplicate copy of the source data and mint a new
        //    snapshot, so the fleet's answer to a query would depend on how many times a
        //    manifest had been applied. Applying a manifest twice has to be a no-op.
        for (ns_name, table) in &seeded {
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
        let storage =
            crate::storage::Storage::from_config(&crate::storage::StorageConfig::InMemory)?;
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
