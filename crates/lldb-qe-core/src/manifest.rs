//! Config-as-data: declare catalogs, namespaces, and tables in a **manifest** instead of code.
//!
//! The engine used to hardcode a single `lldb.tpch` catalog and the eight TPC-H table names.
//! A manifest replaces that with a declarative description of arbitrary
//! `catalogs → namespaces → tables`, so a new schema is a config change, not a code change:
//!
//! ```toml
//! [[catalogs]]
//! name = "shop"
//! warehouse = "file:///tmp/shop-wh"          # required when any table is Iceberg
//! backend = { kind = "memory" }              # or { kind = "sql", uri = "sqlite://…" }
//!
//! [[catalogs.namespaces]]
//! name = "sales"
//!
//! [[catalogs.namespaces.tables]]
//! name = "orders"
//! format = "iceberg"                          # or "listing" (plain parquet, default catalog)
//! source = { type = "parquet", path = "shop/orders.parquet" }   # storage-relative or URL
//! ```
//!
//! [`crate::catalog::apply_manifest`] turns a [`Manifest`] into registered DataFusion tables.
//! TPC-H is now just one manifest built by [`crate::tpch::tpch_manifest`].

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result, bail};
use datafusion::arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

/// The whole declarative catalog description: a set of catalogs to materialize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub catalogs: Vec<CatalogDef>,
}

/// One catalog: a named tree of namespaces, backed by a catalog implementation and (for
/// Iceberg tables) a warehouse location.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogDef {
    /// DataFusion catalog name — the first part of `catalog.namespace.table`.
    pub name: String,
    /// Catalog metadata backend. Defaults to an in-process memory catalog.
    #[serde(default)]
    pub backend: CatalogBackend,
    /// Warehouse URI/path where Iceberg table files live. Required when any table here is
    /// Iceberg; ignored for `Listing`-only catalogs.
    #[serde(default)]
    pub warehouse: Option<String>,
    #[serde(default)]
    pub namespaces: Vec<NamespaceDef>,
}

/// A namespace (a DataFusion schema): the middle part of `catalog.namespace.table`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceDef {
    pub name: String,
    #[serde(default)]
    pub tables: Vec<TableDef>,
}

/// A single table: where its bytes come from, how it is exposed, and (optionally) its schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    /// How the table is exposed to DataFusion. Defaults to Iceberg.
    #[serde(default)]
    pub format: TableFormat,
    /// Where the table's data comes from.
    pub source: TableSource,
    /// Explicit schema. `None` means infer it from the source (the common case for parquet);
    /// required for an [`TableSource::Empty`] table since there is nothing to infer from.
    #[serde(default)]
    pub schema: Option<Vec<ColumnDef>>,
}

/// Where a table's data comes from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableSource {
    /// A parquet file or directory of part files, addressed relative to the active
    /// [`crate::storage::Storage`] (or an absolute URL).
    Parquet { path: String },
    /// No source data — create an empty table from the declared `schema`.
    Empty,
}

/// How a table is surfaced to DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableFormat {
    /// A transactional Iceberg table in the catalog's warehouse (snapshots, evolution).
    #[default]
    Iceberg,
    /// A plain parquet `ListingTable` registered in the default catalog (no Iceberg).
    Listing,
}

/// The catalog metadata backend.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogBackend {
    /// In-process catalog; metadata lives in RAM (per-process, not shared). Dev default.
    #[default]
    Memory,
    /// Persistent SQL catalog (SQLite/Postgres/…). Durable and shareable across processes.
    /// Requires the `sql-catalog` build feature.
    Sql { uri: String },
    /// Remote Iceberg REST catalog. Requires the `rest-catalog` build feature.
    Rest { uri: String },
}

/// A single column in an explicit table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// A type from the small vocabulary understood by [`ColumnDef::to_arrow_field`]
    /// (e.g. `int64`, `string`, `date`, `decimal(15,2)`).
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

impl Manifest {
    /// Parse a manifest from TOML text.
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let manifest: Manifest = toml::from_str(s).context("parsing manifest TOML")?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Load and parse a manifest from a TOML file on disk.
    pub fn from_path(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading manifest {}", path.display()))?;
        Self::from_toml_str(&text)
    }

    /// Reject structurally-invalid manifests before we try to materialize them:
    /// duplicate names, missing warehouse for Iceberg tables, or un-inferable empty tables.
    pub fn validate(&self) -> Result<()> {
        let mut catalog_names = HashSet::new();
        // Listing tables are registered into DataFusion's default catalog under their bare
        // names, so their namespace is flat across the *entire* manifest — two listing tables
        // named `orders` in different namespaces/catalogs would silently clobber each other.
        let mut listing_names = HashSet::new();
        for catalog in &self.catalogs {
            if !catalog_names.insert(catalog.name.as_str()) {
                bail!("duplicate catalog name: {}", catalog.name);
            }
            let has_iceberg = catalog
                .namespaces
                .iter()
                .flat_map(|ns| &ns.tables)
                .any(|t| t.format == TableFormat::Iceberg);
            if has_iceberg && catalog.warehouse.is_none() {
                bail!(
                    "catalog `{}` has Iceberg tables but no `warehouse`",
                    catalog.name
                );
            }

            let mut ns_names = HashSet::new();
            for ns in &catalog.namespaces {
                if !ns_names.insert(ns.name.as_str()) {
                    bail!(
                        "duplicate namespace `{}` in catalog `{}`",
                        ns.name,
                        catalog.name
                    );
                }
                let mut table_names = HashSet::new();
                for table in &ns.tables {
                    if !table_names.insert(table.name.as_str()) {
                        bail!(
                            "duplicate table `{}` in `{}.{}`",
                            table.name,
                            catalog.name,
                            ns.name
                        );
                    }
                    if matches!(table.source, TableSource::Empty) && table.schema.is_none() {
                        bail!(
                            "table `{}.{}.{}` has no source and no schema — nothing to create",
                            catalog.name,
                            ns.name,
                            table.name
                        );
                    }
                    if table.format == TableFormat::Listing {
                        if matches!(table.source, TableSource::Empty) {
                            bail!(
                                "listing table `{}.{}.{}` needs a parquet source",
                                catalog.name,
                                ns.name,
                                table.name
                            );
                        }
                        if !listing_names.insert(table.name.as_str()) {
                            bail!(
                                "duplicate listing table `{}` — listing tables share one flat \
                                 namespace (registered under bare names), so their names must be \
                                 unique across the whole manifest",
                                table.name
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl ColumnDef {
    /// Map this column to an Arrow [`Field`], parsing the small supported type vocabulary.
    pub fn to_arrow_field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            parse_data_type(&self.data_type)?,
            self.nullable,
        ))
    }
}

/// Map a type name from a manifest to an Arrow [`DataType`]. Deliberately small — extend as
/// real schemas need more. `decimal(p,s)` is parsed for precision/scale.
fn parse_data_type(s: &str) -> Result<DataType> {
    let lower = s.trim().to_ascii_lowercase();
    let ty = match lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "int32" | "int" => DataType::Int32,
        "int64" | "bigint" | "long" => DataType::Int64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "string" | "utf8" | "varchar" | "text" => DataType::Utf8,
        "date" | "date32" => DataType::Date32,
        other if other.starts_with("decimal") => {
            let (p, s) =
                parse_decimal_args(other).with_context(|| format!("parsing decimal type `{s}`"))?;
            DataType::Decimal128(p, s)
        }
        other => bail!("unsupported data_type `{other}`"),
    };
    Ok(ty)
}

/// Parse `decimal(precision,scale)` into `(u8, i8)`.
fn parse_decimal_args(s: &str) -> Result<(u8, i8)> {
    let inner = s
        .strip_prefix("decimal")
        .and_then(|r| r.trim().strip_prefix('('))
        .and_then(|r| r.strip_suffix(')'))
        .context("decimal must look like decimal(p,s)")?;
    let (p, sc) = inner
        .split_once(',')
        .context("decimal needs precision,scale")?;
    Ok((p.trim().parse()?, sc.trim().parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_CATALOGS: &str = r#"
        [[catalogs]]
        name = "shop"
        warehouse = "file:///tmp/shop-wh"

        [[catalogs.namespaces]]
        name = "sales"

        [[catalogs.namespaces.tables]]
        name = "orders"
        source = { type = "parquet", path = "shop/orders.parquet" }

        [[catalogs.namespaces.tables]]
        name = "customers"
        format = "listing"
        source = { type = "parquet", path = "shop/customers.parquet" }

        [[catalogs]]
        name = "warehouse"
        warehouse = "file:///tmp/wh"

        [[catalogs.namespaces]]
        name = "inventory"

        [[catalogs.namespaces.tables]]
        name = "stock"
        source = { type = "parquet", path = "wh/stock.parquet" }
    "#;

    #[test]
    fn parses_two_catalogs_with_defaults() -> Result<()> {
        let m = Manifest::from_toml_str(TWO_CATALOGS)?;
        assert_eq!(m.catalogs.len(), 2);
        let shop = &m.catalogs[0];
        assert_eq!(shop.name, "shop");
        assert_eq!(shop.backend, CatalogBackend::Memory); // defaulted
        let orders = &shop.namespaces[0].tables[0];
        assert_eq!(orders.format, TableFormat::Iceberg); // defaulted
        assert_eq!(shop.namespaces[0].tables[1].format, TableFormat::Listing);
        Ok(())
    }

    #[test]
    fn round_trips_through_serde() -> Result<()> {
        let m = Manifest::from_toml_str(TWO_CATALOGS)?;
        let text = toml::to_string(&m)?;
        let again = Manifest::from_toml_str(&text)?;
        assert_eq!(m, again);
        Ok(())
    }

    #[test]
    fn rejects_duplicate_table_names() {
        let toml = r#"
            [[catalogs]]
            name = "c"
            warehouse = "file:///tmp/wh"
            [[catalogs.namespaces]]
            name = "n"
            [[catalogs.namespaces.tables]]
            name = "t"
            source = { type = "parquet", path = "a.parquet" }
            [[catalogs.namespaces.tables]]
            name = "t"
            source = { type = "parquet", path = "b.parquet" }
        "#;
        assert!(Manifest::from_toml_str(toml).is_err());
    }

    #[test]
    fn rejects_listing_name_collision_across_namespaces() {
        // Two listing tables named `orders` in different namespaces would both register under
        // the bare name `orders` in the default catalog and clobber each other.
        let toml = r#"
            [[catalogs]]
            name = "c"
            [[catalogs.namespaces]]
            name = "a"
            [[catalogs.namespaces.tables]]
            name = "orders"
            format = "listing"
            source = { type = "parquet", path = "a.parquet" }
            [[catalogs.namespaces]]
            name = "b"
            [[catalogs.namespaces.tables]]
            name = "orders"
            format = "listing"
            source = { type = "parquet", path = "b.parquet" }
        "#;
        assert!(Manifest::from_toml_str(toml).is_err());
    }

    #[test]
    fn rejects_listing_table_without_source() {
        let toml = r#"
            [[catalogs]]
            name = "c"
            [[catalogs.namespaces]]
            name = "n"
            [[catalogs.namespaces.tables]]
            name = "t"
            format = "listing"
            schema = [{ name = "id", data_type = "int64" }]
        "#;
        assert!(Manifest::from_toml_str(toml).is_err());
    }

    #[test]
    fn rejects_iceberg_without_warehouse() {
        let toml = r#"
            [[catalogs]]
            name = "c"
            [[catalogs.namespaces]]
            name = "n"
            [[catalogs.namespaces.tables]]
            name = "t"
            source = { type = "parquet", path = "a.parquet" }
        "#;
        assert!(Manifest::from_toml_str(toml).is_err());
    }

    #[test]
    fn listing_only_catalog_needs_no_warehouse() -> Result<()> {
        let toml = r#"
            [[catalogs]]
            name = "c"
            [[catalogs.namespaces]]
            name = "n"
            [[catalogs.namespaces.tables]]
            name = "t"
            format = "listing"
            source = { type = "parquet", path = "a.parquet" }
        "#;
        assert!(Manifest::from_toml_str(toml).is_ok());
        Ok(())
    }

    #[test]
    fn rejects_empty_source_without_schema() {
        let toml = r#"
            [[catalogs]]
            name = "c"
            warehouse = "file:///tmp/wh"
            [[catalogs.namespaces]]
            name = "n"
            [[catalogs.namespaces.tables]]
            name = "t"
            source = { type = "empty" }
        "#;
        assert!(Manifest::from_toml_str(toml).is_err());
    }

    #[test]
    fn maps_the_type_vocabulary_to_arrow() -> Result<()> {
        let col = |ty: &str| ColumnDef {
            name: "c".to_string(),
            data_type: ty.to_string(),
            nullable: true,
        };
        assert_eq!(col("int64").to_arrow_field()?.data_type(), &DataType::Int64);
        assert_eq!(col("string").to_arrow_field()?.data_type(), &DataType::Utf8);
        assert_eq!(col("date").to_arrow_field()?.data_type(), &DataType::Date32);
        assert_eq!(
            col("decimal(15,2)").to_arrow_field()?.data_type(),
            &DataType::Decimal128(15, 2)
        );
        assert!(col("nonsense").to_arrow_field().is_err());
        Ok(())
    }
}
