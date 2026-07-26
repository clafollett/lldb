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
//! # Two catalogs, and why the second one exists
//!
//! [`CatalogBackend::Memory`] is an in-process [`MemoryCatalog`](iceberg::memory): catalog
//! state in RAM, data files on disk via `LocalFsStorageFactory`. It needs nothing running,
//! which is why it is the dev default — and it is *per-process*, which is why it cannot be the
//! answer. Every worker built its own copy from its own copy of the manifest, so "what tables
//! exist" and "which snapshot is current" were N independent opinions that nothing reconciled.
//! A stale opinion does not announce itself; it returns the wrong number of rows.
//!
//! [`CatalogBackend::Sql`] replaces those N opinions with one row in Postgres. Catalog state
//! lives in the same database as the rest of the control plane (see [`crate::services`]), so
//! every process that opens the same catalog name against the same URI sees the same tables and
//! the same snapshot pointer — including changes another process made a moment ago. Commits go
//! through a conditional `UPDATE ... WHERE metadata_location = <what I read>`, so two writers
//! racing produce one winner and one retryable conflict rather than a silently lost snapshot.
//!
//! [`CatalogBackend::Rest`] still errors. That is a deployment decision (another service to
//! run, another thing to secure) rather than a missing feature, and `Sql` already satisfies the
//! shared-catalog requirement; the variant stays modelled so adding it later is additive.
//!
//! # The SQL catalog owns its own schema — ours does not describe it
//!
//! `SqlCatalog::new` issues `CREATE TABLE IF NOT EXISTS` for `iceberg_tables` and
//! `iceberg_namespace_properties` every time a catalog is opened. Those two tables are
//! `iceberg-catalog-sql`'s, not ours, so they deliberately do **not** appear in
//! `crates/lldb-qe-core/migrations/` — our migrations own the services schema (accounts, users,
//! warehouses, queries) and nothing else. Two owners for one table is how a schema ends up
//! half-migrated. The bootstrap is idempotent, which is why this one is allowed to be startup
//! magic when [`crate::services::ServicesDb`] is not: `IF NOT EXISTS` on a fixed two-table schema
//! is not a rolling DDL migration.
//!
//! Idempotent is not the same as race-free, though, and the difference bites exactly once per
//! database. `CREATE TABLE IF NOT EXISTS` in Postgres checks for the table and then creates it
//! *without* taking a lock that would make the pair atomic, so two processes opening a catalog
//! against a **fresh** database at the same moment can both pass the check and one then fails on
//! a duplicate-key error from the system catalogs. `iceberg-catalog-sql` surfaces that as an
//! opaque error with no message. Since the whole point of a shared catalog is that a fleet boots
//! against it simultaneously — and [`crate::dml`] opens one per writer — [`Lakehouse::open_sql`]
//! retries a few times before giving up. Once the tables exist the race cannot recur, so the
//! retry costs nothing on any subsequent open.
//!
//! # Storage: the local filesystem, and an honest error for everything else
//!
//! Iceberg 0.10 ships exactly two `StorageFactory` implementations — `LocalFsStorageFactory`
//! and `MemoryStorageFactory`. There is **no** object-store factory: `iceberg::io` has
//! `S3Config`/`GcsConfig`/`AzdlsConfig` property vocabularies but nothing that implements
//! `Storage` behind them. So a SQL catalog here accepts a `file://` warehouse and *refuses* an
//! `s3://` one.
//!
//! Refusing matters more than it looks. `LocalFsStorage::normalize_path` does not reject a
//! scheme it doesn't know — handed `s3://bucket/wh/ns/t`, it happily creates a local directory
//! literally named `s3:/bucket/...`. A silent fallback would therefore write table metadata to
//! one container's disk while every other process looked in the bucket, which is precisely the
//! split-brain this module exists to end. Better a startup error naming what is missing.
//!
//! Note: Iceberg 0.10 does its own file IO (native `std::fs`), independent of the
//! [`crate::storage`] `object_store` layer — so there is no version coupling between them, and
//! also no way to lend Iceberg the S3 store `object_store` already builds.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::prelude::SessionContext;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::io::{LocalFsStorageFactory, StorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;

use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::OnceCell;

use crate::manifest::CatalogBackend;
use crate::services::{ServicesArgs, redact_url};

/// An Iceberg catalog plus the glue to create tables and expose them to DataFusion.
///
/// Generic over the DataFusion catalog name; namespaces and tables are passed per call, so a
/// single `Lakehouse` can hold many namespaces.
pub struct Lakehouse {
    catalog: Arc<dyn Catalog>,
    catalog_name: String,
    /// The compare-and-swap this catalog commits through, when it has one. See
    /// [`CatalogCommitPoint`] and [`crate::dml`].
    commit_point: Option<CatalogCommitPoint>,
}

/// The row in `iceberg_tables` whose `metadata_location` column *is* a table's committed state,
/// plus a connection to the database that arbitrates changes to it.
///
/// [`crate::dml`] needs this because iceberg-rust 0.10 has no public way to hand a catalog an
/// arbitrary [`iceberg::TableUpdate`] — `TableCommit`'s builder is `pub(crate)` and
/// `TransactionAction` is a private trait, so the only committable action the crate exposes is
/// `fast_append`. A copy-on-write `DELETE`/`UPDATE` therefore has to perform the pointer swap
/// itself, and it performs *exactly* the swap `iceberg-catalog-sql` performs: a conditional
/// `UPDATE ... WHERE metadata_location = <what I read>`. Same row, same predicate, same
/// serialization point — so a DML commit and an ordinary `INSERT` commit race each other
/// correctly rather than through two independent mechanisms that happen to touch one table.
///
/// The pool is separate from (and much smaller than) the one `SqlCatalog` holds internally,
/// because that one is private. It is opened lazily: a `Lakehouse` that never runs DML never
/// opens it.
pub(crate) struct CatalogCommitPoint {
    /// Connection URL. Never logged unredacted — see [`redact_url`].
    uri: String,
    pool: OnceCell<PgPool>,
}

impl CatalogCommitPoint {
    /// The pool, connected on first use.
    ///
    /// Two connections is deliberate and sufficient: a commit issues one short `UPDATE`, and the
    /// expensive part of a DML statement (scanning and rewriting the table) holds no connection
    /// at all. A large pool here would only add idle sockets to every process in the fleet.
    pub(crate) async fn pool(&self) -> Result<&PgPool> {
        self.pool
            .get_or_try_init(|| async {
                PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&self.uri)
                    .await
                    .with_context(|| {
                        format!(
                            "connecting to the catalog's commit point at {}",
                            redact_url(&self.uri)
                        )
                    })
            })
            .await
    }
}

impl Lakehouse {
    /// Open an in-process MemoryCatalog whose data files live under `warehouse` on the local
    /// filesystem. `warehouse` must be an absolute path.
    pub async fn open_memory(catalog_name: &str, warehouse: &Path) -> Result<Self> {
        let warehouse_uri = format!("file://{}", warehouse.display());
        Self::open_memory_uri(catalog_name, &warehouse_uri).await
    }

    /// Open a persistent SQL catalog: catalog metadata in `catalog_uri`'s database, table files
    /// under `warehouse` (a path or `file://` URI).
    ///
    /// `catalog_name` is not decoration — it is a *column* in `iceberg_tables`, so it is the
    /// key two processes must agree on to be looking at the same catalog. Same name + same URI
    /// = same tables; a typo in either silently yields an empty catalog rather than an error,
    /// which is worth knowing when a worker reports a table that "does not exist".
    pub async fn open_sql(catalog_name: &str, catalog_uri: &str, warehouse: &str) -> Result<Self> {
        let warehouse_uri = normalize_warehouse_uri(warehouse);
        // Validated up front so a bad URI fails before any connection attempt; recomputed per
        // attempt below because `SqlBindStyle` is neither `Clone` nor `Copy` and the function is a
        // cheap pure mapping over the scheme.
        sql_bind_style_for(catalog_uri)?;
        ensure_sql_driver_compiled(catalog_uri)?;
        let storage = storage_factory_for(&warehouse_uri)?;

        // Opening is retried, and the reason is subtle enough to be worth stating: `SqlCatalog::new`
        // bootstraps its schema with `CREATE TABLE IF NOT EXISTS` on *every* open, and in
        // PostgreSQL that statement is **not atomic** against a concurrent copy of itself. Two
        // processes opening the same catalog at the same moment on a fresh database can both pass
        // the existence check, and the loser fails inserting the table's row type:
        //
        //     duplicate key value violates unique constraint "pg_type_typname_nsp_index"
        //
        // This is exactly the shape a fleet produces — compose starts a coordinator and its workers
        // together, and each opens the catalog — so leaving it would mean a cluster that comes up
        // fine most of the time and fails to start occasionally, which is the worst kind of bug to
        // debug from a log. The bootstrap is idempotent, so retrying is safe and the second attempt
        // finds the tables already there.
        //
        // Only *this* error is retried. Everything else (bad URI, auth, unreachable host) fails on
        // the first attempt with its own message, because retrying those would just delay a clear
        // answer three times over.
        let mut attempt = 0;
        let catalog = loop {
            let result = SqlCatalogBuilder::default()
                .with_storage_factory(Arc::clone(&storage))
                .uri(catalog_uri)
                .warehouse_location(warehouse_uri.clone())
                .sql_bind_style(sql_bind_style_for(catalog_uri)?)
                .load(catalog_name, HashMap::new())
                .await;
            match result {
                Ok(catalog) => break catalog,
                Err(err)
                    if attempt < CATALOG_BOOTSTRAP_ATTEMPTS && is_concurrent_bootstrap(&err) =>
                {
                    attempt += 1;
                    tracing::debug!(
                        catalog = catalog_name,
                        attempt,
                        "another process was bootstrapping the catalog schema; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                }
                // The URI is redacted for the same reason every services-DB message redacts it: a
                // connection failure is the error most likely to be pasted into a ticket.
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "opening sql catalog `{catalog_name}` at {} (warehouse {warehouse_uri})",
                            redact_url(catalog_uri)
                        )
                    });
                }
            }
        };
        Ok(Self {
            catalog: Arc::new(catalog),
            catalog_name: catalog_name.to_string(),
            commit_point: Some(CatalogCommitPoint {
                uri: catalog_uri.to_string(),
                pool: OnceCell::new(),
            }),
        })
    }

    /// Open a catalog for `backend`. Iceberg backends need a `warehouse` (path or URI).
    ///
    /// A `sql` backend with no `uri` falls back to the fleet's services database
    /// (`LLDB_METADATA_*`) — see [`CatalogBackend::Sql`] for why a manifest should not carry a
    /// password. `rest` is still unimplemented and says so.
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
            CatalogBackend::Sql { uri } => {
                let warehouse = warehouse
                    .context("sql catalog requires a `warehouse` (path or file:// URI)")?;
                let uri = resolve_sql_catalog_uri(uri.as_deref())?;
                Self::open_sql(catalog_name, &uri, warehouse).await
            }
            CatalogBackend::Rest { .. } => bail!(
                "rest catalog backend is not implemented — use `{{ kind = \"sql\" }}` for a \
                 catalog shared across processes"
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
            // A MemoryCatalog keeps its table pointers behind a private mutex with no public
            // way to swap one, and in any case it is per-process — there is nothing for two
            // writers to race over and nothing to arbitrate the race if there were. DML says so
            // rather than pretending; see `crate::dml`.
            commit_point: None,
        })
    }

    /// The DataFusion catalog name this lakehouse registers under.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// The compare-and-swap this catalog commits through, if it has one.
    pub(crate) fn commit_point(&self) -> Option<&CatalogCommitPoint> {
        self.commit_point.as_ref()
    }

    /// Create the namespace if it does not already exist.
    ///
    /// The check-then-create is not atomic, and against a *shared* catalog two processes
    /// applying the same manifest can both pass the check. `NamespaceAlreadyExists` from the
    /// loser of that race is the desired end state reached by another route, so it is absorbed
    /// rather than propagated.
    pub async fn ensure_namespace(&self, ns: &NamespaceIdent) -> Result<()> {
        if self.catalog.namespace_exists(ns).await? {
            return Ok(());
        }
        match self.catalog.create_namespace(ns, HashMap::new()).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
            Err(e) => Err(e).with_context(|| format!("creating namespace {}", ns.join("."))),
        }
    }

    /// Whether `ns.table` is present in this catalog.
    pub async fn table_exists(&self, ns: &str, table: &str) -> Result<bool> {
        let tid = TableIdent::new(NamespaceIdent::new(ns.to_string()), table.to_string());
        self.catalog
            .table_exists(&tid)
            .await
            .with_context(|| format!("checking whether {ns}.{table} exists"))
    }

    /// Create an Iceberg table in `ns` from an Arrow schema. Field IDs are auto-assigned
    /// (Arrow schemas from parquet carry none), matching how the loader has always worked.
    ///
    /// Fails if the table already exists — see [`Self::ensure_table_from_arrow`] for the
    /// declarative form a manifest wants.
    pub async fn create_table_from_arrow(
        &self,
        ns: &NamespaceIdent,
        name: &str,
        arrow_schema: &ArrowSchema,
    ) -> Result<()> {
        let creation = table_creation(name, arrow_schema)?;
        self.catalog
            .create_table(ns, creation)
            .await
            .with_context(|| format!("creating iceberg table {name}"))?;
        Ok(())
    }

    /// Create `ns.name` if the catalog does not already have it. Returns whether *this call*
    /// created it.
    ///
    /// A memory catalog starts empty every time, so this always creates. A persistent SQL
    /// catalog does not: the second process to apply a manifest — or the same process on the
    /// next restart — finds the tables already there. "Already exists" is the manifest being
    /// satisfied, not a failure, so applying a manifest twice must be a no-op rather than an
    /// error. The returned flag is what tells [`crate::catalog::apply_manifest`] whether to run
    /// the seeding `INSERT`, so a restart does not append the source data a second time.
    pub async fn ensure_table_from_arrow(
        &self,
        ns: &NamespaceIdent,
        name: &str,
        arrow_schema: &ArrowSchema,
    ) -> Result<bool> {
        if self.table_exists(&ns.join("."), name).await? {
            return Ok(false);
        }
        let creation = table_creation(name, arrow_schema)?;
        match self.catalog.create_table(ns, creation).await {
            Ok(_) => Ok(true),
            // Lost the race with another process applying the same manifest. The table exists,
            // which is all that was wanted — and *they* own the seeding, hence `false`.
            Err(e) if e.kind() == ErrorKind::TableAlreadyExists => Ok(false),
            Err(e) => Err(e).with_context(|| format!("creating iceberg table {name}")),
        }
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

/// Build the [`TableCreation`] for an Arrow schema, auto-assigning field IDs (Arrow schemas
/// read from parquet carry none).
fn table_creation(name: &str, arrow_schema: &ArrowSchema) -> Result<TableCreation> {
    let ice_schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .with_context(|| format!("converting arrow schema for {name}"))?;
    Ok(TableCreation::builder()
        .name(name.to_string())
        .schema(ice_schema)
        .properties(HashMap::new())
        .build())
}

/// How many times to re-attempt a catalog open that lost the `CREATE TABLE IF NOT EXISTS` race.
///
/// Small on purpose: the race is resolved the instant the winner commits, so one retry almost
/// always suffices and a large budget would only slow down a genuine failure.
const CATALOG_BOOTSTRAP_ATTEMPTS: u32 = 3;

/// True if this error is the non-atomic `CREATE TABLE IF NOT EXISTS` race described in
/// [`Lakehouse::open_sql`], rather than something worth surfacing.
///
/// Matched on the message because that is the only thing available: the failure arrives as an
/// `iceberg::Error` that has already rendered its sqlx cause to a string, so there is no SQLSTATE
/// left to inspect. The trade is acceptable here in a way it would not be for classifying
/// behaviour permanently — the operation being retried is idempotent, the budget is three, and the
/// worst case of a missed match is the same error the caller would have seen anyway.
fn is_concurrent_bootstrap(err: &iceberg::Error) -> bool {
    let message = err.to_string();
    message.contains("duplicate key value violates unique constraint")
        && (message.contains("pg_type") || message.contains("pg_class"))
}

/// The scheme of a URI, lowercased — `postgres://a/b` → `postgres`, `sqlite:x.db` → `sqlite`,
/// `/tmp/wh` → `None`. Split on `:` rather than `://` because SQLite URIs have no authority.
fn uri_scheme(uri: &str) -> Option<String> {
    let (scheme, _) = uri.split_once(':')?;
    // RFC 3986: a scheme starts with a letter, then letters, digits, `+`, `-` or `.`. Two extra
    // restrictions on top of that, both deliberate:
    //
    // - **At least two characters.** A single letter before a colon is a Windows drive
    //   (`C:\warehouse`), not a scheme. RFC 3986 would permit a one-letter scheme, but none is in
    //   use and every scheme this function must recognise is far longer, so reading `C:` as a path
    //   is correct every time it matters. Getting this wrong is not cosmetic: `C:\warehouse` would
    //   be read as scheme `c` and rejected with "iceberg 0.10 provides no StorageFactory for
    //   `c://`" — a baffling message for what is simply a local path.
    // - **A leading digit disqualifies it**, so a path fragment like `2024:snapshot` stays a path.
    let mut chars = scheme.chars();
    if scheme.len() < 2 || !chars.next().is_some_and(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
        return None;
    }
    Some(scheme.to_ascii_lowercase())
}

/// Which placeholder syntax the catalog must emit for this database.
///
/// `iceberg-catalog-sql` goes through sqlx's `Any` driver, which does **not** rewrite
/// placeholders — it hands the SQL to the backend verbatim. So this is not a preference: get it
/// wrong and Postgres rejects every statement the catalog issues (`?` is not a Postgres
/// parameter). Derived from the scheme rather than hardcoded so the mapping stays a property of
/// the dialect.
fn sql_bind_style_for(uri: &str) -> Result<SqlBindStyle> {
    let scheme = uri_scheme(uri)
        .with_context(|| format!("sql catalog uri `{}` has no scheme", redact_url(uri)))?;
    match scheme.as_str() {
        "postgres" | "postgresql" => Ok(SqlBindStyle::DollarNumeric),
        "sqlite" | "mysql" | "mariadb" => Ok(SqlBindStyle::QMark),
        other => bail!(
            "sql catalog uri scheme `{other}` is not a known SQL dialect \
             (expected postgres, postgresql, sqlite, mysql or mariadb)"
        ),
    }
}

/// Reject a dialect this build cannot actually connect to.
///
/// `install_default_drivers()` can only register drivers compiled into sqlx, and this workspace
/// compiles `postgres` alone (issue #14 made Postgres *the* backend, and a second driver is a
/// second dependency tree under a version cap that has no room in it). Without this check a
/// `sqlite://` URI would fail deep inside sqlx with "no database driver found", which tells an
/// operator nothing about the fix.
fn ensure_sql_driver_compiled(uri: &str) -> Result<()> {
    let scheme = uri_scheme(uri).unwrap_or_default();
    if matches!(scheme.as_str(), "postgres" | "postgresql") {
        return Ok(());
    }
    bail!(
        "sql catalog uri scheme `{scheme}` needs an sqlx driver this build does not compile in \
         — the services database is Postgres, so the catalog is too; use a `postgres://` uri"
    )
}

/// Pick the Iceberg [`StorageFactory`] that can actually read and write `warehouse_uri`.
///
/// Only the local filesystem qualifies in Iceberg 0.10 (see the module docs). The error for
/// everything else is deliberately loud: `LocalFsStorage` would *accept* an `s3://` path and
/// turn it into a local directory named `s3:/bucket/...`, publishing table metadata to a disk
/// no other process can read while the catalog rows insist it is in the bucket.
fn storage_factory_for(warehouse_uri: &str) -> Result<Arc<dyn StorageFactory>> {
    match uri_scheme(warehouse_uri).as_deref() {
        // `None` means a bare path, which `normalize_warehouse_uri` has already made `file://`.
        Some("file") | None => Ok(Arc::new(LocalFsStorageFactory)),
        Some(other) => bail!(
            "warehouse `{warehouse_uri}`: iceberg 0.10 provides no StorageFactory for \
             `{other}://` — it ships only LocalFsStorageFactory and MemoryStorageFactory. \
             Falling back to local disk would write table metadata where no other process can \
             read it, which is the exact failure a shared catalog exists to prevent, so this is \
             an error instead. Use a `file://` warehouse on storage every process can reach \
             until an object-store StorageFactory is available."
        ),
    }
}

/// Resolve the catalog's database URI: the manifest's if it has one, otherwise the fleet's
/// services database from `LLDB_METADATA_*`.
///
/// The fallback is the point of the optional `uri` — a manifest is committed config and must
/// not carry a password, while the control-plane credentials are already in every process's
/// environment. See [`CatalogBackend::Sql`].
fn resolve_sql_catalog_uri(manifest_uri: Option<&str>) -> Result<String> {
    if let Some(uri) = manifest_uri.map(str::trim).filter(|u| !u.is_empty()) {
        return Ok(uri.to_string());
    }
    ServicesArgs::from_env()?.resolve_url()?.context(
        "sql catalog has no `uri` and no services database is configured — set the manifest's \
         `backend.uri`, or point the process at the control plane with --metadata-url / \
         --metadata-host (LLDB_METADATA_URL / LLDB_METADATA_HOST)",
    )
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

    #[test]
    fn uri_scheme_stops_at_the_first_colon() {
        assert_eq!(uri_scheme("postgres://u@h/db").as_deref(), Some("postgres"));
        // SQLite URIs have no authority, so splitting on `://` would miss the scheme entirely.
        assert_eq!(uri_scheme("sqlite:catalog.db").as_deref(), Some("sqlite"));
        assert_eq!(
            uri_scheme("POSTGRESQL://h/db").as_deref(),
            Some("postgresql")
        );
        assert_eq!(uri_scheme("/tmp/warehouse"), None);
        assert_eq!(uri_scheme("no-colon-here"), None);
        // A path that merely contains a colon is not a scheme.
        assert_eq!(uri_scheme("/tmp/odd:name"), None);
        // A Windows drive letter is a path, not a one-letter scheme. Left unhandled this reads as
        // scheme `c` and the warehouse is rejected for having no `c://` StorageFactory.
        assert_eq!(uri_scheme(r"C:\warehouse"), None);
        assert_eq!(uri_scheme("c:/warehouse"), None);
        // A scheme starts with a letter, so a leading digit keeps it a path.
        assert_eq!(uri_scheme("2024:snapshot"), None);
        // …but the RFC-legal punctuation still parses.
        assert_eq!(
            uri_scheme("postgres+ssl://h/db").as_deref(),
            Some("postgres+ssl")
        );
    }

    #[test]
    fn bind_style_follows_the_dialect() -> Result<()> {
        // Postgres speaks `$1`; sqlx's Any driver passes placeholders through untouched, so the
        // wrong choice here breaks every statement the catalog issues.
        assert_eq!(
            sql_bind_style_for("postgres://lldb@db/lldb")?,
            SqlBindStyle::DollarNumeric
        );
        assert_eq!(
            sql_bind_style_for("postgresql://lldb:pw@db:5432/lldb?sslmode=disable")?,
            SqlBindStyle::DollarNumeric
        );
        assert_eq!(
            sql_bind_style_for("sqlite:catalog.db")?,
            SqlBindStyle::QMark
        );
        assert_eq!(sql_bind_style_for("mysql://h/db")?, SqlBindStyle::QMark);
        // Unknown dialect and no dialect both have to say so rather than guess.
        assert!(sql_bind_style_for("mongodb://h/db").is_err());
        assert!(sql_bind_style_for("just-a-string").is_err());
        Ok(())
    }

    #[test]
    fn only_postgres_has_a_compiled_driver() {
        assert!(ensure_sql_driver_compiled("postgres://lldb@db/lldb").is_ok());
        assert!(ensure_sql_driver_compiled("postgresql://lldb@db/lldb").is_ok());
        // A dialect with a correct bind style is still unusable without its sqlx driver, and
        // the error must name the reason rather than leave sqlx to say "no driver found".
        let err = ensure_sql_driver_compiled("sqlite:catalog.db")
            .expect_err("sqlite has no driver in this build")
            .to_string();
        assert!(err.contains("sqlite"), "{err}");
        assert!(err.contains("postgres://"), "{err}");
    }

    #[test]
    fn storage_factory_takes_local_paths_and_refuses_object_stores() {
        assert!(storage_factory_for("file:///tmp/wh").is_ok());
        assert!(storage_factory_for("/tmp/wh").is_ok());

        // The load-bearing case: `LocalFsStorage` would silently accept this and write to a
        // local directory named `s3:/bucket/wh`, so the fallback must not exist.
        let err = storage_factory_for("s3://bucket/wh")
            .expect_err("iceberg 0.10 has no S3 StorageFactory")
            .to_string();
        assert!(err.contains("s3://"), "{err}");
        assert!(err.contains("StorageFactory"), "{err}");
        assert!(storage_factory_for("gs://bucket/wh").is_err());
        assert!(storage_factory_for("memory://wh").is_err());
    }

    #[test]
    fn explicit_catalog_uri_wins_over_the_environment() -> Result<()> {
        assert_eq!(
            resolve_sql_catalog_uri(Some("postgres://lldb@db/lldb"))?,
            "postgres://lldb@db/lldb"
        );
        // Blank reads as absent, and then it depends on the environment — which this test does
        // not mutate (`set_var` is unsafe and would race the rest of the binary), so it only
        // asserts the branch it can: with a services DB configured the fallback must find it.
        if ServicesArgs::from_env()?.resolve_url()?.is_none() {
            assert!(resolve_sql_catalog_uri(None).is_err());
            assert!(resolve_sql_catalog_uri(Some("   ")).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn open_rejects_the_rest_backend_and_warehouseless_sql() {
        let rest = CatalogBackend::Rest {
            uri: "http://localhost:8181".to_string(),
        };
        assert!(Lakehouse::open("c", &rest, None).await.is_err());

        // A SQL catalog still needs somewhere to put table files.
        let sql = CatalogBackend::Sql {
            uri: Some("postgres://lldb@127.0.0.1:1/lldb".to_string()),
        };
        assert!(Lakehouse::open("c", &sql, None).await.is_err());

        // …and an object-store warehouse is refused before any connection is attempted, so this
        // asserts the message rather than a timeout against a dead port.
        let err = match Lakehouse::open("c", &sql, Some("s3://bucket/wh")).await {
            Ok(_) => panic!("an s3 warehouse must be rejected, not silently localized"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("StorageFactory"), "{err}");
    }
}
