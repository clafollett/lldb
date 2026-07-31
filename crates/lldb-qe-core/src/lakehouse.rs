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
//! `crates/lldb-qe-control/migrations/` — our migrations own the services schema (accounts, users,
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
use crate::tenancy::TenantScope;

/// An Iceberg catalog plus the glue to create tables and expose them to DataFusion.
///
/// Generic over the DataFusion catalog name; namespaces and tables are passed per call, so a
/// single `Lakehouse` can hold many namespaces.
///
/// # Two names, on purpose
///
/// A lakehouse answers to two catalog names and they are *not* interchangeable:
///
/// - [`Self::catalog_name`] is what SQL says — the name this catalog registers under in a
///   [`SessionContext`], the first segment of `catalog.namespace.table`, and what [`crate::rbac`]
///   grants are written against.
/// - [`Self::iceberg_catalog_name`] is what the *storage* says — the value in
///   `iceberg_tables.catalog_name`, which is that table's leading primary-key column and the
///   discriminator every statement `iceberg-catalog-sql` issues filters on.
///
/// They are equal for an untenanted catalog and differ under a [`TenantScope`], which is the whole
/// mechanism of per-tenant catalogs: each account's session registers *its* catalog under the
/// manifest's declared name, so `SELECT … FROM lldb.sales.orders` stays portable across tenants
/// while resolving to a different set of rows and a different directory for each one. Confusing
/// the two is the bug this doc comment exists to prevent — using the SQL name to commit would put
/// two tenants back in one catalog, and using the storage name in SQL would make every query name
/// its own account id.
pub struct Lakehouse {
    catalog: Arc<dyn Catalog>,
    catalog_name: String,
    iceberg_catalog_name: String,
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
    ///
    /// Untenanted: a memory catalog is per-process and dies with it, so there is nothing for two
    /// tenants to share and nothing to partition. The tenant-scoped path is [`Self::open`], which
    /// is what a manifest goes through.
    pub async fn open_memory(catalog_name: &str, warehouse: &Path) -> Result<Self> {
        let warehouse_uri = format!("file://{}", warehouse.display());
        Self::open_memory_uri(catalog_name, &TenantScope::untenanted(), &warehouse_uri).await
    }

    /// Open a persistent SQL catalog: catalog metadata in `catalog_uri`'s database, table files
    /// under `warehouse` (a path or `file://` URI), both partitioned by `scope`.
    ///
    /// `catalog_name` is not decoration — `scope` turns it into a *column* value in
    /// `iceberg_tables`, so it is the key two processes must agree on to be looking at the same
    /// catalog. Same name + same scope + same URI = same tables; a mismatch in any of them
    /// silently yields an empty catalog rather than an error, which is worth knowing when a worker
    /// reports a table that "does not exist".
    ///
    /// `scope` moves the warehouse root as well as the name, and it has to:
    /// `iceberg-catalog-sql` builds a table's location as `{warehouse}/{namespace}/{table}` with no
    /// catalog name in it, so two tenanted catalogs over one warehouse root would separate their
    /// rows and then collide on disk. See [`TenantScope`].
    pub async fn open_sql(
        catalog_name: &str,
        scope: &TenantScope,
        catalog_uri: &str,
        warehouse: &str,
    ) -> Result<Self> {
        let iceberg_catalog_name = scope.iceberg_catalog_name(catalog_name);
        let warehouse_uri = scope.warehouse_uri(&normalize_warehouse_uri(warehouse));
        // All validated up front so a bad configuration fails before any connection attempt, with
        // a message about the configuration rather than one raised from inside a dependency.
        ensure_catalog_name_fits(catalog_name, scope, &iceberg_catalog_name)?;
        // `sql_bind_style_for` is recomputed per attempt below because `SqlBindStyle` is neither
        // `Clone` nor `Copy` and the function is a cheap pure mapping over the scheme.
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
                .load(&iceberg_catalog_name, catalog_properties())
                .await;
            match result {
                Ok(catalog) => break catalog,
                Err(err)
                    if attempt < CATALOG_BOOTSTRAP_ATTEMPTS && is_concurrent_bootstrap(&err) =>
                {
                    attempt += 1;
                    tracing::debug!(
                        catalog = iceberg_catalog_name,
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
                            "opening sql catalog `{iceberg_catalog_name}` at {} (warehouse \
                             {warehouse_uri})",
                            redact_url(catalog_uri)
                        )
                    });
                }
            }
        };
        Ok(Self {
            catalog: Arc::new(catalog),
            catalog_name: catalog_name.to_string(),
            iceberg_catalog_name,
            commit_point: Some(CatalogCommitPoint {
                uri: catalog_uri.to_string(),
                pool: OnceCell::new(),
            }),
        })
    }

    /// Open a catalog for `backend`, partitioned by `scope`. Iceberg backends need a `warehouse`
    /// (path or URI).
    ///
    /// A `sql` backend with no `uri` falls back to the fleet's services database
    /// (`LLDB_METADATA_*`) — see [`CatalogBackend::Sql`] for why a manifest should not carry a
    /// password. `rest` is still unimplemented and says so.
    pub async fn open(
        catalog_name: &str,
        scope: &TenantScope,
        backend: &CatalogBackend,
        warehouse: Option<&str>,
    ) -> Result<Self> {
        match backend {
            CatalogBackend::Memory => {
                let warehouse = warehouse
                    .context("memory catalog requires a `warehouse` (path or file:// URI)")?;
                Self::open_memory_uri(catalog_name, scope, &normalize_warehouse_uri(warehouse))
                    .await
            }
            CatalogBackend::Sql { uri } => {
                let warehouse = warehouse
                    .context("sql catalog requires a `warehouse` (path or file:// URI)")?;
                let uri = resolve_sql_catalog_uri(uri.as_deref())?;
                Self::open_sql(catalog_name, scope, &uri, warehouse).await
            }
            CatalogBackend::Rest { .. } => bail!(
                "rest catalog backend is not implemented — use `{{ kind = \"sql\" }}` for a \
                 catalog shared across processes"
            ),
        }
    }

    async fn open_memory_uri(
        catalog_name: &str,
        scope: &TenantScope,
        warehouse_uri: &str,
    ) -> Result<Self> {
        // A memory catalog is per-process, so its *name* needs no partitioning — two tenants in
        // one process already hold two independent `MemoryCatalog`s. The warehouse still does:
        // both of those catalogs write real files, and unscoped they would write them to the same
        // directory. Scoping only what actually collides keeps the dev default's on-disk layout
        // recognizable.
        let catalog = MemoryCatalogBuilder::default()
            // Without a storage factory the "files" would live in RAM; we want them on disk.
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                catalog_name,
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    scope.warehouse_uri(warehouse_uri),
                )]),
            )
            .await
            .context("building MemoryCatalog")?;
        Ok(Self {
            catalog: Arc::new(catalog),
            catalog_name: catalog_name.to_string(),
            iceberg_catalog_name: catalog_name.to_string(),
            // A MemoryCatalog keeps its table pointers behind a private mutex with no public
            // way to swap one, and in any case it is per-process — there is nothing for two
            // writers to race over and nothing to arbitrate the race if there were. DML says so
            // rather than pretending; see `crate::dml`.
            commit_point: None,
        })
    }

    /// The DataFusion catalog name this lakehouse registers under — what SQL says.
    ///
    /// The same for every tenant, by design. See the type's docs for why there are two names.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// The value in `iceberg_tables.catalog_name` — what the storage says, and the column that
    /// keeps one tenant's rows out of another's.
    ///
    /// Equal to [`Self::catalog_name`] for an untenanted catalog. [`crate::dml`] commits against
    /// *this* one, because its `UPDATE` is a statement about a row, not about a query.
    pub fn iceberg_catalog_name(&self) -> &str {
        &self.iceberg_catalog_name
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

/// How large `SqlCatalog`'s private connection pool may grow, per catalog, per process.
///
/// **Four, not the crate's default of ten, and the reason is per-tenant catalogs.** Before
/// [`TenantScope`] existed a process held one SQL catalog, so `SqlCatalog`'s undersized-by-nobody
/// default of 10 connections (plus [`CatalogCommitPoint`]'s lazy 2) cost at most 12 sockets and was
/// not worth an opinion. A catalog per tenant multiplies that by the number of tenants *and* by
/// every coordinator and worker process in the fleet, which turns an unremarkable default into the
/// dominant running cost of this design — and into a way for one busy deployment to exhaust
/// Postgres's `max_connections` on catalog handles that are almost always idle.
///
/// Four is the per-coordinator query concurrency
/// ([`DEFAULT_MAX_CONCURRENT_QUERIES`](crate::scheduler::DEFAULT_MAX_CONCURRENT_QUERIES)), which is
/// the real bound on how many catalog statements one process can have in flight at once: catalog
/// work is short metadata reads and a pointer swap, never the scan. The pool is also a *ceiling*
/// rather than a resident count — `iceberg-catalog-sql` sets a 10 s idle timeout, so an idle
/// tenant's connections are returned rather than held.
const CATALOG_POOL_MAX_CONNECTIONS: u32 = 4;

/// Properties handed to `SqlCatalogBuilder::load`.
///
/// `iceberg-catalog-sql` reads its pool settings out of this map and `parse().unwrap()`s them, so
/// the value must be a plain integer — which is why it is rendered from a typed constant rather
/// than written as a literal string. Unrecognized keys are forwarded to the `FileIO` and ignored
/// there, so this map is not a place to put anything storage-facing.
fn catalog_properties() -> HashMap<String, String> {
    HashMap::from([(
        "pool.max-connections".to_string(),
        CATALOG_POOL_MAX_CONNECTIONS.to_string(),
    )])
}

/// How many times to re-attempt a catalog open that lost the `CREATE TABLE IF NOT EXISTS` race.
///
/// Small on purpose: the race is resolved the instant the winner commits, so one retry almost
/// always suffices and a large budget would only slow down a genuine failure.
const CATALOG_BOOTSTRAP_ATTEMPTS: u32 = 3;

/// The width of `iceberg_tables.catalog_name`, which `iceberg-catalog-sql` declares as
/// `VARCHAR(255) NOT NULL` and we do not own — the table is created by that crate, so this is a
/// fact about a dependency's schema rather than a policy of ours.
const CATALOG_NAME_LIMIT: usize = 255;

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
///
/// Crate-visible because [`crate::iceberg_scan`] asks the same question of an Iceberg data-file
/// path, and "is this a URI or a path" must be one ruling, not two — in particular the Windows
/// drive-letter case below, which a second implementation would get wrong.
pub(crate) fn uri_scheme(uri: &str) -> Option<String> {
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

/// Reject a scoped catalog name that will not fit `iceberg_tables.catalog_name`.
///
/// `iceberg-catalog-sql` declares that column `VARCHAR(255) NOT NULL`, and [`TenantScope`] prepends
/// `acct_<id>__` to whatever the manifest declared — so a name that is legal untenanted can be over
/// the limit once scoped. Without this the manifest is accepted, the tenanted open reaches Postgres,
/// and the operator gets a value-too-long error raised from inside a dependency that names neither
/// the tenant nor the string that was actually too long. The whole point of checking here is to say
/// the computed name out loud.
///
/// Alongside the other pre-connection validators for the same reason they are: a configuration this
/// build cannot honour should fail before a socket is opened, with a message about the
/// configuration.
///
/// Only the SQL path needs it. A `MemoryCatalog` writes no `iceberg_tables` row — it holds its
/// tables in a per-process map with no column widths at all — and [`Lakehouse::open_memory_uri`]
/// does not scope the catalog name in the first place, so there is no limit there to overrun and a
/// check would be false precision.
fn ensure_catalog_name_fits(declared: &str, scope: &TenantScope, scoped: &str) -> Result<()> {
    // Characters, not bytes: Postgres `VARCHAR(n)` bounds character length, so `len()` would refuse
    // a multi-byte name that actually fits.
    let length = scoped.chars().count();
    if length > CATALOG_NAME_LIMIT {
        bail!(
            "catalog name `{scoped}` is {length} characters; the limit is {CATALOG_NAME_LIMIT} \
             (`iceberg_tables.catalog_name` is VARCHAR(255)). It is the manifest's catalog \
             `{declared}` scoped to tenant `{scope}`; shorten the declared name by at least {} \
             characters.",
            length - CATALOG_NAME_LIMIT
        );
    }
    Ok(())
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
        // Untenanted, so SQL's name and the storage's name are the same word.
        assert_eq!(lake.iceberg_catalog_name(), "lldb");
        Ok(())
    }

    #[tokio::test]
    async fn a_tenanted_memory_catalog_keeps_its_files_to_itself() -> Result<()> {
        // Two accounts, one manifest, one warehouse root, the same qualified table name. The
        // catalogs are separate for free (a memory catalog is per-process); what has to be checked
        // is that they do not write over each other, since `iceberg-catalog-sql`'s layout — which
        // the memory catalog shares — puts no catalog name in the path.
        let warehouse = tempfile::tempdir()?;
        let uri = format!("file://{}", warehouse.path().display());
        let ns = NamespaceIdent::new("sales".to_string());
        let schema = ArrowSchema::new(vec![datafusion::arrow::datatypes::Field::new(
            "id",
            datafusion::arrow::datatypes::DataType::Int64,
            false,
        )]);

        for id in [1i64, 2] {
            let lake = Lakehouse::open_memory_uri("lldb", &TenantScope::account(id), &uri).await?;
            lake.ensure_namespace(&ns).await?;
            assert!(lake.ensure_table_from_arrow(&ns, "orders", &schema).await?);
            // Same SQL name for both tenants — that is the point of the two-name split.
            assert_eq!(lake.catalog_name(), "lldb");
            let table = lake.load_table("sales", "orders").await?;
            assert!(
                table.metadata().location().contains(&format!("acct_{id}")),
                "a tenant's table must live under its own warehouse root: {}",
                table.metadata().location()
            );
        }
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
    fn a_catalog_name_that_only_overruns_once_scoped_is_refused_by_its_computed_value() {
        // 250 characters: comfortably inside `VARCHAR(255)` as the manifest declares it, and
        // therefore a manifest that worked before per-tenant catalogs existed.
        let declared = "c".repeat(250);
        assert!(
            ensure_catalog_name_fits(
                &declared,
                &TenantScope::untenanted(),
                &TenantScope::untenanted().iceberg_catalog_name(&declared),
            )
            .is_ok(),
            "unscoped, this name fits — the refusal below must be about the prefix"
        );

        let scope = TenantScope::account(7);
        let scoped = scope.iceberg_catalog_name(&declared);
        let err = ensure_catalog_name_fits(&declared, &scope, &scoped)
            .expect_err("`acct_7__` + 250 characters is over the column width")
            .to_string();

        // "Too long" alone reproduces the problem this exists to fix: the operator cannot see what
        // was computed. So the message has to carry the computed name, the tenant, the declared
        // name, the actual length and the limit.
        assert!(err.contains(&scoped), "{err}");
        assert!(err.contains("acct_7"), "{err}");
        assert!(err.contains(&declared), "{err}");
        assert!(
            err.contains(&format!("{} characters", scoped.chars().count())),
            "{err}"
        );
        assert!(err.contains("255"), "{err}");
    }

    #[test]
    fn a_catalog_name_is_measured_in_characters_not_bytes() {
        // Postgres bounds `VARCHAR(n)` by characters. Measuring bytes would refuse this name, which
        // is 255 characters and fits, on the strength of its encoding.
        let declared = "é".repeat(255);
        assert_eq!(declared.len(), 510);
        assert!(ensure_catalog_name_fits(&declared, &TenantScope::untenanted(), &declared).is_ok());
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
    fn the_catalog_pool_is_sized_rather_than_defaulted() {
        // Left empty, `iceberg-catalog-sql` applies its own default of 10 per catalog — which a
        // catalog per tenant multiplies by tenants and by processes. The key spelling is the
        // crate's, and it `parse().unwrap()`s the value, so both are pinned here.
        let props = catalog_properties();
        assert_eq!(
            props.get("pool.max-connections").map(String::as_str),
            Some("4")
        );
        assert!(
            props["pool.max-connections"].parse::<u32>().is_ok(),
            "iceberg-catalog-sql unwraps this parse; a non-integer would panic at open"
        );
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
        let scope = TenantScope::untenanted();
        let rest = CatalogBackend::Rest {
            uri: "http://localhost:8181".to_string(),
        };
        assert!(Lakehouse::open("c", &scope, &rest, None).await.is_err());

        // A SQL catalog still needs somewhere to put table files.
        let sql = CatalogBackend::Sql {
            uri: Some("postgres://lldb@127.0.0.1:1/lldb".to_string()),
        };
        assert!(Lakehouse::open("c", &scope, &sql, None).await.is_err());

        // …and an object-store warehouse is refused before any connection is attempted, so this
        // asserts the message rather than a timeout against a dead port.
        let err = match Lakehouse::open("c", &scope, &sql, Some("s3://bucket/wh")).await {
            Ok(_) => panic!("an s3 warehouse must be rejected, not silently localized"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("StorageFactory"), "{err}");
    }
}
