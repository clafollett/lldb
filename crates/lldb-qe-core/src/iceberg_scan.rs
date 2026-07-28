//! Resolving an Iceberg scan into the **files it was planned against**, on the coordinator.
//!
//! `iceberg-datafusion` plans a table read as an [`IcebergTableScan`], which is a DataFusion
//! *extension* node: it holds a live `iceberg::table::Table` — a catalog handle, an `iceberg::io`
//! FileIO, table metadata — and resolves its own files when it executes. That is a perfectly good
//! node to run in one process and an impossible one to ship anywhere, and it is why no Iceberg
//! query could be distributed at all before this module existed. Two independent walls:
//!
//! 1. **It cannot be serialized.** [`crate::remote::LldbCodec`] is the only
//!    [`PhysicalExtensionCodec`](datafusion_proto::physical_plan::PhysicalExtensionCodec) in the
//!    build and it encodes exactly one node, [`FlightReaderExec`](crate::remote::FlightReaderExec).
//!    So any plan containing an `IcebergTableScan` failed at the Flight boundary rather than
//!    running on a worker.
//! 2. **It cannot be sliced.** [`crate::scan_split::split_scan`] distributes *IO* by handing each
//!    worker disjoint byte ranges of a [`FileScanConfig`]. An `IcebergTableScan` is not one, so
//!    even a scan that somehow crossed the wire would have been read whole by every worker.
//!
//! # The move: resolve, don't encode
//!
//! The tempting fix is to teach the codec about `IcebergTableScan` — encode the table identifier
//! and the snapshot id, and let each worker load the table from the catalog and re-plan its files.
//! That is the wrong shape three times over. It would put a catalog connection (and a catalog
//! *credential*) in every worker; it would re-do manifest planning `n` times instead of once; and,
//! worst, it would make each worker resolve "which files are in this snapshot" independently, so a
//! concurrent commit could leave two workers of the same query reading two different tables. Naming
//! the snapshot id narrows that but does not close it — the file list is still recomputed remotely
//! from state the coordinator does not control.
//!
//! [`resolve_iceberg_scans`] instead rewrites each `IcebergTableScan`, **on the coordinator, before
//! anything is staged**, into a plain Parquet [`DataSourceExec`] over the concrete data files of the
//! snapshot the scan was planned against. One move, three problems:
//!
//! - `datafusion-proto` already knows how to encode a `ParquetSource` scan, so the plan is natively
//!   serializable with no codec change at all.
//! - It is a [`FileScanConfig`], so [`split_scan`](crate::scan_split::split_scan) slices it into
//!   byte ranges exactly as it does a listing table — Iceberg queries get scan-level distribution
//!   for free.
//! - The snapshot is pinned **by construction**. The file list *is* the snapshot; it travels inside
//!   the plan bytes. A worker cannot resolve "current" because it is never asked to, and needs no
//!   catalog access of any kind. [`scanned_data_files`] makes that assertable rather than assumed.
//!
//! # What is deliberately dropped, and why it is safe
//!
//! The Iceberg predicate is **not** carried onto the replacement node. `IcebergTableProvider`'s
//! `supports_filters_pushdown` returns `Inexact` for every filter, so DataFusion always leaves a
//! `FilterExec` above the scan and the rows are filtered there regardless. Manifest- and file-level
//! pruning still happens, because the predicate is handed to `TableScan::plan_files()` here — what
//! is lost is only parquet row-group pruning inside the files that survive that. An IO
//! optimization, not a correctness property.
//!
//! Related, and worth stating so nobody hunts for it: the replacement arrives *after* DataFusion's
//! physical optimizer has run, so no filter is pushed into it either — the `FilterExec` above it
//! and any dynamic filter a `TopK` sort maintains were both wired up against the Iceberg node. The
//! answer is the same; the parquet reader just does not get to prune with them. Re-running the
//! optimizer over the rewritten plan would fix that and is not attempted here, because it would
//! also be free to re-shape the plan out from under the partition-count invariant below.
//!
//! The limit is carried (`FileScanConfigBuilder::with_limit`) for the same reason and with the same
//! status: `IcebergTableScan` implements neither `fetch()` nor `with_fetch()`, so DataFusion's
//! `LimitPushdown` cannot have removed the `GlobalLimitExec` above it. Correctness comes from that
//! retained limit node; this is an IO hint that stops a worker reading a terabyte to answer a
//! `LIMIT 10`.
//!
//! # The one-partition rule
//!
//! `IcebergTableScan` reports `Partitioning::UnknownPartitioning(1)`, and every parent in the plan
//! carries [`PlanProperties`](datafusion::physical_plan::PlanProperties) the optimizer computed
//! against a one-partition child. `FileScanConfig::output_partitioning` reports
//! `UnknownPartitioning(file_groups.len())`. So the replacement uses **exactly one**
//! [`FileGroup`], however many files the snapshot has: a replacement claiming `n` partitions would
//! leave, say, a `CoalescePartitionsExec` above it still believing it has one input, which is a
//! wrong-answer bug rather than a slow one. Re-slicing that single group across the fleet is
//! [`split_scan`](crate::scan_split::split_scan)'s job and happens later, which is precisely the
//! path a listing table already takes.
//!
//! # Refusals
//!
//! A plain parquet read is not a complete Iceberg reader, and the gap between the two is where
//! silent wrong answers live. Every case below is **refused with an error naming the reason**
//! rather than approximated:
//!
//! - **Row-level deletes.** Position and equality delete files are applied by Iceberg's reader and
//!   by nothing else; reading the data files alone would resurrect deleted rows.
//! - **A non-Parquet data file.** Avro/ORC/Puffin are legal Iceberg data formats and
//!   `ParquetSource` cannot read them.
//! - **A partitioned table.** Identity-transformed partition columns live in manifest metadata, not
//!   in the data file, so a plain parquet read of a partitioned table can be missing columns
//!   entirely. iceberg-rust 0.10 does not populate `FileScanTask::partition_spec` on the native
//!   scan path (it is a hardcoded `None` with a `TODO`), so the honest check is against the table
//!   metadata's partition specs, which is what this does.
//! - **A partial byte range.** iceberg 0.10 always emits `start = 0, length = file_size_in_bytes`,
//!   so a task that says otherwise means the library changed under us — and silently reading the
//!   whole file when told to read a range duplicates rows.
//! - **Files spanning more than one object store.** A `FileScanConfig` names exactly one
//!   `ObjectStoreUrl`.
//! - **An object store this session cannot resolve.** Checked here so "the fleet cannot read this
//!   bucket" is a coordinator-side error naming the scheme instead of a failure deep inside a
//!   remote stage.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use object_store::path::Path as StorePath;
use url::Url;

use crate::lakehouse::uri_scheme;
use crate::scan_split::file_scan_config;

/// Rewrite every [`IcebergTableScan`] in `plan` into a Parquet scan over the data files of the
/// snapshot it was planned against.
///
/// Call this on the coordinator **before** the plan is staged, sliced or serialized — see the
/// module docs for why that ordering is the whole design. A plan with no Iceberg scan comes back as
/// the identical `Arc`, matching the convention [`crate::staging::plan_distributed`] uses, so a
/// caller can cheaply tell "nothing happened" from "something did".
///
/// The walk is hand-written rather than a [`TreeNode::transform`] because
/// `TableScan::plan_files()` is async and `transform` is not. It is post-order: children are
/// resolved first, then the node is rebuilt only if one of them actually moved.
///
/// A [`FlightReaderExec`](crate::remote::FlightReaderExec) reports no children, so a sub-plan that
/// has already been wrapped for a worker is naturally left alone — its scans are not this plan's IO
/// and, having been through here on the way in, are already resolved.
///
/// # Errors
/// Every refusal listed in the module docs, plus anything the Iceberg planner itself reports while
/// listing the snapshot's files.
pub async fn resolve_iceberg_scans(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    resolve_node(ctx, plan).await
}

/// The data files a resolved plan will read, in plan order, as fully-qualified locations
/// (`file:///warehouse/ns/t/data/x.parquet`, `s3://bucket/…`).
///
/// This is what turns "the snapshot travels with the plan" from a claim into an assertion: the list
/// a plan carries can be compared against the list the snapshot contains, before and after the plan
/// crosses the wire. It reads whatever file-scan leaves the plan has, so it also reports the files
/// of an ordinary listing table — the question it answers is "which bytes does this plan name",
/// which is the question worth asking of either.
///
/// Scans inside a [`FlightReaderExec`](crate::remote::FlightReaderExec) are invisible for the same
/// reason [`crate::scan_split::file_scan_count`] cannot see them: a remote stage is a leaf here.
pub fn scanned_data_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut files = Vec::new();
    plan.apply(|node| {
        if let Some(config) = file_scan_config(node) {
            let base = config.object_store_url.as_str();
            for group in &config.file_groups {
                for file in group.iter() {
                    files.push(format!("{base}{}", file.object_meta.location));
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("collecting scanned files does not error");
    files
}

/// Post-order rewrite of one node. Boxed because the recursion is async.
fn resolve_node<'a>(
    ctx: &'a SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        if let Some(scan) = plan.as_any().downcast_ref::<IcebergTableScan>() {
            return resolve_scan(ctx, scan).await;
        }

        // Cloned into owned handles so the borrow of `plan` ends before `with_new_children`
        // consumes it.
        let children: Vec<Arc<dyn ExecutionPlan>> =
            plan.children().into_iter().map(Arc::clone).collect();
        if children.is_empty() {
            return Ok(plan);
        }

        let mut resolved = Vec::with_capacity(children.len());
        let mut changed = false;
        for child in &children {
            let new_child = resolve_node(ctx, Arc::clone(child)).await?;
            changed |= !Arc::ptr_eq(&new_child, child);
            resolved.push(new_child);
        }
        if !changed {
            return Ok(plan);
        }
        let name = plan.name().to_string();
        plan.with_new_children(resolved)
            .map_err(|e| anyhow!("rebuilding {name} over its resolved children: {e}"))
    })
}

/// Turn one [`IcebergTableScan`] into a Parquet [`DataSourceExec`] over its snapshot's files.
async fn resolve_scan(
    ctx: &SessionContext,
    scan: &IcebergTableScan,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = scan.table();
    let ident = table.identifier().to_string();

    // Asked of the table metadata before anything is planned: a table whose *shape* we cannot read
    // is refused for free, rather than after walking every manifest in its snapshot to build a file
    // list that is going to be thrown away.
    reject_unreadable_table(&ident, table)?;

    // The same scan the node would have built for itself at execute time (see
    // `iceberg_datafusion::physical_plan::scan::get_batch_stream`), so the file list this produces
    // is the file list it would have read.
    let mut builder = match scan.snapshot_id() {
        Some(id) => table.scan().snapshot_id(id),
        None => table.scan(),
    };
    builder = match scan.projection() {
        Some(columns) => builder.select(columns.to_vec()),
        None => builder.select_all(),
    };
    if let Some(predicate) = scan.predicates() {
        // Kept for manifest/file pruning only — the rows are filtered by the `FilterExec` the
        // `Inexact` pushdown left above this node. See the module docs.
        builder = builder.with_filter(predicate.clone());
    }
    let table_scan = builder
        .build()
        .with_context(|| format!("building an iceberg scan for {ident}"))?;

    // The snapshot the *planner* resolved, not the one the SQL happened to name: with no explicit
    // `snapshot_id` this is whatever was current at plan time, and pinning it is the entire point.
    let snapshot_id = table_scan.snapshot().map(|s| s.snapshot_id());

    let tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .with_context(|| format!("planning the files of {ident}"))?
        .try_collect()
        .await
        .with_context(|| format!("listing the data files of {ident}"))?;

    let mut stores: BTreeSet<String> = BTreeSet::new();
    let mut files = Vec::with_capacity(tasks.len());
    for task in &tasks {
        reject_unreadable_task(&ident, task)?;
        let (store_url, location) = split_object_store_uri(&task.data_file_path)
            .with_context(|| format!("locating data file `{}` of {ident}", task.data_file_path))?;
        stores.insert(store_url.as_str().to_string());
        let mut file = PartitionedFile::new(location.as_ref(), task.file_size_in_bytes);
        // `PartitionedFile::new` runs the string through `Path::from`, which re-escapes anything
        // that already looks escaped. Assigning the parsed `Path` keeps a data file whose name
        // needed percent-encoding addressable.
        file.object_meta.location = location;
        files.push(file);
    }

    if stores.len() > 1 {
        bail!(
            "{ident}'s snapshot spans {} object stores ({}); a DataFusion file scan names exactly \
             one, so this table cannot be distributed. Keep a table's data files in one bucket, \
             or query it through a single-node session.",
            stores.len(),
            stores.iter().cloned().collect::<Vec<_>>().join(", ")
        );
    }

    // With no files there is no file to derive a store from, so fall back to the table's own
    // location — same scheme and authority, and it keeps the empty case going through the same
    // "can this session reach that store" check as every other case.
    let store_url = match stores.iter().next() {
        Some(url) => ObjectStoreUrl::parse(url).map_err(|e| anyhow!("{e}"))?,
        None => {
            split_object_store_uri(table.metadata().location())
                .with_context(|| {
                    format!(
                        "locating the warehouse of {ident} at `{}`",
                        table.metadata().location()
                    )
                })?
                .0
        }
    };

    // Resolved here, on the coordinator, so an unreachable or unregistered store is an error that
    // names the scheme rather than a failure inside a remote stage on some worker.
    ctx.runtime_env()
        .object_store(&store_url)
        .with_context(|| {
            format!(
                "{ident}'s data files live in `{store_url}`, which this session has no object \
                 store for — register the backend (--storage s3 / --storage local) that serves it"
            )
        })?;

    tracing::info!(
        table = %ident,
        snapshot_id = ?snapshot_id,
        data_files = files.len(),
        object_store = %store_url,
        "pinned an iceberg scan to its snapshot's data files for distribution"
    );

    let schema = scan.schema();
    let source = ParquetSource::new(Arc::clone(&schema));
    let config = FileScanConfigBuilder::new(store_url, Arc::new(source))
        // Exactly one group, always — see the module docs' one-partition rule.
        .with_file_group(FileGroup::new(files))
        .with_limit(scan.limit())
        .build();
    let replacement = DataSourceExec::from_data_source(config);

    // A schema that drifted here would be wrong everywhere above it, and quietly: parents were
    // planned against the Iceberg node's schema and would keep their column indices. Cheap to
    // check, so check.
    if replacement.schema() != schema {
        bail!(
            "internal: the parquet replacement for {ident} has schema {:?}, not the iceberg \
             scan's {:?}",
            replacement.schema(),
            schema
        );
    }
    Ok(replacement)
}

/// Refuse a table whose *shape* a plain parquet read cannot reproduce.
fn reject_unreadable_table(ident: &str, table: &Table) -> Result<()> {
    // iceberg-rust 0.10 hardcodes `FileScanTask::partition_spec` to `None` on the native scan path
    // ("TODO: Pass actual PartitionSpec through context chain"), so the per-file check the task
    // struct invites would never fire and a partitioned table would resolve to files that are
    // missing their identity-partition columns. Asking the table metadata instead is the check that
    // actually holds. It is deliberately conservative: a spec using only non-identity transforms
    // (bucket, truncate) does keep its source columns in the file and would in principle be
    // readable, but distinguishing the cases means reproducing Iceberg's constant-vs-column rule,
    // and refusing a query is recoverable in a way from answering it wrongly is not.
    if let Some(spec) = table
        .metadata()
        .partition_specs_iter()
        .find(|spec| !spec.is_unpartitioned())
    {
        bail!(
            "{ident} is partitioned (spec {} has {} field(s)); identity-partition columns live in \
             iceberg manifest metadata rather than in the data files, so distributing it as a \
             plain parquet scan could drop columns. Distributed reads of partitioned tables are \
             not supported yet — query this table on a single node.",
            spec.spec_id(),
            spec.fields().len()
        );
    }
    Ok(())
}

/// Refuse a scan task a plain parquet read would answer wrongly.
fn reject_unreadable_task(ident: &str, task: &FileScanTask) -> Result<()> {
    if !task.deletes.is_empty() {
        bail!(
            "{ident} has {} row-level delete file(s) attached to `{}`; iceberg applies position \
             and equality deletes in its own reader, so a plain parquet scan would resurrect \
             deleted rows. Compact the table (rewrite the data files) before distributing reads \
             of it.",
            task.deletes.len(),
            task.data_file_path
        );
    }
    if task.data_file_format != DataFileFormat::Parquet {
        bail!(
            "{ident}'s data file `{}` is {:?}, and the distributed reader is parquet-only. Rewrite \
             the table as parquet to distribute reads of it.",
            task.data_file_path,
            task.data_file_format
        );
    }
    if task.start != 0 || task.length != task.file_size_in_bytes {
        bail!(
            "{ident}'s scan task for `{}` covers bytes {}..{} of a {}-byte file. iceberg 0.10 \
             always plans whole files, so this means the library's task shape changed — and \
             reading the whole file anyway would duplicate rows. Refusing rather than guessing.",
            task.data_file_path,
            task.start,
            task.start.saturating_add(task.length),
            task.file_size_in_bytes
        );
    }
    Ok(())
}

/// Split a data-file URI into the object store that serves it and the path within that store.
///
/// Handles both a schemed URI (`file:///wh/ns/t/data/x.parquet`, `s3://bucket/wh/…`) and a bare
/// absolute path. The scheme test is [`uri_scheme`], deliberately: it already encodes this
/// codebase's ruling that a one-letter "scheme" is a Windows drive, and having two conventions for
/// "is this a URI" is how a Windows path ends up rejected for having no `c://` backend.
fn split_object_store_uri(uri: &str) -> Result<(ObjectStoreUrl, StorePath)> {
    let Some(_) = uri_scheme(uri) else {
        // A bare path is not percent-encoded, so `Path::from` — which escapes — is the right
        // constructor, and `from_url_path` would mangle a literal `%` in a filename.
        return Ok((ObjectStoreUrl::local_filesystem(), StorePath::from(uri)));
    };
    let url = Url::parse(uri).with_context(|| format!("`{uri}` is not a valid URI"))?;
    let base = format!("{}://{}", url.scheme(), url.authority());
    let store_url = ObjectStoreUrl::parse(&base)
        .map_err(|e| anyhow!("`{uri}` has no usable object-store authority: {e}"))?;
    let path = StorePath::from_url_path(url.path())
        .with_context(|| format!("`{uri}` has a path no object store can address"))?;
    Ok((store_url, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use iceberg::NamespaceIdent;

    use crate::lakehouse::Lakehouse;
    use crate::manifest::{
        CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef,
    };
    use crate::remote::{deserialize_plan, serialize_plan};
    use crate::scan_split::split_scan;
    use crate::storage::StorageConfig;
    use crate::tenancy::TenantScope;
    use crate::{TableSource, apply_manifest, build_session};

    const NS: &str = "sales";

    /// A single unpartitioned Iceberg table on an in-process memory catalog over `warehouse`.
    ///
    /// A memory catalog is enough — and is the point. The whole design is that a worker needs no
    /// catalog, so a test that proves the resolved plan carries its own files does not need a
    /// shared one either. No Postgres, no generated data.
    fn manifest(warehouse: &Path) -> Manifest {
        Manifest {
            catalogs: vec![CatalogDef {
                name: "lldb".to_string(),
                backend: CatalogBackend::Memory,
                warehouse: Some(format!("file://{}", warehouse.display())),
                namespaces: vec![NamespaceDef {
                    name: NS.to_string(),
                    tables: vec![TableDef {
                        name: "orders".to_string(),
                        format: Default::default(), // Iceberg
                        source: TableSource::Empty,
                        schema: Some(vec![
                            ColumnDef {
                                name: "id".to_string(),
                                data_type: "int64".to_string(),
                                nullable: false,
                            },
                            ColumnDef {
                                name: "label".to_string(),
                                data_type: "string".to_string(),
                                nullable: true,
                            },
                        ]),
                    }],
                }],
            }],
        }
    }

    /// A session with `lldb.sales.orders` created and `rows` rows appended, plus the lakehouse the
    /// manifest produced (so a test can ask what snapshot it is at).
    async fn seeded(warehouse: &Path, rows: usize) -> Result<(SessionContext, Lakehouse)> {
        let (ctx, storage) = build_session(StorageConfig::Local(warehouse.to_path_buf())).await?;
        let manifest = manifest(warehouse);
        let mut lakes =
            apply_manifest(&ctx, &storage, &manifest, &TenantScope::untenanted()).await?;
        if rows > 0 {
            let values = (0..rows)
                .map(|i| format!("({i}, 'row-{i}')"))
                .collect::<Vec<_>>()
                .join(", ");
            ctx.sql(&format!(
                "INSERT INTO lldb.{NS}.orders (id, label) VALUES {values}"
            ))
            .await?
            .collect()
            .await?;
        }
        Ok((ctx, lakes.remove(0)))
    }

    async fn physical(ctx: &SessionContext, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(ctx.sql(sql).await?.create_physical_plan().await?)
    }

    /// True if `plan` still contains an unresolved Iceberg node.
    fn has_iceberg_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let mut found = false;
        plan.apply(|node| {
            if node.as_any().downcast_ref::<IcebergTableScan>().is_some() {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    /// The wall this module exists to remove: before resolution the plan cannot be encoded at all,
    /// after it the same plan round-trips through the Flight codec.
    #[tokio::test]
    async fn an_iceberg_plan_becomes_serializable() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, _lake) = seeded(tmp.path(), 8).await?;
        let plan = physical(&ctx, &format!("SELECT id, label FROM lldb.{NS}.orders")).await?;

        // Bypassing this module is still an error — but one that names the step that was skipped
        // rather than reading as "Iceberg is unsupported". (`LldbCodec::try_encode`'s side of this
        // lives in `remote::encode_failure`; it is asserted here because building a real
        // `IcebergTableScan` needs a real catalog, which this module's tests already have.)
        let err = serialize_plan(Arc::clone(&plan))
            .expect_err("an IcebergTableScan has no encoding — that is the bug");
        let message = err.to_string();
        assert!(message.contains("IcebergTableScan"), "got: {message}");
        assert!(
            message.contains("resolve_iceberg_scans"),
            "the error must name the fix: {message}"
        );

        let resolved = resolve_iceberg_scans(&ctx, Arc::clone(&plan)).await?;
        assert!(!has_iceberg_scan(&resolved), "the scan must be replaced");

        let bytes = serialize_plan(Arc::clone(&resolved)).expect("a parquet scan encodes");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("and decodes");
        assert_eq!(back.schema(), plan.schema());
        assert_eq!(
            scanned_data_files(&back),
            scanned_data_files(&resolved),
            "the snapshot's file list must survive the wire — that is what pins it"
        );
        Ok(())
    }

    /// The replacement has to be indistinguishable to its parents: same schema, one partition.
    #[tokio::test]
    async fn the_replacement_keeps_the_schema_and_the_partition_count() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, _lake) = seeded(tmp.path(), 32).await?;
        // Three separate appends, so the snapshot has several data files and a naive one-group-per-
        // file replacement would report three partitions.
        for i in 0..2 {
            ctx.sql(&format!(
                "INSERT INTO lldb.{NS}.orders (id, label) VALUES ({i}, 'extra')"
            ))
            .await?
            .collect()
            .await?;
        }

        let plan = physical(&ctx, &format!("SELECT id, label FROM lldb.{NS}.orders")).await?;
        let scan = find_scan(&plan).expect("a bare projection leaves the iceberg scan visible");
        let resolved = resolve_iceberg_scans(&ctx, Arc::clone(&scan)).await?;

        assert_eq!(
            resolved.schema(),
            scan.schema(),
            "schemas must be identical"
        );
        assert_eq!(
            resolved.properties().partitioning.partition_count(),
            1,
            "parents were planned against a one-partition child"
        );
        assert!(
            scanned_data_files(&resolved).len() > 1,
            "test setup: several appends should leave several data files"
        );
        Ok(())
    }

    /// The claim in one assertion: the files the plan names are the files the snapshot contains.
    #[tokio::test]
    async fn the_resolved_files_are_the_snapshots_files() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, lake) = seeded(tmp.path(), 16).await?;
        let plan = physical(&ctx, &format!("SELECT id, label FROM lldb.{NS}.orders")).await?;
        let resolved = resolve_iceberg_scans(&ctx, plan).await?;

        let expected = snapshot_data_files(&lake).await?;
        let mut actual = scanned_data_files(&resolved);
        actual.sort();
        assert_eq!(actual, expected);
        assert!(!expected.is_empty(), "test setup: the insert wrote a file");
        Ok(())
    }

    /// A resolved Iceberg plan is now byte-range sliceable, and the slices cover it exactly once —
    /// the property `scan_split` guarantees for a listing table, inherited for free.
    #[tokio::test]
    async fn a_resolved_plan_slices_into_disjoint_ranges() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, _lake) = seeded(tmp.path(), 4096).await?;
        let plan = physical(&ctx, &format!("SELECT id, label FROM lldb.{NS}.orders")).await?;

        assert!(
            split_scan(Arc::clone(&plan), 4).is_err(),
            "an unresolved iceberg plan has no file-scan leaf to slice"
        );

        let resolved = resolve_iceberg_scans(&ctx, plan).await?;
        let whole = scanned_bytes(&resolved);
        let slices = split_scan(Arc::clone(&resolved), 4)?;
        assert_eq!(slices.len(), 4);
        assert_eq!(
            slices.iter().map(scanned_bytes).sum::<i64>(),
            whole,
            "the slices must cover the snapshot's bytes exactly once"
        );

        // …and the rows still add up, which is what "exactly once" is actually about.
        let mut rows = 0;
        for slice in &slices {
            rows += collect(Arc::clone(slice), ctx.task_ctx())
                .await?
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
        }
        assert_eq!(rows, 4096);
        Ok(())
    }

    /// A resolved plan answers the same question as the unresolved one. If it did not, everything
    /// above is bookkeeping over a wrong answer.
    #[tokio::test]
    async fn the_resolved_plan_returns_the_same_rows() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, _lake) = seeded(tmp.path(), 100).await?;
        for sql in [
            format!("SELECT count(*) FROM lldb.{NS}.orders"),
            format!("SELECT sum(id) FROM lldb.{NS}.orders WHERE id > 40"),
            format!("SELECT id FROM lldb.{NS}.orders ORDER BY id LIMIT 5"),
        ] {
            let direct = collect(physical(&ctx, &sql).await?, ctx.task_ctx()).await?;
            // A *fresh* physical plan for the second run, not the one just executed: a
            // `SortExec` in TopK mode carries a dynamic filter that execution narrows as it goes
            // (`filter=[id < 4]` after a `LIMIT 5`), and `with_new_children` carries that state
            // across. Re-running one plan object would compare a warm plan against a cold one and
            // blame this module for the difference.
            let resolved = resolve_iceberg_scans(&ctx, physical(&ctx, &sql).await?).await?;
            let through = collect(resolved, ctx.task_ctx()).await?;
            assert_eq!(
                format!(
                    "{}",
                    datafusion::arrow::util::pretty::pretty_format_batches(&through)?
                ),
                format!(
                    "{}",
                    datafusion::arrow::util::pretty::pretty_format_batches(&direct)?
                ),
                "`{sql}` must answer the same either way"
            );
        }
        Ok(())
    }

    /// A table with no snapshot still resolves — to one empty group, so the partition count the
    /// parents were planned against survives.
    #[tokio::test]
    async fn an_empty_table_resolves_to_one_empty_file_group() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (ctx, _lake) = seeded(tmp.path(), 0).await?;
        let plan = physical(&ctx, &format!("SELECT id, label FROM lldb.{NS}.orders")).await?;
        let resolved = resolve_iceberg_scans(&ctx, plan).await?;

        assert!(scanned_data_files(&resolved).is_empty());
        let scan = find_file_scan(&resolved).expect("still a file scan");
        assert_eq!(
            scan.properties().partitioning.partition_count(),
            1,
            "zero files is still one partition"
        );
        assert!(collect(resolved, ctx.task_ctx()).await?.is_empty());
        Ok(())
    }

    /// Nothing to do must cost nothing and change nothing — the same `Arc` back, which is how a
    /// caller distinguishes "no Iceberg here" from "resolved".
    #[tokio::test]
    async fn a_plan_with_no_iceberg_scan_is_returned_unchanged() -> Result<()> {
        let ctx = SessionContext::new();
        let plan = physical(&ctx, "SELECT 1 + 1 AS n").await?;
        let resolved = resolve_iceberg_scans(&ctx, Arc::clone(&plan)).await?;
        assert!(
            Arc::ptr_eq(&plan, &resolved),
            "an untouched plan must come back as the identical Arc"
        );
        Ok(())
    }

    /// Deletes are applied by iceberg's reader and by nothing else, so a table carrying them must
    /// be refused rather than read as bare parquet. Driven through the real `DELETE` path would
    /// need a SQL catalog, so this asserts on the check itself with a hand-built task.
    #[test]
    fn a_task_with_delete_files_is_refused() {
        let mut task = sample_task();
        task.deletes.push(iceberg::scan::FileScanTaskDeleteFile {
            file_path: "file:///wh/ns/t/data/del.parquet".to_string(),
            file_size_in_bytes: 10,
            file_type: iceberg::spec::DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        });
        let err = reject_unreadable_task("lldb.sales.orders", &task)
            .expect_err("deletes must be refused");
        let message = err.to_string();
        assert!(message.contains("delete file"), "{message}");
        assert!(message.contains("resurrect deleted rows"), "{message}");
    }

    #[test]
    fn a_non_parquet_data_file_is_refused() {
        let mut task = sample_task();
        task.data_file_format = DataFileFormat::Avro;
        let err =
            reject_unreadable_task("lldb.sales.orders", &task).expect_err("avro is not readable");
        let message = err.to_string();
        assert!(message.contains("Avro"), "{message}");
        assert!(message.contains("parquet-only"), "{message}");
    }

    #[test]
    fn a_partial_byte_range_is_refused() {
        let mut task = sample_task();
        task.start = 4;
        task.length = 100;
        let err = reject_unreadable_task("lldb.sales.orders", &task)
            .expect_err("iceberg 0.10 never plans a partial file");
        let message = err.to_string();
        assert!(message.contains("covers bytes 4..104"), "{message}");
        assert!(message.contains("duplicate rows"), "{message}");
    }

    /// A partitioned table is refused on the *metadata*, because iceberg 0.10 never fills in the
    /// per-task partition spec (see `reject_unreadable_table`).
    #[tokio::test]
    async fn a_partitioned_table_is_refused() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let (_ctx, lake) = seeded(tmp.path(), 4).await?;
        let table = lake.load_table(NS, "orders").await?;
        // Sanity: the unpartitioned table this module's other tests use passes the check.
        reject_unreadable_table("lldb.sales.orders", &table)
            .expect("an unpartitioned table is readable");

        let partitioned = partitioned_table(tmp.path()).await?;
        let err = reject_unreadable_table("lldb.sales.by_day", &partitioned)
            .expect_err("a partitioned table cannot be read as bare parquet");
        let message = err.to_string();
        assert!(message.contains("is partitioned"), "{message}");
        assert!(message.contains("single node"), "{message}");
        Ok(())
    }

    /// Two files in different buckets cannot become one `FileScanConfig`, and saying so beats
    /// silently reading half the table.
    #[test]
    fn uris_split_into_a_store_and_a_path() -> Result<()> {
        let (store, path) = split_object_store_uri("file:///wh/ns/t/data/a.parquet")?;
        assert_eq!(store.as_str(), "file:///");
        assert_eq!(path.as_ref(), "wh/ns/t/data/a.parquet");
        // Re-joining is what `scanned_data_files` does, so it has to round-trip.
        assert_eq!(
            format!("{}{path}", store.as_str()),
            "file:///wh/ns/t/data/a.parquet"
        );

        let (store, path) = split_object_store_uri("s3://bucket/wh/ns/t/data/a.parquet")?;
        assert_eq!(store.as_str(), "s3://bucket/");
        assert_eq!(path.as_ref(), "wh/ns/t/data/a.parquet");

        // A bare path is a local path, and a Windows drive letter is not a scheme — the ruling
        // `lakehouse::uri_scheme` already makes, reused rather than re-decided.
        let (store, path) = split_object_store_uri("/wh/ns/t/data/a.parquet")?;
        assert_eq!(store.as_str(), "file:///");
        assert_eq!(path.as_ref(), "wh/ns/t/data/a.parquet");
        assert_eq!(
            split_object_store_uri(r"C:\wh\a.parquet")?.0.as_str(),
            "file:///"
        );
        Ok(())
    }

    /// A store nothing in the session can serve is a coordinator-side error naming the scheme,
    /// not a mysterious failure on a worker.
    #[tokio::test]
    async fn an_unregistered_object_store_is_named() -> Result<()> {
        let ctx = SessionContext::new();
        let (store, _) = split_object_store_uri("s3://nope/wh/t/data/a.parquet")?;
        assert!(
            ctx.runtime_env().object_store(&store).is_err(),
            "an unregistered bucket must not resolve — this is the condition resolve_scan reports"
        );
        Ok(())
    }

    // -- helpers -------------------------------------------------------------------------------

    /// A whole-file parquet task with no deletes: the shape iceberg 0.10 actually emits.
    fn sample_task() -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: 1024,
            start: 0,
            length: 1024,
            record_count: Some(10),
            data_file_path: "file:///wh/ns/t/data/a.parquet".to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(iceberg::spec::Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    /// Create a table partitioned by an identity transform, which is the shape the refusal is
    /// about. Built straight against a `MemoryCatalog` rather than through [`Lakehouse`], because
    /// [`crate::manifest`] has no way to declare a partition spec — and adding a catalog handle to
    /// the public API just to reach this shape from a test would be the tail wagging the dog.
    async fn partitioned_table(warehouse: &Path) -> Result<Table> {
        use std::collections::HashMap;

        use iceberg::io::LocalFsStorageFactory;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{
            NestedField, PrimitiveType, Schema as IceSchema, Transform, Type, UnboundPartitionSpec,
        };
        use iceberg::{Catalog, CatalogBuilder, TableCreation};

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "part",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await?;
        let ns = NamespaceIdent::new(NS.to_string());
        catalog.create_namespace(&ns, HashMap::new()).await?;

        let schema = IceSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "day",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()?;
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "day", Transform::Identity)?
            .build();
        let creation = TableCreation::builder()
            .name("by_day".to_string())
            .schema(schema)
            .partition_spec(spec)
            .properties(HashMap::new())
            .build();
        Ok(catalog.create_table(&ns, creation).await?)
    }

    /// The `IcebergTableScan` sub-plan of `plan`, as an `Arc` (so it can be resolved on its own).
    fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut found = None;
        plan.apply(|node| {
            if node.as_any().downcast_ref::<IcebergTableScan>().is_some() {
                found = Some(Arc::clone(node));
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    fn find_file_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut found = None;
        plan.apply(|node| {
            if file_scan_config(node).is_some() {
                found = Some(Arc::clone(node));
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    /// Bytes a plan's scan ranges cover — the same measure `scan_split`'s own tests use.
    fn scanned_bytes(plan: &Arc<dyn ExecutionPlan>) -> i64 {
        let mut total = 0;
        plan.apply(|node| {
            if let Some(config) = file_scan_config(node) {
                for group in &config.file_groups {
                    for file in group.iter() {
                        total += match &file.range {
                            Some(r) => r.end - r.start,
                            None => file.object_meta.size as i64,
                        };
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        total
    }

    /// The data-file paths of the table's current snapshot, straight from the catalog — the
    /// independent answer `scanned_data_files` is checked against.
    async fn snapshot_data_files(lake: &Lakehouse) -> Result<Vec<String>> {
        let table = lake.load_table(NS, "orders").await?;
        let scan = table.scan().select_all().build()?;
        let mut files: Vec<String> = scan
            .plan_files()
            .await?
            .map_ok(|task| task.data_file_path)
            .try_collect()
            .await?;
        files.sort();
        Ok(files)
    }
}
