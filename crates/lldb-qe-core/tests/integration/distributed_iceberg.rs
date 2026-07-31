//! An **Iceberg** query, distributed across a real fleet, answering exactly what one node does.
//!
//! [`lldb_qe_core::iceberg_scan`] made an `IcebergTableScan` shippable and sliceable by rewriting it,
//! on the coordinator, into a parquet scan over the concrete data files of the snapshot it was
//! planned against. Its own tests prove that rewrite in isolation: the plan serializes, the file list
//! matches the snapshot, the slices are disjoint. None of them start a worker. This file is the
//! end-to-end half — the properties that only exist once plan bytes have actually crossed a wire:
//!
//! 1. **An Iceberg query distributes and the answer is unchanged.** Driven through
//!    [`execute_query`], the same funnel a coordinator runs, against ≥2 in-process Flight workers.
//!    Both a `GROUP BY` and a join-then-`GROUP BY` are checked, because those are the shapes
//!    [`plan_distributed`] actually cuts; the staged plan is asserted to contain a
//!    [`FlightReaderExec`](lldb_qe_core::FlightReaderExec) so a plan that quietly took the
//!    offload-whole-to-one-worker path cannot pass as a distributed one.
//! 2. **The snapshot the coordinator planned against is the snapshot every worker reads.** A
//!    resolved plan is captured, a *new* snapshot is then committed to the table without re-planning,
//!    and the captured plan — executed across the fleet *after* that commit — still returns the
//!    pre-commit answer while a freshly planned query returns the post-commit one. That is the
//!    difference between "the file list travels inside the plan bytes" and "each worker resolves
//!    current", which is the split-brain a shared catalog exists to prevent. The same file list is
//!    also asserted to survive a `serialize_plan`/`deserialize_plan` round trip byte-for-byte,
//!    because the wire is where it would be lost.
//! 3. **The workers genuinely executed.** Every worker is built with
//!    [`serve_worker_with_cache`](lldb_qe_core::flight::serve_worker_with_cache) so the test holds
//!    its [`StageCache`]; the answer is only accepted if the fleet's `execution_count` moved. Without
//!    that, a coordinator-local fallback would produce the right rows and prove nothing.
//! 4. **The fleet's IO is one table scan, not one per worker.** The map stages' byte ranges are
//!    summed per distinct stage and compared against the snapshot's own size — the property
//!    [`split_scan`](lldb_qe_core::split_scan) exists for, now inherited by Iceberg for free.
//!
//! No Postgres and no generated data: a [`MemoryCatalog`] over a `tempdir` warehouse, seeded with
//! `INSERT INTO`. That is not just convenience — a memory catalog is per-process, so the workers
//! here *cannot* see the coordinator's catalog even in principle. A passing distributed query is
//! therefore direct evidence that a worker needs no catalog access, which is the whole point of
//! resolving scans on the coordinator.
//!
//! [`MemoryCatalog`]: https://docs.rs/iceberg/0.10/iceberg/memory/index.html

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use lldb_qe_core::flight::{self, deserialize_plan, serialize_plan};
use lldb_qe_core::manifest::{
    CatalogBackend, CatalogDef, ColumnDef, Manifest, NamespaceDef, TableDef,
};
use lldb_qe_core::tenancy::TenantScope;
use lldb_qe_core::{
    FlightReaderExec, Lakehouse, StageCache, Storage, StorageConfig, TableSource, apply_manifest,
    contains_flight_reader, execute_query, plan_distributed, resolve_iceberg_scans,
    scanned_data_files, stage_id_of,
};
use tokio::net::TcpListener;

use crate::support::Servers;

const NS: &str = "sales";

/// A fleet of `n` in-process Flight workers, each with a cache the test can read.
///
/// The caches are the evidence for property 3: a worker's `execution_count` is a worker-side fact,
/// so a rise in it cannot be produced by anything the coordinator did on its own.
struct Fleet {
    urls: Vec<String>,
    caches: Vec<Arc<StageCache>>,
    /// The workers themselves, stopped when this fleet is dropped — see [`Servers`].
    _servers: Servers,
}

impl Fleet {
    async fn start(n: usize) -> anyhow::Result<Self> {
        let mut urls = Vec::with_capacity(n);
        let mut caches = Vec::with_capacity(n);
        let mut servers = Servers::new();
        for _ in 0..n {
            // Port 0: the OS picks a free one, so the whole file is safe under a parallel
            // `cargo test --workspace`.
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let cache = Arc::new(StageCache::new());
            let served = Arc::clone(&cache);
            servers.spawn(async move {
                // A worker gets a bare `SessionContext` — no catalog, no manifest, no warehouse
                // path. It can only read what the plan names.
                flight::serve_worker_with_cache(listener, SessionContext::new(), served)
                    .await
                    .expect("worker serve");
            });
            urls.push(format!("http://{addr}"));
            caches.push(cache);
        }
        Ok(Self {
            urls,
            caches,
            _servers: servers,
        })
    }

    /// Total stage materializations across the fleet.
    fn executions(&self) -> usize {
        self.caches.iter().map(|c| c.execution_count()).sum()
    }

    /// Total rows the fleet streamed back to the coordinator.
    fn rows_served(&self) -> usize {
        self.caches.iter().map(|c| c.rows_served()).sum()
    }
}

/// Two unpartitioned Iceberg tables on a memory catalog rooted at `warehouse`.
fn manifest(warehouse: &Path) -> Manifest {
    let col = |name: &str, ty: &str| ColumnDef {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: false,
    };
    Manifest {
        catalogs: vec![CatalogDef {
            name: "lldb".to_string(),
            backend: CatalogBackend::Memory,
            warehouse: Some(format!("file://{}", warehouse.display())),
            namespaces: vec![NamespaceDef {
                name: NS.to_string(),
                tables: vec![
                    TableDef {
                        name: "orders".to_string(),
                        format: Default::default(), // Iceberg
                        source: TableSource::Empty,
                        schema: Some(vec![
                            col("o_id", "int64"),
                            col("o_cust", "int64"),
                            col("o_amount", "int64"),
                        ]),
                    },
                    TableDef {
                        name: "customers".to_string(),
                        format: Default::default(),
                        source: TableSource::Empty,
                        schema: Some(vec![col("c_id", "int64"), col("c_region", "string")]),
                    },
                ],
            }],
        }],
    }
}

/// A session configured the way a coordinator facing a fleet is: several target partitions, and the
/// repartition thresholds low enough that the optimizer inserts its shuffle seams on test-sized
/// data. Without those seams there is no distribution boundary for `plan_distributed` to cut, and
/// the query would take the offload-whole-plan path — a different property than the one under test.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    let opts = cfg.options_mut();
    opts.optimizer.repartition_file_min_size = 1;
    opts.optimizer.hash_join_single_partition_threshold = 0;
    opts.optimizer.hash_join_single_partition_threshold_rows = 0;
    SessionContext::new_with_config(cfg)
}

/// `orders_batches` × `rows_per_batch` order rows and `customers` customer rows, appended as one
/// `INSERT` per batch so the snapshot ends up with **several data files** — which is what gives
/// byte-range slicing something to slice.
async fn seeded(
    warehouse: &Path,
    orders_batches: i64,
    rows_per_batch: i64,
    customers: i64,
) -> anyhow::Result<(SessionContext, Lakehouse)> {
    let ctx = distributing_ctx();
    let storage = Storage::from_config(&StorageConfig::Local(warehouse.to_path_buf()))?;
    let mut lakes = apply_manifest(
        &ctx,
        &storage,
        &manifest(warehouse),
        &TenantScope::untenanted(),
    )
    .await?;

    for batch in 0..orders_batches {
        let values = (0..rows_per_batch)
            .map(|i| {
                let id = batch * rows_per_batch + i;
                format!("({id}, {}, {})", id % customers, 10 + id % 97)
            })
            .collect::<Vec<_>>()
            .join(", ");
        run(
            &ctx,
            &format!("INSERT INTO lldb.{NS}.orders VALUES {values}"),
        )
        .await?;
    }

    let values = (0..customers)
        .map(|i| format!("({i}, 'r{}')", i % 4))
        .collect::<Vec<_>>()
        .join(", ");
    run(
        &ctx,
        &format!("INSERT INTO lldb.{NS}.customers VALUES {values}"),
    )
    .await?;

    Ok((ctx, lakes.remove(0)))
}

async fn run(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(ctx.sql(sql).await?.collect().await?)
}

/// A rendering of a result that is stable enough to compare two runs with: batch boundaries and
/// row order within a partition are execution details, the multiset of rows is the answer.
fn rendered(batches: &[RecordBatch]) -> anyhow::Result<Vec<String>> {
    let mut lines: Vec<String> = pretty_format_batches(batches)?
        .to_string()
        .lines()
        .map(str::to_string)
        .collect();
    lines.sort();
    Ok(lines)
}

async fn physical(ctx: &SessionContext, sql: &str) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
    Ok(ctx.sql(sql).await?.create_physical_plan().await?)
}

/// The staged form of `sql`: resolved, then cut into stages for `fleet`. This is exactly what
/// `engine::run_on_fleet` builds; reproduced here only so a test can *inspect* it before it runs.
async fn staged(
    ctx: &SessionContext,
    sql: &str,
    fleet: &[String],
) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
    let resolved = resolve_iceberg_scans(ctx, physical(ctx, sql).await?).await?;
    plan_distributed(resolved, fleet)
}

/// Bytes a plan's file-scan ranges cover, ignoring anything behind a `FlightReaderExec` (a remote
/// stage is a leaf here). Same measure `scan_split`'s own tests use.
fn local_scanned_bytes(plan: &Arc<dyn ExecutionPlan>) -> i64 {
    let mut total = 0;
    plan.apply(|node| {
        if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
            return Ok(TreeNodeRecursion::Jump);
        }
        if let Some(exec) = node
            .as_any()
            .downcast_ref::<datafusion::datasource::source::DataSourceExec>()
            && let Some(config) =
                exec.data_source()
                    .as_any()
                    .downcast_ref::<datafusion::datasource::physical_plan::FileScanConfig>()
        {
            for group in &config.file_groups {
                for file in group.iter() {
                    total += match &file.range {
                        Some(range) => range.end - range.start,
                        None => file.object_meta.size as i64,
                    };
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a plan does not error");
    total
}

/// Every distinct remote stage in `plan`, keyed the way a worker keys it: by the content hash of
/// the plan bytes it is sent. Two `FlightReaderExec`s that differ only in `remote_partition` are one
/// stage and are materialized once, so counting them twice would over-report the fleet's IO.
fn distinct_remote_stages(
    plan: &Arc<dyn ExecutionPlan>,
) -> anyhow::Result<Vec<Arc<dyn ExecutionPlan>>> {
    let mut inners = Vec::new();
    plan.apply(|node| {
        if let Some(reader) = node.as_any().downcast_ref::<FlightReaderExec>() {
            inners.push(Arc::clone(reader.inner()));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a plan does not error");

    let mut seen = HashSet::new();
    let mut stages = Vec::new();
    for inner in inners {
        let id = stage_id_of(&serialize_plan(Arc::clone(&inner))?);
        if seen.insert(id) {
            stages.push(inner);
        }
    }
    Ok(stages)
}

/// Total size of the data files of the table's current snapshot, straight from the catalog.
async fn snapshot_bytes(lake: &Lakehouse, table: &str) -> anyhow::Result<i64> {
    use futures::TryStreamExt;
    let handle = lake.load_table(NS, table).await?;
    let scan = handle.scan().select_all().build()?;
    let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
    Ok(tasks.iter().map(|t| t.file_size_in_bytes as i64).sum())
}

// -- 1. an iceberg query distributes, and the answer does not change --------------------------

/// A grouped aggregation over an Iceberg table, run across a two-worker fleet through the same
/// `execute_query` a coordinator calls, must equal the single-node answer row for row — and must
/// visibly have gone through the fleet.
#[tokio::test]
async fn an_iceberg_aggregation_distributes_and_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (ctx, _lake) = seeded(tmp.path(), 4, 700, 40).await?;
    let fleet = Fleet::start(2).await?;

    let sql = format!(
        "SELECT o_cust, count(*) AS n, sum(o_amount) AS total \
         FROM lldb.{NS}.orders WHERE o_amount > 20 GROUP BY o_cust"
    );

    // The staged shape is asserted before the run: a plan with no FlightReaderExec would still
    // return the right rows (the engine offloads it whole to one worker), and would prove nothing
    // about distribution.
    let staged = staged(&ctx, &sql, &fleet.urls).await?;
    assert!(
        contains_flight_reader(&staged),
        "the aggregation must have been cut into remote stages, not offloaded whole:\n{}",
        datafusion::physical_plan::displayable(staged.as_ref()).indent(false)
    );

    let before = fleet.executions();
    let distributed = execute_query(&ctx, &sql, &fleet.urls).await?;
    let single_node = run(&ctx, &sql).await?;

    assert_eq!(
        rendered(&distributed)?,
        rendered(&single_node)?,
        "a distributed iceberg aggregation must answer exactly what one node answers"
    );
    assert_eq!(
        distributed.iter().map(RecordBatch::num_rows).sum::<usize>(),
        40,
        "test setup: one group per customer"
    );
    assert!(
        fleet.executions() > before,
        "the rows must have come off a worker, not a coordinator-local fallback"
    );
    assert!(fleet.rows_served() > 0, "a worker must have streamed rows");
    Ok(())
}

/// The same, for a join feeding an aggregation — the shape whose seam is a hash shuffle rather than
/// a sliced scan, and the one where *both* Iceberg tables have to be pinned and shipped.
#[tokio::test]
async fn an_iceberg_join_distributes_and_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (ctx, _lake) = seeded(tmp.path(), 3, 600, 24).await?;
    let fleet = Fleet::start(3).await?;

    let sql = format!(
        "SELECT c.c_region, count(*) AS n, sum(o.o_amount) AS total \
         FROM lldb.{NS}.orders o JOIN lldb.{NS}.customers c ON o.o_cust = c.c_id \
         GROUP BY c.c_region"
    );

    let staged = staged(&ctx, &sql, &fleet.urls).await?;
    assert!(
        contains_flight_reader(&staged),
        "the join must have been cut into remote stages:\n{}",
        datafusion::physical_plan::displayable(staged.as_ref()).indent(false)
    );

    let before = fleet.executions();
    let distributed = execute_query(&ctx, &sql, &fleet.urls).await?;
    let single_node = run(&ctx, &sql).await?;

    assert_eq!(
        rendered(&distributed)?,
        rendered(&single_node)?,
        "a distributed iceberg join must answer exactly what one node answers"
    );
    assert_eq!(
        distributed.iter().map(RecordBatch::num_rows).sum::<usize>(),
        4,
        "test setup: four regions"
    );
    assert!(
        fleet.executions() > before,
        "the join's stages must have run on workers"
    );
    Ok(())
}

// -- 2. the snapshot travels inside the plan ---------------------------------------------------

/// The property this whole design exists for: a resolved plan names its snapshot's files, so a
/// commit that lands *after* planning is invisible to it — on every worker, without any of them
/// consulting a catalog.
///
/// The proof is a race run in slow motion. Plan and resolve; commit a new snapshot; then run the
/// already-resolved plan across the fleet. If any worker resolved "the current files" the extra rows
/// would appear. They must not — while a query planned after the commit must see them, or the test
/// is asserting that writes do not work.
#[tokio::test]
async fn a_resolved_plan_reads_its_own_snapshot_after_a_later_commit() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (ctx, lake) = seeded(tmp.path(), 3, 500, 20).await?;
    let fleet = Fleet::start(2).await?;

    let sql = format!("SELECT count(*) AS n, sum(o_amount) AS total FROM lldb.{NS}.orders");

    // Planned and resolved against the snapshot as it is now. From here on the plan is a closed
    // object: it names 3 data files and nothing can add to that list.
    let pinned_snapshot = lake.current_snapshot_id(NS, "orders").await?;
    let resolved = resolve_iceberg_scans(&ctx, physical(&ctx, &sql).await?).await?;
    let pinned_files = scanned_data_files(&resolved);
    assert_eq!(
        pinned_files.len(),
        3,
        "test setup: three appends leave three data files"
    );

    let expected_before = run(&ctx, &sql).await?;

    // The file list is what pins the snapshot, so it has to survive the wire byte-for-byte — this
    // is the step where "the coordinator knows" would become "the worker guesses".
    let bytes = serialize_plan(Arc::clone(&resolved))?;
    let round_tripped = deserialize_plan(&bytes, &ctx)?;
    assert_eq!(
        scanned_data_files(&round_tripped),
        pinned_files,
        "the pinned file list must survive serialize/deserialize unchanged"
    );

    // A concurrent writer commits. Nothing re-plans.
    run(
        &ctx,
        &format!("INSERT INTO lldb.{NS}.orders VALUES (999000, 1, 1000000)"),
    )
    .await?;
    let moved_snapshot = lake.current_snapshot_id(NS, "orders").await?;
    assert_ne!(
        moved_snapshot, pinned_snapshot,
        "test setup: the insert must have committed a new snapshot"
    );

    // Now run the plan that was resolved *before* that commit, across the fleet. `plan_distributed`
    // + `collect` is precisely what `engine::run_on_fleet` does once resolution has happened; going
    // through `execute_query` here would re-plan and defeat the experiment.
    let staged = plan_distributed(Arc::clone(&resolved), &fleet.urls)?;
    assert!(
        contains_flight_reader(&staged),
        "the pinned plan must still be distributed, or the workers are not involved"
    );
    let before = fleet.executions();
    let distributed = collect(staged, ctx.task_ctx()).await?;
    assert!(
        fleet.executions() > before,
        "the pinned plan must have executed on workers"
    );

    assert_eq!(
        rendered(&distributed)?,
        rendered(&expected_before)?,
        "a plan pinned to a snapshot must not see a commit that landed after it was planned — if \
         this fails, a worker resolved the table itself"
    );

    // …and the pin is a pin, not a cache: a query planned now sees the new row.
    let after = run(&ctx, &sql).await?;
    assert_ne!(
        rendered(&after)?,
        rendered(&expected_before)?,
        "test setup: the commit must be visible to a freshly planned query"
    );
    let distributed_after = execute_query(&ctx, &sql, &fleet.urls).await?;
    assert_eq!(
        rendered(&distributed_after)?,
        rendered(&after)?,
        "a freshly planned distributed query must see the new snapshot"
    );
    Ok(())
}

// -- 4. the fleet reads the table once ---------------------------------------------------------

/// Slicing is why an Iceberg scan was worth resolving rather than replicating: the fleet's map
/// stages between them cover the snapshot's bytes exactly once, not once per worker.
///
/// Asserted on the staged plan's byte ranges rather than on timing, so it cannot flake. Stages are
/// deduplicated by the content hash a worker keys its cache on, because two consumers of one stage
/// cost one materialization.
#[tokio::test]
async fn the_fleet_scans_the_snapshot_once_between_them() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let (ctx, lake) = seeded(tmp.path(), 4, 800, 30).await?;
    let fleet = Fleet::start(4).await?;

    let sql = format!("SELECT o_cust, count(*) FROM lldb.{NS}.orders GROUP BY o_cust");
    let staged = staged(&ctx, &sql, &fleet.urls).await?;
    assert!(contains_flight_reader(&staged));

    let stages = distinct_remote_stages(&staged)?;
    assert!(
        stages.len() > 1,
        "test setup: the scan must have been split across the fleet, got {} stage(s)",
        stages.len()
    );
    // `local_scanned_bytes` stops at a remote leaf, so the measure would silently miss the IO of a
    // nested stage. This shape has none — assert that rather than assume it, or a future planner
    // change could hide a doubled scan behind an extra Flight hop.
    for stage in &stages {
        assert!(
            distinct_remote_stages(stage)?.is_empty(),
            "a map stage over a sliced scan must be a leaf stage, or the byte measure is incomplete"
        );
    }
    let fanned: i64 = stages.iter().map(local_scanned_bytes).sum();
    let whole = snapshot_bytes(&lake, "orders").await?;
    assert!(whole > 0, "test setup: the snapshot has data files");
    assert_eq!(
        fanned + local_scanned_bytes(&staged),
        whole,
        "the fleet's map stages must cover the snapshot exactly once between them, not {}× it",
        fanned as f64 / whole as f64
    );
    Ok(())
}
