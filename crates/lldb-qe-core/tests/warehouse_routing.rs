//! Warehouse routing end-to-end, in process: a warehouse's **size drives the observed fan-out**
//! of a real query, two differently-sized warehouses run independently against the same catalog,
//! and a suspended warehouse never reaches the network at all.
//!
//! This is issue #16's "resizing a warehouse changes the observed parallelism, with no redeploy"
//! criterion, proven the same way `fleet_discovery.rs` proves fan-out: N in-process workers on
//! distinct `127.0.0.1` ports, a real DataFusion plan, and an assertion on which workers the
//! rewritten plan actually references.
//!
//! # Why a fake resolver rather than real DNS
//!
//! The production chain is: a warehouse row says `size = N` → the actuator runs N tasks → Cloud
//! Map answers `<warehouse>.lldb.local` with those N addresses → [`discover_workers`] expands
//! them → the plan fans across N. Only the middle link needs a cloud. So these tests inject the
//! resolver ([`discover_workers_with`]) and have it answer a warehouse's DNS name with exactly
//! the addresses of the workers standing in for that warehouse's tasks — which is *precisely*
//! what Cloud Map does for a service at `desiredCount: N`. Everything on either side of that link
//! is the real code path: the real `Warehouse::endpoint` guard, the real template rendering, the
//! real discovery expansion, the real `plan_distributed` rewrite, the real Flight calls.
//!
//! Nothing here needs Postgres: [`Warehouse`] is a plain record, so a row can be stated directly
//! and the routing behaviour tested without a control plane. `warehouse_lifecycle.rs` covers the
//! other half — that the database produces exactly these records.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::warehouse::{Warehouse, WarehouseState};
use lldb_qe_core::{
    DEFAULT_WAREHOUSE_ENDPOINT, FlightReaderExec, discover_workers_with, flight, plan_distributed,
};
use tokio::net::TcpListener;

/// A warehouse row as the services database would hand it back, without needing one.
fn warehouse(name: &str, size: i32, state: WarehouseState) -> Warehouse {
    let now = Utc::now();
    Warehouse {
        id: 1,
        account_id: 1,
        name: name.to_string(),
        size,
        state,
        created_at: now,
        updated_at: now,
    }
}

/// Start `count` in-process workers and return their `SocketAddr`s — the stand-ins for a
/// warehouse's ECS tasks.
async fn start_warehouse_tasks(count: usize) -> Result<Vec<SocketAddr>> {
    let mut addrs = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            flight::serve_worker(listener, SessionContext::new())
                .await
                .expect("worker serve");
        });
        addrs.push(addr);
    }
    Ok(addrs)
}

/// A resolver that answers each warehouse's DNS name with that warehouse's task addresses, the
/// way Cloud Map answers a service name with one A-record per healthy task. Any other name is an
/// error — a query must never resolve a warehouse it was not routed to.
fn cloud_map(
    table: Vec<(String, Vec<SocketAddr>)>,
) -> impl Fn(String) -> std::future::Ready<Result<Vec<SocketAddr>>> {
    move |authority: String| {
        let result = match table.iter().find(|(name, _)| *name == authority) {
            Some((_, addrs)) => Ok(addrs.clone()),
            None => Err(anyhow::anyhow!("no warehouse registered as `{authority}`")),
        };
        std::future::ready(result)
    }
}

/// `"<warehouse>.lldb.local:50051"` — the authority the default template renders to, which is the
/// key the fake resolver is keyed by.
fn authority(name: &str) -> String {
    format!("{name}.lldb.local:50051")
}

/// Seed a parquet file with several row groups so a scan can be split between map workers.
fn seed_parquet(dir: &std::path::Path, rows: i64, groups: i64) -> Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let g: Vec<String> = (0..rows).map(|i| format!("g{}", i % groups)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(g)),
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
        ],
    )?;
    let path = dir.join("rows.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(128))
        .build();
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// A session configured so tiny test data still yields a real distribution boundary.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    cfg.options_mut().optimizer.repartition_file_min_size = 1;
    SessionContext::new_with_config(cfg)
}

/// The distinct worker URLs the rewritten plan pulls from.
fn referenced_worker_urls(plan: &Arc<dyn ExecutionPlan>) -> BTreeSet<String> {
    let mut urls = BTreeSet::new();
    plan.apply(|node| {
        if let Some(reader) = node.as_any().downcast_ref::<FlightReaderExec>() {
            urls.insert(reader.worker_url().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking the plan does not error");
    urls
}

fn sorted_counts(batches: &[RecordBatch]) -> Result<Vec<GroupCount>> {
    let mut counts = extract_group_counts(batches)?;
    counts.sort();
    Ok(counts)
}

const SQL: &str = "SELECT g, count(*) AS cnt FROM rows GROUP BY g";

#[tokio::test]
async fn two_warehouses_of_different_sizes_run_independently() -> Result<()> {
    // The headline criterion: two named warehouses, different sizes, the *same* catalog and the
    // same data — each query fans across its own warehouse's workers and touches no other's.
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;

    let small = warehouse("wh-small", 1, WarehouseState::Running);
    let large = warehouse("wh-large", 3, WarehouseState::Running);
    let small_tasks = start_warehouse_tasks(small.size as usize).await?;
    let large_tasks = start_warehouse_tasks(large.size as usize).await?;
    let resolve = cloud_map(vec![
        (authority(&small.name), small_tasks.clone()),
        (authority(&large.name), large_tasks.clone()),
    ]);

    // One catalog, shared: compute is what differs between the two runs, not storage.
    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    let oracle = sorted_counts(&ctx.sql(SQL).await?.collect().await?)?;

    let mut fleets = Vec::new();
    for wh in [&small, &large] {
        let endpoint = wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?;
        let fleet = discover_workers_with(std::slice::from_ref(&endpoint), &resolve).await?;
        assert_eq!(
            fleet.len(),
            wh.size as usize,
            "warehouse `{}` (size {}) must discover exactly its own tasks",
            wh.name,
            wh.size
        );

        let plan = ctx.sql(SQL).await?.create_physical_plan().await?;
        let dist = plan_distributed(plan, &fleet)?;
        assert_eq!(
            referenced_worker_urls(&dist),
            fleet.iter().cloned().collect::<BTreeSet<_>>(),
            "the plan must fan across exactly warehouse `{}`'s fleet",
            wh.name
        );

        // Both warehouses answer the same question correctly — same storage, same catalog.
        assert_eq!(
            sorted_counts(&collect(dist, ctx.task_ctx()).await?)?,
            oracle,
            "warehouse `{}` returned a different answer",
            wh.name
        );
        fleets.push(fleet);
    }

    // The independence claim, stated as a set fact: no worker is shared between the two pools.
    let small_fleet: BTreeSet<_> = fleets[0].iter().collect();
    let large_fleet: BTreeSet<_> = fleets[1].iter().collect();
    assert!(
        small_fleet.is_disjoint(&large_fleet),
        "warehouses must not share compute: {small_fleet:?} vs {large_fleet:?}"
    );
    assert_eq!(small_fleet.len(), 1);
    assert_eq!(large_fleet.len(), 3);
    Ok(())
}

#[tokio::test]
async fn resizing_a_warehouse_changes_the_observed_parallelism() -> Result<()> {
    // "Resizing changes the observed parallelism of a query, with no redeploy." Nothing about the
    // engine changes between these two runs: the same binary, the same session, the same SQL.
    // The only difference is the warehouse's size — and therefore how many tasks its DNS name
    // answers with, which discovery re-reads on every invocation.
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;

    let name = "wh-elastic";
    // Four tasks exist; the warehouse's size decides how many of them are registered under its
    // name at any moment, exactly as an ECS `desiredCount` does.
    let tasks = start_warehouse_tasks(4).await?;

    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    let oracle = sorted_counts(&ctx.sql(SQL).await?.collect().await?)?;

    let mut observed = Vec::new();
    for size in [2, 4, 1] {
        let wh = warehouse(name, size, WarehouseState::Running);
        let registered = tasks[..size as usize].to_vec();
        let resolve = cloud_map(vec![(authority(name), registered)]);

        let endpoint = wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?;
        let fleet = discover_workers_with(std::slice::from_ref(&endpoint), &resolve).await?;

        let plan = ctx.sql(SQL).await?.create_physical_plan().await?;
        let dist = plan_distributed(plan, &fleet)?;
        let fanout = referenced_worker_urls(&dist).len();

        // The answer must stay correct at every size — elasticity that changes results is not
        // elasticity, it is a bug.
        assert_eq!(
            sorted_counts(&collect(dist, ctx.task_ctx()).await?)?,
            oracle
        );
        observed.push(fanout);
    }

    assert_eq!(
        observed,
        vec![2, 4, 1],
        "the plan's fan-out must follow the warehouse size, up and down"
    );
    Ok(())
}

#[tokio::test]
async fn a_suspended_warehouse_is_refused_before_any_network_call() -> Result<()> {
    // Suspending frees the compute; the guard is what stops a query from trying to use compute
    // that is no longer there. It fires on the *row*, before discovery — so the failure is a
    // clear "resume it" and not a DNS timeout or a connection refused.
    let suspended = warehouse("wh-idle", 4, WarehouseState::Suspended);
    let err = suspended
        .endpoint(DEFAULT_WAREHOUSE_ENDPOINT)
        .expect_err("a suspended warehouse must not yield an endpoint");
    let msg = format!("{err:#}");
    assert!(msg.contains("wh-idle"), "{msg}");
    assert!(msg.contains("lldb-qe-warehouse resume"), "{msg}");

    // The same warehouse, resumed, routes normally — the suspension is state, not damage.
    let resumed = warehouse("wh-idle", 4, WarehouseState::Running);
    assert_eq!(
        resumed.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?,
        "http://wh-idle.lldb.local:50051"
    );
    Ok(())
}

#[tokio::test]
async fn a_query_never_resolves_a_warehouse_it_was_not_routed_to() -> Result<()> {
    // Routing correctness has a negative half: warehouse `a`'s endpoint must not be answerable by
    // warehouse `b`'s registration. With one name per warehouse that is structural, and this
    // pins it — a regression that dropped the name from the template would show up here as a
    // successful resolution instead of an error.
    let tasks = start_warehouse_tasks(1).await?;
    let resolve = cloud_map(vec![(authority("wh-registered"), tasks)]);

    let stranger = warehouse("wh-unregistered", 2, WarehouseState::Running);
    let endpoint = stranger.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?;
    let err = discover_workers_with(std::slice::from_ref(&endpoint), &resolve)
        .await
        .expect_err("an unregistered warehouse has no fleet");
    let chain = format!("{err:#}");
    assert!(chain.contains("wh-unregistered"), "{chain}");
    Ok(())
}
