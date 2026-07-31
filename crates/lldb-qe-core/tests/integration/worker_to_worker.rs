//! Worker-to-worker exchange: data moves between workers without passing through the
//! coordinator.
//!
//! The shape under test:
//!
//! ```text
//!   coordinator ──do_get──▶ worker 2 ──do_get──▶ worker 1 ──▶ scan
//! ```
//!
//! The coordinator ships worker 2 a plan whose *leaf* is a `FlightReaderExec` pointing at
//! worker 1. Worker 2 must therefore decode a custom node (proving `LldbCodec` is wired into
//! the transport, not just the unit tests) and open its own Flight connection to worker 1. The
//! coordinator only ever connects to worker 2 — the map output never touches it.
//!
//! No external data: the test seeds its own parquet in a tempdir, so it runs everywhere.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use lldb_qe_core::distributed::extract_group_counts;
use lldb_qe_core::{FlightReaderExec, flight};
use tokio::net::TcpListener;

use crate::support::Servers;

/// Start an in-process worker on a random port; returns its URL.
///
/// The handle goes into the caller's [`Servers`] rather than being dropped: a dropped `JoinHandle`
/// detaches the task instead of stopping it, and since #44 this is one binary, so a detached worker
/// holds its port for the rest of the run.
async fn start_worker(workers: &mut Servers) -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    workers.spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Write a small parquet file the workers can all read from disk.
fn seed_parquet(dir: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )?;
    let path = dir.join("rows.parquet");
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

#[tokio::test]
async fn a_worker_pulls_from_another_worker() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path())?;

    let mut fleet = Servers::new();
    let map_worker = start_worker(&mut fleet).await?;
    let reduce_worker = start_worker(&mut fleet).await?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Stage 1 (runs on the map worker): the scan itself.
    let scan = ctx
        .sql("SELECT g, v FROM rows")
        .await?
        .create_physical_plan()
        .await?;
    let scan = Arc::new(CoalescePartitionsExec::new(scan));

    // Stage 2 (runs on the reduce worker): read stage 1 *from the map worker*.
    let stage2: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::new(&map_worker, 0, scan)?);

    // The coordinator asks only the reduce worker. For it to answer, it must decode the
    // FlightReaderExec leaf and pull from the map worker itself.
    let batches = flight::fetch(&reduce_worker, 0, stage2).await?;

    let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
    assert_eq!(
        rows, 5,
        "all seeded rows should arrive via the two-hop path"
    );

    let total: i64 = batches
        .iter()
        .map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v is Int64")
                .iter()
                .flatten()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total, 15, "values must survive both hops intact");
    Ok(())
}

/// A two-stage aggregation where the reduce runs on a worker, not the coordinator — the
/// worker-to-worker exchange that [`lldb_qe_core::plan_distributed`] now composes automatically.
#[tokio::test]
async fn aggregation_reduces_on_a_worker() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path())?;

    let mut fleet = Servers::new();
    let map_worker = start_worker(&mut fleet).await?;
    let reduce_worker = start_worker(&mut fleet).await?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Map stage: partial per-group counts on the map worker.
    let map = ctx
        .sql("SELECT g, count(*) AS cnt FROM rows GROUP BY g")
        .await?
        .create_physical_plan()
        .await?;
    let map = Arc::new(CoalescePartitionsExec::new(map));
    let remote_map: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::new(&map_worker, 0, map)?);

    // Reduce stage: the coordinator asks the reduce worker, which pulls the map output.
    let batches = flight::fetch(&reduce_worker, 0, remote_map).await?;

    let mut counts = extract_group_counts(&batches)?;
    counts.sort();
    assert_eq!(counts, vec![("a".to_string(), 3), ("b".to_string(), 2)]);
    Ok(())
}
