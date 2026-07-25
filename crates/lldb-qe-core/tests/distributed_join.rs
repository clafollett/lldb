//! The staging planner distributes a partitioned hash join and the result matches single-node.
//!
//! This is the join half of issue #4's "done when": a hash join distributes with **both sides
//! shuffled on the join key**, with no bespoke code. `plan_distributed` finds the
//! `HashJoinExec(Partitioned)` seam, and for each hash bucket builds a reduce stage that pulls that
//! bucket from each side's map producer over Flight (a FlightReaderExec nested inside a
//! FlightReaderExec). The coordinator unions the reduce outputs. We assert the distributed join
//! equals the single-node join, row for row.
//!
//! No external data: the test seeds two small multi-file parquet tables in a tempdir. Multiple
//! files per table give the scans multiple partitions, which is what makes the optimizer insert the
//! hash-repartition seam this planner cuts at.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::{flight, plan_distributed};
use tokio::net::TcpListener;

/// A `(g, a.v, b.v)` output row, in a form we can sort and compare.
type Row = (String, i64, i64);

async fn start_worker() -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

/// Seed a table as a directory of `files` parquet files (so the listing scan has multiple
/// partitions). `keys` controls the join-key cardinality; `salt` shifts values so the two tables
/// differ but still overlap on keys.
fn seed_table(
    dir: &std::path::Path,
    name: &str,
    files: usize,
    rows: i64,
    keys: i64,
    salt: i64,
) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let tdir = dir.join(name);
    std::fs::create_dir_all(&tdir)?;
    for f in 0..files {
        let g: Vec<String> = (0..rows).map(|i| format!("k{}", i % keys)).collect();
        let v: Vec<i64> = (0..rows).map(|i| i + salt + (f as i64) * 1000).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(g)),
                Arc::new(Int64Array::from(v)),
            ],
        )?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(128))
            .build();
        let file = std::fs::File::create(tdir.join(format!("part{f}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(tdir)
}

/// A session that forces the partitioned (shuffle) hash-join path on tiny data.
fn distributing_ctx() -> SessionContext {
    let mut cfg = SessionConfig::new().with_target_partitions(4);
    let opts = cfg.options_mut();
    opts.optimizer.repartition_file_min_size = 1;
    opts.optimizer.hash_join_single_partition_threshold = 0;
    opts.optimizer.hash_join_single_partition_threshold_rows = 0;
    SessionContext::new_with_config(cfg)
}

/// Extract `(g, a.v, b.v)` rows from a `SELECT a.g, a.v, b.v` result, sorted for comparison.
fn rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<Row>> {
    let mut out = Vec::new();
    for batch in batches {
        let g = cast(batch.column(0), &DataType::Utf8)?;
        let av = cast(batch.column(1), &DataType::Int64)?;
        let bv = cast(batch.column(2), &DataType::Int64)?;
        let g = g.as_any().downcast_ref::<StringArray>().unwrap();
        let av = av.as_any().downcast_ref::<Int64Array>().unwrap();
        let bv = bv.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            out.push((g.value(i).to_string(), av.value(i), bv.value(i)));
        }
    }
    out.sort();
    Ok(out)
}

#[tokio::test]
async fn distributed_hash_join_matches_single_node() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let a = seed_table(tmp.path(), "a", 2, 300, 7, 0)?;
    let b = seed_table(tmp.path(), "b", 2, 200, 7, 500)?;

    let workers = vec![start_worker().await?, start_worker().await?];

    let ctx = distributing_ctx();
    ctx.register_parquet("a", a.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    ctx.register_parquet("b", b.to_str().unwrap(), ParquetReadOptions::default())
        .await?;

    let sql = "SELECT a.g, a.v, b.v FROM a JOIN b ON a.g = b.g";

    // Distributed: the planner shuffles both sides on `g` and reduces per bucket on the workers.
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let dist: Arc<dyn ExecutionPlan> = plan_distributed(plan, &workers)?;
    let distributed = rows(&collect(dist, ctx.task_ctx()).await?)?;

    // Single-node oracle.
    let expected = rows(&ctx.sql(sql).await?.collect().await?)?;

    assert!(
        !expected.is_empty(),
        "the join must actually match some rows"
    );
    assert_eq!(
        distributed, expected,
        "distributed hash join must equal the single-node join, row for row"
    );
    Ok(())
}
