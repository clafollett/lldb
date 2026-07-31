//! A `FlightReaderExec` claims its sub-plan's whole `eq_properties`; here those claims are checked
//! against the rows a real worker actually hands back.
//!
//! Issue #110 is not a defensive change. It **widens what the leaf says about its own output**, and
//! a widened claim that is false is a wrong answer with no error — a `SortPreservingMergeExec` that
//! believes a bogus ordering, a projection that folds a column to a constant it never had. So the
//! bar this file has to clear is higher than the one a round-trip test clears:
//! `assert_eq!(reader.eq_properties(), inner.eq_properties())` would pass on a leaf that carried a
//! *lie*, because it only ever compares the claim with the claim it was copied from. Every
//! assertion below reads a claim out of the leaf and then goes looking for it **in the delivered
//! batches**, so the two can disagree and the disagreement is what fails.
//!
//! Two shapes, because the constants component has two meanings that are not interchangeable
//! (`datafusion-physical-expr` 53.1, `equivalence/class.rs`):
//!
//! | Test | Claim | What the delivered rows must show |
//! | - | - | - |
//! | [`an_equivalence_class_and_a_uniform_constant_hold_of_the_rows_a_worker_delivers`] | `a = b` is an equivalence class; `c = 5` is `Uniform` | `a == b` and `c == 5` on every row |
//! | [`a_heterogeneous_constant_holds_within_the_partition_the_leaf_names`] | `c` is `Heterogeneous` — constant *within each* partition | `c` is one value throughout partition 0 and a **different** one throughout partition 1 |
//!
//! The second is the one the "one whole input partition" argument rests on, and it is the reason
//! this leaf must **not** imitate `CoalescePartitionsExec` and call
//! `clear_per_partition_constants()`: a `Heterogeneous` constant is real information about the
//! rows a leaf delivers, and only a node that fuses partitions has to give it up.
//!
//! Needs nothing — no Postgres, no Docker, no TPC-H data. It seeds parquet into a `tempdir` and
//! runs a worker on `127.0.0.1`, so it is one of the tests a bare `cargo test` really runs.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{AcrossPartitions, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use lldb_qe_core::{FlightReaderExec, flight};
use tokio::net::TcpListener;

use crate::support::Servers;

/// Seed `a`, `b`, `c` so both shapes have rows to find: `a = b` on the even rows, and `c` cycling
/// through three values so a filter on one of them is a real restriction rather than the whole
/// table.
fn seed_parquet(dir: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let rows = 600i64;
    let a: Vec<i64> = (0..rows).map(|i| i % 7).collect();
    let b: Vec<i64> = (0..rows)
        .map(|i| if i % 2 == 0 { i % 7 } else { i % 7 + 1 })
        .collect();
    let c: Vec<i64> = (0..rows).map(|i| [5, 7, 9][(i % 3) as usize]).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(Int64Array::from(c)),
        ],
    )?;
    let path = dir.join("abc.parquet");
    let file = std::fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(path)
}

/// A session that does **not** repartition: `RepartitionExec` calls `clear_orderings` and
/// `clear_per_partition_constants` itself, so a plan that grew one would be measuring DataFusion's
/// hygiene rather than this leaf's. One partition per filtered scan also makes the union below
/// exactly two partitions, which is what lets a test name one of them.
async fn session(path: &std::path::Path) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    Ok(ctx)
}

/// A real, healthy in-process worker on a random `127.0.0.1` port. The handle goes into the
/// caller's [`Servers`] — a dropped `JoinHandle` detaches rather than stops, and this is one binary.
async fn start_worker(servers: &mut Servers) -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    servers.spawn(async move {
        flight::serve_worker(listener, SessionContext::new())
            .await
            .expect("worker serve");
    });
    Ok(format!("http://{addr}"))
}

fn col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

/// Every `i64` in column `index`, concatenated across `batches` in delivery order.
fn values(batches: &[RecordBatch], index: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the seeded columns are Int64");
            assert_eq!(arr.null_count(), 0, "the seeded columns are non-null");
            arr.values().to_vec()
        })
        .collect()
}

/// Check **every** claim in `eq` against `batches`, and return how many were checkable.
///
/// This is the half that makes a widened-but-false claim fail: the loop is driven by what the leaf
/// says, not by what the test expects, so a constant or a class the code invents out of nothing is
/// looked for in the rows and not found. Non-`Column` members are skipped — a literal in a class is
/// the value the class is constant at, and the constant sweep below already covers it.
fn claims_hold_of(eq: &EquivalenceProperties, batches: &[RecordBatch], label: &str) -> usize {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0, "{label}: no rows came back, so nothing is proven");
    let mut checked = 0;

    for class in eq.eq_group().iter() {
        let members: Vec<&Column> = class
            .iter()
            .filter_map(|e| e.as_any().downcast_ref::<Column>())
            .collect();
        for pair in members.windows(2) {
            let (left, right) = (pair[0], pair[1]);
            assert_eq!(
                values(batches, left.index()),
                values(batches, right.index()),
                "{label}: the leaf claims {left} and {right} are equal per row, and the rows it \
                 delivered disagree"
            );
            checked += 1;
        }
    }

    for constant in eq.constants() {
        let Some(column) = constant.expr.as_any().downcast_ref::<Column>() else {
            continue;
        };
        let seen = values(batches, column.index());
        match &constant.across_partitions {
            // The value is named, so the rows have to *be* it. This is the branch a false claim
            // dies in: nothing here is derived from the query, only from what the leaf asserts.
            AcrossPartitions::Uniform(Some(ScalarValue::Int64(Some(expected)))) => {
                assert!(
                    seen.iter().all(|v| v == expected),
                    "{label}: the leaf claims {column} is uniformly {expected}, delivered {seen:?}"
                );
            }
            // Constant, value unstated (`Uniform(None)`) or per-partition (`Heterogeneous`). Either
            // way one output partition is one value, which is exactly what this leaf delivers.
            AcrossPartitions::Uniform(_) | AcrossPartitions::Heterogeneous => {
                assert!(
                    seen.windows(2).all(|w| w[0] == w[1]),
                    "{label}: the leaf claims {column} is constant in the partition it delivers, \
                     delivered {seen:?}"
                );
            }
        }
        checked += 1;
    }

    checked
}

/// `WHERE a = b` is an equivalence class and `WHERE c = 5` a `Uniform` constant. Both cross the
/// Flight hop inside the leaf's properties, and both are true of the batches that come back.
#[tokio::test]
async fn an_equivalence_class_and_a_uniform_constant_hold_of_the_rows_a_worker_delivers()
-> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let ctx = session(&seed_parquet(tmp.path())?).await?;
    let mut servers = Servers::new();
    let worker = start_worker(&mut servers).await?;

    let inner = ctx
        .sql("SELECT a, b, c FROM t WHERE a = b AND c = 5")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        inner.properties().partitioning.partition_count(),
        1,
        "test setup: one partition, so the leaf reads the whole filtered relation"
    );

    let reader: Arc<dyn ExecutionPlan> =
        Arc::new(FlightReaderExec::new(&worker, 0, Arc::clone(&inner))?);
    let batches = collect(Arc::clone(&reader), ctx.task_ctx()).await?;

    // The claims are really there — otherwise the sweep below would be checking an empty set and
    // passing for the wrong reason. Stated as the *leaf's* properties, never the sub-plan's.
    let eq = &reader.properties().eq_properties;
    let (a, b, c) = (col("a", 0), col("b", 1), col("c", 2));
    assert!(
        eq.eq_group()
            .get_equivalence_class(&a)
            .is_some_and(|cls| cls.contains(&b)),
        "the leaf must carry the equivalence class `a = b` created"
    );
    assert_eq!(
        eq.is_expr_constant(&c),
        Some(AcrossPartitions::Uniform(Some(ScalarValue::Int64(Some(5))))),
        "the leaf must carry `c = 5` as the uniform constant it is"
    );

    // And they are true of what a worker delivered, which is the whole point.
    assert!(
        claims_hold_of(eq, &batches, "single partition") >= 2,
        "both claims must have been checked against the rows"
    );
    // Spelled out once, so the generic sweep above cannot be the only thing standing between a
    // wrong answer and a green test.
    assert_eq!(values(&batches, 0), values(&batches, 1), "a == b");
    assert!(values(&batches, 2).iter().all(|v| *v == 5), "c == 5");
    Ok(())
}

/// A `Heterogeneous` constant is the claim "constant *within each* partition", and this leaf
/// exposes one input partition as its one output partition — so the claim lands on exactly the rows
/// it delivers. Two leaves over the same union, naming different partitions, must therefore each
/// see a constant `c`, and a **different** one from each other.
#[tokio::test]
async fn a_heterogeneous_constant_holds_within_the_partition_the_leaf_names() -> anyhow::Result<()>
{
    let tmp = tempfile::tempdir()?;
    let ctx = session(&seed_parquet(tmp.path())?).await?;
    let mut servers = Servers::new();
    let worker = start_worker(&mut servers).await?;

    // Two single-partition scans constant at different values, unioned: `calculate_union_binary`
    // keeps a constant both sides agree on and demotes it to `Heterogeneous` when the values
    // differ, which is this shape exactly.
    let mut branches = Vec::new();
    for value in [5, 7] {
        branches.push(
            ctx.sql(&format!("SELECT a, b, c FROM t WHERE c = {value}"))
                .await?
                .create_physical_plan()
                .await?,
        );
    }
    let union = UnionExec::try_new(branches)?;
    assert_eq!(
        union.properties().partitioning.partition_count(),
        2,
        "test setup: one partition per branch, so a leaf can name one of them"
    );
    let c = col("c", 2);
    assert_eq!(
        union.properties().eq_properties.is_expr_constant(&c),
        Some(AcrossPartitions::Heterogeneous),
        "test setup: two different constant values must union to a per-partition constant"
    );

    let mut delivered = Vec::new();
    for partition in 0..2u32 {
        let reader: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::new(
            &worker,
            partition,
            Arc::clone(&union),
        )?);
        let eq = &reader.properties().eq_properties;
        assert_eq!(
            eq.is_expr_constant(&c),
            Some(AcrossPartitions::Heterogeneous),
            "partition {partition}: the leaf must carry the per-partition constant, and must not \
             strengthen it to Uniform — it does not know the value"
        );

        let batches = collect(Arc::clone(&reader), ctx.task_ctx()).await?;
        assert!(
            claims_hold_of(eq, &batches, &format!("partition {partition}")) >= 1,
            "partition {partition}: the constant must have been checked against the rows"
        );
        let seen = values(&batches, 2);
        delivered.push(seen[0]);
    }

    // Without this the test would pass on a leaf that ignored `remote_partition` and served the
    // same partition twice: "constant" would hold and mean nothing. The values are the proof that
    // the constant is per-partition and that the leaf delivered the partition it named.
    assert_eq!(
        delivered,
        vec![5, 7],
        "each leaf must deliver its own partition, and the two must be constant at different values"
    );
    Ok(())
}
