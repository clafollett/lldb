//! The generic catalog goal, proven without TPC-H: declare an **arbitrary** schema as a
//! [`Manifest`] and materialize it. Unlike the other integration tests this one seeds its own
//! tiny parquet, so it always runs (no SF1 data / `tpchgen-cli` needed) and exercises both
//! table formats and a non-`tpch` namespace end-to-end.

use datafusion::arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use lldb_qe_core::manifest::{
    CatalogDef, Manifest, NamespaceDef, TableDef, TableFormat, TableSource,
};
use lldb_qe_core::tenancy::TenantScope;
use lldb_qe_core::{StorageConfig, apply_manifest, build_session};

/// Write `select_sql`'s rows to a parquet file at `path` via DataFusion `COPY`.
async fn seed_parquet(path: &str, select_sql: &str) -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    ctx.sql(&format!(
        "COPY ({select_sql}) TO '{path}' STORED AS PARQUET"
    ))
    .await?
    .collect()
    .await?;
    Ok(())
}

/// Single-scalar `i64` from a `SELECT count(*)`-style result.
fn scalar_i64(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .value(0)
}

#[tokio::test]
async fn arbitrary_schema_loads_through_the_manifest() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let warehouse = tempfile::tempdir()?;

    // A schema that has nothing to do with TPC-H: shop.sales.orders (Iceberg) + a plain
    // `customers` listing table.
    let orders_path = root.path().join("orders.parquet");
    let customers_path = root.path().join("customers.parquet");
    seed_parquet(
        orders_path.to_str().unwrap(),
        "SELECT * FROM (VALUES (1,'open'),(2,'shipped'),(3,'open')) AS t(id, status)",
    )
    .await?;
    seed_parquet(
        customers_path.to_str().unwrap(),
        "SELECT * FROM (VALUES (1,'ada'),(2,'grace')) AS t(id, name)",
    )
    .await?;

    let manifest = Manifest {
        catalogs: vec![CatalogDef {
            name: "shop".to_string(),
            backend: Default::default(),
            warehouse: Some(format!("file://{}", warehouse.path().display())),
            namespaces: vec![NamespaceDef {
                name: "sales".to_string(),
                tables: vec![
                    TableDef {
                        name: "orders".to_string(),
                        format: TableFormat::Iceberg,
                        source: TableSource::Parquet {
                            path: orders_path.to_string_lossy().into_owned(),
                        },
                        schema: None,
                    },
                    TableDef {
                        name: "customers".to_string(),
                        format: TableFormat::Listing,
                        source: TableSource::Parquet {
                            path: customers_path.to_string_lossy().into_owned(),
                        },
                        schema: None,
                    },
                ],
            }],
        }],
    };

    let (ctx, storage) = build_session(StorageConfig::Local(root.path().to_path_buf())).await?;
    let lakes = apply_manifest(&ctx, &storage, &manifest, &TenantScope::untenanted()).await?;
    assert_eq!(lakes.len(), 1, "one catalog has Iceberg tables");

    // Iceberg table resolves by its arbitrary three-part name.
    let orders = ctx
        .sql("SELECT count(*) FROM shop.sales.orders")
        .await?
        .collect()
        .await?;
    assert_eq!(scalar_i64(&orders), 3);

    // Grouped query works through the Iceberg catalog.
    let by_status = ctx
        .sql("SELECT status, count(*) AS n FROM shop.sales.orders GROUP BY status ORDER BY status")
        .await?
        .collect()
        .await?;
    let groups: usize = by_status.iter().map(|b| b.num_rows()).sum();
    assert_eq!(groups, 2, "two order statuses: open, shipped");

    // Listing table resolves by its bare name in the default catalog.
    let customers = ctx
        .sql("SELECT count(*) FROM customers")
        .await?
        .collect()
        .await?;
    assert_eq!(scalar_i64(&customers), 2);

    // Snapshot inspection is namespace-aware.
    let snap = lakes[0].current_snapshot_id("sales", "orders").await?;
    assert!(snap.is_some(), "the seed INSERT produced a snapshot");
    let summary = lakes[0].snapshot_summary("sales", "orders").await?;
    assert_eq!(summary.get("total-records").map(String::as_str), Some("3"));

    Ok(())
}
