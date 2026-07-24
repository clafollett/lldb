//! TPC-H queries and physical-plan inspection.
//!
//! A physical plan is a **tree of operators** (`ExecutionPlan` nodes), each streaming
//! `RecordBatch`es to its parent. DataFusion already runs that tree in parallel across CPU
//! cores by splitting each operator's output into *partitions*. "Distributed" (Phases 3–4)
//! is the same idea across machines: cut the tree at a partition boundary, ship the pieces to
//! workers, stream Arrow between them.
//!
//! [`physical_plan_string`] renders the tree so you can *see* those operators —
//! `DataSourceExec` (the scan), `FilterExec`, `AggregateExec` (Partial then Final),
//! `RepartitionExec` (the local shuffle), `CoalescePartitionsExec` (the gather).

use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;

/// TPC-H Q1 — Pricing Summary Report. Heavy grouped aggregation over the whole `lineitem`
/// scan; returns 4 rows (the return-flag × line-status combinations).
pub const Q1: &str = "\
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity)                                       AS sum_qty,
    sum(l_extendedprice)                                  AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity)                                       AS avg_qty,
    avg(l_extendedprice)                                  AS avg_price,
    avg(l_discount)                                       AS avg_disc,
    count(*)                                              AS count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus";

/// TPC-H Q6 — Forecasting Revenue Change. A tight scan + filter + single sum; returns 1 row.
/// The classic case where columnar pushdown shines.
pub const Q6: &str = "\
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate <  date '1994-01-01' + interval '1' year
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24";

/// The SQL for a supported TPC-H query number (currently Q1 and Q6).
pub fn query(n: u8) -> Option<&'static str> {
    match n {
        1 => Some(Q1),
        6 => Some(Q6),
        _ => None,
    }
}

/// Execute `sql` and collect all result batches.
pub async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.sql(sql).await?.collect().await?)
}

/// Render the optimized physical plan for `sql` as an indented operator tree.
pub async fn physical_plan_string(ctx: &SessionContext, sql: &str) -> Result<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(format!("{}", displayable(plan.as_ref()).indent(true)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_queries_resolve() {
        assert!(query(1).is_some());
        assert!(query(6).is_some());
        assert!(query(2).is_none());
        assert!(Q1.contains("lineitem"));
        assert!(Q6.contains("l_discount"));
    }
}
