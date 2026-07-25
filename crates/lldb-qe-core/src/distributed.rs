//! Result helpers shared by the distributed tests and benches.
//!
//! This module used to own `distributed_group_count`: a hand-written, three-stage distributed
//! grouped `COUNT(*)` — map each byte-range slice on a worker, hash-shuffle the partials on the
//! coordinator, reduce. It proved the shuffle worked, but it distributed *one* query written in
//! Rust rather than *arbitrary* SQL. [`crate::staging::plan_distributed`] replaced it: the shuffle
//! now falls out of the physical plan's own distribution boundaries, no query-specific code.
//!
//! What survives here is the small, still-useful bit: pulling `(group, count)` pairs out of an
//! aggregation's result batches, and the type alias for them. Tests use it to compare a
//! distributed answer against the single-node oracle.

use anyhow::{Context, Result};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

/// A group value and its (partial or final) count.
pub type GroupCount = (String, i64);

/// Pull `(group: Utf8, count: Int64)` pairs out of aggregation result batches.
pub fn extract_group_counts(batches: &[RecordBatch]) -> Result<Vec<GroupCount>> {
    let mut out = Vec::new();
    for batch in batches {
        // Cast defensively: DataFusion/arrow read Parquet strings as Utf8View, and count(*)
        // may be Int64 or a decimal depending on the plan — normalize both here.
        let group_col = cast(batch.column(0), &DataType::Utf8).context("casting group to Utf8")?;
        let count_col =
            cast(batch.column(1), &DataType::Int64).context("casting count to Int64")?;
        let groups = group_col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 yields StringArray");
        let counts = count_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cast to Int64 yields Int64Array");
        for i in 0..batch.num_rows() {
            out.push((groups.value(i).to_string(), counts.value(i)));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn extracts_and_casts_group_counts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![3, 2])),
            ],
        )
        .unwrap();

        let pairs = extract_group_counts(&[batch]).unwrap();
        assert_eq!(pairs, vec![("a".to_string(), 3), ("b".to_string(), 2)]);
    }
}
