//! Phase 2 scorecard: single-node TPC-H latency baseline.
//!
//! Registers the SF1 tables once, then times Q1 and Q6. This baseline is what the
//! distributed engine (Phases 3–5) is measured against — the whole point of the project is
//! knowing when shipping work to workers actually beats one fast local node.
//!
//! Run with `cargo bench -p lldb-qe-core`. Requires generated data (`./scripts/bootstrap.sh`).

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use lldb_qe_core::{StorageConfig, build_session, register_tpch_parquet, tpch};
use tokio::runtime::Runtime;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

fn tpch_single_node(c: &mut Criterion) {
    if !data_dir().join("sf1/lineitem.parquet").exists() {
        eprintln!("skipping tpch bench: no data — run ./scripts/bootstrap.sh");
        return;
    }

    let rt = Runtime::new().unwrap();
    let ctx = rt.block_on(async {
        let (ctx, storage) = build_session(StorageConfig::Local(data_dir()))
            .await
            .unwrap();
        register_tpch_parquet(&ctx, &storage, "sf1").await.unwrap();
        ctx
    });

    let mut group = c.benchmark_group("tpch-sf1-single-node");
    group.sample_size(10); // full-table scans; keep wall-clock reasonable
    for (name, sql) in [("q1", tpch::Q1), ("q6", tpch::Q6)] {
        group.bench_function(name, |b| {
            b.to_async(&rt)
                .iter(|| async { tpch::run(&ctx, sql).await.unwrap() });
        });
    }
    group.finish();
}

criterion_group!(benches, tpch_single_node);
criterion_main!(benches);
