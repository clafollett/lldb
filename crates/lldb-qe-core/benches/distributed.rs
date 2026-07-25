//! Phase 5 scorecard: distributed vs single-node, on the same grouped COUNT(*).
//!
//! At SF1 the distributed path is expected to be *slower* — even though each worker now scans
//! only its own byte-range slice of `orders`, the plan serialization and gRPC round-trips still
//! cost more than a query that already fits in one machine's cache. That's the lesson:
//! distribution is a tax you pay to break the single-machine memory/IO wall, not a free win.
//!
//! Run with `cargo bench -p lldb-qe-core --bench distributed` (needs `./scripts/bootstrap.sh`).

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use lldb_qe_core::{
    StorageConfig, build_session, distributed_group_count, flight, register_tpch_parquet,
};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data")
}

fn distributed_vs_single(c: &mut Criterion) {
    if !data_dir().join("sf1/orders.parquet").exists() {
        eprintln!("skipping distributed bench: no data — run ./scripts/bootstrap.sh");
        return;
    }

    let rt = Runtime::new().unwrap();
    let (ctx, workers) = rt.block_on(async {
        let mut workers = Vec::new();
        for _ in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                flight::serve_worker(listener, SessionContext::new())
                    .await
                    .unwrap();
            });
            workers.push(format!("http://{addr}"));
        }
        let (ctx, storage) = build_session(StorageConfig::Local(data_dir()))
            .await
            .unwrap();
        register_tpch_parquet(&ctx, &storage, "sf1").await.unwrap();
        (ctx, workers)
    });

    let mut group = c.benchmark_group("orders-group-count");
    group.sample_size(10);
    group.bench_function("single-node", |b| {
        b.to_async(&rt).iter(|| async {
            ctx.sql("SELECT o_orderstatus, count(*) FROM orders GROUP BY o_orderstatus")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        });
    });
    group.bench_function("distributed-2-workers", |b| {
        b.to_async(&rt).iter(|| async {
            distributed_group_count(&ctx, &workers, "orders", "o_orderstatus")
                .await
                .unwrap()
        });
    });
    group.finish();
}

criterion_group!(benches, distributed_vs_single);
criterion_main!(benches);
