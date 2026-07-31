//! Phase 5 scorecard: distributed vs single-node, on the same grouped COUNT(*).
//!
//! At SF1 the distributed path is expected to be *slower* — even though each worker now scans
//! only its own byte-range slice of `orders`, the plan serialization and gRPC round-trips still
//! cost more than a query that already fits in one machine's cache. That's the lesson:
//! distribution is a tax you pay to break the single-machine memory/IO wall, not a free win.
//!
//! Run with `cargo bench -p lldb-qe-core --features benches --bench distributed` — this target
//! carries `required-features = ["benches"]`, so without the flag it is not built and not run
//! (needs `./scripts/bootstrap.sh`).

use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use lldb_qe_core::{StorageConfig, build_session, flight, plan_distributed, register_tpch_parquet};
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
            // Deliberately detached, unlike the integration tests (#48). Dropping a `JoinHandle`
            // does not cancel its task, so these two workers live until the process exits — which
            // is exactly what this benchmark wants: they must outlive every sample, and the process
            // runs one benchmark and stops. The reason it is a leak in `tests/integration` is that
            // #44 made that a single long-lived process shared by two dozen files; a bench harness
            // is still the one-binary-one-job shape those files used to have.
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
            // The staging planner rewrites the same grouped COUNT(*) into map/reduce stages: the
            // partial aggregate is sliced across the workers, the final aggregate reduces on the
            // coordinator. `collect` drives it; the FlightReaderExec leaves make the remote calls.
            let plan = ctx
                .sql("SELECT o_orderstatus, count(*) FROM orders GROUP BY o_orderstatus")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let distributed = plan_distributed(plan, &workers).unwrap();
            collect(distributed, ctx.task_ctx()).await.unwrap()
        });
    });
    group.finish();
}

criterion_group!(benches, distributed_vs_single);
criterion_main!(benches);
