//! A worker lost mid-query is reassigned, and the query still returns the **correct** answer.
//!
//! This is issue #15's "done when", proven end-to-end with no external data, no Docker, and nothing
//! beyond `127.0.0.1`. The property that makes it possible lives in [`lldb_qe_core::stage_cache`]: a
//! stage is content-addressed and materialized once per worker, so re-running it somewhere else is
//! idempotent and yields identical output. Fault tolerance here is therefore not "retry and hope" —
//! it is "run the same deterministic stage on another node".
//!
//! # Why a fault-injecting worker instead of killing a process
//!
//! Killing a real worker mid-flight races the query: whether the kill lands before, during, or after
//! the pull decides which code path the test exercises, and a flaky fault-tolerance test is worse
//! than none. So the fleet here contains a Flight service that **counts** the requests it receives
//! and answers each with a configured [`tonic::Status`]. That makes the failure deterministic and —
//! crucially — makes the *number of attempts* observable, which is the only way to prove the
//! difference between "retried" and "not retried".
//!
//! The four things under test:
//!
//! 1. **Reassignment is correct, not merely non-erroring.** A stage whose primary is a port nothing
//!    listens on (a real connection-refused) reassigns to a healthy fallback, and the distributed
//!    answer equals the single-node answer.
//! 2. **Mid-RPC worker loss recovers.** The primary accepts the connection and then fails with
//!    `UNAVAILABLE` — the "worker died while serving" shape. The query still returns the right
//!    answer, and the counter proves the faulty worker really was contacted.
//! 3. **A fatal error is not retried.** The primary answers `INVALID_ARGUMENT` — what our own
//!    `do_get` returns for a bad ticket or a plan that will not deserialize. The query must fail
//!    *immediately*, with exactly one request recorded: every worker runs the identical build, so
//!    replaying it across the fleet would multiply a bug by the fleet size and hide it.
//!
//! Exhaustion ("every candidate dead still names them") lives with its ancestor assertion in
//! `fleet_discovery.rs`.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::Stream;
use lldb_qe_core::distributed::{GroupCount, extract_group_counts};
use lldb_qe_core::{flight, plan_distributed};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// A Flight worker that never serves anything: it counts every `do_get` and answers with the status
/// it was built with. The counter is the load-bearing part — it is how a test distinguishes "the
/// retry loop tried this worker once" from "the retry loop replayed a deterministic failure".
#[derive(Clone)]
struct FaultyWorker {
    requests: Arc<AtomicUsize>,
    code: Code,
}

#[tonic::async_trait]
impl FlightService for FaultyWorker {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Err(Status::new(self.code, "injected fault"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

/// Start a real, healthy in-process worker on a random `127.0.0.1` port.
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

/// Start a worker that accepts connections and then fails every `do_get` with `code`. Returns its
/// URL and the request counter.
async fn start_faulty_worker(code: Code) -> anyhow::Result<(String, Arc<AtomicUsize>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let service = FaultyWorker {
        requests: Arc::clone(&requests),
        code,
    };
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("faulty worker serve");
    });
    Ok((format!("http://{addr}"), requests))
}

/// A `host:port` with nothing listening: bind a port, read its address, then drop the listener. A
/// connection there is refused — a task that vanished without a chance to say goodbye.
async fn dead_worker() -> anyhow::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(format!("http://{addr}"))
}

/// Seed a parquet file with several row groups, so a byte-range split can divide it between map
/// workers.
fn seed_parquet(
    dir: &std::path::Path,
    rows: i64,
    groups: i64,
) -> anyhow::Result<std::path::PathBuf> {
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

fn sorted_counts(batches: &[RecordBatch]) -> anyhow::Result<Vec<GroupCount>> {
    let mut counts = extract_group_counts(batches)?;
    counts.sort();
    Ok(counts)
}

const SQL: &str = "SELECT g, count(*) AS cnt FROM rows GROUP BY g";

/// Make the retry loop's `tracing` output visible under `cargo test -- --nocapture`, so a reviewer
/// can *see* which worker failed and where its stage was reassigned. Best-effort and idempotent:
/// several `#[tokio::test]`s share one process, and only the first installs a subscriber.
fn show_retry_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("lldb_qe_core=warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Register the seeded parquet and return the session plus the single-node answer to [`SQL`] — the
/// oracle every reassignment test is measured against.
async fn ctx_with_oracle(
    path: &std::path::Path,
) -> anyhow::Result<(SessionContext, Vec<GroupCount>)> {
    let ctx = distributing_ctx();
    ctx.register_parquet(
        "rows",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    let oracle = sorted_counts(&ctx.sql(SQL).await?.collect().await?)?;
    Ok((ctx, oracle))
}

/// Run [`SQL`] distributed across `fleet` and return the sorted group counts.
async fn run_distributed(
    ctx: &SessionContext,
    fleet: &[String],
) -> anyhow::Result<Vec<GroupCount>> {
    let plan = ctx.sql(SQL).await?.create_physical_plan().await?;
    let dist = plan_distributed(plan, fleet)?;
    sorted_counts(&collect(dist, ctx.task_ctx()).await?)
}

#[tokio::test]
async fn a_stage_whose_primary_is_gone_is_reassigned_and_the_answer_is_correct()
-> anyhow::Result<()> {
    show_retry_logs();
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;
    let (ctx, oracle) = ctx_with_oracle(&path).await?;

    // The middle worker is a port nothing listens on: its slice's pull is refused at connect time,
    // and its fallbacks (the rest of the fleet, rotated) are healthy.
    let fleet = vec![
        start_worker().await?,
        dead_worker().await?,
        start_worker().await?,
    ];

    let distributed = run_distributed(&ctx, &fleet).await?;

    // Correctness is the point. "It didn't error" would also be true of a query that silently
    // dropped the dead worker's slice, which is exactly the bug this must not have.
    assert_eq!(
        distributed, oracle,
        "a reassigned stage must produce the single-node answer, row for row"
    );
    let total: i64 = distributed.iter().map(|(_, c)| c).sum();
    assert_eq!(
        total, 2000,
        "every seeded row is accounted for exactly once"
    );
    Ok(())
}

#[tokio::test]
async fn a_worker_that_fails_mid_rpc_is_reassigned_and_the_answer_is_correct() -> anyhow::Result<()>
{
    show_retry_logs();
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;
    let (ctx, oracle) = ctx_with_oracle(&path).await?;

    // `UNAVAILABLE` from a worker that *did* accept the connection: the shape of a task that died
    // or drained while serving, rather than one that was never there.
    let (faulty, requests) = start_faulty_worker(Code::Unavailable).await?;
    let fleet = vec![start_worker().await?, faulty, start_worker().await?];

    let distributed = run_distributed(&ctx, &fleet).await?;

    assert_eq!(
        distributed, oracle,
        "reassignment after a mid-RPC failure must still equal the single-node answer"
    );
    // Without this the test would pass even if the planner had quietly stopped using the faulty
    // worker — it would prove nothing about retrying.
    assert!(
        requests.load(Ordering::SeqCst) >= 1,
        "the faulty worker must actually have been contacted"
    );
    Ok(())
}

#[tokio::test]
async fn a_fatal_error_is_surfaced_immediately_not_replayed_across_the_fleet() -> anyhow::Result<()>
{
    show_retry_logs();
    let tmp = tempfile::tempdir()?;
    let path = seed_parquet(tmp.path(), 2000, 6)?;
    let (ctx, _oracle) = ctx_with_oracle(&path).await?;

    // `INVALID_ARGUMENT` is what our own `do_get` returns for a bad ticket, a stage-id mismatch, or
    // a plan that will not deserialize. Every worker runs the identical build, so every worker would
    // answer identically: retrying would turn one clear bug into a fleet-wide load spike and bury
    // the cause under a target-exhaustion message.
    let (faulty, requests) = start_faulty_worker(Code::InvalidArgument).await?;
    let fleet = vec![start_worker().await?, faulty.clone(), start_worker().await?];

    let plan = ctx.sql(SQL).await?.create_physical_plan().await?;
    let dist = plan_distributed(plan, &fleet)?;
    let err = collect(dist, ctx.task_ctx())
        .await
        .expect_err("a request-level fault must fail the query, healthy fallbacks or not");

    let chain = format!("{err}");
    let addr = faulty.strip_prefix("http://").unwrap();
    assert!(
        chain.contains(addr),
        "the error must still name the worker that failed `{addr}`, got: {chain}"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a fatal status must be surfaced after exactly one request, not replayed"
    );
    Ok(())
}
