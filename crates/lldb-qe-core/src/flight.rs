//! Arrow Flight transport: ship a physical plan to a worker, stream Arrow batches back.
//!
//! This is the foundation of distributed execution. The moves:
//!
//! 1. **Coordinator** serializes a physical sub-plan to bytes ([`datafusion_proto`]) and puts
//!    them in a Flight `Ticket` alongside the partition index to run.
//! 2. **Worker** ([`WorkerFlightService`]) receives the ticket in `do_get`, deserializes the
//!    plan, calls `plan.execute(partition, ..)`, and streams the resulting `RecordBatch`es
//!    back encoded as `FlightData`.
//! 3. **Coordinator** decodes the `FlightData` stream back into `RecordBatch`es.
//!
//! Arrow Flight is just gRPC that speaks Arrow natively — the batches cross the wire with no
//! row-by-row (de)serialization. In Phase 3 there is no distribution *logic* yet: we prove a
//! sub-plan can round-trip and execute remotely. Phase 4 builds the shuffle on top.
//!
//! Ticket wire format (little): `partition: u32 LE` ++ `serialized physical plan`.
//!
//! Note: coordinator and worker MUST run the identical DataFusion build — serialized plans
//! are not cross-version compatible.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

/// Boxed tonic response stream.
type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Encode a Flight ticket: 4-byte little-endian partition index, then the plan bytes.
fn encode_ticket(partition: u32, plan_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + plan_bytes.len());
    buf.extend_from_slice(&partition.to_le_bytes());
    buf.extend_from_slice(plan_bytes);
    buf
}

/// Decode a Flight ticket into `(partition, plan_bytes)`.
fn decode_ticket(ticket: &[u8]) -> Result<(u32, &[u8])> {
    if ticket.len() < 4 {
        return Err(anyhow!("ticket too short: {} bytes", ticket.len()));
    }
    let (head, plan_bytes) = ticket.split_at(4);
    let partition = u32::from_le_bytes(head.try_into().expect("4 bytes"));
    Ok((partition, plan_bytes))
}

/// Serialize a physical plan for shipping to a worker.
///
/// Uses [`LldbCodec`], not the default codec, so plans containing our own nodes survive the
/// trip. That is what makes worker-to-worker exchange possible: a reduce stage is just a plan
/// whose leaves are [`FlightReaderExec`]s pointing at map workers, and a worker can only run
/// such a plan if it can decode those leaves.
///
/// [`LldbCodec`]: crate::remote::LldbCodec
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub fn serialize_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
    crate::remote::serialize_plan(plan).context("serializing physical plan")
}

/// Deserialize a physical plan received from a coordinator, executable in `ctx`.
pub fn deserialize_plan(bytes: &[u8], ctx: &SessionContext) -> Result<Arc<dyn ExecutionPlan>> {
    // The proto decoder resolves UDFs/runtime from a TaskContext, not the session directly.
    let task_ctx = ctx.task_ctx();
    crate::remote::deserialize_plan(bytes, task_ctx.as_ref()).context("deserializing physical plan")
}

// ---------------------------------------------------------------------------
// Worker side — a Flight server that executes sub-plans.
// ---------------------------------------------------------------------------

/// A stateless worker: executes whatever physical plan arrives over Flight.
#[derive(Clone)]
pub struct WorkerFlightService {
    ctx: SessionContext,
}

impl WorkerFlightService {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

/// Serve the worker on `listener` until the process ends.
pub async fn serve_worker(listener: TcpListener, ctx: SessionContext) -> Result<()> {
    let service = FlightServiceServer::new(WorkerFlightService::new(ctx));
    Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("Flight server terminated")
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;

    /// Execute the sub-plan in the ticket and stream its batches back.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (partition, plan_bytes) = decode_ticket(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;

        let plan = deserialize_plan(plan_bytes, &self.ctx)
            .map_err(|e| Status::internal(format!("deserialize plan: {e}")))?;

        let batches = plan
            .execute(partition as usize, self.ctx.task_ctx())
            .map_err(|e| Status::internal(format!("execute partition {partition}: {e}")))?;

        // Adapt DataFusion errors to Flight errors, then Arrow-encode the batch stream.
        let batches = batches.map_err(|e| FlightError::ExternalError(Box::new(e)));
        let flight_data = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(|e| Status::internal(format!("flight encode: {e}")));

        Ok(Response::new(Box::pin(flight_data)))
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

// ---------------------------------------------------------------------------
// Coordinator side — fetch a partition's results from a worker.
// ---------------------------------------------------------------------------

/// Ship `plan` to the worker at `worker_url` (e.g. `http://127.0.0.1:50051`), execute its
/// `partition`, and collect the streamed result batches.
pub async fn fetch(
    worker_url: &str,
    partition: u32,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<RecordBatch>> {
    let plan_bytes = serialize_plan(plan)?;
    let stream = fetch_stream(worker_url.to_string(), partition, plan_bytes).await?;
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .context("decoding flight data stream")?;
    Ok(batches)
}

/// Like [`fetch`], but hands back the *stream* instead of collecting it, and takes plan bytes
/// that are already serialized.
///
/// This is what [`FlightReaderExec`] needs: an `ExecutionPlan` must produce batches lazily, so
/// buffering a whole remote partition into a `Vec` first would defeat the point — a reduce
/// stage should start work on the first batch, not the last.
///
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
pub async fn fetch_stream(
    worker_url: String,
    partition: u32,
    plan_bytes: Vec<u8>,
) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static> {
    let ticket = Ticket {
        ticket: encode_ticket(partition, &plan_bytes).into(),
    };

    let channel = Channel::from_shared(worker_url.clone())
        .with_context(|| format!("invalid worker url {worker_url}"))?
        .connect()
        .await
        .with_context(|| format!("connecting to worker {worker_url}"))?;
    let mut client = FlightServiceClient::new(channel);

    let flight_data = client
        .do_get(ticket)
        .await
        .context("do_get request")?
        .into_inner()
        .map_err(|status| FlightError::ExternalError(Box::new(status)));

    Ok(FlightRecordBatchStream::new_from_flight_data(flight_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_roundtrips() {
        let ticket = encode_ticket(7, b"plan-bytes");
        let (partition, plan) = decode_ticket(&ticket).unwrap();
        assert_eq!(partition, 7);
        assert_eq!(plan, b"plan-bytes");
    }

    #[test]
    fn short_ticket_errors() {
        assert!(decode_ticket(&[1, 2]).is_err());
    }
}
