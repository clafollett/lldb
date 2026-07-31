//! Remote execution as a *plan node*, and the codec that lets it cross the wire.
//!
//! Up to now distribution lived outside the plan: [`crate::distributed`] builds SQL strings,
//! ships each to a worker, and reduces the results on the coordinator. That proves the idea but
//! it is not how an engine works — the coordinator is a bottleneck, and "distributed" applies to
//! one hand-written aggregation rather than to arbitrary SQL.
//!
//! [`FlightReaderExec`] closes that gap. It is a **leaf** node holding a sub-plan plus the
//! address of the worker that should run it. Executing it ships the sub-plan over Flight and
//! streams the results back, lazily.
//!
//! The payoff is composition. Because the node is serializable, a plan *containing*
//! `FlightReaderExec` leaves can itself be shipped to a worker:
//!
//! ```text
//!   coordinator                worker R (reduce)            workers M1..Mn (map)
//!   ───────────                ─────────────────            ────────────────────
//!   FlightReaderExec(R) ──────▶ FinalAggregate
//!                                 └─ FlightReaderExec(M1) ──▶ PartialAggregate(slice 1)
//!                                 └─ FlightReaderExec(Mn) ──▶ PartialAggregate(slice n)
//! ```
//!
//! The map output flows M→R directly; the coordinator only collects R's final rows. That is a
//! genuine worker-to-worker shuffle.
//!
//! **Why `do_get` and not `do_exchange`.** `do_exchange` is bidirectional streaming, which suits
//! a *push* shuffle where producers send partitions to consumers. This is a **pull** shuffle:
//! the consumer opens `do_get` against each producer. Pull is what Ballista does, it reuses the
//! transport already built, and it makes the plan tree the single source of truth about who
//! talks to whom. Pull's natural hazard — a producer would re-run its sub-plan once per consumer
//! that pulls from it — is defused on the worker: `do_get` materializes each producer stage once
//! into a [`crate::stage_cache::StageCache`] and serves every consumer (and every output
//! partition) from that one buffer. So the `R` reduce stages that all pull the same map producer
//! now share a single execution. See [`crate::flight`] for the stage-id ticket and the cache.
//!
//! Serialization: [`LldbCodec`] is a real [`PhysicalExtensionCodec`], replacing the
//! `DefaultPhysicalExtensionCodec` that could only handle built-in nodes.
//!
//! **Fault tolerance.** A leaf that names exactly one worker makes that worker a single point of
//! failure for the whole query. Since a stage is content-addressed and materialized once per worker
//! (the [`crate::stage_cache::StageCache`] property), re-running it somewhere else is idempotent and
//! yields identical output — so the leaf now carries an ordered list of **fallback** workers and
//! reassigns itself on a transport failure. See [`FlightReaderExec::execute`] for the one
//! correctness constraint that buys (a partition is collected in full before anything is emitted)
//! and [`crate::retry`] for which failures are worth reassigning at all.
//!
//! **The partition a leaf names is checked where the leaf is built.** `remote_partition` selects a
//! partition of `inner`, and whoever constructs the leaf is holding `inner` — so both constructors
//! are fallible and there is deliberately no unchecked twin beside them. Deferring the check to the
//! pull does not merely move it later, it moves it onto another machine and behind the whole cost of
//! the stage: a worker reaches the range test only after a connect, a plan deserialize and a *full*
//! materialization into its [`StageCache`](crate::stage_cache::StageCache), and then answers
//! `InvalidArgument`, which [`crate::retry`] classifies fatal — so the query dies having paid for
//! the stage and learned nothing the coordinator did not already know. A checked constructor
//! standing next to an unchecked one would leave the invariant opt-in, which is the shape being
//! removed; [`LldbCodec::try_decode`] runs the same check on the way in, so a plan arriving from a
//! peer that names a partition its own sub-plan does not have is refused rather than run.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::TryStreamExt;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use url::Url;

use crate::flight;
use crate::retry::RetryPolicy;

/// Reads one partition of a sub-plan that executes on a remote worker.
///
/// From the local plan's point of view this is a leaf: [`children`] is empty, because the inner
/// plan does not run here. Optimizer passes therefore leave the remote stage alone, which is
/// what we want — it was already planned by whoever built it.
///
/// [`children`]: ExecutionPlan::children
#[derive(Debug, Clone)]
pub struct FlightReaderExec {
    /// Worker to pull from first, e.g. `http://worker-1:50051`.
    worker_url: String,
    /// Ordered failover targets, tried after `worker_url` when a pull fails retriably. Empty means
    /// "no reassignment possible" — a one-worker fleet, or a leaf built by [`Self::new`].
    fallbacks: Vec<String>,
    /// Which partition of the remote plan to request.
    remote_partition: u32,
    /// The sub-plan the worker should execute. Deliberately **not** a child.
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl FlightReaderExec {
    /// Wrap `inner` so it runs on `worker_url` instead of locally, with no failover targets.
    ///
    /// This is the pre-fault-tolerance shape and it still means exactly what it always did: one
    /// worker, one chance. Use [`with_fallbacks`](Self::with_fallbacks) to hand the leaf the rest of
    /// the fleet.
    ///
    /// Errors if `remote_partition` is not a partition of `inner` — see the module header for why
    /// that is refused here rather than discovered on the worker.
    pub fn new(
        worker_url: impl Into<String>,
        remote_partition: u32,
        inner: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        Self::with_fallbacks(worker_url, Vec::new(), remote_partition, inner)
    }

    /// Wrap `inner` so it runs on `worker_url`, falling back through `fallbacks` in order if that
    /// worker is lost.
    ///
    /// The primary is unchanged by the presence of fallbacks — placement policy stays with the
    /// staging planner, and this list only says *where else the same work is valid*, which is
    /// everywhere, because the stage is content-addressed and re-materializes identically.
    ///
    /// Errors if `remote_partition` is not a partition of `inner` — see the module header for why
    /// that is refused here rather than discovered on the worker.
    pub fn with_fallbacks(
        worker_url: impl Into<String>,
        fallbacks: Vec<String>,
        remote_partition: u32,
        inner: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let available = inner.properties().partitioning.partition_count();
        if remote_partition as usize >= available {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec would read partition {remote_partition} of a remote stage that \
                 exposes {available}; the leaf and the sub-plan it names disagree, which is a bug \
                 in whatever staged this plan"
            )));
        }
        let schema = inner.schema();
        // A remote read hands back the producer's partition batch-for-batch, in order — Flight is a
        // stream, not a set — so whatever ordering the remote plan guarantees *within* a partition
        // survives the hop. Carrying that ordering across keeps a consumer like
        // `SortPreservingMergeExec` honest about what it is merging: without it a distributed sort
        // would report its output as unordered even though every input stream is sorted. Only the
        // ordering is carried, not partitioning — see below.
        let eq = match inner.properties().output_ordering() {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(schema, [ordering.iter().cloned()])
            }
            None => EquivalenceProperties::new(schema),
        };
        // We surface exactly one partition: the single remote partition we were asked to read.
        // The remote stream is finite, and — see `execute` — this node buffers it in full before
        // emitting, so downstream sees one complete batch set rather than an incremental trickle.
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            worker_url: worker_url.into(),
            fallbacks,
            remote_partition,
            inner,
            properties,
        })
    }

    pub fn worker_url(&self) -> &str {
        &self.worker_url
    }

    /// The failover targets, in the order they will be tried after the primary.
    pub fn fallbacks(&self) -> &[String] {
        &self.fallbacks
    }

    pub fn remote_partition(&self) -> u32 {
        self.remote_partition
    }

    /// The sub-plan that runs remotely.
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }

    /// Every worker this leaf may pull from, primary first, **deduplicated by
    /// [`WorkerIdentity`]** preserving order.
    ///
    /// Dedup matters: the staging planner hands each leaf the rest of the fleet, and a fleet list
    /// that happens to contain the primary would otherwise spend one of the bounded attempts
    /// re-dialing the node we already know is gone. Comparing the strings is not enough to deliver
    /// that, because two spellings of one node are not hypothetical — see [`WorkerIdentity`].
    ///
    /// The *survivor* is the original spelling, never the normalized one: the normalization exists
    /// to answer "same node?", and what gets dialed is what the planner was configured with.
    fn candidates(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(1 + self.fallbacks.len());
        for url in std::iter::once(&self.worker_url).chain(self.fallbacks.iter()) {
            if seen.insert(WorkerIdentity::of(url)) {
                out.push(url.clone());
            }
        }
        out
    }
}

/// What two spellings of the same worker have in common: the origin a dial actually routes on.
///
/// A worker URL is an **origin**. [`crate::tls::dial`] hands it to `Channel::from_shared`, which
/// connects on scheme, host and port, and gRPC composes its own request path from the service and
/// method — so `http://w:50051` and `http://w:50051/` are one node, differing only by the trailing
/// slash a URL serializer adds, and `http://W:50051` is that node again because DNS does not
/// distinguish case either.
///
/// Near-duplicate spellings are a fact of the fleet rather than a hypothetical:
/// [`crate::discovery`] expands one DNS endpoint into one URL per task IP, and the fleet list a
/// staged leaf carries is assembled from whatever an operator wrote on the command line beside it.
/// Since attempts are bounded, one dead node spelled two ways burns two of them — exactly the waste
/// the dedup exists to prevent.
///
/// [`Verbatim`](Self::Verbatim) is for a string `url` cannot parse. It keys on itself, and folding
/// two such strings together on a guess would be worse than trying both: an unparseable URL fails
/// at the dial as `http::uri::InvalidUri`, which [`crate::retry`] deliberately classifies *fatal*
/// rather than replaying it across the fleet, so it costs one attempt and stops.
#[derive(Debug, PartialEq, Eq, Hash)]
enum WorkerIdentity {
    Origin {
        scheme: String,
        host: String,
        /// `port_or_known_default`, so `http://w` and `http://w:80` are one node. `None` only for a
        /// scheme with no default port, which nothing here dials.
        port: Option<u16>,
    },
    Verbatim(String),
}

impl WorkerIdentity {
    fn of(url: &str) -> Self {
        match Url::parse(url) {
            // Parsing is not enough — the result has to *have* an origin. `w1:50051`, which is what
            // an operator writes when they forget the scheme, parses happily as scheme `w1` with
            // `50051` in its **path** and no host at all. Treating that as an origin would key every
            // such spelling on `(scheme, "", None)`, so `w1:50051` and `w1:60000` would be one
            // identity and the second worker would be silently dropped from the candidate list.
            // Falling through to `Verbatim` is the same call made just below for the same reason:
            // when we cannot say two strings are one node, trying both costs one bounded attempt
            // and guessing wrong costs a worker.
            // Parsing is not enough — the result has to *have* an origin. `w1:50051`, which is what
            // an operator writes when they forget the scheme, parses happily as scheme `w1` with
            // `50051` in its **path** and no host at all. Treating that as an origin would key every
            // such spelling on `(scheme, "", None)`, so `w1:50051` and `w1:60000` would be one
            // identity and the second worker would be silently dropped from the candidate list.
            // Falling through to `Verbatim` is the same call made just below for the same reason:
            // when we cannot say two strings are one node, trying both costs one bounded attempt
            // and guessing wrong costs a worker.
            Ok(parsed) => match parsed.host_str() {
                Some(host) => Self::Origin {
                    scheme: parsed.scheme().to_string(),
                    host: host.to_string(),
                    port: parsed.port_or_known_default(),
                },
                None => Self::Verbatim(url.to_string()),
            },
            Err(_) => Self::Verbatim(url.to_string()),
        }
    }
}

impl DisplayAs for FlightReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FlightReaderExec: worker={}, partition={}",
            self.worker_url, self.remote_partition
        )?;
        if !self.fallbacks.is_empty() {
            write!(f, ", fallbacks={}", self.fallbacks.len())?;
        }
        Ok(())
    }
}

/// The name [`FlightReaderExec`] reports, named once because [`encode_failure`] keys a diagnostic
/// off it and the two must not drift apart.
const FLIGHT_READER_EXEC_NAME: &str = "FlightReaderExec";

impl ExecutionPlan for FlightReaderExec {
    fn name(&self) -> &str {
        FLIGHT_READER_EXEC_NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf: `inner` executes on the worker, not in this plan tree.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(format!(
                "FlightReaderExec is a leaf and takes no children, got {}",
                children.len()
            )))
        }
    }

    /// Pull this leaf's partition, reassigning to a fallback worker if the primary is lost.
    ///
    /// # Why the whole partition is buffered before anything is emitted
    ///
    /// This node used to hand batches downstream as they arrived off the wire. That is the right
    /// shape for a healthy pull — and it is unrecoverable. A worker lost *mid-stream* has already
    /// given the consumer some batches; the replacement worker re-materializes the stage from the
    /// top (precisely what makes re-running it safe), so resuming the pull elsewhere would re-emit
    /// those batches and **silently duplicate rows**. A retry that corrupts the answer is worse than
    /// the failure it replaces.
    ///
    /// So the retry unit is "fetch this partition *completely*": nothing is emitted downstream until
    /// a full `Vec<RecordBatch>` is in hand, which makes reassignment safe by construction — either
    /// a candidate delivers the whole partition or its partial work is discarded.
    ///
    /// The cost is buffering one partition of one stage on the puller. That is the same order of
    /// memory the producing worker already holds for that stage in its [`StageCache`], so it does not
    /// change the fleet's memory profile in kind, only in placement. True incremental streaming with
    /// resume is possible but needs per-batch sequencing (a producer-side cursor so a reassigned pull
    /// can say "resume after batch *k*"); that is a follow-up, not built here.
    ///
    /// # Where the per-request authorization comes from
    ///
    /// `context`, and nowhere else. This leaf is *serialized into a plan*, so nothing per-call can
    /// travel with it — the same fact that makes the fleet token and the TLS trust ambient. But a
    /// plan assertion is per **request**, not per process, so ambience is exactly wrong for it: a
    /// worker executing this leaf is forwarding *its caller's* authorization to the next worker.
    /// [`TaskContext`] is the one channel that is per-request and reaches every operator by
    /// construction, which is why [`crate::plan_assertion::task_ctx_with`] puts it there and this
    /// reads it back. `None` means an open fleet, which presents nothing and is checked for nothing.
    ///
    /// [`StageCache`]: crate::stage_cache::StageCache
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec exposes 1 partition, asked for {partition}"
            )));
        }

        // Serialize eagerly so a bad plan fails here rather than inside the stream. It also means
        // every candidate ships byte-identical bytes, hence the same content-addressed stage id.
        let plan_bytes = serialize_plan(Arc::clone(&self.inner))?;
        let candidates = self.candidates();
        let remote_partition = self.remote_partition;
        let schema = self.schema();
        // Read here rather than inside the stream: `execute` runs in the frame that owns the
        // request, while the stream is polled wherever the runtime happens to drive it.
        let assertion = crate::plan_assertion::forwarded(&context);

        // Connecting is async but `execute` is not, so defer the whole round-trip into the
        // stream: nothing touches the network until the consumer polls.
        let stream = futures::stream::once(async move {
            let batches = flight::fetch_partition_with_failover(
                &candidates,
                remote_partition,
                plan_bytes,
                &RetryPolicy::default(),
                assertion.as_ref(),
            )
            .await
            // `anyhow::Error` is not itself a `std::error::Error`, but it converts into a
            // boxed one — which keeps the underlying cause chain (the candidates tried, connect
            // refused, worker-side status) instead of flattening it to a string.
            .map_err(|e: anyhow::Error| DataFusionError::External(e.into()))?;
            Ok::<_, DataFusionError>(futures::stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/// Upper bound on the fallback list a decoder will accept.
///
/// The count is read from the buffer before the URLs are, so a corrupt or hostile buffer could
/// otherwise ask us to reserve four billion strings. No plausible fleet has a thousand workers
/// standing behind one stage; a bound this loose never binds in practice and turns a garbage length
/// into an error instead of an allocation.
const MAX_ENCODED_FALLBACKS: u32 = 1024;

/// Fixed marker every encoded [`FlightReaderExec`] opens with, ahead of the version byte.
///
/// A version byte alone would be a *probabilistic* check, which is the wrong guarantee for a field
/// whose entire job is deterministic diagnosis. The payload that predates versioning began with
/// `u32 LE url_len`, so its first byte is that length's low byte — and a worker URL of length 1,
/// 257, 513 … makes it `0x01`, colliding with `FORMAT_VERSION` exactly. Such a payload would clear
/// the version check and then misparse, failing later with a "truncated url" that points at the
/// bytes rather than at the fleet. Four bytes no length prefix can spell turn that into a refusal
/// on the first field, every time.
const WIRE_MAGIC: &[u8; 4] = b"LLDB";

/// Wire format version, following [`WIRE_MAGIC`] in every encoded [`FlightReaderExec`].
///
/// Bumped whenever the encoding below changes; a decoder refuses anything else rather than guessing,
/// exactly as [`crate::plan_assertion`] does with its own payload. Note what a mismatch *means*
/// here: this byte is written and read by the same build, so seeing another value at all says the
/// two ends of a Flight hop are not the same binary. The magic guards the version's meaning — past
/// the magic, this byte is known to be a version and not some other field's low byte.
const FORMAT_VERSION: u8 = 1;

/// Extension codec that teaches `datafusion_proto` about [`FlightReaderExec`].
///
/// Wire format:
///
/// ```text
/// WIRE_MAGIC (4 bytes, b"LLDB") | u8 FORMAT_VERSION
///   | u32 LE url_len | url | u32 LE remote_partition
///   | u32 LE fallback_count | (u32 LE len | url)*
///   | serialized inner plan
/// ```
///
/// The fallback list sits **before** the inner plan because the plan is "the rest of the buffer" —
/// it has no length prefix of its own, so anything variable-length has to precede it.
///
/// There is no backward-compatibility obligation across a bump, and the header does not create one:
/// the coordinator and every worker must run the identical build (`CLAUDE.md`), which the Flight
/// boundary already requires for DataFusion plan bytes to mean the same thing on both ends. What
/// the header buys is the *failure*. Without it a fleet running two builds fed a changed field
/// layout into an unchanged buffer shape and parsed it into garbage; with it the decode refuses on
/// the first field it reads, so "your fleet is not one build" is read off the error rather than
/// inferred from a wrong answer. Both halves are checked in order — magic, then version — because
/// only the magic can rule out a payload that predates versioning entirely.
///
/// The output schema is *not* encoded — it is recovered by deserializing the inner plan and
/// asking it, which keeps the two from ever disagreeing. Encoding the inner plan recurses
/// through this same codec, so nested remote stages work.
#[derive(Debug, Default, Clone)]
pub struct LldbCodec;

impl PhysicalExtensionCodec for LldbCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Magic first, version second. The order is the point: only the magic can reject a payload
        // that predates versioning, whose first byte is a length's low byte and can equal any
        // version we might pick.
        let rest = take_magic(buf)?;
        let (version, rest) = take_u8(rest)?;
        if version != FORMAT_VERSION {
            // `Execution`, not `Internal` like its neighbours below: `Internal`'s `Display` appends
            // "likely caused by a bug in DataFusion's code, please file a bug report", which is
            // precisely the wrong thing to tell the one person who can fix this. A version mismatch
            // is a deployment fact, not a code defect.
            return Err(DataFusionError::Execution(format!(
                "FlightReaderExec wire format version {version}, but this build speaks \
                 {FORMAT_VERSION}: the plan came from a different build of lldb than this process \
                 is running. Your fleet is not one build — every coordinator and worker must run \
                 the identical binary, so check that a rolling deploy finished and that every role \
                 runs one image tag."
            )));
        }
        let (url, rest) = take_str(rest)?;
        let (remote_partition, rest) = take_u32(rest)?;
        let (fallback_count, mut rest) = take_u32(rest)?;
        if fallback_count > MAX_ENCODED_FALLBACKS {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec fallback count {fallback_count} exceeds the maximum of \
                 {MAX_ENCODED_FALLBACKS}; the payload is corrupt"
            )));
        }
        let mut fallbacks = Vec::with_capacity(fallback_count as usize);
        for _ in 0..fallback_count {
            let (fallback, tail) = take_str(rest)?;
            fallbacks.push(fallback);
            rest = tail;
        }

        let inner = datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
            rest, ctx, self,
        )?;
        // Through the checked constructor, so the range test the coordinator already made is made
        // again here rather than trusted: these bytes came off a socket.
        Ok(Arc::new(FlightReaderExec::with_fallbacks(
            url,
            fallbacks,
            remote_partition,
            inner,
        )?))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        let reader = node
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .ok_or_else(|| encode_failure(&node))?;

        if reader.fallbacks.len() > MAX_ENCODED_FALLBACKS as usize {
            return Err(DataFusionError::Internal(format!(
                "FlightReaderExec has {} fallbacks, more than the {MAX_ENCODED_FALLBACKS} the wire \
                 format carries",
                reader.fallbacks.len()
            )));
        }

        // Built into a scratch buffer and appended once, at the end, because everything below this
        // line can fail and `buf` belongs to the caller. `datafusion-proto` happens to discard it on
        // error today — every caller does — but that is a caller invariant nobody wrote down and
        // nothing enforces, and the failures are not theoretical: `put_str` refuses a string the
        // length prefix cannot carry, and `encode_plan` refuses an un-encodable sub-plan, both after
        // the header is already written. Appending on success alone means a failed encode is a
        // no-op on `buf` whatever the caller does with it next.
        let mut encoded = Vec::new();
        encoded.extend_from_slice(WIRE_MAGIC);
        encoded.push(FORMAT_VERSION);
        put_str(&mut encoded, &reader.worker_url)?;
        encoded.extend_from_slice(&reader.remote_partition.to_le_bytes());
        // `as u32` is sound here and only here: the bound checked above is 1024.
        encoded.extend_from_slice(&(reader.fallbacks.len() as u32).to_le_bytes());
        for fallback in &reader.fallbacks {
            put_str(&mut encoded, fallback)?;
        }
        // `encode_plan`, not `serialize_plan`: the guidance walk in `serialize_plan` already
        // descended through this node's `inner` — that is precisely why it is hand-written — so
        // re-running it here would re-walk the same sub-tree once per level of nesting to find
        // what the top-level walk already proved absent.
        encoded.extend_from_slice(&encode_plan(Arc::clone(&reader.inner))?);

        buf.append(&mut encoded);
        Ok(())
    }
}

/// The refusal for a node whose un-encodability is worth *explaining*, or `None` for a node this
/// codec either encodes or has nothing particular to say about.
///
/// [`IcebergTableScan`] gets its own message because it is the one un-encodable node a *correct*
/// query routinely produces, and because the fix is a step the caller skipped rather than a missing
/// feature: [`crate::iceberg_scan::resolve_iceberg_scans`] turns it into a parquet scan
/// `datafusion-proto` already understands, and [`crate::engine`] runs that before anything is
/// staged. Reaching serialization with one still in the plan means some path bypassed that funnel —
/// which is worth saying out loud, since the generic "cannot encode IcebergTableScan" reads like
/// "Iceberg is unsupported" and would send someone off to write a codec that should not exist.
///
/// A node *named* [`FlightReaderExec`] that is not one gets its own message for a sharper reason:
/// the generic one would be a flat lie. This codec encodes exactly that node, so "cannot encode
/// FlightReaderExec" denies something true and sends the reader hunting for a missing feature. The
/// only way to be in that branch is for the `TypeId`s to differ — two builds of the defining crate
/// in one dependency graph — so the name is all there is to go on, precisely because the type
/// identity that would normally answer the question is the thing that has broken. Hence the leading
/// downcast: a *real* reader answers `None`, because it encodes.
fn guided_refusal(node: &Arc<dyn ExecutionPlan>) -> Option<DataFusionError> {
    if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
        return None;
    }
    if node.name() == FLIGHT_READER_EXEC_NAME {
        return Some(DataFusionError::Execution(format!(
            "LldbCodec was handed a node calling itself {FLIGHT_READER_EXEC_NAME}, which this codec \
             does encode — so it is not the {FLIGHT_READER_EXEC_NAME} this build defines. Two \
             versions of lldb-qe-core are linked into this process: your fleet is not one build. \
             Run `cargo tree -d` to name the duplicate, and check that every coordinator and worker \
             runs one image tag."
        )));
    }
    if node.as_any().downcast_ref::<IcebergTableScan>().is_some() {
        return Some(DataFusionError::NotImplemented(
            "LldbCodec cannot encode IcebergTableScan: it holds a live catalog handle and resolves \
             its own files, so it is deliberately not serialized. Call \
             `iceberg_scan::resolve_iceberg_scans` to pin the scan to its snapshot's data files \
             before the plan is staged — `engine::run_on_fleet` already does, so a plan that gets \
             here bypassed it."
                .to_string(),
        ));
    }
    None
}

/// The error for a node this codec has no encoding for — the backstop under
/// [`refuse_before_encoding`], reached only when `datafusion-proto` hands us a node the walk did
/// not know to refuse.
///
/// Anything this returns is delivered *mangled*: see [`refuse_before_encoding`] for what the encode
/// path does to a codec error and why the guided refusals are hoisted out of here.
fn encode_failure(node: &Arc<dyn ExecutionPlan>) -> DataFusionError {
    guided_refusal(node).unwrap_or_else(|| {
        DataFusionError::NotImplemented(format!("LldbCodec cannot encode {}", node.name()))
    })
}

/// Refuse a plan containing a node whose refusal carries guidance — **before** `datafusion-proto`
/// is asked to encode it.
///
/// # Why the codec cannot be the messenger
///
/// `datafusion-proto` wraps every error an extension codec returns on the **encode** path
/// (`physical_plan/mod.rs:593`):
///
/// ```text
/// internal_err!("Unsupported plan and extension codec failed with [{e}]. Plan: {plan_clone:?}")
/// ```
///
/// Three things happen to a message on the way out of there. It is nested inside "Unsupported plan
/// and extension codec failed with […]"; `internal_err!` makes it a [`DataFusionError::Internal`],
/// whose `Display` appends *"likely caused by a bug in DataFusion's code … file a bug report"*; and
/// a full `Debug` dump of the whole physical plan is appended behind it. So the one refusal an
/// operator is meant to *act* on arrived reading like an upstream defect, after a plan dump — which
/// invites an issue against Apache DataFusion, or a widened check, when the fix is a call that was
/// skipped. The **decode** path has no such wrapper (it propagates codec errors verbatim), which is
/// why [`take_magic`]'s and the version check's refusals have always arrived intact and this one
/// did not.
///
/// So the answer is not to write the message more carefully; it is to not be inside the codec when
/// it is written. Everything [`guided_refusal`] names is caught here, on the coordinator, before a
/// byte is serialized, and returned as our own error.
///
/// # Why the recursion is hand-written
///
/// [`TreeNode::apply`] follows [`ExecutionPlan::children`], and a [`FlightReaderExec`]'s `inner` is
/// deliberately **not** a child — so `apply` would walk straight past every remote stage's sub-plan,
/// which is exactly where a bypassed `resolve_iceberg_scans` leaves a scan that only fails once the
/// stage is being encoded. This descends into `inner` as well.
///
/// # What it costs
///
/// One `TypeId` comparison and at most one `&str` comparison per node, once per top-level
/// [`serialize_plan`] — against an encode that proto-serializes every one of those nodes and
/// allocates a buffer for the result. Measured on a resolved 10-node aggregate plan: **1.1 µs for
/// the walk against 168 µs for the encode behind it**, so it is 0.6% of the work it guards and
/// scales with it. And it is paid once rather than once per level of nesting, because
/// [`LldbCodec::try_encode`] recurses through [`encode_plan`] rather than back through
/// [`serialize_plan`].
///
/// # What it does not cover
///
/// A node with no guidance to lose — one neither this codec nor `datafusion-proto` can encode and
/// that we have never written advice for — still reaches [`encode_failure`] and still comes back
/// wrapped. Covering it would mean duplicating `datafusion-proto`'s own "can I encode this" dispatch
/// over every built-in node, which is a copy that silently rots at every version bump. The class
/// worth protecting is the class we have something to say about.
///
/// [`TreeNode::apply`]: datafusion::common::tree_node::TreeNode::apply
fn refuse_before_encoding(plan: &Arc<dyn ExecutionPlan>) -> DFResult<()> {
    if let Some(err) = guided_refusal(plan) {
        return Err(err);
    }
    for child in plan.children() {
        refuse_before_encoding(child)?;
    }
    if let Some(reader) = plan.as_any().downcast_ref::<FlightReaderExec>() {
        refuse_before_encoding(&reader.inner)?;
    }
    Ok(())
}

/// Serialize a plan with this codec, so nested [`FlightReaderExec`] nodes survive.
///
/// A plan holding a node we can explain is refused *here*, by [`refuse_before_encoding`], rather
/// than by the codec — read that function for what `datafusion-proto` does to an error the codec
/// returns, and why nothing an operator must act on may be raised from inside it.
pub fn serialize_plan(plan: Arc<dyn ExecutionPlan>) -> DFResult<Vec<u8>> {
    refuse_before_encoding(&plan)?;
    encode_plan(plan)
}

/// The encode itself, with no guidance walk ahead of it.
///
/// Split out for [`LldbCodec::try_encode`], which is already *inside* a walk that covered its
/// `inner` sub-plan. Nothing else should call it: [`serialize_plan`] is the entry point that runs
/// the walk.
fn encode_plan(plan: Arc<dyn ExecutionPlan>) -> DFResult<Vec<u8>> {
    let bytes =
        datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(plan, &LldbCodec)?;
    Ok(bytes.to_vec())
}

/// Deserialize a plan produced by [`serialize_plan`].
pub fn deserialize_plan(bytes: &[u8], ctx: &TaskContext) -> DFResult<Arc<dyn ExecutionPlan>> {
    datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(bytes, ctx, &LldbCodec)
}

/// Append a length-prefixed string.
fn put_str(buf: &mut Vec<u8>, s: &str) -> DFResult<()> {
    buf.extend_from_slice(&len_prefix(s.len())?.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

/// The `u32` length prefix for a `len`-byte field, or a refusal rather than a wrapped cast.
///
/// `len as u32` is unreachable-by-construction today — the only strings encoded here are worker
/// URLs — but the failure it would produce is the one this wire format has already spent a version
/// byte and a magic marker avoiding: a length that wraps writes a *shorter* prefix than the bytes
/// that follow, so the decode succeeds, hands back a truncated URL, and the fleet dials somewhere
/// that does not exist. Nothing downstream can tell that from a misconfiguration. An error names it
/// at the only point where the truth is still known.
///
/// Split out of [`put_str`] because that is what makes the bound testable: proving `as u32` wraps
/// needs a 4 GiB string, proving this needs an integer.
fn len_prefix(len: usize) -> DFResult<u32> {
    u32::try_from(len).map_err(|_| {
        DataFusionError::Internal(format!(
            "FlightReaderExec string field is {len} bytes, more than the {} its u32 length prefix \
             can carry; encoding it would truncate the round trip instead of failing",
            u32::MAX
        ))
    })
}

/// Consume and validate the leading [`WIRE_MAGIC`], returning everything after it.
///
/// This is the first thing a decode reads, and the only check that can rule out a payload written
/// before the header existed — see [`WIRE_MAGIC`] for why the version byte cannot do it alone. The
/// refusal reaches the same conclusion as the version one because it has the same single cause: a
/// buffer that does not open with these four bytes was written by something that is not this
/// format, and the only thing that writes this format is another lldb build.
fn take_magic(buf: &[u8]) -> DFResult<&[u8]> {
    if buf.starts_with(WIRE_MAGIC) {
        return Ok(&buf[WIRE_MAGIC.len()..]);
    }
    // Hex, because whatever is there is by definition not the text we expected, and a lossy
    // string rendering of arbitrary bytes hides exactly the difference worth seeing.
    let found: Vec<String> = buf
        .iter()
        .take(WIRE_MAGIC.len())
        .map(|b| format!("{b:02x}"))
        .collect();
    Err(DataFusionError::Execution(format!(
        "FlightReaderExec payload does not start with the lldb wire marker {:?} — it begins [{}] \
         and is only {} byte(s) long. Nothing but an lldb build writes this format, so the plan \
         came from a different build than this process is running: your fleet is not one build. \
         Every coordinator and worker must run the identical binary, so check that a rolling \
         deploy finished and that every role runs one image tag.",
        String::from_utf8_lossy(WIRE_MAGIC),
        found.join(" "),
        buf.len()
    )))
}

fn take_u8(buf: &[u8]) -> DFResult<(u8, &[u8])> {
    match buf.split_first() {
        Some((first, rest)) => Ok((*first, rest)),
        None => Err(DataFusionError::Internal(
            "truncated FlightReaderExec payload: empty, so not even a format version byte"
                .to_string(),
        )),
    }
}

fn take_u32(buf: &[u8]) -> DFResult<(u32, &[u8])> {
    if buf.len() < 4 {
        return Err(DataFusionError::Internal(format!(
            "truncated FlightReaderExec payload: wanted 4 bytes, had {}",
            buf.len()
        )));
    }
    let (head, rest) = buf.split_at(4);
    Ok((u32::from_le_bytes(head.try_into().expect("4 bytes")), rest))
}

fn take_str(buf: &[u8]) -> DFResult<(String, &[u8])> {
    let (len, rest) = take_u32(buf)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(DataFusionError::Internal(format!(
            "truncated FlightReaderExec url: wanted {len} bytes, had {}",
            rest.len()
        )));
    }
    let (s, rest) = rest.split_at(len);
    let s = String::from_utf8(s.to_vec()).map_err(|e| {
        DataFusionError::Internal(format!("FlightReaderExec url is not utf-8: {e}"))
    })?;
    Ok((s, rest))
}

/// Assert `msg` did **not** come out through `datafusion-proto`'s encode-side wrapper.
///
/// One spelling of that wrapper, shared with `iceberg_scan`'s tests deliberately: both modules are
/// asserting the same property about the same upstream `internal_err!`, and two hand-written
/// approximations of it would drift. Lives here because the wrapper is this module's problem —
/// see [`refuse_before_encoding`] for what it does to a message and why it must not be entered.
///
/// The four substrings are the four things the wrapper adds: the nesting, `Internal`'s bug-report
/// boilerplate (two spellings, since the URL and the prose are separable), and the plan dump.
#[cfg(test)]
pub(crate) fn assert_no_datafusion_wrapper(msg: &str) {
    assert!(
        !msg.contains("datafusion/issues"),
        "a refusal the operator must act on must not read as a DataFusion bug; got: {msg}"
    );
    assert!(
        !msg.contains("bug in DataFusion's code"),
        "the Internal-error boilerplate must not be attached; got: {msg}"
    );
    assert!(
        !msg.contains("Unsupported plan and extension codec failed"),
        "the refusal must not be nested inside datafusion-proto's wrapper; got: {msg}"
    );
    assert!(
        !msg.contains("Plan: "),
        "a plan Debug dump must not be appended to it; got: {msg}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A well-formed payload header — magic then version — for tests that hand-build the rest.
    fn header() -> Vec<u8> {
        let mut buf = WIRE_MAGIC.to_vec();
        buf.push(FORMAT_VERSION);
        buf
    }

    /// A tiny local plan to stand in for a remote stage.
    async fn sample_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx.sql("SELECT n FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    /// A local plan with exactly `partitions` output partitions, for the tests that need a
    /// `remote_partition` other than 0 — which the constructor now range-checks.
    ///
    /// Built straight off the table provider rather than through SQL on purpose: the physical
    /// optimizer's repartitioning is a function of the machine's core count, and a test that needs
    /// "partition 3 exists" must not depend on how many cores CI has.
    async fn sample_plan_with_partitions(
        ctx: &SessionContext,
        partitions: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]; partitions]).unwrap();
        let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(
            plan.properties().partitioning.partition_count(),
            partitions,
            "test setup: the stand-in stage must really expose that many partitions"
        );
        plan
    }

    #[tokio::test]
    async fn is_a_leaf_that_keeps_the_inner_schema() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::new("http://w:50051", 0, Arc::clone(&inner)).unwrap();

        assert!(
            reader.children().is_empty(),
            "the remote stage must not be a local child"
        );
        assert_eq!(reader.schema(), inner.schema());
        assert_eq!(reader.properties().partitioning.partition_count(), 1);
    }

    /// Order survives the hop, so the reader advertises it. A distributed sort leans on this: the
    /// coordinator's `SortPreservingMergeExec` merges remote sorted runs, and a reader that claimed
    /// no ordering would make the merged plan look unordered to anything inspecting it.
    #[tokio::test]
    async fn a_remote_read_carries_the_inner_plans_ordering() {
        let ctx = SessionContext::new();
        let unsorted = sample_plan(&ctx).await;
        assert!(
            unsorted.properties().output_ordering().is_none(),
            "test setup: a bare scan has no ordering"
        );
        assert!(
            FlightReaderExec::new("http://w:50051", 0, unsorted)
                .unwrap()
                .properties()
                .output_ordering()
                .is_none(),
            "no ordering to carry, none claimed"
        );

        let sorted = ctx
            .sql("SELECT n FROM t ORDER BY n")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let expected = sorted
            .properties()
            .output_ordering()
            .expect("test setup: an ORDER BY plan is ordered")
            .clone();
        let reader = FlightReaderExec::new("http://w:50051", 0, sorted).unwrap();
        assert_eq!(
            reader.properties().output_ordering(),
            Some(&expected),
            "the reader must advertise the producer's ordering"
        );
    }

    #[tokio::test]
    async fn round_trips_through_the_codec() {
        let ctx = SessionContext::new();
        let inner = sample_plan_with_partitions(&ctx, 4).await;
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://worker-2:50051", 3, inner).unwrap());

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");

        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");
        assert_eq!(back.worker_url(), "http://worker-2:50051");
        assert_eq!(back.remote_partition(), 3);
        assert_eq!(back.schema(), plan.schema());
    }

    /// The property the whole design rests on: a plan *containing* a remote read is itself
    /// serializable, so a reduce stage can be shipped to a worker with its map leaves intact.
    #[tokio::test]
    async fn nested_remote_stages_survive() {
        let ctx = SessionContext::new();
        let inner = sample_plan_with_partitions(&ctx, 2).await;
        let map = Arc::new(FlightReaderExec::new("http://map:50051", 1, inner).unwrap());
        let reduce: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://reduce:50051", 0, map).unwrap());

        let bytes = serialize_plan(Arc::clone(&reduce)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");

        let outer = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("outer");
        assert_eq!(outer.worker_url(), "http://reduce:50051");
        let nested = outer
            .inner()
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("nested");
        assert_eq!(nested.worker_url(), "http://map:50051");
        assert_eq!(nested.remote_partition(), 1);
    }

    #[test]
    fn truncated_payloads_error_rather_than_panic() {
        assert!(take_u32(&[1, 2]).is_err());
        assert!(take_str(&[9, 0, 0, 0, b'a']).is_err());
    }

    /// A leaf naming a partition its sub-plan does not have is refused **here**, with no worker
    /// involved. The alternative is the whole point: the same plan reaches a worker, connects,
    /// deserializes, materializes the entire stage, and only then answers `InvalidArgument` — which
    /// [`crate::retry`] classifies fatal, so the query dies at that cost with nothing learned.
    #[tokio::test]
    async fn a_partition_the_inner_plan_does_not_have_is_refused_locally() {
        let ctx = SessionContext::new();
        let inner = sample_plan_with_partitions(&ctx, 2).await;

        assert!(
            FlightReaderExec::new("http://w:50051", 1, Arc::clone(&inner)).is_ok(),
            "test premise: the last real partition must be accepted"
        );
        let err = FlightReaderExec::new("http://w:50051", 2, Arc::clone(&inner))
            .expect_err("partition 2 of a 2-partition stage does not exist");
        let msg = err.to_string();
        assert!(
            msg.contains("partition 2") && msg.contains("exposes 2"),
            "the refusal must name what was asked for and what exists; got: {msg}"
        );

        assert!(
            FlightReaderExec::with_fallbacks(
                "http://w1:50051",
                vec!["http://w2:50051".into()],
                7,
                inner,
            )
            .is_err(),
            "the fallback-carrying constructor is the same constructor"
        );
    }

    /// The same check on the way *in*: these bytes came off a socket, so the range test the
    /// coordinator made is made again rather than trusted.
    #[tokio::test]
    async fn a_decoded_leaf_naming_a_missing_partition_is_refused() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        // Hand-built, because a well-formed encoder cannot produce this payload any more.
        let mut buf = header();
        put_str(&mut buf, "http://w:50051").unwrap();
        buf.extend_from_slice(&9u32.to_le_bytes()); // remote_partition
        buf.extend_from_slice(&0u32.to_le_bytes()); // fallback_count
        buf.extend_from_slice(&serialize_plan(inner).expect("inner plan"));

        let err = codec
            .try_decode(&buf, &[], &task_ctx)
            .expect_err("a leaf whose partition its own sub-plan lacks must not be executed");
        assert!(err.to_string().contains("partition 9"), "got: {err}");
    }

    /// A leaf built by `new` has no failover targets — the pre-fault-tolerance meaning, preserved.
    #[tokio::test]
    async fn a_plain_reader_has_no_fallbacks() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::new("http://w:50051", 0, inner).unwrap();
        assert!(reader.fallbacks().is_empty());
        assert_eq!(reader.candidates(), vec!["http://w:50051".to_string()]);
    }

    /// The primary must not burn one of the bounded attempts twice just because the fleet list it
    /// was handed also contains it.
    #[tokio::test]
    async fn candidates_are_the_primary_then_fallbacks_deduplicated_in_order() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::with_fallbacks(
            "http://w1:50051",
            vec![
                "http://w2:50051".into(),
                "http://w1:50051".into(), // the primary again
                "http://w3:50051".into(),
                "http://w2:50051".into(), // and a repeat fallback
            ],
            0,
            inner,
        )
        .unwrap();
        assert_eq!(
            reader.candidates(),
            vec![
                "http://w1:50051".to_string(),
                "http://w2:50051".to_string(),
                "http://w3:50051".to_string(),
            ]
        );
    }

    /// Two spellings of one worker are one candidate. `discovery.rs` expands a DNS endpoint into a
    /// URL per task IP and an operator writes the fleet list by hand beside it, so a trailing slash
    /// or a capital letter is a realistic way for the same node to appear twice — and since attempts
    /// are bounded, a dead node spelled two ways burns two of them, which is the exact waste the
    /// dedup exists to prevent.
    #[tokio::test]
    async fn candidates_are_deduplicated_by_host_identity_not_by_spelling() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let reader = FlightReaderExec::with_fallbacks(
            "http://w1:50051",
            vec![
                "http://w1:50051/".into(), // the primary, with the slash a serializer adds
                "http://W1:50051".into(),  // the primary again — DNS is case-insensitive
                "http://w2:50051".into(),  // a genuinely different node
                "http://w2:50051/".into(), // …and its other spelling
                "https://w2:50051".into(), // a different scheme *is* a different endpoint
                "http://w2".into(),        // port 80, so not w2:50051 either
                "not a url at all".into(), // unparseable: kept, keyed on itself
                "not a url at all".into(), // …but still only once
                "also not a url".into(),
            ],
            0,
            inner,
        )
        .unwrap();
        assert_eq!(
            reader.candidates(),
            vec![
                "http://w1:50051".to_string(),
                "http://w2:50051".to_string(),
                "https://w2:50051".to_string(),
                "http://w2".to_string(),
                "not a url at all".to_string(),
                "also not a url".to_string(),
            ],
            "one node is one candidate, and the surviving spelling is the one we were handed"
        );
    }

    /// A default port and an explicit one are the same endpoint; anything else about the URL is not
    /// part of a worker's identity.
    #[test]
    fn a_workers_identity_is_its_scheme_host_and_port() {
        assert_eq!(
            WorkerIdentity::of("http://w:80"),
            WorkerIdentity::of("http://w"),
            "the scheme's default port is the port"
        );
        assert_eq!(
            WorkerIdentity::of("https://w:443/"),
            WorkerIdentity::of("https://W"),
            "…and it is scheme-specific"
        );
        assert_ne!(
            WorkerIdentity::of("http://w:50051"),
            WorkerIdentity::of("https://w:50051"),
            "a plaintext dial and a TLS dial are not interchangeable"
        );
        assert_ne!(
            WorkerIdentity::of("://nonsense"),
            WorkerIdentity::of("also nonsense"),
            "two strings we cannot parse are not assumed to be one node"
        );
        // Parses, but into a scheme and a path with no host — the shape a forgotten `http://`
        // produces. Keying these on their origin would make every one of them equal.
        assert_ne!(
            WorkerIdentity::of("w1:50051"),
            WorkerIdentity::of("w1:60000"),
            "a URL that parses without a host is not an origin, and two of them are not one node"
        );
    }

    #[tokio::test]
    async fn fallbacks_round_trip_through_the_codec() {
        let ctx = SessionContext::new();
        let inner = sample_plan_with_partitions(&ctx, 4).await;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            FlightReaderExec::with_fallbacks(
                "http://worker-2:50051",
                vec![
                    "http://worker-3:50051".into(),
                    "http://worker-4:50051".into(),
                ],
                3,
                inner,
            )
            .unwrap(),
        );

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");
        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");

        assert_eq!(back.worker_url(), "http://worker-2:50051");
        assert_eq!(back.remote_partition(), 3);
        assert_eq!(
            back.fallbacks(),
            ["http://worker-3:50051", "http://worker-4:50051"],
            "failover targets must survive the trip, in order"
        );
        assert_eq!(back.schema(), plan.schema());
    }

    #[tokio::test]
    async fn an_empty_fallback_list_round_trips() {
        // The one-worker-fleet shape: a zero count, then straight into the inner plan bytes.
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            FlightReaderExec::with_fallbacks("http://only:50051", Vec::new(), 0, inner).unwrap(),
        );

        let bytes = serialize_plan(Arc::clone(&plan)).expect("encode");
        let back = deserialize_plan(&bytes, ctx.task_ctx().as_ref()).expect("decode");
        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");
        assert!(back.fallbacks().is_empty());
    }

    /// A fallback list that promises more URLs than the buffer holds must error, and a wild count
    /// must be refused before it becomes an allocation.
    #[tokio::test]
    async fn a_truncated_or_absurd_fallback_list_errors_rather_than_panics() {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        // url "u", partition 0, promises 2 fallbacks, then runs out after one.
        let mut truncated = header();
        put_str(&mut truncated, "u").unwrap();
        truncated.extend_from_slice(&0u32.to_le_bytes());
        truncated.extend_from_slice(&2u32.to_le_bytes());
        put_str(&mut truncated, "http://w:50051").unwrap();
        assert!(codec.try_decode(&truncated, &[], &task_ctx).is_err());

        // A count far beyond the bound is refused outright.
        let mut absurd = header();
        put_str(&mut absurd, "u").unwrap();
        absurd.extend_from_slice(&0u32.to_le_bytes());
        absurd.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = codec
            .try_decode(&absurd, &[], &task_ctx)
            .expect_err("an absurd fallback count must be rejected");
        assert!(
            err.to_string().contains("exceeds the maximum"),
            "got: {err}"
        );
    }

    /// The version byte leads the payload and survives a direct codec round trip.
    #[tokio::test]
    async fn the_payload_leads_with_the_format_version_and_round_trips() {
        let ctx = SessionContext::new();
        let inner = sample_plan_with_partitions(&ctx, 4).await;
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        let node: Arc<dyn ExecutionPlan> = Arc::new(
            FlightReaderExec::with_fallbacks(
                "http://worker-2:50051",
                vec!["http://worker-3:50051".into()],
                3,
                inner,
            )
            .unwrap(),
        );
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::clone(&node), &mut buf)
            .expect("encode");

        assert!(
            buf.starts_with(WIRE_MAGIC),
            "the magic must lead, so a decoder can rule out a foreign payload before reading fields"
        );
        assert_eq!(
            buf.get(WIRE_MAGIC.len()),
            Some(&FORMAT_VERSION),
            "the version must sit immediately behind the magic"
        );

        let back = codec.try_decode(&buf, &[], &task_ctx).expect("decode");
        let back = back
            .as_any()
            .downcast_ref::<FlightReaderExec>()
            .expect("still a FlightReaderExec");
        assert_eq!(back.worker_url(), "http://worker-2:50051");
        assert_eq!(back.remote_partition(), 3);
        assert_eq!(back.fallbacks(), ["http://worker-3:50051"]);
    }

    /// A payload from another build is refused, and the refusal names both versions — the fleet is
    /// running two binaries, and the error has to be readable as exactly that.
    #[tokio::test]
    async fn a_foreign_format_version_is_refused_naming_both_versions() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        let node: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://w:50051", 0, inner).unwrap());
        let mut buf = Vec::new();
        codec.try_encode(node, &mut buf).expect("encode");

        // The magic is left intact and everything behind the version is a payload this build
        // understands perfectly. Only the version differs — which is the whole failure mode: same
        // shape, different meaning, and the magic is what proves it got this far honestly.
        let foreign = FORMAT_VERSION.wrapping_add(7);
        buf[WIRE_MAGIC.len()] = foreign;

        let err = codec
            .try_decode(&buf, &[], &task_ctx)
            .expect_err("a payload from another build must be refused, not parsed");
        let msg = err.to_string();
        assert!(
            msg.contains(&format!("version {foreign}")),
            "the error must name the version in the bytes; got: {msg}"
        );
        assert!(
            msg.contains(&format!("speaks {FORMAT_VERSION}")),
            "the error must name the version this build expects; got: {msg}"
        );
        assert!(
            msg.contains("fleet is not one build"),
            "the error must say what a mismatch always means; got: {msg}"
        );
        assert!(
            !msg.contains("datafusion/issues"),
            "a deployment fact must not be reported as a DataFusion bug; got: {msg}"
        );
    }

    /// The refusal has to survive the path that actually runs. A worker never calls
    /// [`LldbCodec::try_decode`] itself — it calls [`deserialize_plan`], and `datafusion-proto`
    /// sits in between. This flips the version byte inside a real serialized plan and demands the
    /// message still arrives intact.
    #[tokio::test]
    async fn the_refusal_survives_the_real_deserialize_path() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        // Distinctive enough to locate unambiguously inside the proto blob.
        let url = "http://version-probe:50051";
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FlightReaderExec::new(url, 0, inner).unwrap());
        let mut bytes = serialize_plan(plan).expect("encode");

        // The extension payload is `magic | version | u32 LE url_len | url | …`, so the version
        // byte sits five bytes ahead of the url and the magic nine. Asserted rather than assumed,
        // so a format change fails here instead of quietly testing nothing.
        let at = bytes
            .windows(url.len())
            .position(|w| w == url.as_bytes())
            .expect("the url is in the serialized plan");
        let version_at = at - 5;
        assert_eq!(
            &bytes[at - 5 - WIRE_MAGIC.len()..version_at],
            &WIRE_MAGIC[..],
            "expected the magic immediately ahead of the version byte"
        );
        assert_eq!(
            bytes[version_at], FORMAT_VERSION,
            "expected the version byte five bytes ahead of the url"
        );
        bytes[version_at] = FORMAT_VERSION.wrapping_add(1);

        let err = deserialize_plan(&bytes, ctx.task_ctx().as_ref())
            .expect_err("a plan from another build must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("fleet is not one build")
                && msg.contains(&format!("speaks {FORMAT_VERSION}")),
            "the refusal must reach the caller intact through datafusion-proto; got: {msg}"
        );
    }

    /// Truncated at every point in the header, and empty: all errors, none a panic.
    #[tokio::test]
    async fn a_payload_truncated_in_the_header_errors_rather_than_panics() {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        assert!(take_u8(&[]).is_err(), "an empty buffer has no version byte");
        assert!(
            codec.try_decode(&[], &[], &task_ctx).is_err(),
            "an empty payload must not panic"
        );
        assert!(
            codec.try_decode(&WIRE_MAGIC[..2], &[], &task_ctx).is_err(),
            "half a magic must not panic"
        );
        assert!(
            codec.try_decode(WIRE_MAGIC, &[], &task_ctx).is_err(),
            "a magic with no version behind it must not panic"
        );
        assert!(
            codec.try_decode(&header(), &[], &task_ctx).is_err(),
            "a header with no url behind it must not panic"
        );
    }

    /// A payload that is not this format at all is refused on the magic, and lands on the same
    /// conclusion as a version mismatch — because it has the same one cause.
    #[tokio::test]
    async fn a_wrong_magic_is_refused_with_the_same_diagnosis() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        let node: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://w:50051", 0, inner).unwrap());
        let mut buf = Vec::new();
        codec.try_encode(node, &mut buf).expect("encode");
        buf[0] = b'X';

        let err = codec
            .try_decode(&buf, &[], &task_ctx)
            .expect_err("a payload that is not this format must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("wire marker"),
            "the error must name the check that failed; got: {msg}"
        );
        assert!(
            msg.contains("fleet is not one build"),
            "the magic refusal must reach the same conclusion as the version one; got: {msg}"
        );
        assert!(
            !msg.contains("datafusion/issues"),
            "a deployment fact must not be reported as a DataFusion bug; got: {msg}"
        );
    }

    /// **The reason the magic exists.** A payload written before the header was added begins with
    /// `u32 LE url_len`, so its first byte is that length's *low byte* — and a worker URL of length
    /// 1 (or 257, or 513) makes that byte `0x01`, colliding with `FORMAT_VERSION` exactly. A
    /// version check alone would wave such a payload through and then misparse it, reporting a
    /// "truncated url" that points at the bytes instead of at the fleet. The magic makes it
    /// deterministic.
    #[tokio::test]
    async fn a_legacy_payload_whose_url_len_collides_with_the_version_is_refused_on_the_magic() {
        let ctx = SessionContext::new();
        let inner = sample_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let codec = LldbCodec;

        // Exactly the pre-version encoding: no magic, no version, straight into `u32 LE url_len`.
        // A one-character url is what makes the low byte collide.
        let mut legacy = Vec::new();
        put_str(&mut legacy, "u").unwrap();
        legacy.extend_from_slice(&0u32.to_le_bytes()); // remote_partition
        legacy.extend_from_slice(&0u32.to_le_bytes()); // fallback_count
        legacy.extend_from_slice(&serialize_plan(inner).expect("inner plan"));

        assert_eq!(
            legacy[0], FORMAT_VERSION,
            "test setup: this payload's first byte must actually collide with the version, \
             otherwise this test proves nothing"
        );

        let err = codec
            .try_decode(&legacy, &[], &task_ctx)
            .expect_err("a pre-version payload must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("wire marker"),
            "it must be the magic that catches this, not a later field; got: {msg}"
        );
        assert!(
            msg.contains("fleet is not one build"),
            "the operator must be pointed at the fleet; got: {msg}"
        );
        assert!(
            !msg.contains("truncated"),
            "the collision must not survive into a misleading downstream parse error; got: {msg}"
        );
    }

    /// A *different type* that calls itself `FlightReaderExec` — which is exactly what a second
    /// version of `lldb-qe-core` in one dependency graph looks like from the codec's side: the name
    /// matches, the `TypeId` does not, and the downcast returns `None`.
    #[derive(Debug)]
    struct ImposterFlightReader(Arc<PlanProperties>);

    impl DisplayAs for ImposterFlightReader {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{FLIGHT_READER_EXEC_NAME}")
        }
    }

    impl ExecutionPlan for ImposterFlightReader {
        fn name(&self) -> &str {
            FLIGHT_READER_EXEC_NAME
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.0
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            unimplemented!("exists only to be refused by the codec, never executed")
        }
    }

    /// The other half of a two-build fleet: the encode side would otherwise deny a node it
    /// provably encodes. The refusal has to name the real cause instead.
    ///
    /// Asserted against the codec directly, which is the backstop under the pre-serialization walk
    /// — the walk is what makes this message *deliverable*, and that is asserted separately.
    #[test]
    fn a_foreign_node_of_the_same_name_is_reported_as_two_builds_not_as_unencodable() {
        let plan = imposter();
        let mut buf = Vec::new();
        let err = LldbCodec
            .try_encode(plan, &mut buf)
            .expect_err("a FlightReaderExec from another build cannot be encoded");
        let msg = err.to_string();
        assert!(
            !msg.contains("LldbCodec cannot encode FlightReaderExec"),
            "the generic message denies something true; got: {msg}"
        );
        assert!(
            msg.contains("Two versions of lldb-qe-core are linked into this process"),
            "the error must name the real cause; got: {msg}"
        );
        assert!(
            msg.contains("fleet is not one build"),
            "the error must read the same way the decode-side refusal does; got: {msg}"
        );
    }

    #[tokio::test]
    async fn codec_refuses_unknown_nodes() {
        let ctx = SessionContext::new();
        let plan = sample_plan(&ctx).await;
        let mut buf = Vec::new();
        assert!(LldbCodec.try_encode(plan, &mut buf).is_err());
    }

    /// `buf` belongs to the caller, and a failed encode must be a no-op on it. Every current caller
    /// happens to discard the buffer on error — that is an invariant nobody wrote down and nothing
    /// enforced, and this is what replaces it.
    ///
    /// The failure is provoked where it really lives: the header, url, partition and fallbacks are
    /// all written before `encode_plan` is asked for the sub-plan, so an un-encodable `inner` is
    /// exactly a mid-write error.
    #[test]
    fn a_failed_encode_leaves_the_callers_buffer_untouched() {
        let staged: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://w:50051", 0, imposter()).unwrap());

        // Non-empty on the way in, so the assertion distinguishes "appended nothing" from
        // "truncated to empty".
        let prior = b"bytes the caller already had".to_vec();
        let mut buf = prior.clone();
        LldbCodec
            .try_encode(staged, &mut buf)
            .expect_err("the sub-plan is un-encodable, so the encode must fail");
        assert_eq!(
            buf, prior,
            "a failed encode must not leave a partial payload in the caller's buffer"
        );
    }

    /// The bound [`put_str`] enforces, tested at the seam that makes it testable at all: proving
    /// `s.len() as u32` wraps would need a 4 GiB string, proving [`len_prefix`] refuses needs an
    /// integer. Unreachable with worker URLs — the point is that a wrapped length writes a *shorter*
    /// prefix than the bytes behind it, so the decode succeeds and hands back a truncated URL, which
    /// is indistinguishable from a misconfigured fleet.
    ///
    /// 64-bit only because on a 32-bit target `usize::MAX == u32::MAX`, so there is no length to
    /// refuse and the expression below would overflow rather than test anything.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn a_string_longer_than_the_length_prefix_can_carry_is_refused() {
        assert_eq!(len_prefix(0).unwrap(), 0);
        assert_eq!(
            len_prefix(u32::MAX as usize).unwrap(),
            u32::MAX,
            "the largest representable length is still legal"
        );

        let too_long = u32::MAX as usize + 1;
        let err = len_prefix(too_long).expect_err("one byte past the prefix must not wrap");
        let msg = err.to_string();
        assert!(
            msg.contains(&too_long.to_string()) && msg.contains("truncate"),
            "the refusal must name the length and what it would otherwise do; got: {msg}"
        );
    }

    /// The mechanism this file exists to guarantee: a refusal with guidance is raised *before*
    /// `physical_plan_to_bytes_with_extension_codec` is entered, so nothing wraps it.
    ///
    /// The two-builds diagnosis is used as the probe because it can be provoked with a local type;
    /// `iceberg_scan.rs` proves the same property for the `IcebergTableScan` refusal, which needs a
    /// real catalog to construct.
    #[test]
    fn a_guided_refusal_reaches_the_caller_without_datafusion_protos_wrapper() {
        let plan: Arc<dyn ExecutionPlan> = imposter();

        let err = serialize_plan(plan).expect_err("a foreign FlightReaderExec cannot be encoded");
        let msg = err.to_string();
        assert!(
            msg.contains("Two versions of lldb-qe-core are linked into this process"),
            "the guidance must survive the trip; got: {msg}"
        );
        assert_no_datafusion_wrapper(&msg);
    }

    /// The reason the walk is hand-written rather than a `TreeNode::apply`: a `FlightReaderExec`'s
    /// `inner` is not a child, so a children-only walk sees a perfectly encodable leaf and hands
    /// the sub-plan to the codec — which is the one place the wrapper would reappear.
    #[test]
    fn the_walk_descends_into_a_remote_stages_sub_plan() {
        let staged: Arc<dyn ExecutionPlan> =
            Arc::new(FlightReaderExec::new("http://w:50051", 0, imposter()).unwrap());
        assert!(
            staged.children().is_empty(),
            "test setup: the sub-plan must be invisible to a children-only walk, or this proves \
             nothing"
        );

        let err = serialize_plan(staged).expect_err("the nested node is still un-encodable");
        let msg = err.to_string();
        assert!(
            msg.contains("Two versions of lldb-qe-core are linked into this process"),
            "a refusal from inside a remote stage must arrive the same way; got: {msg}"
        );
        assert_no_datafusion_wrapper(&msg);
    }

    /// An [`ImposterFlightReader`] as a plan, with the properties a leaf needs.
    fn imposter() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        Arc::new(ImposterFlightReader(Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ))))
    }
}
