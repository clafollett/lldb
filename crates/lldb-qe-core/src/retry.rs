//! Retry *policy*: is this failure worth trying somewhere else, and how long should we wait?
//!
//! Distribution turns one process's fate into N processes' fate. A stage pulled from a worker that
//! has just been scaled in, rescheduled, or OOM-killed takes the whole query down with it — which
//! is what issue #6 made *visible* (the dead node is named) and deliberately left unfixed. Fixing it
//! needs two things: a way to re-run the stage somewhere else, and a way to tell which failures
//! deserve that. The first already exists — stages are content-addressed and the worker
//! materializes each one exactly once into a [`crate::stage_cache::StageCache`], so re-running a
//! stage on another worker is idempotent and yields identical output. This module is the second.
//!
//! # Why classification is the whole game
//!
//! Retrying is not free and it is not always right. Two failures arrive at the same pull boundary
//! wearing the same clothes:
//!
//! - **The worker is gone.** Connection refused, a broken connection mid-stream, `UNAVAILABLE` from
//!   a draining task. Nothing about the request was wrong; another worker will answer it correctly.
//!   *Retriable.*
//! - **The request is wrong.** The plan does not deserialize (`INVALID_ARGUMENT`), the stage failed
//!   to materialize (`INTERNAL`), the partition index is out of range. Every worker in the fleet
//!   runs the identical build (see `CLAUDE.md`), so every worker will fail identically. Retrying
//!   turns one clear error into `N` copies of itself, `N`× the latency, and a fleet-wide load
//!   spike — while hiding the bug that caused it. *Fatal.*
//!
//! Those two tonic status codes are exactly what [`crate::flight`]'s `do_get` produces for its own
//! faults, which is what makes the mapping below a real contract and not guesswork.
//!
//! # The default is `Fatal`, on purpose
//!
//! An error we do not recognize gets [`Retriability::Fatal`]. The asymmetry is deliberate: an
//! unnecessary retry of a deterministic failure amplifies it `N` times and buries the cause under a
//! target-exhaustion message, whereas refusing to retry something that *was* transient surfaces a
//! clear, named failure that an operator (or a future commit here) can act on. Failing loudly on the
//! unknown is how the retry loop stays honest about what it actually understands.

use std::time::Duration;

use arrow_flight::error::FlightError;
use tonic::{Code, Status};

/// Whether a failed stage pull is worth reassigning to another worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retriability {
    /// Transport loss or a worker-side "not me / not now" — another worker will answer correctly.
    Retriable,
    /// Deterministic given the request. Every identical worker fails identically, so surface it.
    Fatal,
}

/// Classify a stage-pull failure by walking the error's cause chain.
///
/// The chain matters: by the time a failure reaches the pull boundary it is wrapped in
/// [`anyhow`] context (`connecting to worker …`, `streaming partition … from worker …`) and often
/// in a [`FlightError`] as well, so the load-bearing [`Status`] or [`tonic::transport::Error`] sits
/// several layers down. We walk from the outside in and take the **first** layer we recognize —
/// the innermost recognized cause is the one that actually describes the fault, and the outer
/// layers are ours.
///
/// Anything unrecognized is [`Retriability::Fatal`]; see the module docs for why that asymmetry is
/// the safe one.
pub fn classify(err: &anyhow::Error) -> Retriability {
    for cause in err.chain() {
        if let Some(verdict) = classify_std(cause) {
            return verdict;
        }
    }
    Retriability::Fatal
}

/// Recognize one link of a cause chain, or `None` if this link says nothing about retriability.
fn classify_std(err: &(dyn std::error::Error + 'static)) -> Option<Retriability> {
    if let Some(status) = err.downcast_ref::<Status>() {
        return Some(classify_code(status.code()));
    }
    // A `tonic::transport::Error` is the connect/connection layer failing: refused, reset, closed
    // mid-stream. That is a worker problem, never a request problem.
    //
    // One caveat, deliberately not "fixed": tonic also uses this type for an invalid URI, which is
    // a permanent config fault rather than worker loss. It cannot be told apart from outside the
    // crate — `Kind` is `pub(crate)` with no accessor, so the only signal is the Display string
    // ("invalid URI" vs "transport error"), and pinning correctness to a dependency's error text is
    // a worse bug than the one it fixes. It does not bite us: the pull path builds its channel with
    // `Channel::from_shared`, which returns `http::uri::InvalidUri` — a distinct type that falls to
    // the unknown-is-fatal default. Both behaviors are pinned by tests below. If a future call site
    // reaches for `Endpoint::from_shared` instead, validate the URL there rather than trying to
    // classify it here.
    if err.downcast_ref::<tonic::transport::Error>().is_some() {
        return Some(Retriability::Retriable);
    }
    if let Some(flight) = err.downcast_ref::<FlightError>() {
        return classify_flight(flight);
    }
    None
}

/// Unwrap a [`FlightError`] to the transport fault underneath it.
///
/// `fetch_stream` boxes the worker's `Status` into [`FlightError::ExternalError`], and the Flight
/// decoder can hand back [`FlightError::Tonic`] directly, so both wrappers have to be seen through.
/// A decode/protocol error, by contrast, is a genuine disagreement about bytes: it is not a
/// transport fault, so it falls through to the `Fatal` default rather than being replayed against
/// the fleet.
fn classify_flight(err: &FlightError) -> Option<Retriability> {
    match err {
        FlightError::Tonic(status) => Some(classify_code(status.code())),
        FlightError::ExternalError(inner) => {
            let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(inner.as_ref());
            while let Some(err) = cause {
                if let Some(verdict) = classify_std(err) {
                    return Some(verdict);
                }
                cause = err.source();
            }
            None
        }
        _ => None,
    }
}

/// The gRPC status-code contract, written out in full rather than with a catch-all so that adding a
/// code to the retriable set is a deliberate, reviewable edit.
fn classify_code(code: Code) -> Retriability {
    match code {
        // The worker is unreachable, shutting down, overloaded, or the call died in flight. Nothing
        // is wrong with the request; a healthy worker answers it identically.
        Code::Unavailable
        | Code::Unknown
        | Code::Aborted
        | Code::Cancelled
        | Code::DeadlineExceeded
        | Code::ResourceExhausted => Retriability::Retriable,

        // Deterministic given the request. `InvalidArgument` is what our `do_get` returns for a bad
        // ticket, a stage-id mismatch, a partition out of range, or a plan that will not
        // deserialize; `Internal` is what it returns when the stage failed to materialize. Both
        // reproduce on every worker in an identical fleet.
        Code::InvalidArgument
        | Code::Internal
        | Code::Unimplemented
        | Code::NotFound
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::AlreadyExists
        | Code::DataLoss
        | Code::Unauthenticated => Retriability::Fatal,

        // `Ok` should never reach an error path at all; treat the impossible as fatal so it is
        // reported instead of quietly replayed.
        Code::Ok => Retriability::Fatal,
    }
}

/// How long to wait between reassignment attempts.
///
/// Backoff exists because the interesting failure is *correlated*: a scale-in event, a rolling
/// deploy, or a node loss tends to take a worker away at the same moment several stages are pulling
/// from it. Hammering the next candidate instantly turns one dead node into a thundering herd. The
/// waits are deliberately small — the retry budget is bounded by the candidate list, not by a
/// clock, so this is a breather, not a recovery window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Wait before the first reassignment.
    pub base_backoff: Duration,
    /// Ceiling on the wait, however many candidates have already failed.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    /// 50 ms doubling to a 1 s cap: long enough to let a connection-refused storm settle, short
    /// enough that a fleet-wide outage still fails the query in seconds rather than minutes.
    fn default() -> Self {
        Self {
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl RetryPolicy {
    /// The wait before attempt `attempt` (0-based: attempt 0 is the first *retry*, i.e. the second
    /// candidate).
    ///
    /// Exponential, saturating at [`max_backoff`](Self::max_backoff). Written with `checked_shl`
    /// and `checked_mul` rather than `2u32.pow(attempt)` because a retry path is the last place
    /// that should be able to panic: an arithmetic overflow while handling a worker loss would
    /// convert a recoverable failure into a coordinator crash.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
        self.base_backoff
            .checked_mul(factor)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_err(code: Code) -> anyhow::Error {
        anyhow::Error::new(Status::new(code, "test"))
    }

    #[test]
    fn transport_loss_and_worker_unavailability_are_retriable() {
        for code in [
            Code::Unavailable,
            Code::Unknown,
            Code::Aborted,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
        ] {
            assert_eq!(
                classify(&status_err(code)),
                Retriability::Retriable,
                "{code:?} means the worker, not the request"
            );
        }
    }

    #[test]
    fn request_level_faults_are_fatal() {
        // `InvalidArgument` (bad ticket / plan deserialize) and `Internal` (stage failed to
        // materialize) are exactly what our own `do_get` emits — the two that must never be
        // replayed across the fleet.
        for code in [
            Code::InvalidArgument,
            Code::Internal,
            Code::Unimplemented,
            Code::NotFound,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::AlreadyExists,
            Code::DataLoss,
            Code::Unauthenticated,
            Code::Ok,
        ] {
            assert_eq!(
                classify(&status_err(code)),
                Retriability::Fatal,
                "{code:?} reproduces on every identical worker"
            );
        }
    }

    /// A *genuine* connect failure — the worker-loss shape this whole module exists for.
    ///
    /// This deliberately makes a real TCP attempt against a port nothing is listening on rather
    /// than synthesizing a `transport::Error` from a malformed URI. Those are different faults
    /// that happen to share a Rust type, and a test that conflates them documents the opposite of
    /// the truth: it reads as "a malformed URI is retriable", which is exactly the wrong lesson
    /// (see the sibling test below, and `flight::fetch_partition`).
    #[tokio::test]
    async fn a_connect_failure_is_retriable() {
        // Bind then drop, so the port is real, unused, and refuses immediately.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let err = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("a well-formed url parses")
            .connect()
            .await
            .expect_err("nothing is listening on that port");
        assert_eq!(classify(&anyhow::Error::new(err)), Retriability::Retriable);
    }

    /// A malformed worker URL is a coordinator-side configuration fault: it reproduces identically
    /// on every worker, so replaying it across the fleet only turns one bug into N.
    ///
    /// This pins the type the *production* path actually produces. `Channel::from_shared` — what
    /// [`crate::flight::fetch_partition`] calls — returns `http::uri::InvalidUri`, which hits the
    /// documented unknown-is-fatal default. Note that `Endpoint::from_shared` returns a
    /// `transport::Error` for the same input; the two constructors disagree, which is precisely
    /// why this is worth a test that names the constructor under test.
    #[test]
    fn a_malformed_worker_url_is_fatal() {
        let err = tonic::transport::Channel::from_shared("not a uri at all")
            .expect_err("a malformed uri must not parse");
        assert_eq!(classify(&anyhow::Error::new(err)), Retriability::Fatal);
    }

    #[test]
    fn an_unrecognized_error_is_fatal() {
        // The documented asymmetry: retrying what we do not understand would multiply a real bug by
        // the fleet size and hide it behind a target-exhaustion message.
        assert_eq!(
            classify(&anyhow::anyhow!("something we have never seen")),
            Retriability::Fatal
        );
    }

    #[test]
    fn classification_sees_through_anyhow_context() {
        // This is the shape the pull boundary actually produces: the status is buried under the
        // context `fetch_stream` adds.
        let err = anyhow::Error::new(Status::unavailable("worker draining"))
            .context("do_get request to http://w:50051")
            .context("streaming partition 0 from worker http://w:50051");
        assert_eq!(classify(&err), Retriability::Retriable);
    }

    #[test]
    fn classification_sees_through_flight_error_wrappers() {
        // `fetch_stream` boxes the worker's status into `ExternalError`; the decoder can also hand
        // back `Tonic` directly. Both have to unwrap to the same verdict.
        let external = anyhow::Error::new(FlightError::ExternalError(Box::new(
            Status::unavailable("connection reset mid-stream"),
        )))
        .context("streaming partition 3 from worker http://w:50051");
        assert_eq!(classify(&external), Retriability::Retriable);

        let tonic = anyhow::Error::new(FlightError::from(Status::invalid_argument("bad ticket")));
        assert_eq!(classify(&tonic), Retriability::Fatal);

        // A decode error is a disagreement about bytes, not a transport fault: fatal by default.
        let decode = anyhow::Error::new(FlightError::DecodeError("truncated".into()));
        assert_eq!(classify(&decode), Retriability::Fatal);
    }

    #[test]
    fn backoff_is_monotone_capped_and_never_overflows() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.backoff(0), Duration::from_millis(50));
        assert_eq!(policy.backoff(1), Duration::from_millis(100));
        assert_eq!(policy.backoff(2), Duration::from_millis(200));

        let mut previous = Duration::ZERO;
        for attempt in 0..=64 {
            let wait = policy.backoff(attempt);
            assert!(wait >= previous, "backoff must not go backwards");
            assert!(wait <= policy.max_backoff, "backoff must respect the cap");
            previous = wait;
        }
        // Attempt 64 is far past `u32`'s shift range: it must saturate, not panic.
        assert_eq!(policy.backoff(64), policy.max_backoff);
        assert_eq!(policy.backoff(u32::MAX), policy.max_backoff);
    }

    #[test]
    fn a_policy_with_a_tiny_cap_still_behaves() {
        // A cap below the base is legal (and degenerate): every wait is the cap, still monotone.
        let policy = RetryPolicy {
            base_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_millis(10),
        };
        assert_eq!(policy.backoff(0), Duration::from_millis(10));
        assert_eq!(policy.backoff(5), Duration::from_millis(10));
    }
}
