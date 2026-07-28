//! **Cancelling a running query** — the registry a `do_action("cancel", <query id>)` reaches into,
//! and the signal it delivers.
//!
//! # The gap this closes
//!
//! Hanging up already removed a *queued* query: the future awaiting [`Admission::acquire`] is
//! dropped, its `QueuePlace` gives the place back, and `ActiveQuery` closes the history row. A query
//! that already held a slot ran to completion regardless, **holding that slot the whole time**. On a
//! warehouse with a bounded number of slots that is not merely untidy: one expensive mistake
//! occupies a slot for its full duration and everything queued behind it waits for a query nobody
//! wants any more. Returning the slot is the entire point of this module; a cancellation that
//! marked history and left the slot held would have done nothing useful.
//!
//! # How a slot is actually returned — by dropping a future, not by calling anything
//!
//! There is no `QuerySlot::release()`, deliberately ([`crate::scheduler`] argues why), and this
//! module does not add one. Cancellation is delivered as a **signal**, and the query's own task is
//! what acts on it: [`crate::server`]'s `run_query` runs the admit-and-execute future in a
//! `tokio::select!` against [`RunningQuery::cancelled`], so a cancellation drops that future — which
//! drops the [`QuerySlot`](crate::scheduler::QuerySlot) guard, which hands the permit straight to the
//! next waiter in line. The queue advances because the guard already worked that way for failures
//! and panics; cancellation is just a fourth way out of the same scope.
//!
//! That is also why the registry stores a `watch::Sender` and nothing that resembles a task handle.
//! Aborting a `JoinHandle` would cancel the task from outside and leave nobody to write the history
//! row; signalling from inside keeps every exit path running the same bookkeeping.
//!
//! One consequence of that order deserves naming rather than discovering. `tokio::select!` drops the
//! losing future *before* it evaluates the winning branch's handler, so the slot goes back before
//! the `cancelled` row is written — and the next waiter can be admitted, and stamped `started_at`,
//! a fraction of a millisecond before the cancelled row is stamped `finished_at`. A sweep line over
//! history ([`crate::query_log::peak_concurrency`]) therefore reads a sliver of overlap that did not
//! really happen. The alternative is holding a warehouse's slot across a control-plane round trip
//! so that a *reporting instrument* comes out tidier, which is precisely the trade this module
//! exists to stop making. `query_cancel`'s acceptance test asserts the overlap is one write's worth
//! of time rather than one query's, which is the property that actually matters.
//!
//! # Three boundaries, stated plainly
//!
//! 1. **It is per coordinator process.** The registry holds the queries *this* process is running.
//!    A cancel for a query owned by another coordinator is answered "not running here" rather than
//!    forwarded, for the same reason admission is per process ([`crate::scheduler`]): there is no
//!    shared state between coordinators, and inventing one for cancellation alone would be half of
//!    fleet-wide admission built in the wrong place. Send the cancel to the coordinator whose
//!    `queries.coordinator` value names it.
//! 2. **The handle is the history row's id**, which means cancellation needs a services database.
//!    Without one a query has no id (`QueryOutcome::query_id` is `None`), so there is nothing to
//!    name it by and the registry stays empty. Consistent with the rest of the system — no control
//!    plane, no control-plane features — and it costs a single-node user nothing, since a single-node
//!    user can hang up.
//! 3. **Worker-side work drains on its own.** See the next section; this is the choice the issue
//!    asked to be explicit about.
//!
//! # Propagation to the fleet: what this does, and what it does not
//!
//! **This does not send a cancellation across the Flight boundary.** No `do_action("cancel")` is
//! issued to workers, and a worker holds no notion of the query a stage belongs to. What actually
//! happens when the coordinator drops its execution is worth being precise about, because it is
//! neither "nothing" nor "everything":
//!
//! - The coordinator's `do_get` streams to its workers are dropped, which resets those gRPC streams.
//!   A worker still *producing* into a reset stream stops when its send fails, so a large scan being
//!   streamed back does stop fairly promptly. That is a side effect of the transport, not a
//!   guarantee this module makes.
//! - A stage that has already been **materialized into the worker's [`StageCache`]**, or is midway
//!   through materializing, runs to completion regardless: the cache deliberately decouples
//!   producing a stage from the consumers pulling it (that is what makes a shuffle materialize
//!   once), so no consumer going away can stop it.
//!
//! So the honest statement is: **the coordinator's slot is returned immediately and deterministically;
//! worker-side CPU drains on its own, promptly but not immediately, and with no upper bound this
//! module can state.** That is a legitimate first step and it is deliberately the whole of it —
//! a real fleet-side cancellation needs a query identifier travelling inside the ticket, a
//! per-query index on each worker, and a decision about what a half-materialized cache entry means
//! for the next consumer that asks for it. Each of those is a design with its own failure modes, and
//! none of them is needed to stop one expensive mistake from holding a warehouse's slot.
//!
//! # Authorization lives in [`crate::server`], not here
//!
//! This module is a map and a channel. Whether a caller may cancel a given query — the tenant
//! boundary and the [`Privilege::Cancel`](crate::rbac::Privilege::Cancel) grant — is decided by
//! [`Coordinator::cancel_query`](crate::server::Coordinator::cancel_query), which is where the
//! credential is, for the same reason [`crate::rbac`] is a pure function of grants and a plan: a
//! registry that checked permissions would be a registry that could not be tested without a
//! database.
//!
//! [`Admission::acquire`]: crate::scheduler::Admission::acquire
//! [`StageCache`]: crate::stage_cache::StageCache

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

/// The `do_action` type a client sends to stop a query.
pub const CANCEL_ACTION: &str = "cancel";

/// What the server answers a successful `do_action("cancel", ...)` with.
///
/// A body rather than an empty stream: a Flight client that gets zero results back cannot tell a
/// server that cancelled the query from one that ignored the action.
pub const CANCEL_ACCEPTED: &str = "cancelled";

/// Encode a query id for a cancel action's body.
///
/// Decimal ASCII, which is the *same* spelling the server already returns in the `lldb-query-id`
/// response header — so a client copies the header's bytes into the body verbatim and never has to
/// know an encoding at all. A little-endian `i64` would be marginally cheaper to parse and would
/// force every client to learn a second wire format for eight bytes.
pub fn encode_cancel_body(query_id: i64) -> Vec<u8> {
    query_id.to_string().into_bytes()
}

/// Decode a cancel action's body, or say precisely what is wrong with it.
pub fn decode_cancel_body(body: &[u8]) -> anyhow::Result<i64> {
    let text = std::str::from_utf8(body).map_err(|_| {
        anyhow::anyhow!("a cancel action's body must be a decimal query id in UTF-8")
    })?;
    text.trim().parse::<i64>().map_err(|_| {
        anyhow::anyhow!(
            "`{text}` is not a query id: send the decimal id the submission returned in the \
             `lldb-query-id` response header"
        )
    })
}

/// Who asked for a query to stop.
///
/// A struct rather than a bare string because the *reason text stored on the history row* is
/// composed from it in one place ([`Cancellation::reason`]), so every cancelled row reads the same
/// way whoever wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancellation {
    /// The user the credential named. `None` only where there was no identity to record — a
    /// coordinator with no services database, or one running `--allow-anonymous` against a caller
    /// that sent nothing.
    pub requested_by: Option<String>,
}

impl Cancellation {
    /// A cancellation attributed to `user`.
    pub fn by(user: impl Into<String>) -> Self {
        Self {
            requested_by: Some(user.into()),
        }
    }

    /// A cancellation with no identity behind it. See [`Cancellation::requested_by`].
    pub fn anonymous() -> Self {
        Self { requested_by: None }
    }

    /// What is written into `queries.error`.
    ///
    /// It says *cancelled* in the first word so a row is greppable as one, names who asked, and
    /// states the consequence — that no results reached the client — because "cancelled" alone
    /// leaves a reader wondering whether partial output was delivered. It never was: batches are
    /// collected whole before any are sent (see [`crate::engine`]).
    pub fn reason(&self) -> String {
        match &self.requested_by {
            Some(user) => format!(
                "cancelled: user `{user}` stopped this query with do_action(\"cancel\"). Its \
                 admission slot was returned to the warehouse at once; no results reached the \
                 client."
            ),
            None => "cancelled: this query was stopped with do_action(\"cancel\") by an \
                     unauthenticated caller (this coordinator enforces no identity). Its admission \
                     slot was returned to the warehouse at once; no results reached the client."
                .to_string(),
        }
    }
}

/// What the registry knows about a query that is running here — enough to decide whether the caller
/// may stop it, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningQueryInfo {
    /// The tenant that owns it. The tenant boundary is checked against this and never against
    /// anything the caller said.
    pub account_id: i64,
    /// The gate this query holds a slot on — a warehouse name, or [`DEFAULT_FLEET_KEY`] for a query
    /// routed at a raw `--workers` fleet. It is also the object a `CANCEL` grant is written
    /// against, which is the right pairing: the privilege names the compute the cancellation frees.
    ///
    /// Taken from the live target rather than re-derived from `queries.warehouse_id`, which is
    /// `ON DELETE SET NULL` and would read as "no warehouse" for a query whose warehouse was
    /// dropped mid-flight.
    ///
    /// [`DEFAULT_FLEET_KEY`]: crate::scheduler::DEFAULT_FLEET_KEY
    pub admission_key: String,
}

/// Every query this coordinator is currently running, by id.
///
/// Cheaply cloneable (it is an `Arc` inside), because the guard handed out by
/// [`QueryRegistry::register`] has to keep a handle in order to remove itself.
#[derive(Debug, Clone, Default)]
pub struct QueryRegistry {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// `std::sync::Mutex`, not tokio's: every critical section is a hash lookup and none awaits.
    running: Mutex<HashMap<i64, Entry>>,
    /// Distinguishes one registration of an id from a later one. See [`RunningQuery::drop`].
    next_token: AtomicU64,
}

#[derive(Debug)]
struct Entry {
    token: u64,
    info: RunningQueryInfo,
    signal: watch::Sender<Option<Cancellation>>,
}

impl QueryRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a query as running here, returning the guard that de-registers it.
    ///
    /// The guard is the only way out, exactly like [`QuerySlot`](crate::scheduler::QuerySlot): there
    /// is no `unregister(id)`, so no exit path — success, `?`, panic, or the request future being
    /// dropped — can leave a finished query looking cancellable. A stale entry would be worse than
    /// untidy: the id is a database identity value and is never reused, but a leaked entry pins a
    /// `watch` channel for the life of the process and makes `len()`, which an operator would
    /// reasonably read as "queries in flight", drift upward forever.
    pub fn register(
        &self,
        query_id: i64,
        account_id: i64,
        admission_key: impl Into<String>,
    ) -> RunningQuery {
        let (signal, receiver) = watch::channel(None);
        let token = self.inner.next_token.fetch_add(1, Ordering::AcqRel);
        let info = RunningQueryInfo {
            account_id,
            admission_key: admission_key.into(),
        };
        self.lock().insert(
            query_id,
            Entry {
                token,
                info,
                signal,
            },
        );
        RunningQuery {
            registry: self.clone(),
            query_id,
            token,
            receiver,
        }
    }

    /// What is known about `query_id`, or `None` if it is not running here.
    ///
    /// Deliberately separate from [`QueryRegistry::cancel`] so a caller can authorize *before* it
    /// signals — there is no interleaving in which a refused cancellation still stops a query,
    /// because signalling is a second call the refusal never reaches.
    pub fn describe(&self, query_id: i64) -> Option<RunningQueryInfo> {
        self.lock().get(&query_id).map(|entry| entry.info.clone())
    }

    /// Signal `query_id` to stop. `false` if it is not running here — which includes the query
    /// having finished between a [`describe`](QueryRegistry::describe) and this call.
    ///
    /// Idempotent: cancelling twice is one cancellation, because the first value written to the
    /// channel is the one the query observes and the second changes nothing it can see.
    pub fn cancel(&self, query_id: i64, cancellation: Cancellation) -> bool {
        let mut running = self.lock();
        let Some(entry) = running.get_mut(&query_id) else {
            return false;
        };
        if entry.signal.borrow().is_none() {
            // `send` cannot fail: the receiver lives in the guard, and the guard is what keeps this
            // entry in the map. `send_replace` avoids having to reason about that at every call.
            entry.signal.send_replace(Some(cancellation));
        }
        true
    }

    /// How many queries are running here. For logs and tests; not a scheduling input.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing is running here.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<i64, Entry>> {
        self.inner
            .running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A query's registration, removed when this is dropped.
///
/// Holds the receiving half of the cancellation channel, so the task that owns the query is also the
/// task that observes the signal — which is what keeps the terminal history write on the same code
/// path as every other outcome.
#[derive(Debug)]
pub struct RunningQuery {
    registry: QueryRegistry,
    query_id: i64,
    /// Which registration of `query_id` this guard belongs to. See [`RunningQuery::drop`].
    token: u64,
    receiver: watch::Receiver<Option<Cancellation>>,
}

impl RunningQuery {
    /// The id this guard registered.
    pub fn query_id(&self) -> i64 {
        self.query_id
    }

    /// Resolve when — and only when — this query is cancelled.
    ///
    /// Never resolves otherwise, which is the property that makes it safe to put in a
    /// `tokio::select!` against the query's own execution: the losing branch of a `select!` is
    /// dropped, so a future that resolved spuriously here would silently kill a healthy query and
    /// return its slot mid-execution. That is why the "sender gone" branch parks forever instead of
    /// treating a closed channel as a cancellation — it cannot happen while this guard is alive
    /// (the guard's own registry entry owns the sender), and if it somehow did, *not* cancelling is
    /// the failure this codebase would rather have.
    pub async fn cancelled(&mut self) -> Cancellation {
        loop {
            // Cloned out before the await so the `Ref` borrow is released; holding it across an
            // await point would deadlock every other reader of the channel.
            let current = self.receiver.borrow_and_update().clone();
            if let Some(cancellation) = current {
                return cancellation;
            }
            if self.receiver.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

impl Drop for RunningQuery {
    /// Remove this query's entry — but only if it is still *this* registration.
    ///
    /// The token guards a case that cannot arise today and would be silent if it ever did: query
    /// ids come from a Postgres identity column and are never reused, so no id is registered twice.
    /// If one ever were, a bare `remove(&id)` from the older guard would delete the *newer* query's
    /// entry and quietly make a running query uncancellable. Comparing tokens costs one integer.
    fn drop(&mut self) {
        let mut running = self.registry.lock();
        if running
            .get(&self.query_id)
            .is_some_and(|entry| entry.token == self.token)
        {
            running.remove(&self.query_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_cancel_body_round_trips_the_id_a_submission_returned() {
        for id in [1i64, 42, i64::MAX, -1] {
            assert_eq!(decode_cancel_body(&encode_cancel_body(id)).unwrap(), id);
        }
        // Whitespace from a shell pipeline is tolerated; anything else is refused with a message
        // naming where the id comes from.
        assert_eq!(decode_cancel_body(b" 7\n").unwrap(), 7);
        for bad in [b"".as_slice(), b"seven", b"7.0", &[0xFF, 0xFE]] {
            let err = decode_cancel_body(bad).expect_err("not an id");
            assert!(!err.to_string().is_empty());
        }
        let err = decode_cancel_body(b"seven").unwrap_err().to_string();
        assert!(err.contains("lldb-query-id"), "{err}");
    }

    #[test]
    fn a_reason_names_who_asked_and_is_greppable_as_a_cancellation() {
        let reason = Cancellation::by("dana").reason();
        assert!(reason.starts_with("cancelled: "), "{reason}");
        assert!(reason.contains("dana"), "{reason}");
        // The consequence, not just the event: a reader must not be left wondering whether partial
        // rows were delivered.
        assert!(reason.contains("no results reached the client"), "{reason}");
        let anonymous = Cancellation::anonymous().reason();
        assert!(anonymous.starts_with("cancelled: "), "{anonymous}");
        assert!(!anonymous.contains("user `"), "{anonymous}");
        // Both fit on a row without truncation.
        for reason in [reason, anonymous] {
            assert!(reason.chars().count() < crate::query_log::MAX_ERROR_LEN);
        }
    }

    #[tokio::test]
    async fn a_registered_query_is_describable_and_cancellable_by_id() {
        let registry = QueryRegistry::new();
        let mut running = registry.register(7, 42, "analytics");
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.describe(7),
            Some(RunningQueryInfo {
                account_id: 42,
                admission_key: "analytics".to_string(),
            })
        );
        assert_eq!(registry.describe(8), None, "only what is registered");

        // Not cancelled yet: the future must not resolve, or a `select!` around it would kill a
        // healthy query.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), running.cancelled())
                .await
                .is_err(),
            "an uncancelled query must never resolve this"
        );

        assert!(registry.cancel(7, Cancellation::by("dana")));
        let cancellation = tokio::time::timeout(Duration::from_secs(5), running.cancelled())
            .await
            .expect("the signal must arrive");
        assert_eq!(cancellation, Cancellation::by("dana"));
    }

    #[tokio::test]
    async fn cancelling_is_idempotent_and_keeps_the_first_asker() {
        let registry = QueryRegistry::new();
        let mut running = registry.register(1, 1, "wh");
        assert!(registry.cancel(1, Cancellation::by("first")));
        assert!(registry.cancel(1, Cancellation::by("second")), "still here");
        assert_eq!(running.cancelled().await, Cancellation::by("first"));
        // …and it stays resolved rather than being a one-shot edge.
        assert_eq!(running.cancelled().await, Cancellation::by("first"));
    }

    #[test]
    fn cancelling_something_that_is_not_running_here_is_a_no_op() {
        let registry = QueryRegistry::new();
        assert!(!registry.cancel(999, Cancellation::anonymous()));
        assert!(registry.is_empty());
    }

    #[test]
    fn a_finished_query_de_registers_itself() {
        // The whole reason the guard has no `unregister`: every exit path drops it, so a query that
        // ended can never look cancellable.
        let registry = QueryRegistry::new();
        {
            let _running = registry.register(3, 1, "wh");
            assert_eq!(registry.len(), 1);
        }
        assert!(registry.is_empty());
        assert_eq!(registry.describe(3), None);
        assert!(!registry.cancel(3, Cancellation::anonymous()));
    }

    #[test]
    fn an_older_guard_never_de_registers_a_newer_registration() {
        // Cannot happen with database-issued ids; asserted because the failure mode if it ever did
        // would be a *running* query silently becoming uncancellable, with nothing in the logs.
        let registry = QueryRegistry::new();
        let first = registry.register(5, 1, "wh");
        let _second = registry.register(5, 2, "etl");
        drop(first);
        assert_eq!(
            registry.describe(5).map(|info| info.account_id),
            Some(2),
            "the newer registration must survive the older guard's drop"
        );
    }
}
