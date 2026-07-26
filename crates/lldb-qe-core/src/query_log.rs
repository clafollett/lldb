//! **Query history** — the control plane's record of what ran, when, and how it ended.
//!
//! # Why this is control-plane state
//!
//! Everything about executing a query is data-plane: a plan, some Arrow batches, a stage cache
//! that can be thrown away. The *fact that the query happened* is not. "What did this tenant run
//! last night", "which query is still going", "why did that one fail", "how long did it wait for
//! a slot" are questions asked minutes or months later, by a process that was not the one running
//! the query. So they live in Postgres, next to the accounts and warehouses they reference (see
//! [`crate::services`] for the general argument).
//!
//! # The lifecycle
//!
//! ```text
//!            submit                admit                 finish
//!   (client) ──────▶ queued ──────────────▶ running ──────────────▶ succeeded
//!                      │                       │                 └─▶ failed
//!                      └───────────────────────────────────────────▶ failed
//!                        (rejected, or the coordinator shut down)
//! ```
//!
//! Three timestamps, and the gaps between them are the point:
//!
//! - `submitted_at → started_at` is **queue time**: how long admission control made it wait.
//! - `started_at → finished_at` is **execution time**.
//! - a row with `started_at IS NULL` and a terminal state never ran at all.
//!
//! That is why `started_at` is a separate column rather than being folded into `submitted_at`:
//! without it, a warehouse that is saturated and a warehouse that is slow look identical in the
//! history, and they call for opposite fixes (more compute vs. a better plan).
//!
//! # What this does NOT do
//!
//! - **No reaping.** If a coordinator process dies mid-query, its row stays `running` (or
//!   `queued`) forever — nothing here sweeps them. That is why every row records the
//!   `coordinator` that owns it: a reaper is a later issue, and it will need to know whose rows
//!   to touch. Treat a `running` row from a coordinator that is no longer alive as unknown, not
//!   as running.
//! - **No result storage.** [`QueryRecord::result_rows`] is a count, not a cache. The batches are
//!   streamed to the client and dropped.
//! - **No tenancy enforcement.** Callers pass the `account_id` they resolved; nothing here checks
//!   that the caller is entitled to it. That is #19's job, exactly as in [`crate::warehouse`].
//! - **No retention policy.** History accumulates. `queries_account_submitted_idx` keeps the read
//!   path cheap; deleting old rows is an operator's decision, not this module's.
//!
//! # Conventions
//!
//! Same as [`crate::services`] and [`crate::warehouse`]: runtime `sqlx::query`/`query_as` with
//! bind parameters (never the `query!` macros, which would need a live database at build time),
//! and [`anyhow::Result`] with `.context(...)` naming the query id an operator would grep for.

use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};

use crate::services::ServicesDb;

/// Longest error message stored on a query row.
///
/// A DataFusion failure can carry a whole plan dump, and a fleet under a systematic fault writes
/// one per query. Truncating keeps a bad afternoon from turning history into the largest table in
/// the database; the head of the message is the part that identifies the fault anyway.
pub const MAX_ERROR_LEN: usize = 4000;

/// Where a query is in its life.
///
/// Four states, and the split between `queued` and `running` is the whole reason this issue
/// exists: a scheduler that bounds concurrency *must* be able to say "accepted, but not yet
/// executing", and a history that cannot express that cannot explain a slow afternoon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Accepted by a coordinator and waiting for an admission slot.
    Queued,
    /// Holding a slot and executing.
    Running,
    /// Finished, results delivered.
    Succeeded,
    /// Finished without results. `error` says why.
    Failed,
}

/// Every legal state, in lifecycle order. Kept next to the enum so a fifth state cannot be added
/// without this (and the migration's `CHECK`) being updated.
pub const QUERY_STATES: [QueryState; 4] = [
    QueryState::Queued,
    QueryState::Running,
    QueryState::Succeeded,
    QueryState::Failed,
];

impl QueryState {
    /// The spelling stored in the `state` column and accepted by the migration's `CHECK`.
    pub fn as_str(self) -> &'static str {
        match self {
            QueryState::Queued => "queued",
            QueryState::Running => "running",
            QueryState::Succeeded => "succeeded",
            QueryState::Failed => "failed",
        }
    }

    /// True for a state a query never leaves. The one property callers actually branch on, so it
    /// is stated once here rather than re-derived at each call site.
    pub fn is_terminal(self) -> bool {
        matches!(self, QueryState::Succeeded | QueryState::Failed)
    }
}

impl fmt::Display for QueryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for QueryState {
    type Err = anyhow::Error;

    /// Parse a stored state. An unknown value is an error naming the legal set rather than a
    /// silent fallback — a row the database somehow holds with an uninterpretable state must stop
    /// whoever is reading history, not quietly become "failed".
    fn from_str(s: &str) -> Result<Self> {
        QUERY_STATES
            .into_iter()
            .find(|state| state.as_str() == s)
            .with_context(|| {
                format!(
                    "unknown query state `{s}` (expected one of: {})",
                    QUERY_STATES
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }
}

/// One row of query history.
///
/// Fields are public because this is a record, not an invariant-holding type: the invariants
/// (a legal state, an error only on failure) are enforced by the API below and by the database's
/// own constraints, which is where they survive a second writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRecord {
    pub id: i64,
    /// The owning tenant. Queries are never global.
    pub account_id: i64,
    /// The warehouse it ran on, or `None` for a query routed at a raw `--workers` fleet. Also
    /// `None` once that warehouse is dropped — `ON DELETE SET NULL`, because history must outlive
    /// the compute that served it.
    pub warehouse_id: Option<i64>,
    pub sql_text: String,
    pub state: QueryState,
    /// When the coordinator accepted it, before admission control saw it.
    pub submitted_at: DateTime<Utc>,
    /// When it was admitted and began executing. `None` while queued.
    pub started_at: Option<DateTime<Utc>>,
    /// When it reached a terminal state.
    pub finished_at: Option<DateTime<Utc>>,
    /// Why it failed. Truncated to [`MAX_ERROR_LEN`].
    pub error: Option<String>,
    /// The coordinator process that scheduled it. See the module docs on why this matters.
    pub coordinator: Option<String>,
    /// Rows returned. `None` unless the query succeeded.
    pub result_rows: Option<i64>,
}

impl QueryRecord {
    /// How long admission control made this query wait, once it is known.
    pub fn queue_time(&self) -> Option<TimeDelta> {
        self.started_at.map(|started| started - self.submitted_at)
    }

    /// How long execution took, once it is known. `None` for a query that never started — which
    /// is a different thing from zero, and the reason this is not `unwrap_or_default`.
    pub fn execution_time(&self) -> Option<TimeDelta> {
        match (self.started_at, self.finished_at) {
            (Some(started), Some(finished)) => Some(finished - started),
            _ => None,
        }
    }
}

/// The column list every query lookup returns, in the order [`QueryRow`] expects.
const QUERY_COLUMNS: &str = "id, account_id, warehouse_id, sql_text, state, submitted_at, \
                             started_at, finished_at, error, coordinator, result_rows";

/// The raw row shape. Named so the `query_as` turbofishes below stay readable.
type QueryRow = (
    i64,
    i64,
    Option<i64>,
    String,
    String,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

/// Turn a row into a [`QueryRecord`], failing loudly on a state the schema should have made
/// impossible. Reachable only if someone edits `state` by hand or a future migration adds a value
/// this build does not know — both worth an error rather than a guess.
fn query_from_row(row: QueryRow) -> Result<QueryRecord> {
    let (
        id,
        account_id,
        warehouse_id,
        sql_text,
        state,
        submitted_at,
        started_at,
        finished_at,
        error,
        coordinator,
        result_rows,
    ) = row;
    let state = state
        .parse::<QueryState>()
        .with_context(|| format!("reading query {id}"))?;
    Ok(QueryRecord {
        id,
        account_id,
        warehouse_id,
        sql_text,
        state,
        submitted_at,
        started_at,
        finished_at,
        error,
        coordinator,
        result_rows,
    })
}

/// Cut an error message down to [`MAX_ERROR_LEN`], on a character boundary, marking that it was
/// cut. Byte-slicing a UTF-8 message is a panic waiting for the first non-ASCII table name.
pub fn truncate_error(message: &str) -> String {
    if message.chars().count() <= MAX_ERROR_LEN {
        return message.to_string();
    }
    let head: String = message.chars().take(MAX_ERROR_LEN).collect();
    format!("{head}… (truncated)")
}

impl ServicesDb {
    /// Record a newly accepted query, in `queued`.
    ///
    /// Written **before** admission control is consulted, deliberately: a query that waits ten
    /// minutes for a slot and then gets cancelled is exactly the query an operator needs to see,
    /// and a row that only appears once execution starts cannot show it. The cost is one INSERT
    /// on the submit path, which is dwarfed by planning.
    pub async fn submit_query(
        &self,
        account_id: i64,
        warehouse_id: Option<i64>,
        sql_text: &str,
        coordinator: Option<&str>,
    ) -> Result<QueryRecord> {
        let row = sqlx::query_as::<_, QueryRow>(&format!(
            "INSERT INTO queries (account_id, warehouse_id, sql_text, state, coordinator) \
             VALUES ($1, $2, $3, 'queued', $4) RETURNING {QUERY_COLUMNS}"
        ))
        .bind(account_id)
        .bind(warehouse_id)
        .bind(sql_text)
        .bind(coordinator)
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("recording a submitted query for account {account_id}"))?;
        query_from_row(row)
    }

    /// Mark a query as executing, stamping `started_at` from the **server's** clock.
    ///
    /// `now()` rather than a timestamp the coordinator computed: several coordinators write this
    /// table, and comparing their histories only means something if every timestamp came from one
    /// clock. That is also what makes the overlap of `[started_at, finished_at)` intervals a
    /// usable measure of observed concurrency.
    pub async fn mark_query_running(&self, id: i64) -> Result<QueryRecord> {
        self.set_query_state(id, QueryState::Running, None, None)
            .await
    }

    /// Mark a query as finished successfully, recording how many rows it produced.
    pub async fn mark_query_succeeded(&self, id: i64, result_rows: i64) -> Result<QueryRecord> {
        self.set_query_state(id, QueryState::Succeeded, None, Some(result_rows))
            .await
    }

    /// Mark a query as failed, with the reason.
    ///
    /// Legal from `queued` as well as from `running` — a query rejected by admission control, or
    /// abandoned when the coordinator shut down, never started and must still reach a terminal
    /// state. Its `started_at` stays `NULL`, which is precisely how history says "never ran".
    pub async fn mark_query_failed(&self, id: i64, error: &str) -> Result<QueryRecord> {
        self.set_query_state(id, QueryState::Failed, Some(&truncate_error(error)), None)
            .await
    }

    /// The single UPDATE behind every transition.
    ///
    /// One statement, not read-then-write: unlike a warehouse (whose transitions are contested by
    /// several operators and therefore need [`SELECT ... FOR UPDATE`](crate::warehouse)), a query
    /// row has exactly one writer — the coordinator task that owns it — for its whole life. There
    /// is nothing to race, so a lock would buy nothing and cost a round trip per state change.
    ///
    /// `started_at` is set only on the transition *into* `running`, and `finished_at` only on a
    /// transition into a terminal state, so re-marking cannot smear the timings.
    async fn set_query_state(
        &self,
        id: i64,
        state: QueryState,
        error: Option<&str>,
        result_rows: Option<i64>,
    ) -> Result<QueryRecord> {
        let row = sqlx::query_as::<_, QueryRow>(&format!(
            "UPDATE queries SET \
                 state = $2::TEXT, \
                 started_at = CASE WHEN $2::TEXT = 'running' THEN now() ELSE started_at END, \
                 finished_at = CASE WHEN $2::TEXT IN ('succeeded', 'failed') THEN now() \
                                    ELSE finished_at END, \
                 error = $3::TEXT, \
                 result_rows = COALESCE($4::BIGINT, result_rows) \
             WHERE id = $1 \
             RETURNING {QUERY_COLUMNS}"
        ))
        .bind(id)
        .bind(state.as_str())
        .bind(error)
        .bind(result_rows)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("marking query {id} as {state}"))?
        .with_context(|| format!("query {id} does not exist in the services database"))?;
        query_from_row(row)
    }

    /// Look one query up by id — how a client turns the `lldb-query-id` a submission returned
    /// into a status.
    pub async fn query_by_id(&self, id: i64) -> Result<Option<QueryRecord>> {
        let row = sqlx::query_as::<_, QueryRow>(&format!(
            "SELECT {QUERY_COLUMNS} FROM queries WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up query {id}"))?;
        row.map(query_from_row).transpose()
    }

    /// A tenant's most recent queries, newest first — the history page, and the exact access
    /// pattern `queries_account_submitted_idx` exists for.
    pub async fn list_queries(&self, account_id: i64, limit: i64) -> Result<Vec<QueryRecord>> {
        let rows = sqlx::query_as::<_, QueryRow>(&format!(
            "SELECT {QUERY_COLUMNS} FROM queries WHERE account_id = $1 \
             ORDER BY submitted_at DESC, id DESC LIMIT $2"
        ))
        .bind(account_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing query history for account {account_id}"))?;
        rows.into_iter().map(query_from_row).collect()
    }

    /// Everything not yet terminal, oldest first — "what is in flight", served by the partial
    /// `queries_active_idx`.
    ///
    /// Read it with the module docs in mind: a `running` row proves a coordinator *said* it was
    /// running, not that the process is still alive.
    pub async fn list_active_queries(&self, account_id: i64) -> Result<Vec<QueryRecord>> {
        let rows = sqlx::query_as::<_, QueryRow>(&format!(
            "SELECT {QUERY_COLUMNS} FROM queries \
             WHERE account_id = $1 AND state IN ('queued', 'running') \
             ORDER BY submitted_at, id"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing active queries for account {account_id}"))?;
        rows.into_iter().map(query_from_row).collect()
    }
}

/// The largest number of query intervals that overlap at any instant.
///
/// This is the *observed* concurrency of a set of history rows, computed from the timestamps the
/// database stamped — an instrument entirely independent of the scheduler's own counters. Both
/// are worth having: the scheduler's counter proves its bookkeeping is right, and this proves the
/// bookkeeping described reality.
///
/// A row that never started contributes nothing. A row that started but has no `finished_at` is
/// treated as still running, i.e. its interval extends past every other event.
pub fn peak_concurrency(records: &[QueryRecord]) -> usize {
    // Sweep line over (timestamp, delta). Ends are sorted before starts at the same instant so
    // two queries that touch — one finishing exactly when the next starts — count as one, not
    // two: they were never simultaneously executing. Ties at microsecond resolution are common
    // enough on a fast query that this materially changes the answer.
    let mut events: Vec<(DateTime<Utc>, i32)> = Vec::with_capacity(records.len() * 2);
    for record in records {
        let Some(start) = record.started_at else {
            continue;
        };
        // Still running: its interval has no end yet, so extend it past every other event rather
        // than inventing one — an unfinished query really does overlap everything after it.
        let end = record.finished_at.unwrap_or(DateTime::<Utc>::MAX_UTC);
        events.push((end, -1));
        events.push((start, 1));
    }
    events.sort();

    let mut current = 0i32;
    let mut peak = 0i32;
    for (_, delta) in events {
        current += delta;
        peak = peak.max(current);
    }
    peak.max(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: i64, start: Option<i64>, end: Option<i64>) -> QueryRecord {
        let epoch = DateTime::from_timestamp(1_700_000_000, 0).expect("valid timestamp");
        QueryRecord {
            id,
            account_id: 1,
            warehouse_id: Some(2),
            sql_text: "SELECT 1".to_string(),
            state: if end.is_some() {
                QueryState::Succeeded
            } else {
                QueryState::Running
            },
            submitted_at: epoch,
            started_at: start.map(|s| epoch + TimeDelta::seconds(s)),
            finished_at: end.map(|e| epoch + TimeDelta::seconds(e)),
            error: None,
            coordinator: Some("test".to_string()),
            result_rows: Some(0),
        }
    }

    #[test]
    fn states_round_trip_through_their_stored_spelling() {
        for state in QUERY_STATES {
            assert_eq!(state.as_str().parse::<QueryState>().unwrap(), state);
            assert_eq!(state.to_string(), state.as_str());
        }
    }

    #[test]
    fn an_unknown_state_is_refused_not_guessed() {
        let err = "suceeded".parse::<QueryState>().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("suceeded"), "{msg}");
        for state in QUERY_STATES {
            assert!(
                msg.contains(state.as_str()),
                "must list the legal set: {msg}"
            );
        }
    }

    #[test]
    fn only_finished_states_are_terminal() {
        assert!(!QueryState::Queued.is_terminal());
        assert!(!QueryState::Running.is_terminal());
        assert!(QueryState::Succeeded.is_terminal());
        assert!(QueryState::Failed.is_terminal());
    }

    #[test]
    fn queue_and_execution_time_come_from_the_right_gaps() {
        let mut r = record(1, Some(5), Some(12));
        assert_eq!(r.queue_time(), Some(TimeDelta::seconds(5)));
        assert_eq!(r.execution_time(), Some(TimeDelta::seconds(7)));

        // A query that never started has no execution time — and that is not zero.
        r.started_at = None;
        r.finished_at = None;
        assert_eq!(r.queue_time(), None);
        assert_eq!(r.execution_time(), None);
    }

    #[test]
    fn a_long_error_is_truncated_on_a_character_boundary() {
        // The failure this guards against is byte-slicing a multi-byte message and panicking.
        let message = "é".repeat(MAX_ERROR_LEN + 500);
        let truncated = truncate_error(&message);
        assert!(truncated.ends_with("… (truncated)"), "{truncated}");
        assert_eq!(
            truncated.chars().count(),
            MAX_ERROR_LEN + "… (truncated)".chars().count()
        );
        // Anything that fits is left exactly as it was.
        assert_eq!(truncate_error("boom"), "boom");
    }

    #[test]
    fn peak_concurrency_counts_the_deepest_overlap() {
        // Three queries: [0,10), [2,4), [3,12) → the deepest overlap is 3, at t=3.
        let peak = peak_concurrency(&[
            record(1, Some(0), Some(10)),
            record(2, Some(2), Some(4)),
            record(3, Some(3), Some(12)),
        ]);
        assert_eq!(peak, 3);
    }

    #[test]
    fn queries_that_merely_touch_do_not_overlap() {
        // The bound this instrument has to get right: a limit of 1, perfectly serialized. If ends
        // did not sort before starts, this would read as 2 and a correct scheduler would look
        // broken.
        let peak = peak_concurrency(&[
            record(1, Some(0), Some(5)),
            record(2, Some(5), Some(10)),
            record(3, Some(10), Some(15)),
        ]);
        assert_eq!(peak, 1);
    }

    #[test]
    fn a_query_that_never_started_contributes_nothing() {
        assert_eq!(peak_concurrency(&[record(1, None, None)]), 0);
        assert_eq!(peak_concurrency(&[]), 0);
    }

    #[test]
    fn a_still_running_query_stays_open_to_the_end() {
        // No finished_at: it is still running, so it overlaps everything that started after it.
        let peak = peak_concurrency(&[record(1, Some(0), None), record(2, Some(3), Some(9))]);
        assert_eq!(peak, 2);
    }
}
