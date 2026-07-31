-- Query history grows up: the lifecycle constraints, the two facts the scheduler knows that the
-- stub could not, and the index the "what is running right now" question needs.
--
-- Issue #14 created `queries` as a stub — id, account_id, warehouse_id, sql_text, state,
-- submitted_at, started_at, finished_at, error — because its foreign keys were what made "a query
-- belongs to a tenant and ran on a warehouse" a schema-enforced fact. Issue #18 turns the
-- coordinator into a long-running scheduler, and that is the thing which actually *writes* these
-- rows. This migration makes the column set match what the scheduler knows.
--
-- It does NOT edit 0001. An applied migration is history and sqlx verifies its checksum on every
-- run; a fleet that had already migrated would refuse to start.
--
-- Four changes, all about keeping impossible history out of the table:
--
-- 1. `state` was bare TEXT with a default and no constraint — the same hole 0002 closed for
--    `warehouses.state`, with the same failure mode: a typo'd `'suceeded'` is written happily and
--    then fails at *read* time, in whatever tool is rendering history, far from the writer that
--    caused it. The CHECK moves the failure to the write.
--
-- 2. `error` was writable in any state. A `succeeded` row carrying an error message is not a
--    record, it is a lie; the CHECK makes it unrepresentable.
--
-- 3. `coordinator` records *which process* scheduled the query. This is not bookkeeping trivia:
--    admission control in #18 is per-coordinator-process (one semaphore in one process, see the
--    `scheduler` module), so "how many queries were running at once" is only answerable per
--    coordinator. Storing the id makes that limitation visible in the data instead of hidden in
--    a design doc — and it is what a future reaper would use to find rows abandoned in `queued`
--    or `running` by a coordinator that died.
--
-- 4. `result_rows` records how big the answer was. It is the cheapest useful thing to know about
--    a finished query, and computing it later is impossible — the batches are long gone.

-- Normalize before constraining, exactly as 0002 does. In practice this touches zero rows (only
-- #18's code writes this table, and it did not exist before), but a CHECK that fails to add
-- because of a row nobody remembers is a migration that blocks a deploy. A query whose state we
-- cannot interpret is not running and never will be, so `failed` is the honest resolution.
UPDATE queries
   SET state = 'failed',
       error = COALESCE(error, 'state was not one of queued/running/succeeded/failed')
 WHERE state NOT IN ('queued', 'running', 'succeeded', 'failed');

-- The legal set, named so a violation reports `queries_state_check` rather than an anonymous
-- constraint number. `queued` = admitted to the scheduler but waiting for a slot; `running` = it
-- holds a slot and is executing; `succeeded` / `failed` = terminal.
ALTER TABLE queries
    ADD CONSTRAINT queries_state_check
    CHECK (state IN ('queued', 'running', 'succeeded', 'failed'));

-- An error belongs to a failure and nowhere else. Note the deliberately *one-way* form: a failed
-- query must be allowed to have no message (a panic in a future execution path could produce
-- one), but a succeeded query must never carry one.
ALTER TABLE queries
    ADD CONSTRAINT queries_error_only_when_failed
    CHECK (error IS NULL OR state = 'failed');

-- Which coordinator process scheduled this query. Nullable because history predating this column
-- — and any future writer that legitimately does not know — must still be storable.
ALTER TABLE queries
    ADD COLUMN IF NOT EXISTS coordinator TEXT;

-- Rows the query returned. Nullable on purpose: it is unknown until the query succeeds, and
-- `0` is a real, different answer from "never finished".
ALTER TABLE queries
    ADD COLUMN IF NOT EXISTS result_rows BIGINT;

-- The other access pattern history has: "what is in flight right now" — the operator's first
-- question when a warehouse feels slow. A *partial* index because terminal rows are the
-- overwhelming majority and accumulate forever, while the active set is bounded by the
-- scheduler's own limits; indexing only the active states keeps this index small enough to stay
-- in cache no matter how long the system has been running.
CREATE INDEX IF NOT EXISTS queries_active_idx
    ON queries (warehouse_id, submitted_at)
    WHERE state IN ('queued', 'running');

COMMENT ON COLUMN queries.state IS
    'queued | running | succeeded | failed. queued means admitted but waiting for a slot.';
COMMENT ON COLUMN queries.submitted_at IS
    'When the coordinator accepted the query, before admission control saw it.';
COMMENT ON COLUMN queries.started_at IS
    'When admission granted a slot and execution began. NULL while queued.';
COMMENT ON COLUMN queries.finished_at IS
    'When the query reached a terminal state. finished_at - started_at is execution time, started_at - submitted_at is queue time.';
COMMENT ON COLUMN queries.coordinator IS
    'The coordinator process that scheduled this query. Admission control is per-process, so concurrency limits are only meaningful within one value of this column.';
COMMENT ON COLUMN queries.result_rows IS
    'Rows returned by a succeeded query. NULL when unknown (queued, running, or failed).';
