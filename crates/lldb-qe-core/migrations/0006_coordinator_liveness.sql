-- Coordinator liveness: the one place the fleet answers "is coordinator X still alive?".
--
-- Issue #18 (migration 0004) added `queries.coordinator` "for a future reaper" and deliberately
-- stopped there, so until now the services database recorded *which* process scheduled a query and
-- nothing whatsoever about whether that process still exists. Two open issues — #36 (reap query
-- rows stranded in queued/running) and #37 (fleet-wide admission) — each have to answer that
-- question before they can be built, and neither owns it. This migration is the storage for the one
-- answer both will use; `crates/lldb-qe-core/src/liveness.rs` is the mechanism and carries the four
-- decisions (identity, renewal-failure behaviour, threshold units, who evaluates) in prose.
--
-- It does NOT edit 0004. An applied migration is history and sqlx verifies its checksum on every
-- run; a fleet that had already migrated would refuse to start.
--
-- # Identity is a pair, because one half of it is ambiguous in both directions
--
-- `queries.coordinator` is free-form TEXT from `--coordinator-id`, and when that flag is omitted it
-- defaults to the *bound socket address*. That makes the value ambiguous in two opposite ways:
--
--   * a coordinator restarted on a different port is a brand-new coordinator as far as history is
--     concerned, even though it is the same deployment slot doing the same job; and
--   * a coordinator restarted onto the *same* address inherits the previous process's identity —
--     and its in-flight rows — without ever having run a single one of them.
--
-- So neither "stable across restarts" nor "unique per process" is true of it, and a liveness design
-- that assumes either is wrong here. This table fixes both by splitting the two ideas apart:
--
--   * `slot` is the stable deployment identity — the operator-chosen `--coordinator-id`. It is the
--     PRIMARY KEY, because a slot is a place in the deployment and two processes claiming one place
--     is a misconfiguration the schema should surface rather than store twice.
--   * `incarnation` is the process identity — 128 bits of CSPRNG minted at startup and never
--     reused. It is what makes a restart read as a restart: the slot survives, the incarnation does
--     not.
--
-- `queries.coordinator_incarnation` (added below) is the other half of that fix. Without it a
-- reaper looking at a stranded row would see a live registration for the slot and conclude the row
-- is live, when in fact it belongs to a process that died and was replaced. With it, "the slot is
-- alive but this row was written by an incarnation that is gone" is expressible, which is precisely
-- the case #36 has to get right.
--
-- # There is deliberately no threshold column
--
-- "Not seen recently" is a multiple of the renewal interval, never an independent duration — two
-- knobs would let an operator configure a threshold shorter than the renewal it is judging, which
-- reaps live coordinators. So the row stores `renew_interval_secs` (what this process actually
-- renews at) and the multiple lives in the code as a build constant. A reader judges each row by
-- that row's own interval, so a fleet running mixed intervals is judged correctly rather than by
-- the reader's guess.
--
-- # No foreign keys, in either direction
--
-- Not to `accounts`: a coordinator serves whatever tenant a request proves it is, so it belongs to
-- the deployment, not to a tenant. And not from `queries.coordinator` to `coordinators.slot`:
-- history must outlive the process that wrote it (the same reason `queries.warehouse_id` is
-- ON DELETE SET NULL), and a slot legitimately gets re-taken by a later incarnation.

-- One row per deployment slot. Re-registration is an upsert on the slot, so the table's size is
-- bounded by the number of coordinators an operator has configured, not by how often they restart.
CREATE TABLE IF NOT EXISTS coordinators (
    slot                TEXT        PRIMARY KEY,
    incarnation         TEXT        NOT NULL,
    -- When *this* incarnation took the slot. Moves on every restart, unlike the slot itself.
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The lease. Stamped from the server's clock (now()), never from the coordinator's, so
    -- comparing coordinators means something even when their clocks disagree.
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Set when a coordinator exits cleanly. A clean exit must be observable *promptly* rather than
    -- after the full threshold elapses, and this is what makes that possible without deleting the
    -- row an operator may still want to look at.
    shutdown_at         TIMESTAMPTZ,
    -- What this process renews at. The threshold is a multiple of this and of nothing else.
    renew_interval_secs INTEGER     NOT NULL,
    -- `version+sha` of the running binary. Free to record here and it answers the operator question
    -- that follows the liveness one: is the whole fleet the identical build?
    build_version       TEXT,
    -- Named constraints, so a violation reports a name rather than a number. A blank slot or
    -- incarnation would make every lookup match the wrong thing; a non-positive interval would make
    -- the threshold zero and reap everything.
    CONSTRAINT coordinators_slot_not_blank        CHECK (btrim(slot) <> ''),
    CONSTRAINT coordinators_incarnation_not_blank CHECK (btrim(incarnation) <> ''),
    CONSTRAINT coordinators_renew_interval_positive CHECK (renew_interval_secs > 0)
);

-- The only read this table has: "which coordinators are live right now". A partial index because
-- cleanly-stopped coordinators are never live again under that registration and are excluded by the
-- predicate anyway — the same shape as `queries_active_idx`.
CREATE INDEX IF NOT EXISTS coordinators_live_idx
    ON coordinators (last_seen_at)
    WHERE shutdown_at IS NULL;

-- Which *process* wrote this query row, as opposed to which slot. Nullable because history predates
-- the column, and because an embedding that never registers legitimately has no incarnation to
-- record. NULL means "liveness says nothing about this row's writer", which is the honest reading
-- and is exactly what a reaper must not treat as "dead".
ALTER TABLE queries
    ADD COLUMN IF NOT EXISTS coordinator_incarnation TEXT;

COMMENT ON TABLE coordinators IS
    'One row per coordinator deployment slot. A row is live when shutdown_at IS NULL and last_seen_at is within a fixed multiple of renew_interval_secs. See crates/lldb-qe-core/src/liveness.rs.';
COMMENT ON COLUMN coordinators.slot IS
    'Stable deployment identity (--coordinator-id), matching queries.coordinator. Survives a restart.';
COMMENT ON COLUMN coordinators.incarnation IS
    'Per-process identity, minted at startup and never reused. Does NOT survive a restart - that is the point.';
COMMENT ON COLUMN coordinators.last_seen_at IS
    'Last successful renewal, from the database clock. Liveness is measured from this and nothing else.';
COMMENT ON COLUMN coordinators.shutdown_at IS
    'Set when the coordinator exited cleanly, so a clean exit is observably not-live at once rather than after the threshold.';
COMMENT ON COLUMN coordinators.renew_interval_secs IS
    'How often this process renews. The not-seen-recently threshold is a fixed multiple of this; there is deliberately no separate threshold setting.';
COMMENT ON COLUMN queries.coordinator_incarnation IS
    'The coordinator PROCESS that wrote this row (coordinators.incarnation). A row whose slot is live but whose incarnation is gone belongs to a coordinator that died and was replaced.';
