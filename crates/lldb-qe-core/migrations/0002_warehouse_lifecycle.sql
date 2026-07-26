-- Virtual warehouses grow up: the lifecycle columns and the constraints that make an illegal
-- warehouse unrepresentable.
--
-- Issue #14 created `warehouses` as a stub — id, account_id, name, size, state, created_at —
-- because its foreign key was what made "an account scopes a warehouse" a schema-enforced fact.
-- This migration turns that stub into the thing the compute lifecycle actually runs on. It does
-- NOT edit 0001: an applied migration is history, and sqlx verifies its checksum on every run.
--
-- Two changes, both about keeping bad state out rather than adding features:
--
-- 1. `state` was a bare TEXT column with a default and no constraint. That is exactly how a
--    typo'd `'runing'` — or a future subcommand with a new spelling — gets written and then
--    fails at *read* time, in the coordinator, mid-query, far from whoever caused it. The
--    CHECK moves that failure to the write that caused it. Adding a state later is a migration,
--    which is the right amount of friction for a value the whole fleet must agree on.
--
-- 2. `updated_at` is what makes the lifecycle auditable: "when did this warehouse last change
--    size or state" is the first question asked when compute costs more than expected, and
--    `created_at` cannot answer it. It is maintained by the API's UPDATE statements rather than
--    by a trigger — every mutation here is a single statement that already has to touch the row,
--    and a trigger would hide that write from anyone reading the SQL.

-- The lifecycle timestamp. Backfills to now() for any pre-existing row, which is honest: the
-- rows that exist have never been mutated, so "last changed" is as good as unknown.
ALTER TABLE warehouses
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Normalize before constraining. Nothing but this migration's own API writes `state`, and 0001
-- defaulted it to 'suspended', so in practice this touches zero rows — but a CHECK that fails to
-- add because of a row nobody remembers is a migration that blocks a deploy, and the resolution
-- ("a warehouse whose state we cannot interpret has no compute; call it suspended") is one a
-- human would pick anyway. Doing it explicitly means the deploy does not stop to ask.
UPDATE warehouses SET state = 'suspended' WHERE state NOT IN ('running', 'suspended');

-- The legal set, named so a violation reports `warehouses_state_check` instead of an anonymous
-- constraint number. `running` = compute is provisioned and queries may route here;
-- `suspended` = the pool is scaled to zero and routing must refuse it.
ALTER TABLE warehouses
    ADD CONSTRAINT warehouses_state_check CHECK (state IN ('running', 'suspended'));

-- `size` is the *desired* worker count and stays > 0 even while suspended (0001 already checks
-- it). That is deliberate: suspending must not destroy the size to resume back to, so "how many
-- workers" and "are they running" are two independent facts. A suspended warehouse's ECS service
-- sits at desiredCount 0; the row still remembers it is a 4-worker warehouse.
COMMENT ON COLUMN warehouses.size IS
    'Desired worker count. Retained while suspended - this is what resume scales back up to.';
COMMENT ON COLUMN warehouses.state IS
    'running | suspended. Suspended means desiredCount 0; queries must not route here.';
COMMENT ON COLUMN warehouses.updated_at IS
    'Last lifecycle change (create, resize, suspend, resume).';
