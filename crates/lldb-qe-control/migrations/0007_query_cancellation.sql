-- Cancelling a running query: a fifth query state, the second CHECK that has to widen with it, and
-- the privilege that decides who may do it.
--
-- Issue #18 (migration 0004) gave `queries` four states and two constraints that between them
-- describe the whole lifecycle: `queries_state_check` names the legal set, and
-- `queries_error_only_when_failed` says an explanation belongs to a failure and nowhere else.
-- Issue #38 adds a way to stop a query that is already holding an admission slot, and a stopped
-- query fits neither of the two terminal states 0004 knows: it did not finish, and nothing about it
-- went wrong.
--
-- It does NOT edit 0004 or 0005. An applied migration is history and sqlx verifies its checksum on
-- every run; a fleet that had already migrated would refuse to start. So each constraint below is
-- dropped and re-added here, under a name that still describes what it enforces.
--
-- # Why `cancelled` is its own state and not `failed` with a nice message
--
-- Because the two answer different operational questions. `failed` means "this query tried and
-- could not"; it is the column an operator counts to decide whether the system is healthy, and a
-- cancellation folded into it makes a fleet of careful users look like a fleet of broken queries.
-- A cancelled query consumed compute and returned nothing *on purpose*. Making that distinction
-- representable is a requirement of the issue, and the cost is exactly this migration.
--
-- # Why the *second* constraint has to widen, and why that is the interesting half
--
-- `queries_error_only_when_failed` allows an explanation on a `failed` row and on no other. That is
-- why `crate::server`'s abandonment guard records "the client disconnected" as a *failure* with
-- prose rather than as its own state: prose had nowhere else to live. A `cancelled` row that cannot
-- say who cancelled it, or why, is close to useless — the first question anyone asks of a
-- cancelled query is "who stopped this and when", and the row is the only place that can answer.
-- So the constraint becomes "an explanation belongs to a query that did not succeed", which is the
-- rule it was always reaching for, and it is renamed to say so.
--
-- Note the shape is still deliberately one-way, exactly as 0004 wrote it: an unsuccessful query may
-- carry no message, but a `succeeded` one must never carry one.
--
-- # Why a `CANCEL` privilege rather than reusing `USAGE`
--
-- `USAGE ON WAREHOUSE analytics` is what lets a caller *submit* to that warehouse. If cancelling
-- needed only that, then everyone who may run a query on a warehouse may kill everyone else's on
-- it, and "cancelling somebody else's query needs a grant" would be true only in a technical sense.
-- So cancellation is its own verb, held on the warehouse whose slot it frees — compute, like
-- `USAGE`, and therefore outside the catalog hierarchy (see `crate::rbac::covers_object`).
--
-- One consequence is worth stating rather than discovering: `ALL` is a privilege wildcard, so every
-- existing `ALL ON WAREHOUSE ...` grant now also confers `CANCEL` on that warehouse. That is what
-- `ALL` means and it is applied consistently, but it does silently widen grants that were written
-- before this migration existed. An operator who wants submit-without-kill must write the narrow
-- privileges rather than `ALL`.
--
-- # There is deliberately nothing to normalize, and deliberately no index change
--
-- 0002 and 0004 normalize before they constrain, because both *narrowed* what a column may hold and
-- a CHECK that fails to add because of a row nobody remembers is a migration that blocks a deploy.
-- Every change here is a **widening**: every row that satisfied the old constraint satisfies the new
-- one by construction, so a backfill would be a statement that can only touch zero rows. Saying that
-- out loud is the check.
--
-- `queries_active_idx` (0004) is partial on `state IN ('queued', 'running')` and stays exactly as it
-- is. That predicate is the *active* set, not the "not yet enumerated" set, and `cancelled` is
-- terminal — adding it would put every cancelled row a deployment ever produces into an index whose
-- entire purpose is to stay small enough to remain in cache forever. `list_active_queries` and the
-- reaper's stranded predicate are the same set for the same reason and are likewise untouched;
-- `crate::query_log::active_states_sql` is where that set is now written down once.

-- The legal set, plus the state a query reaches when somebody stops it. Same name as 0004's, so a
-- violation still reports `queries_state_check`.
ALTER TABLE queries DROP CONSTRAINT IF EXISTS queries_state_check;
ALTER TABLE queries
    ADD CONSTRAINT queries_state_check
    CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'));

-- An explanation belongs to a query that did not succeed. Renamed rather than kept, because a
-- constraint called `..._only_when_failed` that also permits `cancelled` is a name that lies to the
-- next reader of `\d queries`.
ALTER TABLE queries DROP CONSTRAINT IF EXISTS queries_error_only_when_failed;
ALTER TABLE queries
    ADD CONSTRAINT queries_error_only_when_unsuccessful
    CHECK (error IS NULL OR state IN ('failed', 'cancelled'));

-- The verb. 0005 declared this CHECK inline on the column, so Postgres named it
-- `grants_privilege_check`; dropping by that name is verified against a live database rather than
-- assumed, because a silent no-op DROP followed by a successful ADD would leave the *old*, narrow
-- constraint in force and every `CANCEL` grant would be rejected at insert time.
ALTER TABLE grants DROP CONSTRAINT IF EXISTS grants_privilege_check;
ALTER TABLE grants
    ADD CONSTRAINT grants_privilege_check
    CHECK (privilege IN ('SELECT', 'INSERT', 'DELETE', 'UPDATE', 'USAGE', 'CANCEL', 'ALL'));

COMMENT ON COLUMN queries.state IS
    'queued | running | succeeded | failed | cancelled. queued means admitted but waiting for a slot; cancelled means someone stopped it through do_action("cancel", <id>) and its slot was returned to the warehouse.';
COMMENT ON COLUMN queries.error IS
    'Why a query did not succeed. Populated for failed and for cancelled rows (a cancellation records who asked); NULL everywhere else, enforced by queries_error_only_when_unsuccessful.';
COMMENT ON COLUMN grants.privilege IS
    'SELECT | INSERT | DELETE | UPDATE | USAGE | CANCEL | ALL. ALL covers every other privilege on the same object, which includes CANCEL. CANCEL is held on a warehouse and permits stopping any query running on it.';
