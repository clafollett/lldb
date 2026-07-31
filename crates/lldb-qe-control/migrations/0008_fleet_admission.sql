-- Fleet-wide admission control: a warehouse's concurrency limit becomes a property of the
-- warehouse instead of a property of a process.
--
-- Issue #18 (migration 0004) bounded concurrency with a `tokio::sync::Semaphore` in one
-- coordinator's memory and said so on `queries.coordinator`: "Admission control is per-process, so
-- concurrency limits are only meaningful within one value of this column." Two coordinators
-- configured `K = 4` therefore ran up to 8 queries on one warehouse and neither could see the
-- other — which is exactly the number an operator scaling for availability would increase without
-- expecting a change in load on their compute. Issue #37 is that caveat, and this table is the
-- shared state it needs.
--
-- It does NOT edit 0004. An applied migration is history and sqlx verifies its checksum on every
-- run; a fleet that had already migrated would refuse to start. So the COMMENT at the bottom is
-- re-issued here rather than corrected in place, which is the only way to retract a sentence that
-- has already shipped.
--
-- # Why K rows rather than one counter
--
-- The obvious shape is a counter per warehouse — read it, compare it to K, increment it. Under
-- Postgres's READ COMMITTED that shape is wrong without a lock: N coordinators can all read
-- `K - 1`, all conclude there is room, and all admit. Fixing it needs an advisory lock or a
-- `SELECT ... FOR UPDATE` on a parent row, which means a transaction and three round trips on the
-- hottest path in the system.
--
-- So the slots are *rows*, numbered `0 .. K-1`, and the primary key does the arbitration. A claim
-- is one statement: pick a claimable number, `INSERT ... ON CONFLICT DO UPDATE` it, and see
-- whether a row comes back. The candidate is chosen at *random* rather than lowest-first, which is
-- a small thing with a real effect — lowest-first makes every simultaneous claimant on a warehouse
-- that has room pick the same number, so all but one lose a race none of them needed to have. See
-- `crate::fleet_admission`, which is where that reasoning lives at length.
-- Two coordinators racing for the same number both reach the unique
-- index; one wins outright and the other is turned into an `ON CONFLICT` whose `WHERE` refuses to
-- take a slot from a live holder — the same repeated-predicate compare-and-swap `crate::reaper`
-- writes with, for the same reason. Over-admission is then impossible by *construction* rather than
-- by argument: there cannot be more than K rows for a warehouse with `slot_no < K`.
--
-- # Why there is no expiry column
--
-- A lease usually carries `expires_at` and a renewal loop that pushes it forward. This one does
-- not, and the reason is that the answer already exists: `coordinators` (migration 0006) is a
-- renewed lease over the *process*, and a query slot is held by a process. So a claim records its
-- holder as the `(slot, incarnation)` pair 0006 defined, and a slot is reclaimable exactly when no
-- live `coordinators` row matches that pair — `crate::liveness`'s `LIVE_PREDICATE`, spliced in
-- verbatim, the same way `crate::reaper` splices it.
--
-- That buys three things. One renewal per coordinator instead of one per running query. One
-- spelling of "alive" in the whole codebase, so a future change to the threshold cannot leave two
-- definitions disagreeing. And no sweep: a dead coordinator's slots are reclaimed by the next
-- coordinator that wants one, on the ordinary claim path, rather than by something that has to be
-- scheduled and could be forgotten.
--
-- The cost is named rather than hidden: a coordinator that cannot renew (a control-plane blip) has
-- its slots reclaimable after the liveness threshold while it is still running the queries holding
-- them. `crate::liveness`'s decision 2 explicitly conceded that its cost would have to be re-argued
-- by "fleet-wide admission handing out a slot this coordinator still holds", and the argument is in
-- `crate::scheduler`'s module docs: a coordinator that cannot reach Postgres also cannot claim, so
-- it falls back to its own local semaphore, and the worst case is N coordinators × K — which is
-- precisely today's behaviour and never worse than it.
--
-- # Why the key is `warehouse_id` and not the warehouse's name
--
-- Warehouse names are unique *per account* (0001), so `('analytics', tenant A)` and
-- `('analytics', tenant B)` are two different warehouses with two different fleets. Keying shared
-- state by the name would silently merge one tenant's concurrency budget into another's. The id is
-- the only globally unique handle, and the foreign key means deleting a warehouse frees its slots
-- rather than leaving rows pointing at nothing.
--
-- A query routed at a raw `--workers` fleet has no warehouse row, and therefore no row here and no
-- fleet-wide bound: it keeps the per-process semaphore it always had. That is deliberate — a
-- `--workers` list is not a control-plane object, so there is nothing for two coordinators to agree
-- they are talking about.

-- One row per *held* slot. Absent means free; the table is empty when nothing is running, so its
-- steady-state size is the number of queries the whole deployment is executing.
CREATE TABLE IF NOT EXISTS admission_slots (
    -- Which warehouse's budget this consumes. CASCADE because a deleted warehouse has no
    -- concurrency left to bound, and a slot pointing at nothing would be unclaimable forever.
    warehouse_id       BIGINT      NOT NULL REFERENCES warehouses (id) ON DELETE CASCADE,
    -- Which of the warehouse's `0 .. K-1` slots. The other half of the primary key, and the thing
    -- that makes the bound structural: a claim only ever proposes a number below the limit.
    slot_no            INTEGER     NOT NULL,
    -- The holder, as `crate::liveness`'s pair. The slot half is what an operator recognises; the
    -- incarnation half is what makes "the coordinator died and restarted onto the same address"
    -- distinguishable from "the coordinator is still running this query" — the case a slot-only
    -- rule gets exactly backwards, and the reason 0006 stores two columns instead of one.
    holder_slot        TEXT        NOT NULL,
    holder_incarnation TEXT        NOT NULL,
    -- Which *claim* this row is, within that process. 128 bits of CSPRNG, minted per claim.
    --
    -- It exists for the one case liveness cannot cover: a slot leaked by a coordinator that is
    -- still alive. Releasing is a `DELETE` issued from a destructor, so it is best effort, and a
    -- leaked row would otherwise shrink a warehouse's concurrency for as long as that process
    -- lives. A coordinator knows exactly which tokens it is holding, so its own rows carrying any
    -- other token are provably stale and are reclaimed on its next claim. It is also what makes the
    -- release a compare-and-swap: a coordinator whose slot was reclaimed while it was partitioned
    -- must not later delete the row its successor now holds.
    holder_token       TEXT        NOT NULL,
    claimed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (warehouse_id, slot_no),
    CONSTRAINT admission_slots_slot_no_non_negative CHECK (slot_no >= 0),
    CONSTRAINT admission_slots_holder_slot_not_blank CHECK (btrim(holder_slot) <> ''),
    CONSTRAINT admission_slots_holder_incarnation_not_blank CHECK (btrim(holder_incarnation) <> ''),
    CONSTRAINT admission_slots_holder_token_not_blank CHECK (btrim(holder_token) <> '')
);

-- No foreign key to `coordinators`, in either direction, for 0006's own reason: a slot is claimed
-- by a *process*, and that process's registration row is legitimately re-taken by a later
-- incarnation or cleaned up by an operator. A constraint would turn either into an error where the
-- correct reading is "this holder is not live, so the slot is claimable".

-- Reclaiming this coordinator's own stale rows is the one read that is not by primary key.
CREATE INDEX IF NOT EXISTS admission_slots_holder_idx
    ON admission_slots (holder_incarnation);

COMMENT ON TABLE admission_slots IS
    'Fleet-wide admission control: one row per query slot a coordinator is holding on a warehouse. A warehouse of size K has slots 0..K-1, so the concurrency bound is the primary key rather than a counter. A slot is reclaimable when its holder (holder_slot, holder_incarnation) has no live row in coordinators, or when the holder is the claiming process and the token is one it no longer holds. See crates/lldb-qe-control/src/fleet_admission.rs.';
COMMENT ON COLUMN admission_slots.holder_token IS
    'Identifies one claim within a holding process, so a leaked row can be reclaimed by its own coordinator and so releasing is a compare-and-swap rather than an unconditional delete.';

-- The retraction this migration exists for. 0004 said concurrency limits were only meaningful
-- within one value of this column; with `admission_slots` behind the fleet they are meaningful
-- across it, so `query_log::peak_concurrency` over an account's whole history now measures the
-- warehouse rather than one process. The column keeps its other jobs — attributing a row to a
-- writer, and letting `crate::reaper` find rows whose writer is gone.
COMMENT ON COLUMN queries.coordinator IS
    'The coordinator slot that scheduled this query (crate::liveness''s stable half; the process half is coordinator_incarnation). Concurrency limits are enforced fleet-wide through admission_slots, so peak concurrency computed across every value of this column is meaningful — the column attributes a row to its writer, it does not scope the bound.';
