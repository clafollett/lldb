-- The cross-query result cache: a query whose inputs have not changed is answered from here.
--
-- Version 0003, not 0002: issue #16 (virtual warehouses) is landing `0002_warehouse_lifecycle.sql`
-- on its own branch at the same time. Migration versions are a global sequence, and two files
-- claiming the same number is a merge conflict that only shows up when both are applied, so this
-- one steps over it deliberately. A gap in the sequence is harmless — sqlx applies whatever
-- versions it finds, in order.
--
-- # What the row is
--
-- One row per (tenant, cache key) — the key being the whole design (see
-- `crates/lldb-qe-core/src/result_cache.rs`). `key_material` is the *entire* key as plain text:
-- the account, the engine build, the catalog the query resolved against, a structural fingerprint
-- of the parsed statement, and every referenced table with the Iceberg snapshot id it was read at.
-- Storing it verbatim, rather than only a digest, is what makes a lookup provably exact: reads
-- compare `key_material = $2`, so no hash collision can ever hand one query another's answer.
--
-- # Why the index is on md5(key_material) and why that is safe
--
-- `key_material` is unbounded (long SQL, many tables), and a btree entry is capped at ~2.7 kB, so
-- it cannot be indexed directly. `md5()` is Postgres-builtin and IMMUTABLE, which an index
-- expression requires — `sha256(convert_to(...))` would be the stronger digest but `convert_to` is
-- only STABLE and Postgres refuses it here.
--
-- md5 is doing *no* correctness work. It is a lookup accelerator: every read also compares the
-- full `key_material`, and every write re-checks it. The worst a collision can do is make two
-- distinct keys contend for one row — one evicts the other, and both then miss and recompute. A
-- wrong answer is not reachable through this index.
--
-- # Invalidation is not this table's job
--
-- Nothing here expires an entry because the data changed; the snapshot ids inside `key_material`
-- do that. A write to a referenced table moves its snapshot id, the next lookup composes a
-- different key, and the stale row is simply never matched again. `expires_at` and the LRU index
-- exist only to *bound* the table, which is why an over-long TTL costs disk and never correctness.

CREATE TABLE IF NOT EXISTS result_cache (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- CASCADE, like every other tenant-scoped table: deleting a tenant deletes its results.
    account_id     BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    -- The full cache key, verbatim. Compared exactly on every read.
    key_material   TEXT        NOT NULL,
    -- The engine build that computed this result. Also inside `key_material`; kept as a column so
    -- an operator can see at a glance which build a fleet's cached rows came from.
    build_version  TEXT        NOT NULL,
    -- The statement as the parser re-renders it. Human-facing only — never compared.
    normalized_sql TEXT        NOT NULL,
    -- `catalog.namespace.table@snapshot` for every input, newline-separated. Human-facing only;
    -- this is what an operator reads when asking "why did that query not hit".
    inputs         TEXT        NOT NULL,
    row_count      BIGINT      NOT NULL,
    -- The result set as an Arrow IPC stream. Bounded by the engine's payload cap before insert;
    -- a result larger than the cap is simply not cached.
    payload        BYTEA       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    last_hit_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    hit_count      BIGINT      NOT NULL DEFAULT 0
);

-- The lookup path, and the uniqueness the upsert infers on. Account first so it is also usable
-- for per-tenant scans.
CREATE UNIQUE INDEX IF NOT EXISTS result_cache_key_idx
    ON result_cache (account_id, md5(key_material));

-- Sweeping expired rows.
CREATE INDEX IF NOT EXISTS result_cache_expiry_idx
    ON result_cache (expires_at);

-- Finding a tenant's least-recently-used entries when the per-account bound is exceeded.
CREATE INDEX IF NOT EXISTS result_cache_lru_idx
    ON result_cache (account_id, last_hit_at);
