-- The services-database foundation: tenant identity, plus the thin stubs later issues fill in.
--
-- Deliberately minimal. This migration's job is to establish *where* control-plane state lives
-- and how it is scoped, not to guess at columns whose owners haven't been written yet. Every
-- table below except `accounts` is a stub whose only real content is its foreign key — that key
-- is what makes "an account scopes a warehouse" a fact the database enforces rather than a
-- convention the application remembers.
--
-- Nothing here uses version-specific syntax: identity columns, `ON CONFLICT`, `TIMESTAMPTZ` and
-- partial-free indexes are all Postgres 10+. Compose and CI run 18.4; this applies unchanged on
-- anything modern.

-- Tenant identity — the root of every ownership chain in this database.
--
-- `GENERATED ALWAYS AS IDENTITY` rather than `BIGSERIAL`: it is the SQL-standard spelling, and
-- "always" means an application cannot accidentally write its own id and desynchronize the
-- sequence. `name` is UNIQUE because it is the human-facing handle — `--account default` has to
-- resolve to exactly one row.
CREATE TABLE IF NOT EXISTS accounts (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Users (stub — issue #19, accounts & RBAC).
--
-- Filled in with credentials, roles and grants there. What matters now is the shape: a user
-- belongs to exactly one account, and names are unique *within* an account, not globally.
CREATE TABLE IF NOT EXISTS users (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

-- Virtual warehouses (stub — issue #16).
--
-- A warehouse is a named, sized pool of workers a query runs on. `size` and `state` are here
-- because they are the two things the *scoping* story needs to be believable — the scheduler's
-- real columns (auto-suspend, scaling policy, cluster assignment) land with #16.
CREATE TABLE IF NOT EXISTS warehouses (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    size       INTEGER     NOT NULL DEFAULT 1 CHECK (size > 0),
    state      TEXT        NOT NULL DEFAULT 'suspended',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name)
);

-- Query history (stub — issue #18).
--
-- `warehouse_id` is nullable and `ON DELETE SET NULL`: history must outlive the warehouse that
-- ran it, otherwise dropping a warehouse would silently erase the audit trail. The account link
-- stays CASCADE — deleting a tenant really does mean deleting the tenant's data.
CREATE TABLE IF NOT EXISTS queries (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id   BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    warehouse_id BIGINT      REFERENCES warehouses (id) ON DELETE SET NULL,
    sql_text     TEXT        NOT NULL,
    state        TEXT        NOT NULL DEFAULT 'queued',
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    error        TEXT
);

-- The access pattern query history is for: "the most recent queries for this tenant". Without
-- this index that page is a full scan plus a sort, and it gets slower every day the system runs.
CREATE INDEX IF NOT EXISTS queries_account_submitted_idx
    ON queries (account_id, submitted_at DESC);
