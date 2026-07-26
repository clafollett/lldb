-- Identity and access control: the users, credentials, roles and grants the engine authenticates
-- and authorizes against.
--
-- Issue #14 created `users` as a stub — id, account_id, name, created_at — because its foreign key
-- was what made "a user belongs to exactly one tenant" a schema-enforced fact. Issue #19 is the one
-- that gives that fact teeth: a request now arrives with a credential, the credential names a user,
-- the user's roles carry grants, and a query that is not covered by a grant is refused before it is
-- dispatched. This migration is the storage for all four steps.
--
-- It does NOT edit 0001. An applied migration is history and sqlx verifies its checksum on every
-- run; a fleet that had already migrated would refuse to start.
--
-- # Why the credential is a hashed API key and not a password
--
-- `api_keys` stores a SHA-256 hex digest, not an argon2/bcrypt/scrypt hash, and that is deliberate
-- rather than an oversight. A slow KDF exists to make *low-entropy, human-chosen* secrets expensive
-- to guess. The tokens this table holds are 32 bytes from the OS CSPRNG; there is nothing to guess,
-- so a KDF would buy no security while costing a dependency and per-request CPU on the hottest path
-- in the system. This is the same reasoning GitHub applies to its own personal access tokens. If a
-- *password* column is ever added here it must not follow this precedent — see `crate::auth`.
--
-- `token_prefix` is the first 12 characters of the token. It has two jobs: it is the lookup key
-- (so verification is one indexed row fetch plus one constant-time compare, not a table scan of
-- every hash), and it is what an operator sees when deciding *which* key to revoke. It is UNIQUE
-- because a duplicate would make that lookup ambiguous; a generated prefix that collides is an
-- insert failure the CLI reports and the operator retries, which at 2^42 possible prefixes is a
-- thing that will not happen.
--
-- The token itself is never stored. It is printed exactly once, at creation.
--
-- # Why every child table carries account_id *and* a composite foreign key
--
-- `api_keys.account_id` is derivable from `users.account_id`, and `grants.account_id` from
-- `roles.account_id`, so both look redundant. They are there because the composite foreign keys
-- below — which need them — make cross-tenant wiring *unrepresentable*: an api_key cannot name a
-- user from another account, a grant cannot name another account's role, and a user cannot be
-- given a role from another tenant. That is the difference between multi-tenancy the application
-- remembers to enforce and multi-tenancy the database enforces. The extra column is also what
-- makes "everything a tenant owns" a single-column scan when a tenant is deleted.
--
-- Everything cascades from `accounts`. Deleting a tenant must not strand a credential that still
-- authenticates.

-- The composite keys the child tables reference. Redundant as *keys* (both are supersets of a
-- primary key), which is exactly why they cost nothing and why Postgres requires them to exist
-- before a composite FK can point at them.
ALTER TABLE users
    ADD CONSTRAINT users_id_account_key UNIQUE (id, account_id);

-- Turning a user off without deleting them. Deleting a user cascades their keys, their role
-- assignments and — via `queries.account_id` staying put — nothing of their history; but it also
-- destroys the record of who they were. Disabling is the reversible, auditable form, and
-- authentication refuses a disabled user's keys without them having to be revoked one by one.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ;

COMMENT ON COLUMN users.disabled_at IS
    'When this user was disabled. Non-NULL means every one of their API keys fails authentication.';

-- Credentials. One row per issued token; the token is not in it.
CREATE TABLE IF NOT EXISTS api_keys (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id   BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    user_id      BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    -- What an operator calls this key ("cli-laptop", "grafana"). Unique per user so revoking by
    -- name is unambiguous.
    name         TEXT        NOT NULL,
    -- First 12 characters of the token: the lookup key, and the only part ever displayed.
    token_prefix TEXT        NOT NULL UNIQUE,
    -- Hex SHA-256 of the whole token. Compared in constant time; see the header.
    token_hash   TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL means "does not expire". A key with an expiry is the better default for a human; a
    -- service key that silently stops working at 3am is the reason it is not mandatory.
    expires_at   TIMESTAMPTZ,
    -- Set once, by revocation. Kept rather than deleted so an audit can still answer "what was
    -- this key, who held it, and when did we take it away".
    revoked_at   TIMESTAMPTZ,
    -- Best-effort last successful authentication. Written outside the request's critical path:
    -- a failed touch must never fail an otherwise valid request.
    last_used_at TIMESTAMPTZ,
    UNIQUE (user_id, name),
    FOREIGN KEY (user_id, account_id) REFERENCES users (id, account_id)
);

-- "Every key this tenant owns" — what `lldb-qe-auth key list` and a tenant deletion both walk.
CREATE INDEX IF NOT EXISTS api_keys_account_idx ON api_keys (account_id);

COMMENT ON COLUMN api_keys.token_hash IS
    'Hex SHA-256 of the token. Not a KDF hash on purpose: the token is 256 bits of CSPRNG output, so there is nothing to brute force.';

-- Roles. A role is a named bag of grants, per tenant.
CREATE TABLE IF NOT EXISTS roles (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (account_id, name),
    -- The composite key `grants` and `user_roles` reference. See the header.
    UNIQUE (id, account_id)
);

-- Role assignment. No `granted_by` column: this build has no notion of *who* ran the CLI, and a
-- column that is always NULL is worse than an absent one because it looks like an answer.
CREATE TABLE IF NOT EXISTS user_roles (
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role_id    BIGINT      NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    -- Carried so the two composite FKs below can force user and role into the same tenant.
    account_id BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, role_id),
    FOREIGN KEY (user_id, account_id) REFERENCES users (id, account_id),
    FOREIGN KEY (role_id, account_id) REFERENCES roles (id, account_id)
);

-- The "which roles does this user hold" direction is the primary key's; this is the other one,
-- "who holds this role", which is what listing a role before deleting it needs.
CREATE INDEX IF NOT EXISTS user_roles_role_idx ON user_roles (role_id);

-- Grants: (role, privilege, object). The unit the plan-time check consults.
--
-- `object_name` is a dotted path whose shape depends on `object_type`:
--   catalog    lldb
--   namespace  lldb.sales
--   table      lldb.sales.orders
--   warehouse  analytics          (a bare name — warehouses are not in the catalog namespace)
--
-- Containment (a namespace grant implying its tables, a catalog grant implying its namespaces) is
-- computed in Rust rather than in SQL, deliberately: it is a pure function of two strings, so it
-- belongs where it can be unit-tested without a database, and where the error it produces can name
-- the exact privilege and object that is missing. See `crate::rbac`.
CREATE TABLE IF NOT EXISTS grants (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  BIGINT      NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    role_id     BIGINT      NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    -- The legal set, named so a violation reports `grants_privilege_check` rather than an
    -- anonymous constraint number. Adding a privilege is a migration, which is the right amount of
    -- friction for a value the whole fleet must agree on.
    privilege   TEXT        NOT NULL
                CHECK (privilege IN ('SELECT', 'INSERT', 'DELETE', 'UPDATE', 'USAGE', 'ALL')),
    object_type TEXT        NOT NULL
                CHECK (object_type IN ('catalog', 'namespace', 'table', 'warehouse')),
    object_name TEXT        NOT NULL CHECK (object_name <> ''),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Granting the same thing twice is one grant, not two. Without this, `revoke` would have to
    -- loop and an operator could never be sure a privilege was actually gone.
    UNIQUE (role_id, privilege, object_type, object_name),
    FOREIGN KEY (role_id, account_id) REFERENCES roles (id, account_id)
);

-- The read path: every grant of every role a user holds, fetched once per query.
CREATE INDEX IF NOT EXISTS grants_role_idx ON grants (role_id);

COMMENT ON COLUMN grants.privilege IS
    'SELECT | INSERT | DELETE | UPDATE | USAGE | ALL. ALL covers every other privilege on the same object.';
COMMENT ON COLUMN grants.object_type IS
    'catalog | namespace | table | warehouse. A catalog grant implies its namespaces; a namespace grant implies its tables.';
COMMENT ON COLUMN grants.object_name IS
    'Dotted path: catalog / catalog.namespace / catalog.namespace.table, or a bare warehouse name.';
