//! **Authentication** — turning a credential on the wire into a tenant-scoped identity.
//!
//! Before this module the coordinator's Flight ticket carried an `account` field and the server
//! believed it. That is not weak authentication, it is *no* authentication: anyone who could reach
//! the port could name any tenant, read any table that tenant could read, and have their query
//! recorded in that tenant's history. This module replaces the claim with a proof.
//!
//! [`crate::rbac`] is the other half — what an identity may *do*. The split is deliberate: this
//! side is I/O against the services database and can fail for transport reasons; that side is a
//! pure function over a grant set.
//!
//! # The credential: an opaque API key, hashed with SHA-256
//!
//! Not a JWT, not OAuth. Those are the "room to grow" the issue names, not the "to start": an API
//! key is one table, one comparison, and revocable by writing one column. A JWT is not revocable at
//! all without the very table a key already needs.
//!
//! The token is `lldb_` followed by base64url of 32 bytes from the OS CSPRNG. What is stored is a
//! **hex SHA-256 of the whole token**, plus its first [`TOKEN_PREFIX_LEN`] characters for lookup.
//! The token itself is never written anywhere; it is returned exactly once, by
//! [`ServicesDb::create_api_key`], and printed exactly once, by `lldb-qe-auth key create`.
//!
//! **SHA-256, deliberately not argon2/bcrypt/scrypt.** A slow KDF exists to make a *low-entropy,
//! human-chosen* secret expensive to guess. There is nothing to guess in 256 bits of CSPRNG output;
//! a KDF would buy no security while costing a dependency and per-request CPU on the hottest path
//! in the system. This is what GitHub does with its own personal access tokens. The comparison is
//! still constant-time ([`subtle`]), because the *digest* is not secret-independent data: comparing
//! it with `==` leaks, byte by byte, how much of a guessed digest is right.
//!
//! If a password column ever lands here, it must not follow this precedent. A password is exactly
//! the low-entropy case a KDF is for.
//!
//! # Where the credential travels: gRPC metadata, never the ticket
//!
//! `authorization: Bearer <token>`, in the Flight request metadata. Not in the ticket — a ticket is
//! a wire payload that gets logged, hashed into a stage id and cached, and a credential must be in
//! none of those places.
//!
//! **The account stops being client-claimed.** After authentication the tenant is derived from the
//! token. If the ticket also names an account it must *equal* the token's, or the request is
//! `PERMISSION_DENIED`; it is never used to select the tenant. See [`crate::server`].
//!
//! # An unconfigured services database is still legal
//!
//! No database means no accounts, no users, no keys and no grants — so there is nothing to
//! authenticate against and nothing to authorize, and every query runs. That is a standing project
//! rule (see CLAUDE.md): `cargo run` in a checkout must not need Postgres. The coordinator says so
//! once, loudly, at startup rather than leaving the posture to be inferred.
//!
//! # Tenant isolation, honestly scoped
//!
//! Warehouses are `account_id`-scoped in the schema, and so are roles, users, keys and grants — the
//! composite foreign keys in migration `0005` make cross-tenant wiring unrepresentable rather than
//! merely unlikely. Two accounts on one deployment therefore cannot name each other's warehouses
//! and cannot be granted each other's tables.
//!
//! **The catalog is partitioned too, by a different mechanism, and the difference is worth
//! knowing.** `iceberg_tables` / `iceberg_namespace_properties` are created and owned by
//! `iceberg-catalog-sql`, not by this schema, so no migration here can add an `account_id` column
//! to them and no foreign key here can constrain them. What [`crate::tenancy`] does instead is give
//! each account **its own catalog and its own warehouse root**: `catalog_name` is already the
//! leading primary-key column of both tables and appears in the `WHERE` clause of every statement
//! that crate issues, so a per-account value partitions the rows exactly as a discriminator column
//! would, and a per-account warehouse root keeps the files apart (the catalog name does not appear
//! in a table's location, so scoping one without the other would collide on disk).
//!
//! The consequence for *this* module: a coordinator registers only the caller's own catalogs into
//! the caller's session, so another tenant's tables are not reachable by any name — the grant check
//! is no longer the only thing between two accounts. A catalog-wide grant is now genuinely
//! catalog-wide within one tenant and reaches nothing outside it.
//!
//! Two honest limits remain, and neither is closed by the above. **Enforcement is per boundary, not
//! per row**: the partitioning is a property of what a session registers, so a component that
//! opened a catalog for the wrong scope would read the wrong tenant's rows — nothing in Postgres
//! would stop it, the way a foreign key stops a cross-tenant grant. And this separates tenants'
//! *layout*, not their *access*: see the worker boundary below, and [`crate::tenancy`] for the full
//! statement of what it does not stop.
//!
//! # The other boundary: the worker fleet
//!
//! Everything above is about the coordinator's front door. A worker's Flight port is a second door,
//! and until now it had no lock at all: any process that could reach it could have an arbitrary
//! physical plan executed, reading whatever the worker's storage credentials could read.
//!
//! [`FleetAuth`] closes it minimally with a **shared fleet secret** (`LLDB_FLEET_TOKEN`). The
//! coordinator sends it on every Flight call; a worker configured with one rejects a request
//! without it as `UNAUTHENTICATED`, constant-time compared.
//!
//! Be precise about what that is and is not. It proves *"you are part of this deployment"*. It does
//! **not** prove "you are user X" — and on its own it made every plan self-authorizing: anything
//! that could present it could have an arbitrary physical plan executed, reading whatever the
//! worker's storage credentials could reach.
//!
//! [`crate::plan_assertion`] is the per-request half, and it is a **second** credential on the same
//! call rather than a replacement for this one. The coordinator, having authenticated a request and
//! authorized its logical plan, mints a short-lived assertion naming the account, the user and the
//! object-store locations that plan may read, signs it with a key derived from this same fleet
//! secret, and sends it in its own metadata header; a worker verifies it and then checks that the
//! plan's own file scans fall inside those locations. So the two claims are now: *which deployment*
//! is calling (this module) and *which request* this is and what it authorizes (that one).
//!
//! Two limits are worth stating here rather than only there, because they are limits of the fleet
//! secret and not of the assertion. The key is **derived from this secret and is therefore
//! symmetric**: a worker can mint as well as verify, so an assertion proves "someone in this fleet
//! authorized this plan", not "the coordinator did" — a compromised worker can still forge one, and
//! only asymmetric keys would change that. And **rotation still means a restart**, for the same
//! reason it always did: this value is read once per process from the environment. Worker ports
//! still belong on a private network.
//!
//! Unset, a worker accepts everything and logs a loud startup warning naming the variable — because
//! `cargo run -p lldb-qe-worker` and the compose demo must keep working with no configuration,
//! while an insecure posture must be impossible to hold *by accident*.

use std::fmt;

use anyhow::{Context, Result, bail};
use base64::Engine;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

// Straight from `lldb_qe_types`, deliberately NOT via `crate::rbac` — that module re-exports these
// but also pulls in DataFusion for its plan walk, and this one has no business needing a query
// engine to look up a grant. Same rule as `crate::config`'s `StorageConfig` import.
use lldb_qe_types::rbac::{Grant, ObjectRef, ObjectType, Privilege, QueryAuthorization};

use crate::services::ServicesDb;

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

/// What every lldb API key starts with. A recognizable prefix is what lets a secret scanner — and a
/// human staring at a config file — identify a leaked credential as *ours*, which is the entire
/// reason every provider stamps one on.
pub const TOKEN_PREFIX: &str = "lldb_";

/// Bytes of entropy behind a token. 256 bits: enough that the digest is not brute-forceable and
/// that the stored SHA-256 is a lossless commitment to it rather than a compression.
const TOKEN_BYTES: usize = 32;

/// How much of a token is stored in the clear, for lookup and for display.
///
/// Twelve characters is `lldb_` plus seven of base64url — about 42 bits. That is not a secret and
/// is not meant to be one: it is an index key and a human handle. Its `UNIQUE` constraint means a
/// collision is an insert error the operator retries, not an ambiguous lookup.
pub const TOKEN_PREFIX_LEN: usize = 12;

/// The `authorization` metadata key both Flight boundaries read. Lowercase because gRPC metadata
/// keys are lowercase on the wire, and tonic will reject a key that is not.
pub const AUTHORIZATION_HEADER: &str = "authorization";

/// The scheme the `authorization` value uses.
const BEARER: &str = "Bearer ";

/// A freshly minted token: the secret to hand to the caller, and the two columns to store.
///
/// The secret is deliberately *not* `Clone` and deliberately not `Debug`-printable (see the manual
/// impl below): the whole design depends on it existing in exactly one place for one moment.
pub struct NewToken {
    /// The token itself. Show it once; it cannot be recovered.
    secret: String,
    /// First [`TOKEN_PREFIX_LEN`] characters — the lookup key and the displayable handle.
    pub prefix: String,
    /// Hex SHA-256 of `secret`.
    pub hash: String,
}

impl NewToken {
    /// Mint a token from the OS CSPRNG.
    pub fn generate() -> Self {
        // `rand::rng()` is the thread-local CSPRNG, seeded from the OS and reseeded periodically.
        // Not `SmallRng`, not a seeded `StdRng` — a predictable token is not a token.
        let mut bytes = [0u8; TOKEN_BYTES];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        // URL-safe and unpadded so the token survives being a header value, a CLI argument, an env
        // var and a `.pgpass`-style file without quoting or escaping.
        let body = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        Self::from_secret(format!("{TOKEN_PREFIX}{body}"))
    }

    /// The derived columns for an already-existing secret. Private because minting is the only
    /// legitimate way to get one; `generate` and the tests are the callers.
    fn from_secret(secret: String) -> Self {
        let hash = token_hash(&secret);
        // Byte-sliced rather than char-sliced: the alphabet is ASCII by construction, and a
        // `char_indices` dance here would only obscure that.
        let prefix = secret[..TOKEN_PREFIX_LEN.min(secret.len())].to_string();
        Self {
            secret,
            prefix,
            hash,
        }
    }

    /// Consume this token, yielding the secret. Consuming is the point: after this call the only
    /// copy is the caller's, and there is no second chance to print it.
    pub fn into_secret(self) -> String {
        self.secret
    }
}

impl fmt::Debug for NewToken {
    /// Hand-written for the same reason [`crate::services::ServicesArgs`]'s is: this type ends up
    /// inside other structs, and the derived impl would print the credential on the first
    /// `tracing::debug!` that touched one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewToken")
            .field("secret", &"****")
            .field("prefix", &self.prefix)
            .field("hash", &self.hash)
            .finish()
    }
}

/// Hex SHA-256 of a token. The only hashing this module does; see the header for why it is not a
/// KDF.
pub fn token_hash(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// The lookup prefix of a presented token, or an error if it cannot be one.
///
/// Rejecting the obviously-malformed before touching the database is not an optimization, it is
/// what stops a flood of junk credentials from becoming a flood of queries.
pub fn token_lookup_prefix(token: &str) -> Result<&str> {
    if !token.starts_with(TOKEN_PREFIX) {
        bail!("not an lldb API key (it must start with `{TOKEN_PREFIX}`)");
    }
    if token.len() < TOKEN_PREFIX_LEN {
        bail!("API key is too short to be valid");
    }
    Ok(&token[..TOKEN_PREFIX_LEN])
}

/// Constant-time comparison of a presented token against a stored digest.
///
/// The digests are hex, so this is a comparison of two ASCII strings — and it still runs in
/// constant time, because a byte-at-a-time `==` on the digest tells an attacker how many leading
/// bytes of their guess were right, which is exactly the feedback that turns 2^256 into 2×32.
pub fn verify_token(presented: &str, stored_hash: &str) -> bool {
    let computed = token_hash(presented);
    // `ct_eq` short-circuits on *length* only, which carries no information here: every hex
    // SHA-256 is 64 characters.
    computed.as_bytes().ct_eq(stored_hash.as_bytes()).into()
}

/// Parse an `authorization` header value into the bearer token it carries.
pub fn bearer_token(header: &str) -> Result<&str> {
    let rest = header
        .strip_prefix(BEARER)
        // Tolerate the lowercase spelling: it is legal per RFC 7235 (the scheme is
        // case-insensitive) and a client that sends it is not wrong, merely unlucky.
        .or_else(|| header.strip_prefix("bearer "))
        .context("authorization header must be `Bearer <token>`")?;
    let rest = rest.trim();
    if rest.is_empty() {
        bail!("authorization header carries an empty bearer token");
    }
    Ok(rest)
}

/// Render a token as an `authorization` header value.
pub fn bearer_header(token: &str) -> String {
    format!("{BEARER}{token}")
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One user, scoped to exactly one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: i64,
    pub account_id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
    /// Non-`None` means every one of this user's keys fails authentication, reversibly.
    pub disabled_at: Option<DateTime<Utc>>,
}

impl User {
    pub fn is_disabled(&self) -> bool {
        self.disabled_at.is_some()
    }
}

/// One issued credential, as stored — which is to say, without the credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKey {
    pub id: i64,
    pub account_id: i64,
    pub user_id: i64,
    pub name: String,
    /// The displayable handle. Never the token.
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
}

impl ApiKey {
    /// Whether this key would authenticate right now, ignoring the token itself.
    pub fn is_usable_at(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|at| at > now)
    }
}

/// One role: a named bag of grants, per tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub id: i64,
    pub account_id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Who a request is, once its credential has been proven.
///
/// This is what replaces the ticket's client-claimed `account`. Everything downstream — the
/// warehouse lookup, the history row, the result-cache key — takes its tenant from here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub account_id: i64,
    pub account_name: String,
    pub user_id: i64,
    pub user_name: String,
    /// Which key was presented. Recorded so "revoke the key that ran this" is answerable.
    pub api_key_id: i64,
    pub api_key_name: String,
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.user_name, self.account_name)
    }
}

/// Why a credential was not accepted.
///
/// Every variant that reaches a remote client is a refusal, and the *reason* is safe to disclose:
/// learning that a key is expired or revoked requires holding it. What is deliberately absent is
/// any signal distinguishing "no such key" from "wrong token for a real key" — both are
/// [`AuthError::Invalid`], because that distinction is the one an attacker who holds nothing would
/// like to have.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// No credential at all.
    #[error(
        "unauthenticated: this coordinator requires an API key. Send `authorization: Bearer \
         <token>` (create one with `lldb-qe-auth key create --user <USER> --name <LABEL>`)"
    )]
    Missing,
    /// Malformed, unknown, or simply wrong.
    #[error("unauthenticated: the API key is not valid")]
    Invalid,
    /// Correct, but withdrawn.
    #[error("unauthenticated: this API key was revoked")]
    Revoked,
    /// Correct, but past its expiry.
    #[error("unauthenticated: this API key expired")]
    Expired,
    /// Correct, but the human behind it is turned off.
    #[error("unauthenticated: the user this API key belongs to is disabled")]
    UserDisabled,
    /// The control plane could not be consulted. Distinct from every refusal above because it is
    /// *our* fault, not the caller's, and a client should retry rather than re-issue credentials.
    #[error("could not verify the API key: {0:#}")]
    Unavailable(anyhow::Error),
}

// ---------------------------------------------------------------------------
// Column lists and row shapes
// ---------------------------------------------------------------------------

const USER_COLUMNS: &str = "id, account_id, name, created_at, disabled_at";
type UserRow = (i64, i64, String, DateTime<Utc>, Option<DateTime<Utc>>);

const API_KEY_COLUMNS: &str = "id, account_id, user_id, name, token_prefix, created_at, \
                               expires_at, revoked_at, last_used_at";
type ApiKeyRow = (
    i64,
    i64,
    i64,
    String,
    String,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

const ROLE_COLUMNS: &str = "id, account_id, name, created_at";
type RoleRow = (i64, i64, String, DateTime<Utc>);

/// A grant joined to its role's name — the shape every read of `grants` returns, because a grant
/// without the role it hangs off cannot be reported usefully.
type GrantRow = (i64, i64, i64, String, String, String, String, DateTime<Utc>);

const GRANT_COLUMNS: &str = "g.id, g.account_id, g.role_id, r.name, g.privilege, g.object_type, \
                             g.object_name, g.created_at";

fn user_from_row(row: UserRow) -> User {
    let (id, account_id, name, created_at, disabled_at) = row;
    User {
        id,
        account_id,
        name,
        created_at,
        disabled_at,
    }
}

fn api_key_from_row(row: ApiKeyRow) -> ApiKey {
    let (
        id,
        account_id,
        user_id,
        name,
        token_prefix,
        created_at,
        expires_at,
        revoked_at,
        last_used_at,
    ) = row;
    ApiKey {
        id,
        account_id,
        user_id,
        name,
        token_prefix,
        created_at,
        expires_at,
        revoked_at,
        last_used_at,
    }
}

fn role_from_row(row: RoleRow) -> Role {
    let (id, account_id, name, created_at) = row;
    Role {
        id,
        account_id,
        name,
        created_at,
    }
}

/// Turn a stored grant into a typed one, failing loudly on a privilege or object type this build
/// does not know. Reachable only if someone edits the row by hand or a future migration adds a
/// value — both worth an error rather than a guess, because guessing here means guessing *in
/// favour of access*.
fn grant_from_row(row: GrantRow) -> Result<Grant> {
    let (id, account_id, role_id, role_name, privilege, object_type, object_name, created_at) = row;
    let privilege = privilege
        .parse::<Privilege>()
        .with_context(|| format!("reading grant {id} on role `{role_name}`"))?;
    let object_type = object_type
        .parse::<ObjectType>()
        .with_context(|| format!("reading grant {id} on role `{role_name}`"))?;
    Ok(Grant {
        id,
        account_id,
        role_id,
        role_name,
        privilege,
        object: ObjectRef {
            object_type,
            name: object_name,
        },
        created_at,
    })
}

// ---------------------------------------------------------------------------
// The database API
// ---------------------------------------------------------------------------

impl ServicesDb {
    // ---- Users ---------------------------------------------------------------------------

    /// Create a user in `account_id`. Fails if the name is taken *within that account* —
    /// `UNIQUE (account_id, name)`, so two tenants may each have an `alice`.
    pub async fn create_user(&self, account_id: i64, name: &str) -> Result<User> {
        validate_identifier("user name", name)?;
        let row = sqlx::query_as::<_, UserRow>(&format!(
            "INSERT INTO users (account_id, name) VALUES ($1, $2) RETURNING {USER_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("creating user `{name}` in account {account_id}"))?;
        Ok(user_from_row(row))
    }

    /// Look a user up by their handle within an account. Another tenant's identically named user
    /// is invisible from here, which is why `account_id` is a parameter and not an afterthought.
    pub async fn user_by_name(&self, account_id: i64, name: &str) -> Result<Option<User>> {
        let row = sqlx::query_as::<_, UserRow>(&format!(
            "SELECT {USER_COLUMNS} FROM users WHERE account_id = $1 AND name = $2"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up user `{name}` in account {account_id}"))?;
        Ok(row.map(user_from_row))
    }

    /// Every user in an account, by name.
    pub async fn list_users(&self, account_id: i64) -> Result<Vec<User>> {
        let rows = sqlx::query_as::<_, UserRow>(&format!(
            "SELECT {USER_COLUMNS} FROM users WHERE account_id = $1 ORDER BY name"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing users in account {account_id}"))?;
        Ok(rows.into_iter().map(user_from_row).collect())
    }

    /// Turn a user off (or, with `disabled = false`, back on) without touching their keys.
    pub async fn set_user_disabled(
        &self,
        account_id: i64,
        name: &str,
        disabled: bool,
    ) -> Result<User> {
        let row = sqlx::query_as::<_, UserRow>(&format!(
            "UPDATE users SET disabled_at = CASE WHEN $3 THEN now() ELSE NULL END \
             WHERE account_id = $1 AND name = $2 RETURNING {USER_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .bind(disabled)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("updating user `{name}`"))?
        .with_context(|| format!("no user named `{name}` in account {account_id}"))?;
        Ok(user_from_row(row))
    }

    // ---- API keys ------------------------------------------------------------------------

    /// Issue a key for `user_id`, returning the stored row **and the token, once**.
    ///
    /// The token is returned rather than logged or stored precisely because this is the only moment
    /// it exists. A caller that drops it has to issue a new key, which is the correct outcome and
    /// the one every credential system with a recovery path gets wrong.
    pub async fn create_api_key(
        &self,
        account_id: i64,
        user_id: i64,
        name: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(ApiKey, NewToken)> {
        validate_identifier("key name", name)?;
        let token = NewToken::generate();
        let row = sqlx::query_as::<_, ApiKeyRow>(&format!(
            "INSERT INTO api_keys (account_id, user_id, name, token_prefix, token_hash, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING {API_KEY_COLUMNS}"
        ))
        .bind(account_id)
        .bind(user_id)
        .bind(name)
        .bind(&token.prefix)
        .bind(&token.hash)
        .bind(expires_at)
        .fetch_one(self.pool())
        .await
        .with_context(|| {
            format!(
                "issuing API key `{name}` for user {user_id} (if this reports a unique-violation \
                 on token_prefix, simply run it again: the generated prefix collided)"
            )
        })?;
        Ok((api_key_from_row(row), token))
    }

    /// Every key issued in an account, newest first. Returns rows, so no token and no digest.
    pub async fn list_api_keys(&self, account_id: i64) -> Result<Vec<ApiKey>> {
        let rows = sqlx::query_as::<_, ApiKeyRow>(&format!(
            "SELECT {API_KEY_COLUMNS} FROM api_keys WHERE account_id = $1 ORDER BY id DESC"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing API keys for account {account_id}"))?;
        Ok(rows.into_iter().map(api_key_from_row).collect())
    }

    /// Revoke a key by `(user, name)`. Returns whether anything changed, so a caller can tell
    /// "revoked" from "already revoked" without a second query.
    ///
    /// The row is kept, not deleted: an audit that cannot answer "which key was this, and when did
    /// we take it away" is not an audit.
    pub async fn revoke_api_key(&self, account_id: i64, user_id: i64, name: &str) -> Result<bool> {
        let affected = sqlx::query(
            "UPDATE api_keys SET revoked_at = now() \
             WHERE account_id = $1 AND user_id = $2 AND name = $3 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(name)
        .execute(self.pool())
        .await
        .with_context(|| format!("revoking API key `{name}`"))?
        .rows_affected();
        Ok(affected > 0)
    }

    // ---- Authentication ------------------------------------------------------------------

    /// Prove a presented token and resolve it to a [`Principal`].
    ///
    /// One indexed lookup on `token_prefix`, one constant-time digest comparison, then the
    /// non-secret checks (revoked, expired, user disabled) in that order. `last_used_at` is touched
    /// afterwards and its failure is swallowed: a services database that cannot record a
    /// housekeeping timestamp must not reject a request that is otherwise perfectly valid.
    pub async fn authenticate(&self, presented: &str) -> Result<Principal, AuthError> {
        let Ok(prefix) = token_lookup_prefix(presented) else {
            return Err(AuthError::Invalid);
        };

        // **One statement, and that is a correctness requirement rather than an optimization.**
        // The digest and the revoked/expired/disabled flags must come from the same snapshot: read
        // the status first and the digest second, and a key revoked between the two reads is
        // authenticated against a stale "not revoked" — the request is accepted precisely because
        // it raced the revocation that was meant to stop it. One row, one point in time, one
        // decision.
        let row = sqlx::query_as::<
            _,
            (
                i64,
                String,
                i64,
                String,
                i64,
                String,
                String,
                Option<DateTime<Utc>>,
                Option<DateTime<Utc>>,
                Option<DateTime<Utc>>,
            ),
        >(
            "SELECT k.id, k.name, k.user_id, u.name, a.id, a.name, k.token_hash, \
                    k.revoked_at, k.expires_at, u.disabled_at \
               FROM api_keys k \
               JOIN users u ON u.id = k.user_id \
               JOIN accounts a ON a.id = k.account_id \
              WHERE k.token_prefix = $1",
        )
        .bind(prefix)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            AuthError::Unavailable(anyhow::Error::new(e).context("looking up the API key"))
        })?;

        let Some((
            key_id,
            key_name,
            user_id,
            user_name,
            account_id,
            account_name,
            token_hash,
            revoked_at,
            expires_at,
            disabled_at,
        )) = row
        else {
            return Err(AuthError::Invalid);
        };

        // Prove possession first, in constant time, and only then look at status — so the reasons
        // below are only ever disclosed to someone who actually holds the key.
        if !verify_token(presented, &token_hash) {
            return Err(AuthError::Invalid);
        }

        // Only now, having proven the caller holds this key, is it safe to say *why* it is refused.
        if revoked_at.is_some() {
            return Err(AuthError::Revoked);
        }
        if expires_at.is_some_and(|at| at <= Utc::now()) {
            return Err(AuthError::Expired);
        }
        if disabled_at.is_some() {
            return Err(AuthError::UserDisabled);
        }

        // Best effort, and deliberately after the decision: this is bookkeeping.
        if let Err(error) = sqlx::query("UPDATE api_keys SET last_used_at = now() WHERE id = $1")
            .bind(key_id)
            .execute(self.pool())
            .await
        {
            tracing::debug!(api_key_id = key_id, %error, "could not record api key use");
        }

        Ok(Principal {
            account_id,
            account_name,
            user_id,
            user_name,
            api_key_id: key_id,
            api_key_name: key_name,
        })
    }

    // ---- Roles ---------------------------------------------------------------------------

    /// Create a role in an account.
    pub async fn create_role(&self, account_id: i64, name: &str) -> Result<Role> {
        validate_identifier("role name", name)?;
        let row = sqlx::query_as::<_, RoleRow>(&format!(
            "INSERT INTO roles (account_id, name) VALUES ($1, $2) RETURNING {ROLE_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("creating role `{name}` in account {account_id}"))?;
        Ok(role_from_row(row))
    }

    /// Look a role up within an account.
    pub async fn role_by_name(&self, account_id: i64, name: &str) -> Result<Option<Role>> {
        let row = sqlx::query_as::<_, RoleRow>(&format!(
            "SELECT {ROLE_COLUMNS} FROM roles WHERE account_id = $1 AND name = $2"
        ))
        .bind(account_id)
        .bind(name)
        .fetch_optional(self.pool())
        .await
        .with_context(|| format!("looking up role `{name}` in account {account_id}"))?;
        Ok(row.map(role_from_row))
    }

    /// Every role in an account, by name.
    pub async fn list_roles(&self, account_id: i64) -> Result<Vec<Role>> {
        let rows = sqlx::query_as::<_, RoleRow>(&format!(
            "SELECT {ROLE_COLUMNS} FROM roles WHERE account_id = $1 ORDER BY name"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing roles in account {account_id}"))?;
        Ok(rows.into_iter().map(role_from_row).collect())
    }

    /// Give a user a role. Idempotent — assigning twice is one assignment, and a script that runs
    /// on every deploy should not have to check first.
    ///
    /// `account_id` is passed rather than derived because the composite foreign keys need it, and
    /// because passing it is what makes a mismatched user/role a *foreign key violation* instead of
    /// a cross-tenant role assignment that succeeds.
    pub async fn assign_role(&self, account_id: i64, user_id: i64, role_id: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO user_roles (user_id, role_id, account_id) VALUES ($1, $2, $3) \
             ON CONFLICT (user_id, role_id) DO NOTHING",
        )
        .bind(user_id)
        .bind(role_id)
        .bind(account_id)
        .execute(self.pool())
        .await
        .with_context(|| format!("assigning role {role_id} to user {user_id}"))?;
        Ok(())
    }

    /// Take a role away from a user. Returns whether anything changed.
    pub async fn unassign_role(&self, user_id: i64, role_id: i64) -> Result<bool> {
        let affected = sqlx::query("DELETE FROM user_roles WHERE user_id = $1 AND role_id = $2")
            .bind(user_id)
            .bind(role_id)
            .execute(self.pool())
            .await
            .with_context(|| format!("unassigning role {role_id} from user {user_id}"))?
            .rows_affected();
        Ok(affected > 0)
    }

    /// The roles a user holds, by name.
    pub async fn roles_of_user(&self, user_id: i64) -> Result<Vec<Role>> {
        let rows = sqlx::query_as::<_, RoleRow>(
            "SELECT r.id, r.account_id, r.name, r.created_at FROM roles r \
             JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = $1 ORDER BY r.name",
        )
        .bind(user_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing roles of user {user_id}"))?;
        Ok(rows.into_iter().map(role_from_row).collect())
    }

    // ---- Grants --------------------------------------------------------------------------

    /// Grant a privilege on an object to a role. Idempotent, because `UNIQUE (role_id, privilege,
    /// object_type, object_name)` makes "granted twice" meaningless.
    pub async fn grant(
        &self,
        account_id: i64,
        role_id: i64,
        privilege: Privilege,
        object: &ObjectRef,
    ) -> Result<()> {
        lldb_qe_types::rbac::validate_object_name(object.object_type, &object.name)?;
        sqlx::query(
            "INSERT INTO grants (account_id, role_id, privilege, object_type, object_name) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (role_id, privilege, object_type, object_name) DO NOTHING",
        )
        .bind(account_id)
        .bind(role_id)
        .bind(privilege.as_str())
        .bind(object.object_type.as_str())
        .bind(&object.name)
        .execute(self.pool())
        .await
        .with_context(|| format!("granting {privilege} on {object} to role {role_id}"))?;
        Ok(())
    }

    /// Revoke exactly one grant. Returns whether a row was removed.
    ///
    /// Exact, not covering: revoking `SELECT ON TABLE t` does not touch a `SELECT ON NAMESPACE`
    /// grant that also reaches `t`. Anything cleverer would mean a revoke silently editing grants
    /// an operator did not name, which is how an outage happens.
    pub async fn revoke(
        &self,
        role_id: i64,
        privilege: Privilege,
        object: &ObjectRef,
    ) -> Result<bool> {
        let affected = sqlx::query(
            "DELETE FROM grants WHERE role_id = $1 AND privilege = $2 \
             AND object_type = $3 AND object_name = $4",
        )
        .bind(role_id)
        .bind(privilege.as_str())
        .bind(object.object_type.as_str())
        .bind(&object.name)
        .execute(self.pool())
        .await
        .with_context(|| format!("revoking {privilege} on {object} from role {role_id}"))?
        .rows_affected();
        Ok(affected > 0)
    }

    /// Every grant held by a role.
    pub async fn grants_of_role(&self, role_id: i64) -> Result<Vec<Grant>> {
        let rows = sqlx::query_as::<_, GrantRow>(&format!(
            "SELECT {GRANT_COLUMNS} FROM grants g JOIN roles r ON r.id = g.role_id \
             WHERE g.role_id = $1 ORDER BY g.object_type, g.object_name, g.privilege"
        ))
        .bind(role_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing grants of role {role_id}"))?;
        rows.into_iter().map(grant_from_row).collect()
    }

    /// Every grant in an account, across every role — what `lldb-qe-auth show` prints.
    pub async fn list_grants(&self, account_id: i64) -> Result<Vec<Grant>> {
        let rows = sqlx::query_as::<_, GrantRow>(&format!(
            "SELECT {GRANT_COLUMNS} FROM grants g JOIN roles r ON r.id = g.role_id \
             WHERE g.account_id = $1 ORDER BY r.name, g.object_type, g.object_name, g.privilege"
        ))
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("listing grants in account {account_id}"))?;
        rows.into_iter().map(grant_from_row).collect()
    }

    /// The flattened union of every grant on every role a user holds — the one query a request
    /// makes before it is authorized.
    ///
    /// Scoped by `account_id` **as well as** `user_id`, which is redundant given the composite
    /// foreign keys and is here anyway: it is the last line of defence against a bug elsewhere
    /// handing this function a user id from the wrong tenant, and it costs an index lookup.
    pub async fn effective_grants(&self, account_id: i64, user_id: i64) -> Result<Vec<Grant>> {
        let rows = sqlx::query_as::<_, GrantRow>(&format!(
            "SELECT {GRANT_COLUMNS} FROM grants g \
               JOIN roles r ON r.id = g.role_id \
               JOIN user_roles ur ON ur.role_id = g.role_id \
              WHERE ur.user_id = $1 AND g.account_id = $2 \
              ORDER BY g.id"
        ))
        .bind(user_id)
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .with_context(|| format!("loading effective grants for user {user_id}"))?;
        rows.into_iter().map(grant_from_row).collect()
    }

    /// Everything the authorization check needs for one request, in one round trip.
    pub async fn authorization_for(&self, principal: &Principal) -> Result<QueryAuthorization> {
        let grants = self
            .effective_grants(principal.account_id, principal.user_id)
            .await?;
        Ok(QueryAuthorization::new(
            principal.account_id,
            principal.user_name.clone(),
            grants,
        ))
    }
}

/// Reject a name that would be confusing or unusable as a handle.
///
/// Deliberately looser than [`crate::warehouse::validate_warehouse_name`]: a user name is not a DNS
/// label, so `alice@example.com` and `Alice` are both fine. What is not fine is leading/trailing
/// whitespace (invisible, and it makes `--user alice` mysteriously not match) or an empty string.
fn validate_identifier(what: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("a {what} must not be empty");
    }
    if name != name.trim() {
        bail!("{what} `{name}` has leading or trailing whitespace");
    }
    if name.len() > 255 {
        bail!("{what} is {} characters; the limit is 255", name.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The worker boundary: a shared fleet secret
// ---------------------------------------------------------------------------

/// Environment variable carrying the shared fleet secret.
pub const FLEET_TOKEN_ENV: &str = "LLDB_FLEET_TOKEN";

/// What a worker requires of the process calling it, and what a coordinator presents.
///
/// See this module's header for the scope of the claim it makes. Two states, both legal:
///
/// - **`Required`** — every Flight call must carry this exact token, constant-time compared.
/// - **`Open`** — no token configured; the worker accepts everything and says so, loudly, once.
///
/// `Open` being the default is not an oversight. `cargo run -p lldb-qe-worker` and the compose demo
/// must work with no configuration at all, which is a standing rule here — but the warning means
/// nobody reaches production having *assumed* the port was closed.
#[derive(Clone, PartialEq, Eq)]
pub enum FleetAuth {
    /// No secret configured. Accept everything.
    Open,
    /// Accept only requests presenting this token.
    Required(String),
}

impl FleetAuth {
    /// Read the ambient configuration from [`FLEET_TOKEN_ENV`].
    ///
    /// A blank or whitespace-only value reads as unset, for the same reason it does in
    /// [`crate::services`]: compose and ECS both cheerfully inject `FOO=` for an unset variable.
    pub fn from_env() -> Self {
        match std::env::var(FLEET_TOKEN_ENV) {
            Ok(token) if !token.trim().is_empty() => Self::Required(token.trim().to_string()),
            _ => Self::Open,
        }
    }

    /// The token to present on an outgoing call, if any.
    pub fn token(&self) -> Option<&str> {
        match self {
            FleetAuth::Open => None,
            FleetAuth::Required(token) => Some(token),
        }
    }

    /// Whether this configuration turns the worker's door into a lock.
    pub fn is_required(&self) -> bool {
        matches!(self, FleetAuth::Required(_))
    }

    /// Check a presented credential. `Ok(())` means serve the request.
    ///
    /// The pure heart of the worker boundary, so the policy is testable without a socket.
    pub fn check(&self, presented: Option<&str>) -> Result<(), AuthError> {
        let FleetAuth::Required(expected) = self else {
            return Ok(());
        };
        let Some(presented) = presented else {
            return Err(AuthError::Missing);
        };
        // Constant-time even though this is a shared secret rather than a per-user one: it is
        // guessable exactly one byte at a time under a naive comparison, and a fleet secret is the
        // single credential worth the most to an attacker here.
        if bool::from(presented.as_bytes().ct_eq(expected.as_bytes())) {
            Ok(())
        } else {
            Err(AuthError::Invalid)
        }
    }

    /// Say what posture this is, once, at startup. Called by the worker's serve loop so an
    /// in-process test worker and the real binary report identically.
    pub fn log_posture(&self) {
        match self {
            FleetAuth::Required(_) => tracing::info!(
                "worker Flight port requires the shared fleet secret ({FLEET_TOKEN_ENV}), and \
                 every request must also carry a plan assertion signed with the key derived from \
                 it: a plan is executed only within the locations its assertion covers"
            ),
            FleetAuth::Open => tracing::warn!(
                "worker Flight port is UNAUTHENTICATED: any process that can reach it may have an \
                 arbitrary physical plan executed with this worker's storage credentials. Set \
                 {FLEET_TOKEN_ENV} to the same value on every coordinator and worker to close it."
            ),
        }
    }
}

impl fmt::Debug for FleetAuth {
    /// Never prints the secret. This type ends up in `#[derive(Debug)]` config structs that
    /// binaries log at startup.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FleetAuth::Open => f.write_str("FleetAuth::Open"),
            FleetAuth::Required(_) => f.write_str("FleetAuth::Required(****)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_token_verifies_against_its_own_hash_and_nothing_else() {
        let token = NewToken::generate();
        let hash = token.hash.clone();
        let prefix = token.prefix.clone();
        let secret = token.into_secret();

        assert!(secret.starts_with(TOKEN_PREFIX), "{secret}");
        assert_eq!(&secret[..TOKEN_PREFIX_LEN], prefix);
        assert!(verify_token(&secret, &hash), "the token must verify");

        // Every near miss must fail: a truncated token, an extra byte, a flipped character.
        assert!(!verify_token(&secret[..secret.len() - 1], &hash));
        assert!(!verify_token(&format!("{secret}x"), &hash));
        let mut flipped = secret.clone();
        flipped.replace_range(TOKEN_PREFIX_LEN..TOKEN_PREFIX_LEN + 1, "-");
        assert_ne!(flipped, secret);
        assert!(!verify_token(&flipped, &hash));

        // And an entirely different token must not verify against this hash.
        assert!(!verify_token(&NewToken::generate().into_secret(), &hash));
    }

    #[test]
    fn tokens_are_unique_and_carry_real_entropy() {
        // Not a statistical test — just proof that the generator is not returning a constant, which
        // is the failure mode a hard-coded seed or a stubbed RNG would produce.
        let a = NewToken::generate();
        let b = NewToken::generate();
        assert_ne!(a.hash, b.hash);
        assert_ne!(a.prefix, b.prefix);
        // 32 bytes base64url-unpadded is 43 characters, plus the 5-character scheme prefix.
        assert_eq!(a.into_secret().len(), TOKEN_PREFIX.len() + 43);
    }

    #[test]
    fn the_hash_is_a_stable_sha256_of_the_whole_token() {
        // Pinned against a known vector so a future refactor cannot silently change the digest and
        // invalidate every key in every deployment.
        assert_eq!(
            token_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(token_hash("abc").len(), 64);
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let token = NewToken::generate();
        let rendered = format!("{token:?}");
        assert!(!rendered.contains(&token.secret), "{rendered}");
        assert!(rendered.contains("****"), "{rendered}");
        // Still useful: the prefix identifies which key this is.
        assert!(rendered.contains(&token.prefix), "{rendered}");

        let fleet = FleetAuth::Required("hunter2".to_string());
        let rendered = format!("{fleet:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }

    #[test]
    fn a_malformed_token_is_rejected_before_the_database_is_touched() {
        assert!(token_lookup_prefix("").is_err());
        assert!(
            token_lookup_prefix("bearer-something").is_err(),
            "wrong scheme prefix"
        );
        assert!(token_lookup_prefix("lldb_").is_err(), "too short");
        // `lldb_` (5) + 7 characters of body = the 12 stored in `api_keys.token_prefix`.
        let ok = token_lookup_prefix("lldb_abcdefghij").expect("well-formed");
        assert_eq!(ok, "lldb_abcdefg");
        assert_eq!(ok.len(), TOKEN_PREFIX_LEN);
    }

    #[test]
    fn the_authorization_header_round_trips() {
        let token = "lldb_abcdefghij";
        assert_eq!(bearer_token(&bearer_header(token)).unwrap(), token);
        // RFC 7235 says the scheme is case-insensitive; a client that sends it lowercase is right.
        assert_eq!(bearer_token("bearer lldb_x").unwrap(), "lldb_x");
        // …and everything that is not a bearer credential is refused rather than guessed at.
        for bad in ["", "lldb_x", "Basic abc", "Bearer", "Bearer   "] {
            assert!(bearer_token(bad).is_err(), "`{bad}` should be refused");
        }
    }

    #[test]
    fn key_usability_is_a_function_of_revocation_and_expiry() {
        let now = Utc::now();
        let base = ApiKey {
            id: 1,
            account_id: 1,
            user_id: 1,
            name: "cli".to_string(),
            token_prefix: "lldb_abcdefg".to_string(),
            created_at: now,
            expires_at: None,
            revoked_at: None,
            last_used_at: None,
        };
        assert!(base.is_usable_at(now), "a fresh, unexpiring key is usable");

        let expired = ApiKey {
            expires_at: Some(now - chrono::Duration::seconds(1)),
            ..base.clone()
        };
        assert!(!expired.is_usable_at(now));

        let future = ApiKey {
            expires_at: Some(now + chrono::Duration::hours(1)),
            ..base.clone()
        };
        assert!(future.is_usable_at(now));

        let revoked = ApiKey {
            revoked_at: Some(now),
            ..base.clone()
        };
        assert!(
            !revoked.is_usable_at(now),
            "revocation beats a future expiry"
        );
    }

    #[test]
    fn identifiers_reject_the_shapes_that_would_not_match_later() {
        validate_identifier("user name", "alice@example.com").expect("emails are fine");
        validate_identifier("user name", "Alice").expect("case is fine");
        for bad in ["", " alice", "alice ", "\talice"] {
            assert!(
                validate_identifier("user name", bad).is_err(),
                "`{bad}` should be refused"
            );
        }
        assert!(validate_identifier("user name", &"a".repeat(256)).is_err());
    }

    #[test]
    fn an_open_fleet_accepts_everything_and_a_required_one_accepts_exactly_the_secret() {
        let open = FleetAuth::Open;
        assert!(open.check(None).is_ok());
        assert!(open.check(Some("anything")).is_ok());
        assert!(!open.is_required());
        assert_eq!(open.token(), None);

        let closed = FleetAuth::Required("s3cret".to_string());
        assert!(closed.is_required());
        assert_eq!(closed.token(), Some("s3cret"));
        closed.check(Some("s3cret")).expect("the right secret");
        assert!(
            matches!(closed.check(None), Err(AuthError::Missing)),
            "no credential must be `Missing`, which maps to UNAUTHENTICATED"
        );
        assert!(matches!(closed.check(Some("")), Err(AuthError::Invalid)));
        assert!(matches!(
            closed.check(Some("s3cre")),
            Err(AuthError::Invalid)
        ));
        assert!(matches!(
            closed.check(Some("s3cretx")),
            Err(AuthError::Invalid)
        ));
    }

    #[test]
    fn refusal_messages_do_not_distinguish_unknown_from_wrong() {
        // The one distinction an attacker holding nothing would like to have. Everything else is
        // safe to disclose, because learning it requires already holding the key.
        let invalid = AuthError::Invalid.to_string();
        assert!(!invalid.contains("unknown"), "{invalid}");
        assert!(!invalid.contains("no such"), "{invalid}");
        assert!(
            AuthError::Missing
                .to_string()
                .contains("lldb-qe-auth key create")
        );
        assert!(AuthError::Revoked.to_string().contains("revoked"));
        assert!(AuthError::Expired.to_string().contains("expired"));
    }
}
