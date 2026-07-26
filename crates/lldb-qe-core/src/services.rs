//! The **services database** — the fleet's shared, transactional control plane.
//!
//! # Why a database at all
//!
//! Everything the engine has needed so far is *data-plane* state: bytes in object storage,
//! Arrow batches in flight, a per-worker [`crate::stage_cache`]. All of it is either immutable
//! or deliberately per-process, which is exactly why a worker can be started, killed, and
//! replaced without ceremony.
//!
//! Control-plane state is the opposite. "Which tables exist", "who owns this warehouse", "is
//! this query still running" are facts the *whole fleet* has to agree on, and they change while
//! the fleet is running. Keeping them in a process's memory means two workers can disagree
//! about what the catalog contains, and the disagreement is invisible until a query returns the
//! wrong answer. Keeping them in a file on object storage means every mutation is a
//! read-modify-write race with no way to say "only if nobody else changed it first".
//!
//! So control-plane state lives in Postgres: one place, shared by every process, with
//! transactions and constraints doing the arbitration. This module is the connection layer for
//! it — a pooled [`ServicesDb`] plus the migrations that define its schema.
//!
//! # What lives here (and what will)
//!
//! This issue lands the foundation and exactly one real table: `accounts`, the tenant identity
//! everything else hangs off. The migration also creates `users`, `warehouses` and `queries` as
//! deliberately thin stubs, because their foreign keys to `accounts` are what make "an account
//! scopes a warehouse" a schema-enforced fact rather than a convention. Later issues own their
//! columns — virtual warehouses (#16) fill in `warehouses`, query history (#18) fills in
//! `queries`, and accounts/RBAC (#19) fills in `users` and starts *enforcing* the tenancy this
//! issue only records.
//!
//! The Iceberg SQL catalog (#8) also lives in this database, but **not** in this schema:
//! `iceberg-catalog-sql` creates and owns `iceberg_tables` and `iceberg_namespace_properties`
//! itself, so they are deliberately absent from `migrations/`. Two owners for one table is how
//! a schema ends up half-migrated — see [`crate::lakehouse`].
//!
//! # Migrations are an explicit step, never startup magic
//!
//! [`ServicesDb::migrate`] is called by one binary — `lldb-qe-migrate` — and by nothing else.
//! Coordinators and workers connect to an already-migrated database; they never migrate it.
//!
//! The temptation is obvious (migrate-on-boot means one less deploy step), and it is a
//! production footgun: a fleet rollout starts N workers at once, all of them would race to apply
//! the same DDL, and the failure mode is a half-migrated schema under load. sqlx's Postgres
//! migrator does take a session advisory lock, so the race is *survivable* rather than
//! corrupting — but "survivable" here means N-1 workers block on DDL during a rollout, and a
//! migration that fails leaves the schema dirty with no obvious owner. Making migration a
//! separate one-shot step means it has an exit code, a log, and a place in the deploy sequence
//! (compose: a `db-migrate` service the other roles `depends_on`).
//!
//! # Configuration: a URL *or* its parts
//!
//! [`ServicesArgs`] accepts `--metadata-url` (`LLDB_METADATA_URL`) and, failing that, composes
//! one from `--metadata-host/-port/-database/-user/-password/-sslmode`. Both forms exist for a
//! concrete reason: ECS/Fargate injects a Secrets Manager password as *its own environment
//! variable* and cannot string-interpolate it into a URL, so the discrete form is the only one
//! the CDK stack can actually wire up. The URL form stays because it is what a human types and
//! what most managed-Postgres providers hand you.
//!
//! Composition goes through [`url::Url`] rather than `format!`, so a password containing `@`,
//! `:`, `/` or `#` is percent-encoded instead of silently producing a URL that parses into a
//! different host.
//!
//! **Passwords never reach the logs.** [`ServicesArgs`] implements [`Debug`] by hand with the
//! password redacted, and every log line or error message that names the URL runs it through
//! [`redact_url`] first — because the single most common way a credential leaks is an operator
//! pasting a connection error into a ticket.
//!
//! # No services DB is a valid state
//!
//! [`ServicesArgs::connect`] returns `Ok(None)` when nothing is configured. Single-node and
//! local development do not need a control plane, and requiring one would make `cargo run` in a
//! checkout need a database. Callers that need it say so themselves, with an error that names
//! the flag.
//!
//! # Runtime queries, not the `query!` macros
//!
//! Every statement here uses `sqlx::query`/`query_as` with bind parameters, never the
//! compile-time-verified `query!` family. Those macros need a live database *at build time*,
//! which would mean `cargo build` — and the Docker image build, and CI's fast path — could not
//! run without Postgres. The migrations are still embedded at compile time (`sqlx::migrate!`
//! reads the directory, not a database), so the binary carries its schema with it.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use clap::Args;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};
use url::Url;

/// The schema, compiled into the binary from `crates/lldb-qe-core/migrations/`.
///
/// `sqlx::migrate!` resolves the directory **at compile time** relative to this crate's
/// manifest, hashes each file, and embeds the SQL — so a container image carries the schema it
/// expects and no migration directory has to be shipped alongside the binary.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Default Postgres port, used when `--metadata-port` is not given.
const DEFAULT_PORT: u16 = 5432;

/// Services-database connection settings, shared by every binary that talks to the control
/// plane. Every field has an env-var fallback so a container is configurable purely through the
/// environment (see the module docs for why both the URL and the discrete form exist).
///
/// [`Debug`] is implemented by hand: the derived one would print the password.
#[derive(Clone, Args)]
pub struct ServicesArgs {
    /// Full Postgres connection URL, e.g. `postgres://user:pass@host:5432/lldb`. Wins over the
    /// discrete `--metadata-*` parts when set.
    #[arg(long, env = "LLDB_METADATA_URL")]
    pub metadata_url: Option<String>,

    /// Services-database host. Setting this (without `--metadata-url`) is what turns the
    /// services DB on; leaving both unset means "no control plane", which is legal.
    #[arg(long, env = "LLDB_METADATA_HOST")]
    pub metadata_host: Option<String>,

    /// Services-database port.
    #[arg(long, env = "LLDB_METADATA_PORT", default_value_t = DEFAULT_PORT)]
    pub metadata_port: u16,

    /// Services-database name.
    #[arg(long, env = "LLDB_METADATA_DATABASE", default_value = "lldb")]
    pub metadata_database: String,

    /// Services-database user.
    #[arg(long, env = "LLDB_METADATA_USER", default_value = "lldb")]
    pub metadata_user: String,

    /// Services-database password. On ECS this arrives as a Secrets Manager–backed environment
    /// variable, which is why it is a discrete field rather than part of a URL.
    #[arg(long, env = "LLDB_METADATA_PASSWORD")]
    pub metadata_password: Option<String>,

    /// Postgres `sslmode` for the composed URL (`disable`, `prefer`, `require`, …).
    #[arg(long, env = "LLDB_METADATA_SSLMODE", default_value = "prefer")]
    pub metadata_sslmode: String,

    /// Maximum pooled connections. Keep it modest: a large fleet multiplies this by the number
    /// of processes, and Postgres charges real memory per backend.
    #[arg(long, env = "LLDB_METADATA_MAX_CONNECTIONS", default_value_t = 8)]
    pub metadata_max_connections: u32,

    /// How long to wait for a pooled connection before giving up, in seconds.
    #[arg(long, env = "LLDB_METADATA_CONNECT_TIMEOUT_SECS", default_value_t = 10)]
    pub metadata_connect_timeout_secs: u64,
}

impl std::fmt::Debug for ServicesArgs {
    /// Redacting by construction. These args end up inside a `#[derive(Debug)]` CLI struct that
    /// binaries log, so the derived impl would print the password on every startup line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServicesArgs")
            .field(
                "metadata_url",
                &self.metadata_url.as_deref().map(redact_url),
            )
            .field("metadata_host", &self.metadata_host)
            .field("metadata_port", &self.metadata_port)
            .field("metadata_database", &self.metadata_database)
            .field("metadata_user", &self.metadata_user)
            .field(
                "metadata_password",
                &self.metadata_password.as_ref().map(|_| REDACTED),
            )
            .field("metadata_sslmode", &self.metadata_sslmode)
            .field("metadata_max_connections", &self.metadata_max_connections)
            .field(
                "metadata_connect_timeout_secs",
                &self.metadata_connect_timeout_secs,
            )
            .finish()
    }
}

/// What a redacted secret prints as.
const REDACTED: &str = "****";

/// Placeholder for a URL that could not be parsed. An unparseable string might still *contain* a
/// password, so it is dropped whole rather than echoed.
const UNPARSEABLE_URL: &str = "<unparseable metadata url (redacted)>";

impl ServicesArgs {
    /// Resolve these args into a connection URL.
    ///
    /// - `--metadata-url` set → that, verbatim.
    /// - else `--metadata-host` set → compose one, percent-encoding the credentials.
    /// - else `None` — no services database is configured, which is a legitimate state (see the
    ///   module docs), not an error.
    pub fn resolve_url(&self) -> Result<Option<String>> {
        if let Some(url) = non_empty(self.metadata_url.as_deref()) {
            return Ok(Some(url.to_string()));
        }
        let Some(host) = non_empty(self.metadata_host.as_deref()) else {
            return Ok(None);
        };

        // Build through `Url` rather than `format!`: a password like `p@ss/word#1` concatenated
        // into a URL string parses as a *different host*, and would fail in a way that tempts an
        // operator to paste the credential into a bug report.
        let mut url = Url::parse("postgresql://placeholder/")
            .context("building the services-database URL")?;
        url.set_host(Some(host))
            .with_context(|| format!("--metadata-host `{host}` is not a valid host"))?;
        url.set_port(Some(self.metadata_port))
            .map_err(|()| anyhow!("--metadata-port {} is not usable", self.metadata_port))?;
        url.set_username(&self.metadata_user)
            .map_err(|()| anyhow!("--metadata-user is not usable in a URL"))?;
        if let Some(password) = non_empty(self.metadata_password.as_deref()) {
            url.set_password(Some(password))
                .map_err(|()| anyhow!("--metadata-password is not usable in a URL"))?;
        }
        url.set_path(&self.metadata_database);
        url.query_pairs_mut()
            .append_pair("sslmode", &self.metadata_sslmode);
        Ok(Some(url.into()))
    }

    /// Build these args from the process environment alone, with no command line in sight.
    ///
    /// Every field already carries an `env =` fallback, so this is just clap applying them —
    /// which means the environment is read *exactly* the way the binaries read it, defaults and
    /// all, instead of a second hand-rolled copy of the same rules that could drift.
    ///
    /// This exists for the one caller that is not a CLI: a manifest declaring a `sql` catalog
    /// with no `uri` (see [`crate::manifest::CatalogBackend::Sql`]). A manifest is
    /// config-as-data that lives in git, so it must not carry a password; the fleet's already
    /// configured `LLDB_METADATA_*` is where that credential belongs.
    pub fn from_env() -> Result<Self> {
        /// A throwaway [`clap::Parser`] whose only job is to give the flattened args an
        /// argv to be absent from, so only the `env =` fallbacks and defaults apply.
        #[derive(clap::Parser)]
        struct EnvOnly {
            #[command(flatten)]
            services: ServicesArgs,
        }
        let parsed = <EnvOnly as clap::Parser>::try_parse_from(["lldb"])
            .context("reading services-database settings (LLDB_METADATA_*) from the environment")?;
        Ok(parsed.services)
    }

    /// Connect to the configured services database, or `Ok(None)` when there isn't one.
    ///
    /// Errors carry the **redacted** URL: an operator needs to know *which* database refused
    /// them, and must not learn the password from a log aggregator.
    pub async fn connect(&self) -> Result<Option<ServicesDb>> {
        let Some(url) = self.resolve_url()? else {
            return Ok(None);
        };
        let db = ServicesDb::connect_with(
            &url,
            self.metadata_max_connections,
            Duration::from_secs(self.metadata_connect_timeout_secs),
        )
        .await?;
        Ok(Some(db))
    }
}

/// A pooled handle on the services database.
///
/// Cheap to clone-by-reference and safe to share: the pool underneath is the thing that handles
/// concurrency, so every subsystem (#8's catalog, #16's warehouses, #18's history) borrows the
/// same one rather than opening its own.
#[derive(Debug, Clone)]
pub struct ServicesDb {
    pool: PgPool,
}

/// One tenant. The root of every ownership chain in the services database: warehouses, users
/// and queries all carry an `account_id`, so "which tenant does this belong to" is answered by a
/// foreign key rather than by convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// The column list every account query returns, in the order [`Account`] expects.
const ACCOUNT_COLUMNS: &str = "id, name, created_at";

impl ServicesDb {
    /// Connect with the default pool settings (8 connections, 10s acquire timeout).
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, 8, Duration::from_secs(10)).await
    }

    /// Connect with explicit pool settings.
    ///
    /// `connect_timeout` bounds *acquiring* a connection, which covers establishing the first
    /// one — so an unreachable database fails in bounded time instead of hanging a startup.
    pub async fn connect_with(
        url: &str,
        max_connections: u32,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(connect_timeout)
            .connect(url)
            .await
            .with_context(|| {
                format!("connecting to the services database at {}", redact_url(url))
            })?;
        Ok(Self { pool })
    }

    /// The underlying pool — the escape hatch for subsystems that own their own SQL (the
    /// Iceberg SQL catalog, the query scheduler) rather than routing it through this type.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply every pending migration.
    ///
    /// Called by `lldb-qe-migrate` and nothing else — see the module docs for why this is not
    /// startup magic. sqlx's Postgres migrator takes a session-level *advisory lock* before
    /// touching anything, so two concurrent callers serialize rather than corrupt: the second
    /// blocks, then finds every version already applied and does nothing. Re-running is a clean
    /// no-op, which is what makes the compose `db-migrate` service safe to restart.
    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR
            .run(&self.pool)
            .await
            .context("applying services-database migrations")?;
        Ok(())
    }

    /// Round-trip a trivial query — proof the pool has a usable connection, not just a URL.
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("services-database health check failed")?;
        Ok(())
    }

    /// Close the pool, waiting for in-flight connections to be returned.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Create a tenant. Fails if the name is taken — `accounts.name` is `UNIQUE`, which is the
    /// point: two tenants sharing a name would make every scoped lookup ambiguous.
    pub async fn create_account(&self, name: &str) -> Result<Account> {
        let row = sqlx::query_as::<_, (i64, String, DateTime<Utc>)>(&format!(
            "INSERT INTO accounts (name) VALUES ($1) RETURNING {ACCOUNT_COLUMNS}"
        ))
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("creating account `{name}`"))?;
        Ok(Account::from(row))
    }

    /// Create the tenant if it does not exist, and return it either way.
    ///
    /// The `DO UPDATE SET name = EXCLUDED.name` looks like a no-op because it is one — but a
    /// bare `DO NOTHING` returns *no row* on conflict, which would force a second round trip and
    /// a race window between them. Writing the name back to itself makes the conflicting row
    /// part of the statement's result, so one statement always yields the account. This is what
    /// makes `lldb-qe-migrate --seed-account` safe to run on every deploy.
    pub async fn ensure_account(&self, name: &str) -> Result<Account> {
        let row = sqlx::query_as::<_, (i64, String, DateTime<Utc>)>(&format!(
            "INSERT INTO accounts (name) VALUES ($1) \
             ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name \
             RETURNING {ACCOUNT_COLUMNS}"
        ))
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("ensuring account `{name}` exists"))?;
        Ok(Account::from(row))
    }

    /// Look a tenant up by name — how a binary turns `--account default` into an id.
    pub async fn account_by_name(&self, name: &str) -> Result<Option<Account>> {
        let row = sqlx::query_as::<_, (i64, String, DateTime<Utc>)>(&format!(
            "SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE name = $1"
        ))
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("looking up account `{name}`"))?;
        Ok(row.map(Account::from))
    }

    /// Look a tenant up by id.
    pub async fn account_by_id(&self, id: i64) -> Result<Option<Account>> {
        let row = sqlx::query_as::<_, (i64, String, DateTime<Utc>)>(&format!(
            "SELECT {ACCOUNT_COLUMNS} FROM accounts WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("looking up account {id}"))?;
        Ok(row.map(Account::from))
    }

    /// Every tenant, oldest first.
    pub async fn list_accounts(&self) -> Result<Vec<Account>> {
        let rows = sqlx::query_as::<_, (i64, String, DateTime<Utc>)>(&format!(
            "SELECT {ACCOUNT_COLUMNS} FROM accounts ORDER BY id"
        ))
        .fetch_all(&self.pool)
        .await
        .context("listing accounts")?;
        Ok(rows.into_iter().map(Account::from).collect())
    }
}

impl From<(i64, String, DateTime<Utc>)> for Account {
    fn from((id, name, created_at): (i64, String, DateTime<Utc>)) -> Self {
        Self {
            id,
            name,
            created_at,
        }
    }
}

/// Replace a connection URL's password with `****`, for logs and error messages.
///
/// Never panics and never leaks: input it cannot parse is dropped entirely rather than echoed,
/// because a malformed string is exactly as likely to contain a credential as a well-formed one.
pub fn redact_url(url: &str) -> String {
    let Ok(mut parsed) = Url::parse(url) else {
        return UNPARSEABLE_URL.to_string();
    };
    if parsed.password().is_none() {
        return parsed.into();
    }
    if parsed.set_password(Some(REDACTED)).is_err() {
        // Only reachable for URLs that cannot carry credentials at all; be conservative.
        return UNPARSEABLE_URL.to_string();
    }
    parsed.into()
}

/// `Some(trimmed)` for a non-blank value; `None` for unset, empty, or whitespace-only. Compose
/// and ECS both happily inject `FOO=` for an unset variable, and that must read as "unset".
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode `%XX` escapes. Test-only, so the encoding assertions can compare against the
    /// original password without pulling `percent-encoding` in as a direct dependency.
    fn percent_decode(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%'
                && i + 2 < bytes.len()
                && let Ok(byte) = u8::from_str_radix(&input[i + 1..i + 3], 16)
            {
                out.push(byte);
                i += 3;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8(out).expect("decoded bytes are valid UTF-8")
    }

    fn args() -> ServicesArgs {
        ServicesArgs {
            metadata_url: None,
            metadata_host: None,
            metadata_port: DEFAULT_PORT,
            metadata_database: "lldb".to_string(),
            metadata_user: "lldb".to_string(),
            metadata_password: None,
            metadata_sslmode: "prefer".to_string(),
            metadata_max_connections: 8,
            metadata_connect_timeout_secs: 10,
        }
    }

    #[test]
    fn unconfigured_is_not_an_error() -> Result<()> {
        // Local/single-node use has no control plane, and must not need one.
        assert_eq!(args().resolve_url()?, None);
        Ok(())
    }

    #[test]
    fn blank_env_vars_read_as_unset() -> Result<()> {
        // `LLDB_METADATA_HOST=` in a compose file is "unset", not "connect to the empty host".
        let mut a = args();
        a.metadata_host = Some("   ".to_string());
        a.metadata_url = Some(String::new());
        assert_eq!(a.resolve_url()?, None);
        Ok(())
    }

    #[test]
    fn explicit_url_wins_over_the_parts() -> Result<()> {
        let mut a = args();
        a.metadata_url = Some("postgres://someone@elsewhere:6000/other".to_string());
        a.metadata_host = Some("ignored".to_string());
        assert_eq!(
            a.resolve_url()?.as_deref(),
            Some("postgres://someone@elsewhere:6000/other")
        );
        Ok(())
    }

    #[test]
    fn discrete_parts_compose_a_url() -> Result<()> {
        let mut a = args();
        a.metadata_host = Some("db.internal".to_string());
        a.metadata_port = 6543;
        a.metadata_database = "services".to_string();
        a.metadata_user = "lldb_app".to_string();
        a.metadata_password = Some("hunter2".to_string());
        a.metadata_sslmode = "require".to_string();
        assert_eq!(
            a.resolve_url()?.as_deref(),
            Some("postgresql://lldb_app:hunter2@db.internal:6543/services?sslmode=require")
        );
        Ok(())
    }

    #[test]
    fn a_password_full_of_reserved_characters_round_trips() -> Result<()> {
        // The whole reason composition goes through `url::Url`: naive `format!` would let the
        // `@` and `/` here redefine the host and the database.
        let password = "p@ss:w/rd#1?&=[]";
        let mut a = args();
        a.metadata_host = Some("db.internal".to_string());
        a.metadata_password = Some(password.to_string());

        let composed = a.resolve_url()?.expect("host is set");
        let parsed = Url::parse(&composed)?;
        assert_eq!(parsed.host_str(), Some("db.internal"));
        assert_eq!(parsed.port(), Some(DEFAULT_PORT));
        assert_eq!(parsed.path(), "/lldb");
        assert_eq!(parsed.username(), "lldb");
        assert_eq!(
            percent_decode(parsed.password().expect("password")),
            password
        );
        // …and the raw string must not carry the reserved characters unescaped.
        assert!(
            !composed.contains("p@ss"),
            "password leaked unescaped: {composed}"
        );
        Ok(())
    }

    #[test]
    fn redact_url_hides_the_password() {
        assert_eq!(
            redact_url("postgres://lldb:hunter2@db.internal:5432/lldb"),
            "postgres://lldb:****@db.internal:5432/lldb"
        );
        // No password to hide: pass it through so the message still identifies the target.
        assert_eq!(
            redact_url("postgres://lldb@db.internal:5432/lldb"),
            "postgres://lldb@db.internal:5432/lldb"
        );
    }

    #[test]
    fn redact_url_never_echoes_garbage() {
        // A string that isn't a URL may still be a password-bearing typo; drop it whole.
        for garbage in ["", "not a url", "://:hunter2@", "postgres://[bad"] {
            let redacted = redact_url(garbage);
            assert_eq!(redacted, UNPARSEABLE_URL, "input: {garbage}");
            assert!(!redacted.contains("hunter2"));
        }
    }

    #[test]
    fn debug_never_prints_the_password() {
        let mut a = args();
        a.metadata_password = Some("hunter2".to_string());
        a.metadata_url = Some("postgres://lldb:swordfish@db.internal/lldb".to_string());
        let rendered = format!("{a:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("swordfish"), "{rendered}");
        assert!(rendered.contains(REDACTED), "{rendered}");
        // Still useful for diagnosis: the non-secret parts survive.
        assert!(rendered.contains("db.internal"), "{rendered}");
    }

    #[test]
    fn from_env_reads_the_same_variables_the_binaries_do() -> Result<()> {
        // Deliberately no env mutation: `set_var` is `unsafe` in edition 2024 and would race the
        // other tests sharing this process. So assert the invariant that holds either way —
        // reading the environment always parses — plus, when the environment really is clean,
        // that it resolves to the legal "no services database" state.
        let args = ServicesArgs::from_env()?;
        let configured = ["LLDB_METADATA_URL", "LLDB_METADATA_HOST"]
            .iter()
            .any(|key| std::env::var(key).is_ok_and(|v| !v.trim().is_empty()));
        if !configured {
            assert_eq!(args.resolve_url()?, None);
        }
        Ok(())
    }

    #[test]
    fn migrations_are_embedded_and_ordered() {
        // `sqlx::migrate!` resolves at compile time, so this holds with no database in sight.
        let versions: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        assert!(!versions.is_empty(), "no migrations were embedded");
        assert!(
            versions.windows(2).all(|w| w[0] < w[1]),
            "migration versions must be strictly ascending, got {versions:?}"
        );
        assert_eq!(versions[0], 1, "the foundation migration is version 1");
    }
}
