//! `lldb-qe-migrate` — bring the services database up to the schema this build expects, then
//! exit.
//!
//! # Why this is a separate binary
//!
//! The obvious alternative is migrate-on-boot: have the coordinator (or every worker) apply
//! pending migrations when it starts. It saves a deploy step and it is a production footgun.
//! Rolling a fleet starts N processes at once, so N processes would race to run the same DDL
//! against the same database. sqlx's Postgres migrator takes a session advisory lock, so the
//! race is survivable rather than corrupting — but survivable means the whole fleet blocks on
//! DDL during a rollout, and a migration that fails mid-way leaves a dirty schema owned by
//! nobody, discovered as a crash loop.
//!
//! Making it one explicit, one-shot step gives migration what an operator actually needs: a
//! single owner, an exit code, a log, and a defined position in the deploy sequence. In compose
//! that is the `db-migrate` service every other role `depends_on`
//! (`condition: service_completed_successfully`); on ECS it is a task you run before the
//! service update.
//!
//! # Seeding
//!
//! `--seed-account` (repeatable; `LLDB_SEED_ACCOUNTS` as a comma-separated list) ensures a
//! tenant exists. It is idempotent — [`ServicesDb::ensure_account`] returns the existing row
//! rather than failing — so this binary is safe to run on every deploy, which is the only way a
//! "run this before the rollout" step stays honest.
//!
//!   lldb-qe-migrate --metadata-url postgres://lldb@localhost/lldb --seed-account default

use anyhow::{Context, Result, bail};
use clap::Parser;
use lldb_qe_core::{ServicesArgs, init_tracing, redact_url};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-migrate",
    about = "Apply lldb services-database migrations and seed accounts",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Account name to create if missing. Repeatable; `LLDB_SEED_ACCOUNTS` takes a
    /// comma-separated list. Idempotent, so re-running a deploy is a no-op.
    #[arg(
        long = "seed-account",
        env = "LLDB_SEED_ACCOUNTS",
        value_delimiter = ',',
        default_value = "default"
    )]
    seed_account: Vec<String>,

    #[command(flatten)]
    services: ServicesArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_core::BUILD_VERSION,
        "starting lldb-qe-migrate"
    );

    // Unlike the coordinator, this binary has nothing to do without a database — say so with the
    // flag names rather than failing later inside the connection layer.
    let Some(url) = cli.services.resolve_url()? else {
        bail!(
            "no services database configured: set --metadata-url (LLDB_METADATA_URL), or \
             --metadata-host (LLDB_METADATA_HOST) plus the other --metadata-* parts"
        );
    };
    let redacted = redact_url(&url);
    tracing::info!(url = %redacted, "connecting to the services database");

    let db = cli
        .services
        .connect()
        .await?
        .expect("a url resolved, so connect() cannot report `unconfigured`");
    db.health_check()
        .await
        .with_context(|| format!("services database at {redacted} is not answering"))?;

    // The migrator's own advisory lock makes this safe even if two of these run at once.
    db.migrate()
        .await
        .with_context(|| format!("migrating the services database at {redacted}"))?;
    // The count is what this build *carries*, not what this run applied — the migrator reports no
    // such number, and re-running is usually a no-op. Named accordingly, because "migrations=1"
    // on a rerun would otherwise read as "one migration was just applied".
    let embedded = lldb_qe_core::services::MIGRATOR.iter().count();
    tracing::info!(
        embedded_migrations = embedded,
        url = %redacted,
        "services-database schema is up to date"
    );

    for name in &cli.seed_account {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let account = db
            .ensure_account(name)
            .await
            .with_context(|| format!("seeding account `{name}`"))?;
        tracing::info!(
            account = %account.name,
            account_id = account.id,
            "account ready"
        );
    }

    db.close().await;
    Ok(())
}
