//! `lldb-qe-reap` — resolve query-history rows whose coordinator is gone, then exit.
//!
//! # What it does, precisely
//!
//! It finds rows in `queries` that are still `queued` or `running` but were written by a
//! coordinator **process** that the control plane no longer believes is alive, and marks them
//! `failed` with a reason distinguishing "never started" from "died mid-flight". The rule it
//! applies, the compare-and-swap that makes it safe next to a live coordinator, and the honest
//! limits of both live in [`lldb_qe_control::reaper`]'s module docs; this binary is the operator's
//! end of it.
//!
//! Two properties worth knowing before scheduling it:
//!
//! - **It is idempotent.** A second run immediately after a first reaps nothing, because the rows
//!   the first one resolved are terminal and terminal rows are not eligible. Running it on a
//!   perfectly healthy fleet is a no-op that reports zero.
//! - **It is bounded.** One run resolves at most `--limit` rows (default
//!   [`DEFAULT_REAP_BATCH`](lldb_qe_control::reaper::DEFAULT_REAP_BATCH)) and says so when it
//!   fills the batch. That is `lldb-qe-core`'s `result_cache` rule for sweeps, for the same
//!   reason: a maintenance task whose cost is proportional to a deployment's entire accumulated
//!   backlog is the one that turns a bad week into an incident.
//!
//! # Why a separate binary, and not a coordinator doing this at startup
//!
//! This is [`lldb_qe_control::liveness`]'s decision 4 and it was made there deliberately. A
//! coordinator that swept for dead peers as it booted would be doing the most dangerous version of
//! this: a fleet restarting together, every member judging the others through a lease that none of
//! them have renewed yet, all of them concluding the others are dead and failing each other's live
//! queries. There is no ordering of that which is safe, so the sweep is a one-shot that runs
//! *outside* any coordinator's lifecycle — the same posture `lldb-qe-migrate` takes toward DDL, and
//! for a closely related reason.
//!
//! Like the other operator tools, its credential is the services database's own, so it is not (and
//! cannot usefully be) access-controlled. Schedule it — a cron entry, an ECS scheduled task, a
//! Kubernetes `CronJob` — at whatever interval you want stranded rows resolved within. Nothing
//! breaks if it never runs; history simply keeps rows nobody will ever finish, and
//! `peak_concurrency` keeps over-reporting.
//!
//!   lldb-qe-reap --metadata-url postgres://lldb@localhost/lldb --dry-run
//!   lldb-qe-reap --metadata-url postgres://lldb@localhost/lldb

use anyhow::{Context, Result, bail};
use clap::Parser;
use lldb_qe_control::query_log::QueryRecord;
use lldb_qe_control::reaper::{DEFAULT_REAP_BATCH, ReapReason, ReapedQuery};
use lldb_qe_control::{ServicesArgs, init_tracing, redact_url};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-reap",
    about = "Resolve query-history rows stranded by a coordinator that is no longer live",
    version = lldb_qe_control::BUILD_VERSION
)]
struct Cli {
    /// Restrict the sweep to one tenant. The default is every account, which is the ordinary case:
    /// a dead coordinator strands whatever it happened to be running and does not care whose it
    /// was. Naming an account is for resolving one tenant's history after an incident.
    #[arg(long, env = "LLDB_ACCOUNT")]
    account: Option<String>,

    /// Most rows to resolve in this run. A run that fills its batch says so; run it again.
    ///
    /// Refused at zero rather than accepted and reinterpreted. [`ServicesDb::reap_stranded_queries`]
    /// clamps a zero batch to one, because a sweep that resolves nothing is never what a caller
    /// meant — but that clamp is a library invariant, and letting it stand in for operator input
    /// would make `--limit 0` reap a row while `report_batch` compared `0 >= 0` and announced the
    /// batch was full. An operator's number should mean what it says or be refused; it should not
    /// quietly become a different number.
    #[arg(
        long,
        env = "LLDB_REAP_LIMIT",
        default_value_t = DEFAULT_REAP_BATCH,
        value_parser = positive_limit,
    )]
    limit: usize,

    /// Report what would be resolved and change nothing.
    ///
    /// Worth making a habit of the first time this runs against a deployment: the rows it lists are
    /// the ones nobody will ever finish, and seeing *which* coordinator produced them is usually
    /// more interesting than the fact that they exist.
    #[arg(long)]
    dry_run: bool,

    #[command(flatten)]
    services: ServicesArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_control::BUILD_VERSION,
        "starting lldb-qe-reap"
    );

    // Like `lldb-qe-migrate` and `lldb-qe-warehouse`, and unlike the coordinator: this binary is
    // *about* the control plane, so an unconfigured services database is a usage error rather than
    // the supported single-node mode. Say so with the flag names.
    let Some(url) = cli.services.resolve_url()? else {
        bail!(
            "no services database configured: set --metadata-url (LLDB_METADATA_URL), or \
             --metadata-host (LLDB_METADATA_HOST) plus the other --metadata-* parts"
        );
    };
    let redacted = redact_url(&url);
    let db = cli
        .services
        .connect()
        .await?
        .expect("a url resolved, so connect() cannot report `unconfigured`");
    db.health_check()
        .await
        .with_context(|| format!("services database at {redacted} is not answering"))?;

    // Context for whoever reads the log afterwards: a sweep that resolved nothing because every
    // coordinator is alive and one that resolved nothing because the registry is empty look
    // identical in the row count, and they mean very different things.
    match db.live_coordinators().await {
        Ok(live) => tracing::info!(
            live_coordinators = live.len(),
            url = %redacted,
            "sweeping for query rows whose coordinator is no longer live"
        ),
        Err(error) => tracing::warn!(
            error = %format!("{error:#}"),
            "could not count live coordinators; this is informational only and the sweep continues"
        ),
    }

    // Resolved to an id up front, and *not* created if missing: creating a tenant as a side effect
    // of a typo'd maintenance command is how ghost tenants accumulate — the same rule
    // `lldb-qe-warehouse` follows.
    let account_id = match &cli.account {
        None => None,
        Some(name) => Some(
            db.account_by_name(name)
                .await?
                .with_context(|| {
                    format!("account `{name}` does not exist in the services database")
                })?
                .id,
        ),
    };

    let result = run(&db, &cli, account_id).await;
    db.close().await;
    result
}

async fn run(db: &lldb_qe_control::ServicesDb, cli: &Cli, account_id: Option<i64>) -> Result<()> {
    if cli.dry_run {
        let stranded = db.list_stranded_queries(account_id, cli.limit).await?;
        if stranded.is_empty() {
            println!("nothing to reap: no queued/running query is owned by a dead coordinator");
            return Ok(());
        }
        print_header();
        for record in &stranded {
            print_candidate(record);
        }
        println!(
            "\n{} row(s) WOULD be marked failed. Nothing was changed — re-run without --dry-run.",
            stranded.len()
        );
        report_batch(stranded.len(), cli.limit);
        return Ok(());
    }

    let mut reaped = db.reap_stranded_queries(account_id, cli.limit).await?;
    if reaped.is_empty() {
        println!("nothing to reap: no queued/running query is owned by a dead coordinator");
        return Ok(());
    }
    // `RETURNING` hands rows back in whatever order the update touched them, which is not the order
    // the sweep chose them in. Sorted here so this listing and `--dry-run`'s read the same way.
    reaped.sort_by_key(|row| (row.submitted_at, row.id));
    print_header();
    for row in &reaped {
        print_reaped(row);
    }
    let never_started = reaped
        .iter()
        .filter(|r| r.reason == ReapReason::NeverStarted)
        .count();
    println!(
        "\nreaped {} row(s): {never_started} never started, {} died mid-flight",
        reaped.len(),
        reaped.len() - never_started
    );
    // The number an operator actually wants out of this run: not "17 rows" but "17 rows, and they
    // belonged to these two processes". A recurring slot here is a coordinator that keeps dying.
    tracing::info!(
        reaped = reaped.len(),
        never_started,
        died_mid_flight = reaped.len() - never_started,
        "resolved query-history rows stranded by dead coordinators"
    );
    report_batch(reaped.len(), cli.limit);
    Ok(())
}

/// Parse `--limit`, refusing zero.
///
/// The refusal is the point. [`ServicesDb::reap_stranded_queries`] clamps a zero batch to one — a
/// sensible library invariant, since a sweep that resolves nothing is never what a caller meant —
/// but a clamp is the wrong response to *operator* input. `--limit 0` would reap one row while
/// [`report_batch`] compared `0 >= 0` and announced the batch was full, so the run would both do
/// something the operator did not ask for and describe it wrongly. A number should mean what it
/// says or be refused.
fn positive_limit(raw: &str) -> Result<usize, String> {
    match raw.parse::<usize>() {
        Ok(0) => Err("must be at least 1; a sweep of zero rows resolves nothing".to_string()),
        Ok(n) => Ok(n),
        Err(e) => Err(format!("`{raw}` is not a whole number: {e}")),
    }
}

/// A full batch means there may be more. Said out loud rather than looped over here, because a
/// sweep that keeps going until the table is clean is unbounded again by another name — and an
/// operator who wants that can run this in a loop with their eyes open.
fn report_batch(rows: usize, limit: usize) {
    if rows >= limit {
        println!(
            "the batch limit of {limit} was reached, so there may be more; run this again \
             (or raise --limit)"
        );
    }
}

fn print_header() {
    println!(
        "{:>10}  {:>8}  {:<16}  {:<40}  SUBMITTED",
        "QUERY", "ACCOUNT", "REASON", "COORDINATOR"
    );
}

fn print_candidate(record: &QueryRecord) {
    println!(
        "{:>10}  {:>8}  {:<16}  {:<40}  {}",
        record.id,
        record.account_id,
        ReapReason::of(record.started_at).to_string(),
        coordinator_of(
            record.coordinator.as_deref(),
            record.coordinator_incarnation.as_deref()
        ),
        record.submitted_at.to_rfc3339()
    );
}

fn print_reaped(row: &ReapedQuery) {
    println!(
        "{:>10}  {:>8}  {:<16}  {:<40}  {}",
        row.id,
        row.account_id,
        row.reason.to_string(),
        coordinator_of(Some(&row.coordinator), Some(&row.coordinator_incarnation)),
        row.submitted_at.to_rfc3339()
    );
}

/// `slot#incarnation`, the rendering [`lldb_qe_control::CoordinatorIdentity`] uses in logs — so a
/// line printed here can be grepped for in the coordinator's own output.
fn coordinator_of(slot: Option<&str>, incarnation: Option<&str>) -> String {
    match (slot, incarnation) {
        (Some(slot), Some(incarnation)) => format!("{slot}#{incarnation}"),
        (Some(slot), None) => slot.to_string(),
        _ => "<unknown>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Zero is refused rather than clamped. The core sweep would turn it into one, so accepting it
    /// here would mean a run that resolves a row the operator did not ask for *and* reports a full
    /// batch, because `report_batch` compares `0 >= 0`. Refusing at the boundary is what keeps the
    /// number an operator typed and the number the sweep uses the same number.
    #[test]
    fn a_zero_limit_is_refused_rather_than_clamped() {
        let err = positive_limit("0").expect_err("zero resolves nothing and must be refused");
        assert!(err.contains("at least 1"), "got: {err}");

        assert_eq!(
            positive_limit("1").expect("one is the smallest useful batch"),
            1
        );
        assert_eq!(positive_limit("500").expect("an ordinary batch"), 500);
    }

    /// A non-number names itself in the error, so an operator who typed `--limit lots` is told
    /// which argument was wrong rather than being handed a bare parse failure.
    #[test]
    fn a_non_numeric_limit_names_what_was_typed() {
        let err = positive_limit("lots").expect_err("not a number");
        assert!(
            err.contains("lots"),
            "the error must quote the input: {err}"
        );
    }
}
