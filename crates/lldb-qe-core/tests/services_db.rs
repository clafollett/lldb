//! The services database, exercised against a **real Postgres** — migrations, the accounts API,
//! and the foreign keys that make an account actually scope a warehouse.
//!
//! Issue #14's "done when" is a schema claim, and a schema claim can only be proven by a server:
//! `CREATE TABLE` typos, a `CHECK` that rejects legal values, an `ON DELETE CASCADE` that was
//! never written — none of them show up in a unit test. So this test needs a database, and it
//! finds one through [`support::resolve_target`]: an explicit `LLDB_TEST_POSTGRES_URL`, else a
//! throwaway container under `LLDB_DOCKER=1`, else a clean skip.
//!
//! The test is safe to run repeatedly against the same database, and concurrently with another
//! copy of itself: every account it creates is named with a pid + nanosecond suffix, and it
//! deletes exactly the rows it made. It never drops anything global — the database it is handed
//! may well be someone's dev instance.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test services_db
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test services_db -- --nocapture

mod support;

use anyhow::{Context, Result};
use lldb_qe_core::services::ServicesDb;
use support::{resolve_target, unique_name};

/// A name no other run — or concurrent copy of this run — will pick.
fn unique_account_name(tag: &str) -> String {
    unique_name(tag)
}

#[tokio::test(flavor = "multi_thread")]
async fn services_database_migrates_and_scopes_a_warehouse() -> Result<()> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP: set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with a Docker \
             daemon, to exercise the services database"
        );
        return Ok(());
    };

    let db = ServicesDb::connect(url).await?;
    db.health_check().await?;

    // ---- Migrations ------------------------------------------------------------------------
    db.migrate().await.context("first migration run")?;
    // Re-running must be a clean no-op: the compose `db-migrate` service runs on every `up`,
    // and a deploy step that only works the first time is not a deploy step.
    db.migrate().await.context("second migration run")?;
    db.health_check().await?;

    // Every table the foundation migration promises, stubs included — later issues extend these
    // rather than creating them, so their absence would break #16/#18/#19 at the wrong moment.
    for table in ["accounts", "users", "warehouses", "queries"] {
        let (exists,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(db.pool())
        .await?;
        assert!(exists, "migration did not create the `{table}` table");
    }

    // ---- Accounts --------------------------------------------------------------------------
    let name = unique_account_name("acct");
    let created = db.create_account(&name).await?;
    assert_eq!(created.name, name);
    assert!(created.id > 0, "identity column produced {}", created.id);

    // `created_at` comes from the server's `now()`, so it should be within minutes of ours; a
    // wildly wrong value means the column defaulted from somewhere unexpected.
    let age = chrono::Utc::now().signed_duration_since(created.created_at);
    assert!(
        age.num_minutes().abs() < 10,
        "created_at is {age} away from now: {}",
        created.created_at
    );

    // Round-trip both lookup paths.
    let by_name = db.account_by_name(&name).await?.expect("account by name");
    assert_eq!(by_name, created);
    let by_id = db.account_by_id(created.id).await?.expect("account by id");
    assert_eq!(by_id, created);
    assert!(
        db.account_by_name(&unique_account_name("missing"))
            .await?
            .is_none(),
        "a name that was never created must not resolve"
    );

    // The UNIQUE constraint is load-bearing: two tenants with one name make every scoped lookup
    // ambiguous. `create_account` must fail; `ensure_account` must return the same row instead.
    assert!(
        db.create_account(&name).await.is_err(),
        "a duplicate account name must be rejected"
    );
    let ensured = db.ensure_account(&name).await?;
    assert_eq!(ensured, created, "ensure_account must be idempotent");
    let ensured_twice = db.ensure_account(&name).await?;
    assert_eq!(ensured_twice.id, created.id);

    let listed = db.list_accounts().await?;
    assert!(
        listed.iter().any(|a| a.id == created.id),
        "list_accounts omitted the account we just created"
    );

    // ---- An account scopes a warehouse -----------------------------------------------------
    // The acceptance criterion, stated as SQL: a warehouse belongs to an account, and deleting
    // the account takes the warehouse with it. That is `ON DELETE CASCADE` doing the work the
    // application would otherwise have to remember to do.
    let (warehouse_id,): (i64,) =
        sqlx::query_as("INSERT INTO warehouses (account_id, name) VALUES ($1, $2) RETURNING id")
            .bind(created.id)
            .bind("wh-primary")
            .fetch_one(db.pool())
            .await
            .context("creating a warehouse scoped to the account")?;

    let (scoped_to,): (i64,) = sqlx::query_as("SELECT account_id FROM warehouses WHERE id = $1")
        .bind(warehouse_id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        scoped_to, created.id,
        "warehouse is not scoped to its account"
    );

    // A warehouse under a tenant that does not exist must be impossible.
    assert!(
        sqlx::query("INSERT INTO warehouses (account_id, name) VALUES ($1, $2)")
            .bind(i64::MAX)
            .bind("orphan")
            .execute(db.pool())
            .await
            .is_err(),
        "a warehouse must not be creatable under a nonexistent account"
    );

    // ---- Cleanup, which is also the cascade assertion ---------------------------------------
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(created.id)
        .execute(db.pool())
        .await
        .context("deleting the test account")?;

    let (survivors,): (i64,) = sqlx::query_as("SELECT count(*) FROM warehouses WHERE id = $1")
        .bind(warehouse_id)
        .fetch_one(db.pool())
        .await?;
    assert_eq!(
        survivors, 0,
        "deleting an account must cascade to its warehouses"
    );
    assert!(
        db.account_by_id(created.id).await?.is_none(),
        "the test account should be gone"
    );

    db.close().await;
    Ok(())
}
