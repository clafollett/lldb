//! The virtual-warehouse lifecycle against a **real Postgres**: the new migration, the API on
//! [`ServicesDb`], and the acceptance criteria of issue #16 stated as facts a server can settle.
//!
//! Everything here is a claim a unit test cannot make. That `state` now rejects a value outside
//! the legal set, that `UNIQUE (account_id, name)` lets two *tenants* both own an `analytics`
//! while one tenant may not own two, that `updated_at` actually moves when a warehouse is
//! resized, that a suspend racing a suspend produces an error rather than a second success — all
//! of them are properties of the schema and of the transaction, not of the Rust.
//!
//! The database is found the same way `services_db.rs` finds it (see [`support`]): an explicit
//! `LLDB_TEST_POSTGRES_URL`, else a throwaway container under `LLDB_DOCKER=1`, else a clean skip
//! so `cargo test` stays green on a laptop with neither.
//!
//! Isolation discipline: every account and warehouse name carries a pid + nanosecond suffix, so
//! this is safe to run against a shared dev instance and concurrently with itself. The test
//! deletes the accounts it created, which cascades to their warehouses; it drops nothing global.
//!
//!   LLDB_TEST_POSTGRES_URL=postgres://lldb@localhost/lldb cargo test -p lldb-qe-core --test integration warehouse_lifecycle
//!   LLDB_DOCKER=1 cargo test -p lldb-qe-core --test integration warehouse_lifecycle -- --nocapture

use crate::support::{self, DbCleanup, resolve_target, unique_name};
use anyhow::{Context, Result};
use lldb_qe_core::discovery::DEFAULT_WAREHOUSE_ENDPOINT;
use lldb_qe_core::services::ServicesDb;
use lldb_qe_core::warehouse::WarehouseState;

/// Skip-or-connect, shared by every test in this file. Returns `None` when there is no database,
/// having already printed why.
async fn db_or_skip(what: &str) -> Result<Option<(ServicesDb, support::Target)>> {
    let target = resolve_target()?;
    let Some(url) = target.url() else {
        eprintln!(
            "SKIP ({what}): set LLDB_TEST_POSTGRES_URL to a Postgres URL, or LLDB_DOCKER=1 with \
             a Docker daemon, to exercise the warehouse lifecycle"
        );
        return Ok(None);
    };
    let db = ServicesDb::connect(url).await?;
    // Idempotent, and every test in this binary needs the schema — whichever runs first applies
    // it, the rest find it applied (sqlx's advisory lock makes the race a wait, not a corruption).
    db.migrate().await.context("applying migrations")?;
    Ok(Some((db, target)))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_lifecycle_round_trips_and_illegal_moves_are_refused() -> Result<()> {
    let Some((db, target)) = db_or_skip("lifecycle").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip returns a connected database");

    let account = db.create_account(&unique_name("wh-acct")).await?;
    // Registered as soon as the row exists rather than deleted at the end, so an assertion failing
    // below still takes it with it — see `support::DbCleanup`.
    let mut cleanup = DbCleanup::new(url);
    cleanup.account(account.id);

    // ---- Two warehouses of different sizes, side by side --------------------------------------
    // The first acceptance criterion, as a control-plane fact: one account, two independently
    // sized pools, neither aware of the other.
    let small = unique_name("wh-small");
    let large = unique_name("wh-large");
    let small_wh = db
        .create_warehouse(account.id, &small, 1, WarehouseState::Running)
        .await?;
    let large_wh = db
        .create_warehouse(account.id, &large, 4, WarehouseState::Running)
        .await?;
    assert_eq!(small_wh.size, 1);
    assert_eq!(large_wh.size, 4);
    assert_eq!(small_wh.state, WarehouseState::Running);
    assert_ne!(small_wh.id, large_wh.id);
    assert_eq!(small_wh.account_id, account.id);

    // …and they route to *different* fleets. This is the whole point of the name being a DNS
    // label: one warehouse cannot accidentally borrow the other's compute.
    assert_eq!(
        small_wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?,
        format!("http://{small}.lldb.local:50051")
    );
    assert_ne!(
        small_wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?,
        large_wh.endpoint(DEFAULT_WAREHOUSE_ENDPOINT)?
    );

    // Both lookups resolve, scoped by account.
    let by_name = db
        .warehouse_by_name(account.id, &small)
        .await?
        .expect("warehouse by name");
    assert_eq!(by_name, small_wh);
    let by_id = db
        .warehouse_by_id(small_wh.id)
        .await?
        .expect("warehouse by id");
    assert_eq!(by_id, small_wh);

    let listed = db.list_warehouses(account.id).await?;
    assert_eq!(listed.len(), 2, "both warehouses belong to this account");

    // A name this account has not used does not resolve.
    assert!(
        db.warehouse_by_name(account.id, &unique_name("wh-absent"))
            .await?
            .is_none()
    );

    // One account may not own two warehouses by one name — that would make `--warehouse <name>`
    // ambiguous, which is exactly what the UNIQUE index rules out.
    assert!(
        db.create_warehouse(account.id, &small, 2, WarehouseState::Running)
            .await
            .is_err(),
        "a duplicate warehouse name within an account must be rejected"
    );

    // ---- Suspend → resume round-trips, and keeps the size -------------------------------------
    let suspended = db.suspend_warehouse(account.id, &large).await?;
    assert_eq!(suspended.state, WarehouseState::Suspended);
    assert_eq!(
        suspended.size, 4,
        "suspending must not destroy the size to resume back to"
    );
    assert!(
        suspended.updated_at >= large_wh.updated_at,
        "a lifecycle change must move updated_at"
    );
    assert_eq!(
        suspended.created_at, large_wh.created_at,
        "created_at is history and must not move"
    );

    // A suspended warehouse is refused by the routing path — the criterion "queries route to the
    // correct warehouse" has a corollary: they must not route to a warehouse with no compute.
    let err = suspended
        .endpoint(DEFAULT_WAREHOUSE_ENDPOINT)
        .expect_err("a suspended warehouse must not be routable");
    let msg = format!("{err:#}");
    assert!(msg.contains(&large), "{msg}");
    assert!(msg.contains("lldb-qe-warehouse resume"), "{msg}");

    // Illegal transitions are refused rather than silently written.
    let err = db
        .suspend_warehouse(account.id, &large)
        .await
        .expect_err("suspending a suspended warehouse is illegal");
    let chain = format!("{err:#}");
    assert!(chain.contains("already suspended"), "{chain}");
    assert!(
        chain.contains(&large),
        "the error must name the warehouse: {chain}"
    );
    // …and the refusal left the row alone.
    assert_eq!(
        db.warehouse_by_name(account.id, &large)
            .await?
            .expect("still there")
            .state,
        WarehouseState::Suspended
    );

    let err = db
        .resume_warehouse(account.id, &small)
        .await
        .expect_err("resuming a running warehouse is illegal");
    assert!(format!("{err:#}").contains("already running"), "{err:#}");

    // Resize is legal *while suspended*: it sets what resume will bring up.
    let resized = db.resize_warehouse(account.id, &large, 8).await?;
    assert_eq!(resized.size, 8);
    assert_eq!(resized.state, WarehouseState::Suspended);

    let resumed = db.resume_warehouse(account.id, &large).await?;
    assert_eq!(resumed.state, WarehouseState::Running);
    assert_eq!(
        resumed.size, 8,
        "resume restores the *current* desired size"
    );
    assert!(resumed.endpoint(DEFAULT_WAREHOUSE_ENDPOINT).is_ok());

    // ---- Resize changes the recorded size, which is what drives fan-out -----------------------
    let grown = db.resize_warehouse(account.id, &small, 3).await?;
    assert_eq!(grown.size, 3);
    assert!(grown.updated_at >= small_wh.updated_at);
    assert_eq!(
        db.warehouse_by_name(account.id, &small)
            .await?
            .expect("still there")
            .size,
        3,
        "the resize must be durable, not just returned"
    );
    // A size that cannot describe a pool of workers is refused before it reaches the database.
    assert!(db.resize_warehouse(account.id, &small, 0).await.is_err());

    // Operations on a warehouse that does not exist name the tool that creates one.
    let missing = unique_name("wh-ghost");
    for err in [
        db.resize_warehouse(account.id, &missing, 2)
            .await
            .unwrap_err(),
        db.suspend_warehouse(account.id, &missing)
            .await
            .unwrap_err(),
        db.resume_warehouse(account.id, &missing).await.unwrap_err(),
    ] {
        let chain = format!("{err:#}");
        assert!(chain.contains(&missing), "{chain}");
        assert!(chain.contains("lldb-qe-warehouse create"), "{chain}");
    }

    // ---- Delete, then cleanup ----------------------------------------------------------------
    assert!(db.delete_warehouse(account.id, &small).await?);
    assert!(
        !db.delete_warehouse(account.id, &small).await?,
        "deleting twice reports `nothing to delete` rather than failing"
    );
    assert!(db.warehouse_by_name(account.id, &small).await?.is_none());

    db.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn two_accounts_can_own_the_same_warehouse_name() -> Result<()> {
    let Some((db, target)) = db_or_skip("tenancy").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip returns a connected database");

    // The tenancy criterion. `UNIQUE (account_id, name)` — not `UNIQUE (name)` — is what makes a
    // warehouse handle meaningful *per tenant*, and it is the reason every lookup in the API
    // takes an account id instead of a name alone.
    let shared_name = unique_name("wh-shared");
    let alice = db.create_account(&unique_name("wh-alice")).await?;
    let bob = db.create_account(&unique_name("wh-bob")).await?;
    let mut cleanup = DbCleanup::new(url);
    cleanup.account(alice.id);
    cleanup.account(bob.id);

    let alice_wh = db
        .create_warehouse(alice.id, &shared_name, 2, WarehouseState::Running)
        .await?;
    let bob_wh = db
        .create_warehouse(bob.id, &shared_name, 6, WarehouseState::Suspended)
        .await?;

    assert_ne!(alice_wh.id, bob_wh.id);
    assert_eq!(alice_wh.name, bob_wh.name);
    assert_eq!(alice_wh.size, 2);
    assert_eq!(bob_wh.size, 6);

    // Each account sees exactly its own, and neither can reach the other's through the API.
    assert_eq!(
        db.warehouse_by_name(alice.id, &shared_name).await?,
        Some(alice_wh.clone())
    );
    assert_eq!(
        db.warehouse_by_name(bob.id, &shared_name).await?,
        Some(bob_wh)
    );
    assert_eq!(db.list_warehouses(alice.id).await?, vec![alice_wh]);

    // Suspending one tenant's warehouse leaves the other's alone.
    db.suspend_warehouse(alice.id, &shared_name).await?;
    assert_eq!(
        db.warehouse_by_name(bob.id, &shared_name)
            .await?
            .expect("bob's warehouse")
            .state,
        WarehouseState::Suspended,
        "bob's was created suspended and stays that way"
    );
    db.resume_warehouse(bob.id, &shared_name).await?;
    assert_eq!(
        db.warehouse_by_name(alice.id, &shared_name)
            .await?
            .expect("alice's warehouse")
            .state,
        WarehouseState::Suspended,
        "resuming bob's must not touch alice's"
    );

    // Deleting a tenant takes its warehouses with it (0001's ON DELETE CASCADE).
    delete_account(&db, alice.id).await?;
    assert!(
        db.warehouse_by_name(alice.id, &shared_name)
            .await?
            .is_none()
    );
    assert!(
        db.warehouse_by_name(bob.id, &shared_name).await?.is_some(),
        "one tenant's deletion must not touch another's"
    );

    db.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_itself_refuses_an_illegal_warehouse() -> Result<()> {
    let Some((db, target)) = db_or_skip("constraints").await? else {
        return Ok(());
    };
    let url = target
        .url()
        .expect("db_or_skip returns a connected database");

    let account = db.create_account(&unique_name("wh-cons")).await?;
    let mut cleanup = DbCleanup::new(url);
    cleanup.account(account.id);
    let name = unique_name("wh-cons-a");

    // The migration is only worth anything if it holds against SQL that bypasses the Rust API —
    // a hand-written UPDATE in a psql session is exactly how a bad state gets in.
    db.create_warehouse(account.id, &name, 2, WarehouseState::Running)
        .await?;

    for illegal in ["runing", "RUNNING", "starting", ""] {
        let result = sqlx::query("UPDATE warehouses SET state = $1 WHERE account_id = $2")
            .bind(illegal)
            .bind(account.id)
            .execute(db.pool())
            .await;
        assert!(
            result.is_err(),
            "the CHECK constraint must reject state `{illegal}`"
        );
    }

    // And the size floor from 0001 still holds through the new migration.
    assert!(
        sqlx::query("UPDATE warehouses SET size = 0 WHERE account_id = $1")
            .bind(account.id)
            .execute(db.pool())
            .await
            .is_err(),
        "size must stay positive even while suspended"
    );

    // `updated_at` exists and is NOT NULL — the column the lifecycle audit hangs off.
    let (nullable,): (String,) = sqlx::query_as(
        "SELECT is_nullable FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'warehouses' \
           AND column_name = 'updated_at'",
    )
    .fetch_one(db.pool())
    .await
    .context("the 0002 migration must add warehouses.updated_at")?;
    assert_eq!(nullable, "NO");

    db.close().await;
    Ok(())
}

/// Remove a test tenant, which cascades to its warehouses.
///
/// Routine teardown is `support::DbCleanup`'s job now; this survives for the one call that is an
/// *assertion* — deleting one tenant mid-test to watch the other's warehouse survive.
async fn delete_account(db: &ServicesDb, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(id)
        .execute(db.pool())
        .await
        .context("deleting the test account")?;
    Ok(())
}
