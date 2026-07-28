//! **Which tenant a catalog belongs to** — the structural half of multi-tenancy.
//!
//! Everything the control plane owns is `account_id`-scoped in the schema, and the composite
//! foreign keys in migration `0005` make cross-tenant wiring *unrepresentable*. The Iceberg
//! catalog was the exception: `iceberg_tables` and `iceberg_namespace_properties` are created and
//! owned by `iceberg-catalog-sql`, carry no account column, and every tenant on a deployment shared
//! one catalog namespace. Isolation over table data therefore rested on the plan-time grant check
//! in [`crate::rbac`] and on nothing else — a check, where everywhere else there was a constraint.
//!
//! A [`TenantScope`] closes that by giving each account its **own catalog and its own warehouse
//! root**. Two knobs, and the important thing about them is that they are *not* independent:
//!
//! 1. **The catalog name partitions the database.** `catalog_name` is already the leading
//!    primary-key column of both of `iceberg-catalog-sql`'s tables, and every statement that crate
//!    issues carries `WHERE catalog_name = ?` — as does our own pointer swap in [`crate::dml`]. So
//!    a distinct catalog name per account buys row separation with no schema change at all. The
//!    "tenant column `iceberg-catalog-sql` does not have" turned out to exist already, under
//!    another name.
//! 2. **The warehouse root partitions the disk, and it *must* move with the name.**
//!    `iceberg-catalog-sql` composes a table's location as `{warehouse_location}/{namespace}/{table}`
//!    — the catalog name does not appear in it. Two per-tenant catalogs sharing one warehouse root,
//!    whose tenants each create `sales.orders`, would be cleanly separated in Postgres and pointed
//!    at the *same directory*. Scoping one without the other is worse than scoping neither, because
//!    it looks separated.
//!
//! # The identity is the account **id**, never its name
//!
//! `accounts.name` is free-form `TEXT` and renameable. Deriving a catalog name from it would need
//! a sanitizer (it becomes a path component and a `VARCHAR(255)` key), and a sanitizer that is not
//! injective silently merges two tenants' catalogs — the exact failure this module exists to
//! prevent. A rename would also orphan a tenant's tables. The primary key has neither problem: it
//! is unique by construction, stable for the account's life, and already path- and SQL-safe.
//!
//! # No services database means no tenants, and that is deliberate
//!
//! [`TenantScope::untenanted`] leaves the catalog name and warehouse exactly as the manifest
//! declares them. It is what a checkout, a laptop and every single-node path get, because without
//! a services database there are no accounts, no keys and no grants — so there is no boundary to
//! draw and nothing that could cross one. This mirrors the standing rule that auth follows the
//! services DB (CLAUDE.md): `cargo run` must never need Postgres, and turning the control plane on
//! is what turns tenancy on.
//!
//! # What this boundary does **not** stop
//!
//! Per-tenant catalogs and per-tenant warehouse roots separate tenants' *layout*. They do not
//! separate their *access*, and the difference matters:
//!
//! - A **coordinator** only ever registers one account's catalogs into that account's session (see
//!   [`crate::engine::TenantSessions`]), so a query cannot name another tenant's catalog — not
//!   because a check refuses it, but because the name does not resolve. That is the improvement.
//! - A **worker** is a different story. Since resolved Iceberg scans name data files by absolute
//!   path (see [`crate::iceberg_scan`]), a worker reads warehouse files with its own credentials
//!   and has no idea whose they are. Any worker that can read tenant A's plan can read tenant B's
//!   files if it is handed a plan naming them. Per-request identity at the worker boundary is a
//!   separate piece of work; until it lands, worker ports stay on a private network and the fleet
//!   secret is what keeps arbitrary plans out.

use std::fmt;

/// The tenant a catalog is materialized for.
///
/// Cheap to clone and to pass around: it is an account id and the naming rules derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TenantScope {
    account_id: Option<i64>,
}

impl TenantScope {
    /// No tenant partitioning: catalog names and warehouse roots are used exactly as declared.
    ///
    /// The single-node shape. See the module docs — with no services database there are no
    /// accounts to keep apart.
    pub fn untenanted() -> Self {
        Self { account_id: None }
    }

    /// The scope of the account with this id.
    pub fn account(account_id: i64) -> Self {
        Self {
            account_id: Some(account_id),
        }
    }

    /// The scope for an optional account id — `None` is [`Self::untenanted`].
    ///
    /// Exists because every caller in the engine holds exactly this shape: an `Option<i64>`
    /// resolved from the control plane, absent when there is no control plane.
    pub fn for_account(account_id: Option<i64>) -> Self {
        Self { account_id }
    }

    /// The account this scope belongs to, if any.
    pub fn account_id(&self) -> Option<i64> {
        self.account_id
    }

    /// Whether this scope actually partitions anything.
    pub fn is_tenanted(&self) -> bool {
        self.account_id.is_some()
    }

    /// The value written to `iceberg_tables.catalog_name` for a catalog the manifest calls
    /// `declared`.
    ///
    /// **This is not the name SQL uses.** A tenant's session registers the catalog under its
    /// declared name, so a query still reads `FROM lldb.sales.orders` whichever tenant runs it —
    /// see [`crate::lakehouse::Lakehouse::catalog_name`] versus
    /// [`crate::lakehouse::Lakehouse::iceberg_catalog_name`]. Putting the account id only in the
    /// storage-facing name is what keeps a query portable between tenants while still giving each
    /// of them its own rows: two accounts' `lldb` catalogs are two different catalogs that happen
    /// to answer to the same word inside their own sessions.
    pub fn iceberg_catalog_name(&self, declared: &str) -> String {
        match self.account_id {
            Some(id) => format!("acct_{id}__{declared}"),
            None => declared.to_string(),
        }
    }

    /// The warehouse root a tenant's table files live under, given the manifest's `declared` root.
    ///
    /// A suffix rather than a prefix so an operator can still point one volume, bucket prefix or
    /// NFS mount at the whole deployment and have the tenants sort themselves out beneath it.
    pub fn warehouse_uri(&self, declared: &str) -> String {
        match self.account_id {
            // `trim_end_matches` rather than a bare join: a manifest may or may not end its
            // warehouse in a slash, and `file:///wh//acct_3` is a *different* directory than
            // `file:///wh/acct_3` to some object stores even though it is the same one on a POSIX
            // filesystem. Normalizing here means the two spellings cannot become two warehouses.
            Some(id) => format!("{}/acct_{id}", declared.trim_end_matches('/')),
            None => declared.to_string(),
        }
    }
}

impl fmt::Display for TenantScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.account_id {
            Some(id) => write!(f, "acct_{id}"),
            None => f.write_str("untenanted"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_untenanted_scope_changes_nothing() {
        let scope = TenantScope::untenanted();
        assert!(!scope.is_tenanted());
        assert_eq!(scope.iceberg_catalog_name("lldb"), "lldb");
        assert_eq!(
            scope.warehouse_uri("file:///var/lib/lldb/wh"),
            "file:///var/lib/lldb/wh"
        );
        assert_eq!(TenantScope::for_account(None), scope);
    }

    #[test]
    fn a_tenant_gets_its_own_catalog_and_its_own_warehouse() {
        let a = TenantScope::account(7);
        let b = TenantScope::account(8);
        // Both knobs move, because moving only one produces a layout that *looks* separated.
        assert_eq!(a.iceberg_catalog_name("lldb"), "acct_7__lldb");
        assert_ne!(
            a.iceberg_catalog_name("lldb"),
            b.iceberg_catalog_name("lldb")
        );
        assert_eq!(a.warehouse_uri("file:///wh"), "file:///wh/acct_7");
        assert_ne!(a.warehouse_uri("file:///wh"), b.warehouse_uri("file:///wh"));
    }

    #[test]
    fn a_trailing_slash_does_not_make_a_second_warehouse() {
        assert_eq!(
            TenantScope::account(3).warehouse_uri("file:///wh/"),
            TenantScope::account(3).warehouse_uri("file:///wh")
        );
    }

    #[test]
    fn two_declared_catalogs_stay_distinct_within_one_tenant() {
        // A manifest may declare several catalogs; scoping must partition by tenant without
        // collapsing them into each other.
        let scope = TenantScope::account(42);
        assert_ne!(
            scope.iceberg_catalog_name("sales"),
            scope.iceberg_catalog_name("ops")
        );
    }
}
