//! **Role-based access control** — the vocabulary, and the check that is a pure function of it.
//!
//! `lldb_qe_core::auth` answers "who is this". This module answers "may they touch that", and the
//! two are deliberately separate types: authentication is a database lookup that can fail for
//! transport reasons, while authorization is a pure function of a grant set, which is what makes
//! it unit-testable without a database, a network or a fleet.
//!
//! The other half — deriving what a *statement* requires, which means walking a DataFusion
//! `LogicalPlan` — is `lldb_qe_core::rbac::required_privileges` and its companion `check_plan`.
//! They stayed behind so this crate can stay free of the query engine; that module re-exports
//! everything below, so a caller sees one `rbac`.
//!
//! # The model, in four nouns
//!
//! - A **privilege** ([`Privilege`]) is a verb: `SELECT`, `INSERT`, `DELETE`, `UPDATE`, `USAGE`,
//!   `CANCEL`, or `ALL`.
//! - An **object** ([`ObjectRef`]) is a typed, dotted path: a catalog, a namespace, a table, or a
//!   warehouse.
//! - A **grant** ([`Grant`]) attaches one privilege on one object to one role.
//! - A **requirement** ([`Requirement`]) is the same pair, derived from a query rather than typed
//!   by an operator. Authorization is the question "is every requirement covered by some grant".
//!
//! Roles and users live in `lldb_qe_core::auth`; the only thing this module needs from them is the
//! flat list of grants a caller's roles add up to.
//!
//! # Containment: why a grant on a namespace is a grant on its tables
//!
//! Granting a role `SELECT` on every table by name is not access control, it is a maintenance
//! burden that quietly stops covering the table someone created this morning. So a grant on a
//! *container* implies its contents:
//!
//! ```text
//!   catalog lldb           ⊃  namespace lldb.sales  ⊃  table lldb.sales.orders
//! ```
//!
//! [`Grant::covers`] computes that with a strict path-prefix test — `lldb.sales` covers
//! `lldb.sales.orders` but never `lldb.salesforce.orders`, which is the bug a naive
//! `starts_with` would ship. Warehouses are deliberately *outside* this hierarchy: a warehouse
//! name is a bare DNS label in its own namespace, so a grant on a catalog implies nothing about
//! compute, and a `USAGE` or `CANCEL` grant on a warehouse implies nothing about data.
//!
//! `ALL` is the one wildcard, and it is a wildcard over *privileges* only, never over objects. That
//! has a consequence worth stating rather than discovering: adding a privilege — as `CANCEL` was
//! added — silently widens every pre-existing `ALL` grant on the same object. An operator who wants
//! submit-without-kill on a warehouse must write the narrow privileges rather than `ALL`.
//!
//! # What this does NOT do
//!
//! - **No column- or row-level security.** A grant is on a table, not on a projection or a
//!   predicate. Masking and row filters are their own issue.
//! - **No role hierarchy.** Roles do not inherit from roles. Snowflake-style role graphs are a real
//!   feature and a real cycle-detection problem; a user simply holds N roles here.
//! - **No ownership, no `WITH GRANT OPTION`.** Granting is an operator action through
//!   `lldb-qe-auth`, which already requires the services-database credentials. There is no
//!   in-SQL `GRANT`, so there is nobody to delegate to yet.
//! - **No deny rules.** Grants are purely additive, so "is this allowed" is a search for one
//!   covering grant and never an ordering problem between allow and deny.
//! - **No cross-tenant reach to refuse.** This used to read "no physical tenant separation of the
//!   catalog", and it was the most important caveat here: one shared catalog meant a catalog-wide
//!   grant to account B really did name account A's tables, and this check was the only thing
//!   between them. It is not any more — `lldb_qe_core::tenancy` gives each account its own catalog
//!   and warehouse root, and a session registers only its own tenant's catalogs, so another
//!   tenant's tables do not resolve at all. What that changes about *this* module is nothing:
//!   `covers_object` is unchanged, and a `Catalog` grant is still a one-segment name covering
//!   everything beneath it. What it changes about the *system* is that the check is now a second
//!   line rather than the only one. Do not read that as slack — a grant is still the only thing
//!   that decides what a user may touch **within** their tenant, which is most of what this module
//!   is for.

use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};

// ---------------------------------------------------------------------------
// Privileges
// ---------------------------------------------------------------------------

/// What a caller may do to an object.
///
/// Deliberately small and deliberately mapped one-to-one onto the statements this engine can
/// execute. There is no `CREATE`, no `DROP` and no `OWNERSHIP` because there is no authorized path
/// that performs them — DDL is refused outright while authorization is in force, and inventing a
/// privilege for it would suggest otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Privilege {
    /// Read rows.
    Select,
    /// Append rows.
    Insert,
    /// Remove rows.
    Delete,
    /// Modify rows.
    Update,
    /// Run something *on* an object without reading it — today, a warehouse.
    Usage,
    /// Stop a query that is running on a warehouse. Held on the warehouse, never on data.
    ///
    /// Separate from [`Privilege::Usage`] on purpose. `USAGE` is what lets a caller *submit* to a
    /// warehouse; if cancelling needed only that, everyone entitled to run a query on a warehouse
    /// would be entitled to kill everyone else's on it, and "cancelling somebody else's query needs
    /// a grant" would be true only in a technical sense. See `lldb_qe_core::cancel`.
    Cancel,
    /// Every other privilege on the same object.
    All,
}

/// Every legal privilege, in the order the CLI's help lists them. Next to the enum so an eighth
/// cannot be added without this — and the migration's `CHECK` — being updated with it.
pub const PRIVILEGES: [Privilege; 7] = [
    Privilege::Select,
    Privilege::Insert,
    Privilege::Delete,
    Privilege::Update,
    Privilege::Usage,
    Privilege::Cancel,
    Privilege::All,
];

impl Privilege {
    /// The spelling stored in `grants.privilege` and accepted by the migration's `CHECK`.
    /// Uppercase, because it is a SQL privilege name and reads as one in an error message.
    pub fn as_str(self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Delete => "DELETE",
            Privilege::Update => "UPDATE",
            Privilege::Usage => "USAGE",
            Privilege::Cancel => "CANCEL",
            Privilege::All => "ALL",
        }
    }

    /// Whether holding `self` satisfies a requirement for `needed`.
    ///
    /// `ALL` is the only non-identity case, and it is a *privilege* wildcard only — it says nothing
    /// about which object the grant is on.
    pub fn satisfies(self, needed: Privilege) -> bool {
        self == needed || self == Privilege::All
    }
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Privilege {
    type Err = anyhow::Error;

    /// Parse a stored or CLI-supplied privilege, case-insensitively so `--privilege select` works.
    /// An unknown value is an error naming the legal set: a grant nobody can interpret must stop
    /// the operation rather than quietly become the weakest privilege.
    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "SELECT" => Ok(Privilege::Select),
            "INSERT" => Ok(Privilege::Insert),
            "DELETE" => Ok(Privilege::Delete),
            "UPDATE" => Ok(Privilege::Update),
            "USAGE" => Ok(Privilege::Usage),
            "CANCEL" => Ok(Privilege::Cancel),
            "ALL" => Ok(Privilege::All),
            other => bail!(
                "unknown privilege `{other}` (expected one of: {})",
                joined(PRIVILEGES.iter().map(|p| p.as_str()))
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

/// The kind of thing a privilege is held on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectType {
    /// A whole catalog: `lldb`.
    Catalog,
    /// A namespace (DataFusion calls it a schema) within a catalog: `lldb.sales`.
    Namespace,
    /// One table: `lldb.sales.orders`.
    Table,
    /// A virtual warehouse, by bare name: `analytics`.
    Warehouse,
}

/// Every legal object type, in containment order (widest first) — which is also the order the CLI
/// lists them, so the help text reads as the hierarchy.
pub const OBJECT_TYPES: [ObjectType; 4] = [
    ObjectType::Catalog,
    ObjectType::Namespace,
    ObjectType::Table,
    ObjectType::Warehouse,
];

impl ObjectType {
    /// The spelling stored in `grants.object_type` and accepted by the migration's `CHECK`.
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectType::Catalog => "catalog",
            ObjectType::Namespace => "namespace",
            ObjectType::Table => "table",
            ObjectType::Warehouse => "warehouse",
        }
    }

    /// How many dot-separated segments a name of this type must have. `None` for a warehouse,
    /// whose name is a DNS label and may not contain a dot at all.
    fn segments(self) -> Option<usize> {
        match self {
            ObjectType::Catalog => Some(1),
            ObjectType::Namespace => Some(2),
            ObjectType::Table => Some(3),
            ObjectType::Warehouse => None,
        }
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ObjectType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "catalog" => Ok(ObjectType::Catalog),
            "namespace" | "schema" => Ok(ObjectType::Namespace),
            "table" => Ok(ObjectType::Table),
            "warehouse" => Ok(ObjectType::Warehouse),
            other => bail!(
                "unknown object type `{other}` (expected one of: {})",
                joined(OBJECT_TYPES.iter().map(|t| t.as_str()))
            ),
        }
    }
}

/// A typed, fully qualified object name.
///
/// "Fully qualified" is not decoration. A requirement is always built from DataFusion's
/// `ResolvedTableReference`, so it is always `catalog.namespace.table`; a grant written as `orders`
/// would therefore match nothing, and [`validate_object_name`] rejects it at the point it is typed
/// rather than leaving an operator to discover it as a permission denial six months later.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectRef {
    pub object_type: ObjectType,
    /// Dotted path for catalog/namespace/table, a bare label for a warehouse.
    pub name: String,
}

impl ObjectRef {
    /// A reference, validated. Prefer this over the struct literal anywhere a human's input is
    /// involved.
    pub fn new(object_type: ObjectType, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_object_name(object_type, &name)?;
        Ok(Self { object_type, name })
    }

    /// A catalog-qualified table, the shape every read requirement takes.
    pub fn table(catalog: &str, namespace: &str, table: &str) -> Self {
        Self {
            object_type: ObjectType::Table,
            name: format!("{catalog}.{namespace}.{table}"),
        }
    }

    /// A warehouse, by name.
    pub fn warehouse(name: impl Into<String>) -> Self {
        Self {
            object_type: ObjectType::Warehouse,
            name: name.into(),
        }
    }
}

impl fmt::Display for ObjectRef {
    /// `table lldb.sales.orders` — the form every error message uses, because "denied on
    /// lldb.sales.orders" leaves the reader guessing which `GRANT` to write.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.object_type, self.name)
    }
}

/// Reject an object name that cannot ever be matched, at the moment it is typed.
///
/// The segment count is the whole check, and it is worth having: a grant is written once by a
/// human and consulted by a machine forever, so `table orders` (which can never equal a resolved
/// `lldb.public.orders`) is a silent, permanent denial unless something refuses it up front.
pub fn validate_object_name(object_type: ObjectType, name: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("an object name must not be empty");
    }
    if name != name.trim() {
        bail!("object name `{name}` has leading or trailing whitespace");
    }
    match object_type.segments() {
        None => {
            if name.contains('.') {
                bail!(
                    "warehouse name `{name}` must not contain `.` — a warehouse is a bare name, \
                     not a catalog path"
                );
            }
        }
        Some(expected) => {
            let actual = name.split('.').count();
            if actual != expected {
                bail!(
                    "{object_type} name `{name}` has {actual} dot-separated segment(s); a \
                     {object_type} is written as `{}`",
                    match object_type {
                        ObjectType::Catalog => "<catalog>",
                        ObjectType::Namespace => "<catalog>.<namespace>",
                        _ => "<catalog>.<namespace>.<table>",
                    }
                );
            }
            if name.split('.').any(str::is_empty) {
                bail!("{object_type} name `{name}` has an empty segment");
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Grants and requirements
// ---------------------------------------------------------------------------

/// One grant, as stored. `role_name` rides along from the join because every message this module
/// produces has to name the role an operator would actually edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub id: i64,
    pub account_id: i64,
    pub role_id: i64,
    pub role_name: String,
    pub privilege: Privilege,
    pub object: ObjectRef,
    pub created_at: DateTime<Utc>,
}

impl Grant {
    /// Whether this grant satisfies `required` — both halves, privilege and object.
    pub fn covers(&self, required: &Requirement) -> bool {
        self.privilege.satisfies(required.privilege)
            && covers_object(&self.object, &required.object)
    }
}

impl fmt::Display for Grant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ON {} TO ROLE {}",
            self.privilege, self.object, self.role_name
        )
    }
}

/// One (privilege, object) pair a query needs. The same shape as a [`Grant`] minus everything
/// about *who* holds it — which is the point: requirements come from the plan, grants come from
/// the database, and authorization is the join.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Requirement {
    pub privilege: Privilege,
    pub object: ObjectRef,
}

impl Requirement {
    pub fn new(privilege: Privilege, object: ObjectRef) -> Self {
        Self { privilege, object }
    }
}

impl fmt::Display for Requirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} on {}", self.privilege, self.object)
    }
}

/// Whether a grant on `held` reaches `required`.
///
/// The strict-prefix test (`is_under`) is the load-bearing line: `lldb.sales` must cover
/// `lldb.sales.orders` and must *not* cover `lldb.salesforce.orders`, and a plain `starts_with`
/// gets the second one wrong.
fn covers_object(held: &ObjectRef, required: &ObjectRef) -> bool {
    match held.object_type {
        // Compute lives in its own namespace. Nothing implies a warehouse and a warehouse implies
        // nothing — granting a role the whole catalog must not silently hand it every warehouse.
        ObjectType::Warehouse => {
            required.object_type == ObjectType::Warehouse && required.name == held.name
        }
        ObjectType::Table => {
            required.object_type == ObjectType::Table && required.name == held.name
        }
        ObjectType::Namespace => match required.object_type {
            ObjectType::Namespace => required.name == held.name,
            ObjectType::Table => is_under(&held.name, &required.name),
            _ => false,
        },
        ObjectType::Catalog => match required.object_type {
            ObjectType::Catalog => required.name == held.name,
            ObjectType::Namespace | ObjectType::Table => is_under(&held.name, &required.name),
            ObjectType::Warehouse => false,
        },
    }
}

/// Whether `child` is a strict dotted descendant of `parent`.
fn is_under(parent: &str, child: &str) -> bool {
    child.len() > parent.len()
        && child.as_bytes()[parent.len()] == b'.'
        && child.starts_with(parent)
}

// ---------------------------------------------------------------------------
// The check
// ---------------------------------------------------------------------------

/// A refusal, as a distinct type rather than a string.
///
/// It travels inside an [`anyhow::Error`] all the way up through
/// `lldb_qe_core::result_cache::execute_cached` and out of the engine, where `lldb_qe_core::server`
/// probes the cause chain for it (`chain().any(|c| c.is::<Denied>())`) to answer
/// `PERMISSION_DENIED` instead of `INTERNAL`.
///
/// A *typed* probe rather than a substring match on the message, for the same reason
/// `lldb-qe-coordinator`'s `is_wrong_catalog` uses one: this decides a client-visible status code,
/// so a reworded message must not be able to change it, and an unrelated failure must never be able
/// to imitate it. Getting that backwards in this direction would report a genuine engine bug as a
/// permissions problem — and, worse, the opposite mistake would report a permissions problem as an
/// internal error, which is how an operator concludes the system is broken rather than locked.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Denied(String);

impl Denied {
    /// Public because `lldb_qe_core::rbac::required_privileges` — the half of this module that
    /// needs a `LogicalPlan` and therefore could not come with it — refuses a statement by
    /// constructing one.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Whether `error` is (or was caused by) an authorization refusal.
pub fn is_denial(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<Denied>())
}

/// The caller's effective grants, plus enough identity to write a useful denial.
///
/// Built once per query — one database round trip in `lldb_qe_core::auth` — and then consulted
/// purely. That split is why every method below is synchronous and testable with a literal
/// `Vec<Grant>`.
#[derive(Debug, Clone)]
pub struct QueryAuthorization {
    /// The tenant every grant here belongs to. Carried so a mis-scoped grant set is an assertion
    /// failure in a test rather than a silent cross-tenant allow.
    pub account_id: i64,
    /// Who is asking, for the denial message.
    pub user_name: String,
    /// The flattened union of every grant on every role this user holds.
    pub grants: Vec<Grant>,
}

impl QueryAuthorization {
    pub fn new(account_id: i64, user_name: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            account_id,
            user_name: user_name.into(),
            grants,
        }
    }

    /// Whether some grant covers `required`.
    pub fn allows(&self, required: &Requirement) -> bool {
        self.grants.iter().any(|g| g.covers(required))
    }

    /// Check one requirement — the warehouse `USAGE` path, which is not derived from a plan.
    pub fn check(&self, required: &Requirement) -> Result<()> {
        if self.allows(required) {
            return Ok(());
        }
        Err(self.denial(std::iter::once(required)))
    }

    /// The denial message.
    ///
    /// It names every missing privilege *and* the command that adds it. An authorization error
    /// that says only "permission denied" is a support ticket; one that says which `GRANT` to write
    /// is a fix. Note what it does not say: which objects exist. A caller with no grants at all
    /// learns only that the objects *they named* are denied to them.
    ///
    /// Public for the same reason [`Denied::new`] is: `lldb_qe_core::rbac::check_plan` refuses
    /// through it, and one spelling of this message is the whole point of it living here.
    pub fn denial<'a>(&self, missing: impl Iterator<Item = &'a Requirement>) -> anyhow::Error {
        let mut lines = Vec::new();
        for r in missing {
            lines.push(format!(
                "{r} (grant it with `lldb-qe-auth grant --role <ROLE> --privilege {} \
                 --object-type {} --object-name {}`)",
                r.privilege, r.object.object_type, r.object.name
            ));
        }
        Denied::new(format!(
            "permission denied for user `{}`: missing {}",
            self.user_name,
            lines.join("; ")
        ))
        .into()
    }
}

/// `a, b, c` — for the "expected one of" tail of a parse error.
fn joined<'a>(items: impl Iterator<Item = &'a str>) -> String {
    items.collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(privilege: Privilege, object_type: ObjectType, name: &str) -> Grant {
        Grant {
            id: 1,
            account_id: 7,
            role_id: 3,
            role_name: "analyst".to_string(),
            privilege,
            object: ObjectRef {
                object_type,
                name: name.to_string(),
            },
            created_at: Utc::now(),
        }
    }

    fn need(privilege: Privilege, object_type: ObjectType, name: &str) -> Requirement {
        Requirement::new(
            privilege,
            ObjectRef {
                object_type,
                name: name.to_string(),
            },
        )
    }

    #[test]
    fn privileges_and_object_types_round_trip_their_stored_spelling() {
        for p in PRIVILEGES {
            assert_eq!(p.as_str().parse::<Privilege>().unwrap(), p);
            // Case-insensitive on the way in, canonical on the way out — `--privilege select` is
            // what a human types and `SELECT` is what the CHECK constraint accepts.
            assert_eq!(p.as_str().to_lowercase().parse::<Privilege>().unwrap(), p);
            assert_eq!(p.to_string(), p.as_str());
        }
        for t in OBJECT_TYPES {
            assert_eq!(t.as_str().parse::<ObjectType>().unwrap(), t);
            assert_eq!(t.as_str().to_uppercase().parse::<ObjectType>().unwrap(), t);
        }
        // A DataFusion "schema" is our "namespace"; accept the word people arrive with.
        assert_eq!(
            "schema".parse::<ObjectType>().unwrap(),
            ObjectType::Namespace
        );
    }

    #[test]
    fn an_unknown_privilege_is_refused_and_lists_the_legal_set() {
        let err = "TRUNCATE".parse::<Privilege>().unwrap_err().to_string();
        assert!(err.contains("TRUNCATE"), "{err}");
        assert!(err.contains("SELECT"), "must list the legal set: {err}");
        let err = "view".parse::<ObjectType>().unwrap_err().to_string();
        assert!(err.contains("warehouse"), "{err}");
    }

    #[test]
    fn all_covers_every_privilege_on_the_same_object() {
        let g = grant(Privilege::All, ObjectType::Table, "lldb.sales.orders");
        for p in PRIVILEGES {
            assert!(
                g.covers(&need(p, ObjectType::Table, "lldb.sales.orders")),
                "ALL must cover {p}"
            );
        }
        // …and nothing about a different object.
        assert!(!g.covers(&need(
            Privilege::Select,
            ObjectType::Table,
            "lldb.sales.lineitem"
        )));
    }

    #[test]
    fn a_narrow_privilege_covers_only_itself() {
        let g = grant(Privilege::Select, ObjectType::Table, "lldb.sales.orders");
        assert!(g.covers(&need(
            Privilege::Select,
            ObjectType::Table,
            "lldb.sales.orders"
        )));
        for p in [
            Privilege::Insert,
            Privilege::Delete,
            Privilege::Update,
            Privilege::Usage,
            Privilege::Cancel,
        ] {
            assert!(
                !g.covers(&need(p, ObjectType::Table, "lldb.sales.orders")),
                "SELECT must not imply {p}"
            );
        }
    }

    /// Cancelling is compute, and it is not implied by being allowed to *use* the compute.
    ///
    /// The pair that matters: `USAGE ON WAREHOUSE analytics` — which every submitter to that
    /// warehouse must hold — must not let its holder stop somebody else's query, or the grant this
    /// privilege exists for would be decorative.
    #[test]
    fn cancelling_is_its_own_privilege_on_a_warehouse() {
        let usage = grant(Privilege::Usage, ObjectType::Warehouse, "analytics");
        let cancel = grant(Privilege::Cancel, ObjectType::Warehouse, "analytics");
        let need_cancel = need(Privilege::Cancel, ObjectType::Warehouse, "analytics");

        assert!(!usage.covers(&need_cancel), "USAGE must not imply CANCEL");
        assert!(cancel.covers(&need_cancel));
        // …and it does not run backwards either: holding CANCEL is not permission to submit.
        assert!(!cancel.covers(&need(Privilege::Usage, ObjectType::Warehouse, "analytics")));
        // Per warehouse, like every other warehouse grant.
        assert!(!cancel.covers(&need(Privilege::Cancel, ObjectType::Warehouse, "etl")));
        // And a catalog-wide grant reaches no warehouse at all, CANCEL included.
        let catalog = grant(Privilege::All, ObjectType::Catalog, "lldb");
        assert!(!catalog.covers(&need_cancel));
        // `ALL` on the warehouse *does* cover it — the honest cost of adding a privilege under an
        // existing wildcard, asserted so nobody discovers it in production instead.
        let all = grant(Privilege::All, ObjectType::Warehouse, "analytics");
        assert!(all.covers(&need_cancel));
    }

    #[test]
    fn containment_runs_catalog_to_namespace_to_table() {
        let catalog = grant(Privilege::Select, ObjectType::Catalog, "lldb");
        let namespace = grant(Privilege::Select, ObjectType::Namespace, "lldb.sales");
        let table = grant(Privilege::Select, ObjectType::Table, "lldb.sales.orders");

        let orders = need(Privilege::Select, ObjectType::Table, "lldb.sales.orders");
        let sales = need(Privilege::Select, ObjectType::Namespace, "lldb.sales");

        // Downward: a container grant reaches its contents.
        assert!(catalog.covers(&orders));
        assert!(catalog.covers(&sales));
        assert!(namespace.covers(&orders));
        assert!(table.covers(&orders));

        // Upward: never. A grant on one table is not a grant on its namespace.
        assert!(!table.covers(&sales));
        assert!(!namespace.covers(&need(Privilege::Select, ObjectType::Catalog, "lldb")));
    }

    #[test]
    fn a_prefix_that_is_not_a_path_prefix_covers_nothing() {
        // The bug a naive `starts_with` ships: `lldb.sales` is a textual prefix of
        // `lldb.salesforce.orders` and must not grant a single row of it.
        let namespace = grant(Privilege::Select, ObjectType::Namespace, "lldb.sales");
        assert!(!namespace.covers(&need(
            Privilege::Select,
            ObjectType::Table,
            "lldb.salesforce.orders"
        )));
        let catalog = grant(Privilege::Select, ObjectType::Catalog, "lldb");
        assert!(!catalog.covers(&need(
            Privilege::Select,
            ObjectType::Table,
            "lldbx.sales.orders"
        )));
        // …and equality is not containment: a namespace grant is not a grant on a *table*
        // spelled identically, because the types differ.
        assert!(!namespace.covers(&need(Privilege::Select, ObjectType::Table, "lldb.sales")));
    }

    #[test]
    fn warehouses_are_outside_the_catalog_hierarchy() {
        // Compute and data are separate grants on purpose: `ALL ON CATALOG lldb` must not hand a
        // role every warehouse in the account, and `USAGE ON WAREHOUSE analytics` must not let it
        // read a single table.
        let catalog = grant(Privilege::All, ObjectType::Catalog, "lldb");
        let warehouse = grant(Privilege::Usage, ObjectType::Warehouse, "analytics");

        assert!(!catalog.covers(&need(Privilege::Usage, ObjectType::Warehouse, "analytics")));
        assert!(warehouse.covers(&need(Privilege::Usage, ObjectType::Warehouse, "analytics")));
        assert!(!warehouse.covers(&need(Privilege::Usage, ObjectType::Warehouse, "etl")));
        assert!(!warehouse.covers(&need(
            Privilege::Select,
            ObjectType::Table,
            "lldb.sales.orders"
        )));
    }

    #[test]
    fn object_names_must_have_the_right_number_of_segments() {
        ObjectRef::new(ObjectType::Catalog, "lldb").expect("catalog");
        ObjectRef::new(ObjectType::Namespace, "lldb.sales").expect("namespace");
        ObjectRef::new(ObjectType::Table, "lldb.sales.orders").expect("table");
        ObjectRef::new(ObjectType::Warehouse, "analytics").expect("warehouse");

        // The silent-denial cases: an unqualified name can never equal a resolved reference.
        let err = ObjectRef::new(ObjectType::Table, "orders")
            .expect_err("a bare table name never matches a resolved one")
            .to_string();
        assert!(err.contains("<catalog>.<namespace>.<table>"), "{err}");
        assert!(ObjectRef::new(ObjectType::Namespace, "sales").is_err());
        assert!(ObjectRef::new(ObjectType::Catalog, "lldb.sales").is_err());
        assert!(ObjectRef::new(ObjectType::Table, "lldb..orders").is_err());
        assert!(ObjectRef::new(ObjectType::Warehouse, "a.b").is_err());
        assert!(ObjectRef::new(ObjectType::Table, " lldb.s.t").is_err());
        assert!(ObjectRef::new(ObjectType::Table, "").is_err());
    }

    #[test]
    fn a_single_requirement_check_is_the_warehouse_path() {
        let usage = Requirement::new(Privilege::Usage, ObjectRef::warehouse("analytics"));
        let authz = QueryAuthorization::new(
            7,
            "dana",
            vec![grant(Privilege::Usage, ObjectType::Warehouse, "analytics")],
        );
        authz.check(&usage).expect("granted");

        let other = Requirement::new(Privilege::Usage, ObjectRef::warehouse("etl"));
        let err = authz.check(&other).expect_err("not granted").to_string();
        assert!(err.contains("USAGE on warehouse etl"), "{err}");
    }

    /// The refusal must be recognizable by *type*, not by its wording — that probe is what
    /// `lldb_qe_core::server` turns into `PERMISSION_DENIED` rather than `INTERNAL`.
    #[test]
    fn a_denial_is_recognizable_through_the_cause_chain() {
        let authz = QueryAuthorization::new(7, "dana", Vec::new());
        let err = authz
            .check(&Requirement::new(
                Privilege::Usage,
                ObjectRef::warehouse("analytics"),
            ))
            .expect_err("nothing granted");
        assert!(is_denial(&err));
        assert!(!is_denial(&anyhow::anyhow!("a broken socket")));
    }
}
