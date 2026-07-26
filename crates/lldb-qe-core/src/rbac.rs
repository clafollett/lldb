//! **Role-based access control** — what a caller is allowed to touch, decided *before* the query
//! leaves the coordinator.
//!
//! [`crate::auth`] answers "who is this". This module answers "may they run that", and the two are
//! deliberately separate types: authentication is a database lookup that can fail for transport
//! reasons, while authorization is a pure function of a grant set and a logical plan, which is what
//! makes it unit-testable without a database, a network or a fleet.
//!
//! # The model, in four nouns
//!
//! - A **privilege** ([`Privilege`]) is a verb: `SELECT`, `INSERT`, `DELETE`, `UPDATE`, `USAGE`, or
//!   `ALL`.
//! - An **object** ([`ObjectRef`]) is a typed, dotted path: a catalog, a namespace, a table, or a
//!   warehouse.
//! - A **grant** ([`Grant`]) attaches one privilege on one object to one role.
//! - A **requirement** ([`Requirement`]) is the same pair, derived from a query rather than typed
//!   by an operator. Authorization is the question "is every requirement covered by some grant".
//!
//! Roles and users live in [`crate::auth`]; the only thing this module needs from them is the flat
//! list of grants a caller's roles add up to.
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
//! compute, and a `USAGE` grant on a warehouse implies nothing about data.
//!
//! # Enforcement is at plan time, on the *logical* plan
//!
//! [`required_privileges`] walks a [`LogicalPlan`] and collects what it needs. The logical plan is
//! the right place and it is the last one: it is where object names are still object names. One
//! optimization pass later the scan is a `ParquetExec` over a list of file paths, and by then
//! "which table is this" is a question you answer by reverse-engineering a path — and, worse, by
//! then the plan is already being cut into stages for dispatch.
//!
//! Two consequences worth stating plainly:
//!
//! - **The check must dominate the result cache.** A cached answer is still that tenant's data, so
//!   the check runs immediately after planning and before any lookup. See
//!   [`crate::result_cache::execute_cached`].
//! - **A statement whose privileges this build cannot name is refused, not allowed.** DDL, `COPY
//!   TO`, `DESCRIBE` and unknown extension nodes reach [`required_privileges`] as an error rather
//!   than as an empty requirement set. `CREATE EXTERNAL TABLE 'file:///etc/passwd'` is exactly the
//!   statement a fail-open default would wave through, and `DESCRIBE t` carries no table name at
//!   all by the time it is a plan (see [`DescribeTable`], which holds only a schema), so there is
//!   nothing to check it against. Failing closed is the only honest answer to "I do not know what
//!   this touches".
//!
//! [`DescribeTable`]: datafusion::logical_expr::logical_plan::DescribeTable
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
//! - **No physical tenant separation of the catalog.** See [`crate::auth`]'s module docs: the
//!   Iceberg SQL catalog's tables are owned by `iceberg-catalog-sql` and are not partitioned by
//!   account, so isolation between tenants is what the check below enforces, not something the
//!   storage layout guarantees.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::{LogicalPlan, WriteOp};

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
    /// Every other privilege on the same object.
    All,
}

/// Every legal privilege, in the order the CLI's help lists them. Next to the enum so a seventh
/// cannot be added without this — and the migration's `CHECK` — being updated with it.
pub const PRIVILEGES: [Privilege; 6] = [
    Privilege::Select,
    Privilege::Insert,
    Privilege::Delete,
    Privilege::Update,
    Privilege::Usage,
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
/// "Fully qualified" is not decoration. A requirement is always built from a
/// [`ResolvedTableReference`](datafusion::common::ResolvedTableReference), so it is always
/// `catalog.namespace.table`; a grant written as `orders` would therefore match nothing, and
/// [`validate_object_name`] rejects it at the point it is typed rather than leaving an operator to
/// discover it as a permission denial six months later.
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
// What a plan requires
// ---------------------------------------------------------------------------

/// Every (privilege, object) pair `plan` needs, resolved against the session's defaults.
///
/// The walk uses `apply_with_subqueries` rather than `apply` for the same reason
/// [`crate::result_cache`] does: a scalar or `IN` subquery's plan hangs off an *expression*, and a
/// plain `apply` walks straight past it — which here would mean `SELECT 1 WHERE x IN (SELECT s FROM
/// secret)` requiring nothing at all.
///
/// Returns an error, never an empty set, for a statement whose object footprint this build cannot
/// name. See the module docs on failing closed.
pub fn required_privileges(
    plan: &LogicalPlan,
    default_catalog: &str,
    default_schema: &str,
) -> Result<BTreeSet<Requirement>> {
    let mut required = BTreeSet::new();
    // Collected rather than returned from the closure because `apply_with_subqueries` wants a
    // DataFusion error, and a refusal here is a *policy* decision that deserves its own message.
    let mut unsupported: Option<String> = None;

    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                let r = scan
                    .table_name
                    .clone()
                    .resolve(default_catalog, default_schema);
                required.insert(Requirement::new(
                    Privilege::Select,
                    ObjectRef::table(&r.catalog, &r.schema, &r.table),
                ));
            }
            LogicalPlan::Dml(dml) => {
                let r = dml
                    .table_name
                    .clone()
                    .resolve(default_catalog, default_schema);
                let object = ObjectRef::table(&r.catalog, &r.schema, &r.table);
                // `Ctas` and `Truncate` are unreachable from this engine's front ends (a `CREATE
                // TABLE AS` arrives as `Ddl`, which is refused below), but they are variants of
                // `WriteOp` and a `_ =>` arm would silently become "allowed" if one ever started
                // arriving. Name them.
                match dml.op {
                    WriteOp::Insert(_) | WriteOp::Ctas => {
                        required.insert(Requirement::new(Privilege::Insert, object));
                    }
                    WriteOp::Delete | WriteOp::Truncate => {
                        required.insert(Requirement::new(Privilege::Delete, object));
                    }
                    WriteOp::Update => {
                        required.insert(Requirement::new(Privilege::Update, object));
                    }
                }
            }
            // Fail closed. Each of these either names no object at all or names one outside the
            // catalog (a filesystem path), so there is nothing to check a grant against.
            LogicalPlan::Ddl(ddl) => unsupported = Some(format!("{} (DDL)", ddl.name())),
            LogicalPlan::Copy(_) => unsupported = Some("COPY TO".to_string()),
            LogicalPlan::DescribeTable(_) => unsupported = Some("DESCRIBE".to_string()),
            LogicalPlan::Extension(ext) => {
                unsupported = Some(format!("the `{}` plan extension", ext.node.name()))
            }
            _ => {}
        }
        Ok(if unsupported.is_some() {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .map_err(|e| anyhow::anyhow!("walking the logical plan for the objects it touches: {e}"))?;

    if let Some(what) = unsupported {
        return Err(Denied::new(format!(
            "{what} cannot be authorized: this build has no privilege that describes what it \
             touches, so it is refused rather than allowed. Run it through an operator tool with \
             direct services-database credentials instead."
        ))
        .into());
    }
    Ok(required)
}

// ---------------------------------------------------------------------------
// The check
// ---------------------------------------------------------------------------

/// A refusal, as a distinct type rather than a string.
///
/// It travels inside an [`anyhow::Error`] all the way up through
/// [`execute_cached`](crate::result_cache::execute_cached) and out of the engine, where
/// [`crate::server`] probes the cause chain for it (`chain().any(|c| c.is::<Denied>())`) to answer
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
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Whether `error` is (or was caused by) an authorization refusal.
pub fn is_denial(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<Denied>())
}

/// The caller's effective grants, plus enough identity to write a useful denial.
///
/// Built once per query — one database round trip in [`crate::auth`] — and then consulted purely.
/// That split is why every function below is synchronous and testable with a literal `Vec<Grant>`.
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

    /// Check everything `plan` touches.
    pub fn check_plan(
        &self,
        plan: &LogicalPlan,
        default_catalog: &str,
        default_schema: &str,
    ) -> Result<()> {
        let required = required_privileges(plan, default_catalog, default_schema)?;
        let missing: Vec<&Requirement> = required.iter().filter(|r| !self.allows(r)).collect();
        if missing.is_empty() {
            return Ok(());
        }
        Err(self.denial(missing.into_iter()))
    }

    /// The denial message.
    ///
    /// It names every missing privilege *and* the command that adds it. An authorization error
    /// that says only "permission denied" is a support ticket; one that says which `GRANT` to write
    /// is a fix. Note what it does not say: which objects exist. A caller with no grants at all
    /// learns only that the objects *they named* are denied to them.
    fn denial<'a>(&self, missing: impl Iterator<Item = &'a Requirement>) -> anyhow::Error {
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
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

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
        ] {
            assert!(
                !g.covers(&need(p, ObjectType::Table, "lldb.sales.orders")),
                "SELECT must not imply {p}"
            );
        }
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

    /// A session with two tables, so the plan walk has real logical plans to walk.
    fn session() -> SessionContext {
        let ctx = SessionContext::new();
        for name in ["orders", "lineitem"] {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
                    .expect("batch");
            ctx.register_batch(name, batch).expect("register");
        }
        ctx
    }

    async fn plan_of(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.state()
            .create_logical_plan(sql)
            .await
            .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
    }

    async fn requirements(sql: &str) -> BTreeSet<Requirement> {
        let ctx = session();
        let plan = plan_of(&ctx, sql).await;
        required_privileges(&plan, "datafusion", "public").expect("requirements")
    }

    #[tokio::test]
    async fn a_select_requires_select_on_the_resolved_table() {
        // The unqualified `orders` resolves against the session defaults, which is why the
        // requirement — and therefore the grant an operator must write — is fully qualified.
        assert_eq!(
            requirements("SELECT a FROM orders").await,
            BTreeSet::from([need(
                Privilege::Select,
                ObjectType::Table,
                "datafusion.public.orders"
            )])
        );
    }

    #[tokio::test]
    async fn a_join_requires_select_on_both_sides() {
        assert_eq!(
            requirements("SELECT o.a FROM orders o JOIN lineitem l ON o.a = l.a").await,
            BTreeSet::from([
                need(
                    Privilege::Select,
                    ObjectType::Table,
                    "datafusion.public.orders"
                ),
                need(
                    Privilege::Select,
                    ObjectType::Table,
                    "datafusion.public.lineitem"
                ),
            ])
        );
    }

    #[tokio::test]
    async fn a_subquery_is_not_walked_past() {
        // The one that a plain `apply` gets wrong: `lineitem`'s scan hangs off an expression, so
        // a walk that does not descend into subqueries would authorize this against `orders`
        // alone and hand over rows of a table the caller was never granted.
        let reqs = requirements("SELECT a FROM orders WHERE a IN (SELECT a FROM lineitem)").await;
        assert!(
            reqs.contains(&need(
                Privilege::Select,
                ObjectType::Table,
                "datafusion.public.lineitem"
            )),
            "the subquery's table must be required: {reqs:?}"
        );
        assert_eq!(reqs.len(), 2, "{reqs:?}");
    }

    #[tokio::test]
    async fn an_insert_requires_insert_on_its_target() {
        // `INSERT INTO t SELECT …` requires both: INSERT on the target, SELECT on the source.
        let reqs = requirements("INSERT INTO orders SELECT a FROM lineitem").await;
        assert!(reqs.contains(&need(
            Privilege::Insert,
            ObjectType::Table,
            "datafusion.public.orders"
        )));
        assert!(reqs.contains(&need(
            Privilege::Select,
            ObjectType::Table,
            "datafusion.public.lineitem"
        )));
    }

    #[tokio::test]
    async fn a_statement_we_cannot_describe_is_refused_rather_than_allowed() {
        // `CREATE EXTERNAL TABLE` names a filesystem path, not a catalog object; there is no
        // privilege in this model that describes it, so authorizing it is impossible and allowing
        // it would be a file-read primitive for anyone with any grant at all.
        let ctx = session();
        let plan = plan_of(
            &ctx,
            "CREATE EXTERNAL TABLE leak (a BIGINT) STORED AS PARQUET LOCATION '/etc/'",
        )
        .await;
        let err = required_privileges(&plan, "datafusion", "public")
            .expect_err("DDL has no expressible privilege")
            .to_string();
        assert!(err.contains("refused rather than allowed"), "{err}");
    }

    #[tokio::test]
    async fn a_missing_grant_denies_and_names_what_to_add() {
        let ctx = session();
        let plan = plan_of(&ctx, "SELECT a FROM orders").await;

        let none = QueryAuthorization::new(7, "dana", Vec::new());
        let err = none
            .check_plan(&plan, "datafusion", "public")
            .expect_err("no grants, no query")
            .to_string();
        assert!(err.contains("dana"), "{err}");
        assert!(
            err.contains("SELECT on table datafusion.public.orders"),
            "{err}"
        );
        assert!(
            err.contains("lldb-qe-auth grant"),
            "the denial must name the fix: {err}"
        );

        // The same query, one grant later.
        let allowed = QueryAuthorization::new(
            7,
            "dana",
            vec![grant(
                Privilege::Select,
                ObjectType::Namespace,
                "datafusion.public",
            )],
        );
        allowed
            .check_plan(&plan, "datafusion", "public")
            .expect("the namespace grant covers the table");
    }

    #[tokio::test]
    async fn every_missing_privilege_is_reported_at_once() {
        // A denial that names one of three missing grants means three round trips through an
        // operator. Name them all.
        let ctx = session();
        let plan = plan_of(
            &ctx,
            "SELECT o.a FROM orders o JOIN lineitem l ON o.a = l.a",
        )
        .await;
        let err = QueryAuthorization::new(7, "dana", Vec::new())
            .check_plan(&plan, "datafusion", "public")
            .expect_err("nothing is granted")
            .to_string();
        assert!(err.contains("datafusion.public.orders"), "{err}");
        assert!(err.contains("datafusion.public.lineitem"), "{err}");
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
}
