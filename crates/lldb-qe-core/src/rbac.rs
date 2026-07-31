//! **Role-based access control** — what a caller is allowed to touch, decided *before* the query
//! leaves the coordinator.
//!
//! The vocabulary — [`Privilege`], [`ObjectRef`], [`Grant`], [`Requirement`], [`Denied`] and
//! [`QueryAuthorization`] — lives in [`lldb_qe_types::rbac`] and is re-exported here, so
//! `crate::rbac::Privilege` still resolves and nothing above this module had to change. It moved
//! because it is a pure function of a grant set: no DataFusion, no Arrow, no object store, and
//! therefore usable by anything that needs to *name* a permission without linking the query engine.
//!
//! What stayed is the half that reads a plan.
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
//! [`check_plan`] is a free function rather than a method on [`QueryAuthorization`] for one
//! mechanical reason: the type is foreign to this crate now, and Rust forbids an inherent `impl` on
//! a foreign type. It composes the two halves — this crate's plan walk, that crate's grant check
//! and denial message — and is what every caller should reach for.

use std::collections::BTreeSet;

use anyhow::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::{LogicalPlan, WriteOp};

pub use lldb_qe_types::rbac::*;

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

/// Check everything `plan` touches against `auth`'s grants.
///
/// Every missing privilege is reported at once — a denial that names one of three missing grants
/// means three round trips through an operator.
pub fn check_plan(
    auth: &QueryAuthorization,
    plan: &LogicalPlan,
    default_catalog: &str,
    default_schema: &str,
) -> Result<()> {
    let required = required_privileges(plan, default_catalog, default_schema)?;
    let missing: Vec<&Requirement> = required.iter().filter(|r| !auth.allows(r)).collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(auth.denial(missing.into_iter()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// A literal grant, so the plan-walk tests can assert against a known grant set. The pure
    /// containment rules this helper also exercises are covered where they live, in
    /// `lldb_qe_types::rbac`.
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
            created_at: chrono::Utc::now(),
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
        let err = check_plan(&none, &plan, "datafusion", "public")
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
        check_plan(&allowed, &plan, "datafusion", "public")
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
        let err = check_plan(
            &QueryAuthorization::new(7, "dana", Vec::new()),
            &plan,
            "datafusion",
            "public",
        )
        .expect_err("nothing is granted")
        .to_string();
        assert!(err.contains("datafusion.public.orders"), "{err}");
        assert!(err.contains("datafusion.public.lineitem"), "{err}");
    }
}
