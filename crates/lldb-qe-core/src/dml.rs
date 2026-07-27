//! `DELETE` and `UPDATE` on Iceberg tables: copy-on-write, one snapshot per statement, and an
//! optimistic commit that two concurrent writers cannot both win.
//!
//! Until now every write here was an **append**: `INSERT ... SELECT` through
//! [`crate::catalog::apply_manifest`], which is the only write path
//! `iceberg-datafusion` 0.10 implements (`IcebergTableProvider::insert_into` rejects anything
//! but [`InsertOp::Append`](datafusion::logical_expr::dml::InsertOp)). A table was therefore
//! effectively write-once. This module makes a row removable and a row changeable.
//!
//! # What iceberg-rust 0.10 gives you, and what it does not
//!
//! This is the constraint the whole design is bent around, so it is worth stating plainly.
//! `iceberg::transaction::Transaction` — the crate's only public route to a commit — offers
//! exactly these actions: `fast_append`, `update_table_properties`, `update_schema`,
//! `replace_sort_order`, `update_location`, `update_statistics`, `upgrade_table_version`,
//! `expire_snapshots`. **There is no overwrite, no rewrite-files, and no delete action of any
//! kind**; `SnapshotProduceOperation::delete_entries` exists but its doc comment says it "is
//! intended for future delete operations" and the one implementation returns an empty vec.
//! Nor can the gap be filled from outside the crate: `TransactionAction` is `pub(crate)`, so a
//! custom action is impossible, and `TableCommit`'s builder is `#[builder(build_method(vis =
//! "pub(crate)"))]`, so `Catalog::update_table` cannot be called with updates of our own.
//! Row-level delete files are equally out of reach — there is a public
//! `EqualityDeleteFileWriter`, but nothing that can commit what it writes.
//!
//! What *is* public is every piece needed to build a snapshot by hand: `ManifestWriterBuilder`,
//! `ManifestListWriter`, `Snapshot::builder`, `TableMetadataBuilder::set_branch_snapshot`,
//! `TableMetadata::write_to`, and `MetadataLocation`. So that is what this module does. It
//! assembles the snapshot itself and then performs the catalog pointer swap itself, because the
//! swap is the one thing `Transaction::commit` would otherwise have done for us.
//!
//! # Copy-on-write, and the honest cost of it
//!
//! A statement is executed as a **whole-table rewrite**:
//!
//! 1. Load the table and pin the snapshot we are about to base the change on.
//! 2. Count the rows the predicate matches. If it matches none, stop — see below.
//! 3. Compute the table's *new full contents* as a `SELECT` over that pinned snapshot:
//!    `DELETE`'s survivors, or `UPDATE`'s `CASE WHEN <pred> THEN <new> ELSE <old> END`.
//! 4. Write those rows as fresh Parquet data files.
//! 5. Build a snapshot whose manifest list contains one manifest of the new files plus one
//!    manifest marking every previously-live file `DELETED`, and commit it.
//!
//! This is copy-on-write taken to its limit: every statement rewrites every file, not just the
//! files the predicate touches. Iceberg's cheaper shapes — rewriting only affected files, or
//! merge-on-read delete files — both need a commit that can *remove* files, and per the section
//! above the crate cannot express one. Narrowing the rewrite to affected files would mean
//! keeping the untouched manifests, which requires marking entries deleted inside a manifest we
//! did not write; the machinery for that (`SnapshotProducer`) is private. So the choice was
//! between an O(table) `DELETE` and no `DELETE`, and an honest O(table) `DELETE` wins. It is
//! correct, it produces a real snapshot, and a follow-up read reflects it.
//!
//! A statement that matches **no rows** commits nothing at all and reports the unchanged
//! snapshot id. Rewriting a whole table to record that nothing happened would be a very
//! expensive way to say "0 rows".
//!
//! # `MERGE` is not implemented, and the reason is not Iceberg
//!
//! `MERGE` is rejected with a clear error rather than approximated. The rewrite engine above
//! could express it — a full outer join against the source, one branch per `WHEN` clause — but
//! `MERGE`'s semantics carry a requirement the others do not: a target row matched by more than
//! one source row is a *cardinality violation* that must fail the statement, not silently pick a
//! row or silently duplicate one. Getting that wrong corrupts data quietly, which is the one
//! outcome worth more than the feature. `DELETE` and `UPDATE` are the operations this issue
//! could land correctly, so those are the ones that landed.
//!
//! # Concurrency: compare-and-swap on the metadata pointer, then retry
//!
//! The correctness requirement is that two writers racing one table never lose an update and
//! never apply one twice. That is bought by [`crate::lakehouse::CatalogCommitPoint`]: a commit
//! is a conditional `UPDATE iceberg_tables SET metadata_location = <new> WHERE
//! metadata_location = <the one I read>`. Postgres serializes the two updates on the row, so
//! exactly one writer sees `rows_affected == 1` and the other sees `0`. This is byte-for-byte
//! the statement `iceberg-catalog-sql` issues for its own commits, which is what makes a DML
//! commit and an ordinary `INSERT` commit race *each other* correctly instead of through two
//! mechanisms that merely happen to touch the same row.
//!
//! **The loser retries, it does not error** (up to [`MAX_ATTEMPTS`], with exponential backoff).
//! Retrying means re-reading the winner's snapshot and re-evaluating the whole statement against
//! it, which yields exactly the state a serial execution of `winner; loser` would — the
//! definition of a serializable outcome. Erroring instead would be *safe* but would push the
//! same retry loop onto every caller, and a caller that re-issued the statement blindly would be
//! doing this, only worse. Note what makes retry sound here: the statement is re-planned from
//! its text against the new base, so `UPDATE t SET n = n + 1 WHERE …` increments once, never
//! twice. Nothing is carried over from the failed attempt except the SQL.
//!
//! Exhausting the retries is a hard error naming the table and the attempt count. It is never a
//! silent partial write: the files an attempt wrote before losing are unreferenced by any
//! snapshot, and are deleted on a best-effort basis before the next attempt.
//!
//! # What this does NOT do
//!
//! - **Requires a SQL catalog.** A `memory` catalog has no transactional commit point (its table
//!   pointers live behind a private mutex, and it is per-process anyway), so DML against one is
//!   an error rather than an unprotected read-modify-write.
//! - **Unpartitioned, format-version-2 tables only.** Both are what this engine creates. A
//!   partitioned table would need the rewrite to route rows to partitions; a v3 table would need
//!   row-lineage accounting on the snapshot. Either is an error, not a silent wrong answer.
//! - **No multi-statement transactions.** Each statement is its own commit. The stretch goal of
//!   session-scoped transactions needs a place to stage uncommitted state across statements,
//!   which is a design decision (and a services-DB schema) of its own.
//! - **No `MERGE`**, per above.
//! - **Predicates may only reference the target table.** `USING`, `FROM`, joins, `RETURNING`,
//!   `ORDER BY` and `LIMIT` on a DML statement are all rejected explicitly.
//! - **Old data files are not garbage-collected.** They stay referenced by the previous
//!   snapshot, which is what makes time travel work; reclaiming them is `expire_snapshots`'
//!   job, and nothing here calls it.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Column;
use datafusion::prelude::{Expr, SessionContext, cast};
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr as SqlExpr, FromTable, ObjectName, Statement,
    TableFactor, TableWithJoins, Update,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::StreamExt;
use iceberg::MetadataLocation;
use iceberg::arrow::{FieldMatchMode, schema_to_arrow_schema};
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestFile, Operation,
    Snapshot, SnapshotSummaryCollector, Summary, TableMetadata,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg_datafusion::table::IcebergStaticTableProvider;

use crate::lakehouse::Lakehouse;
use crate::retry::RetryPolicy;

/// How many times a statement is re-planned against a newer snapshot before giving up.
///
/// Bounded on purpose. An unbounded retry against a table under continuous write load is a
/// livelock that looks like a hang, and this rewrite is O(table) per attempt — the cost of
/// losing is not small, so the honest move after a few losses is to say so.
pub const MAX_ATTEMPTS: u32 = 5;

/// How long a loser waits before re-planning. Shares [`RetryPolicy`] with the stage-reassignment
/// path so the engine has one backoff shape rather than two: exponential from a small base, capped,
/// and computed with saturating arithmetic so a retry can never panic its way into a crash.
///
/// The base is shorter than the worker-loss default because a commit conflict is *proof* that
/// another writer just finished, not a guess that a network problem might have cleared.
const CONFLICT_BACKOFF: RetryPolicy = RetryPolicy {
    base_backoff: Duration::from_millis(25),
    max_backoff: Duration::from_secs(1),
};

/// A manifest entry whose data sequence number is inherited from the manifest list rather than
/// stored. `ManifestWriter::add_file` treats any negative value this way, matching what
/// iceberg's own `SnapshotProducer` does for added entries.
const INHERIT_SEQUENCE_NUMBER: i64 = -1;

/// Which statement ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlKind {
    /// `DELETE FROM t [WHERE p]`
    Delete,
    /// `UPDATE t SET c = e, ... [WHERE p]`
    Update,
}

impl std::fmt::Display for DmlKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DmlKind::Delete => "DELETE",
            DmlKind::Update => "UPDATE",
        })
    }
}

/// What a statement did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmlOutcome {
    /// Which statement ran.
    pub kind: DmlKind,
    /// Rows the predicate matched — deleted, or updated.
    pub rows_changed: u64,
    /// The snapshot the table is on now. Equal to `base_snapshot_id` exactly when
    /// `rows_changed == 0`, because a statement that changes nothing commits nothing.
    pub snapshot_id: Option<i64>,
    /// The snapshot the *successful* attempt was planned against. On a retry this is the
    /// winner's snapshot, not the one the first attempt read.
    pub base_snapshot_id: Option<i64>,
    /// Attempts made, including the one that succeeded. `> 1` means a commit conflict was lost
    /// and the statement was re-planned against the winner's snapshot.
    pub attempts: u32,
}

impl DmlOutcome {
    /// Whether a new snapshot was actually committed.
    pub fn committed(&self) -> bool {
        self.snapshot_id != self.base_snapshot_id
    }
}

/// A parsed, validated `DELETE`/`UPDATE` — everything needed to re-plan it against any snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct DmlStatement {
    kind: DmlKind,
    /// The target's name parts, normalized the way DataFusion normalizes identifiers: bare
    /// idents lowercased, quoted idents left alone.
    name: Vec<String>,
    /// `WHERE`, rendered back to SQL. `None` means "every row".
    predicate: Option<String>,
    /// `SET column = expression`, rendered back to SQL. Empty for `DELETE`.
    assignments: Vec<(String, String)>,
}

impl DmlStatement {
    /// The statement kind.
    pub fn kind(&self) -> DmlKind {
        self.kind
    }

    /// The target table's name parts, `[table]`, `[namespace, table]` or
    /// `[catalog, namespace, table]`.
    pub fn name(&self) -> &[String] {
        &self.name
    }

    /// The `(namespace, table)` this statement targets, given a lakehouse's catalog name.
    ///
    /// A three-part name must name *this* catalog — silently retargeting a statement at whatever
    /// catalog happened to be in hand is how a `DELETE` lands in the wrong warehouse.
    fn resolve(&self, catalog_name: &str) -> Result<(String, String)> {
        match self.name.as_slice() {
            [ns, table] => Ok((ns.clone(), table.clone())),
            [cat, ns, table] if cat == catalog_name => Ok((ns.clone(), table.clone())),
            [cat, _, _] => Err(WrongCatalog {
                named: cat.clone(),
                lakehouse: catalog_name.to_string(),
                statement: self.name.join("."),
            }
            .into()),
            [table] => bail!(
                "`{table}` has no namespace — DML needs `<namespace>.<table>` or \
                 `<catalog>.<namespace>.<table>` so the target is unambiguous"
            ),
            other => bail!(
                "`{}` has {} name parts; expected at most catalog.namespace.table",
                self.name.join("."),
                other.len()
            ),
        }
    }
}

/// Parse `sql` if it is a `DELETE` or `UPDATE` this module can execute.
///
/// Returns `Ok(None)` for any other statement — `SELECT`, `INSERT`, DDL — so a caller can route
/// without pre-classifying. A `DELETE`/`UPDATE`/`MERGE` that this module *cannot* execute is an
/// `Err`, never an `Ok(None)`: falling through to DataFusion would hand the user a confusing
/// "unsupported logical plan" instead of the reason, and falling through silently is how a
/// statement gets dropped on the floor.
pub fn parse(sql: &str) -> Result<Option<DmlStatement>> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .with_context(|| format!("parsing SQL: {sql}"))?;
    if statements.len() != 1 {
        // Not our business to reject a multi-statement script outright — but we cannot claim one.
        return Ok(None);
    }
    match statements.remove(0) {
        Statement::Delete(delete) => parse_delete(delete).map(Some),
        Statement::Update(update) => parse_update(update).map(Some),
        Statement::Merge(_) => bail!(
            "MERGE is not implemented. DELETE and UPDATE are; MERGE additionally requires \
             detecting the cardinality violation of a target row matched by several source rows, \
             and an approximation of that silently corrupts data. Express the change as separate \
             UPDATE / DELETE / INSERT statements."
        ),
        _ => Ok(None),
    }
}

fn parse_delete(d: Delete) -> Result<DmlStatement> {
    if !d.tables.is_empty() {
        bail!("multi-table DELETE is not supported — delete from one table per statement");
    }
    if d.using.is_some() {
        bail!("DELETE ... USING is not supported — the predicate may only reference the target");
    }
    if d.returning.is_some() {
        bail!("DELETE ... RETURNING is not supported");
    }
    if !d.order_by.is_empty() || d.limit.is_some() {
        bail!("DELETE ... ORDER BY / LIMIT is not supported — it makes which rows go arbitrary");
    }
    let tables = match d.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let name = sole_table(&tables, "DELETE")?;
    Ok(DmlStatement {
        kind: DmlKind::Delete,
        name,
        predicate: d.selection.map(render),
        assignments: Vec::new(),
    })
}

fn parse_update(u: Update) -> Result<DmlStatement> {
    if u.from.is_some() {
        bail!("UPDATE ... FROM is not supported — assignments may only reference the target");
    }
    if u.returning.is_some() {
        bail!("UPDATE ... RETURNING is not supported");
    }
    if u.or.is_some() {
        bail!("UPDATE OR <conflict-clause> is a SQLite extension and is not supported");
    }
    if u.limit.is_some() {
        bail!("UPDATE ... LIMIT is not supported — it makes which rows change arbitrary");
    }
    let name = sole_table(std::slice::from_ref(&u.table), "UPDATE")?;
    if u.assignments.is_empty() {
        bail!("UPDATE needs at least one SET assignment");
    }
    let mut assignments = Vec::with_capacity(u.assignments.len());
    for Assignment { target, value } in u.assignments {
        let column = match target {
            AssignmentTarget::ColumnName(name) => single_ident(&name)?,
            AssignmentTarget::Tuple(_) => {
                bail!("UPDATE SET (a, b) = ... is not supported — assign one column at a time")
            }
        };
        if assignments.iter().any(|(c, _): &(String, _)| c == &column) {
            bail!("UPDATE assigns `{column}` more than once");
        }
        assignments.push((column, render(value)));
    }
    Ok(DmlStatement {
        kind: DmlKind::Update,
        name,
        predicate: u.selection.map(render),
        assignments,
    })
}

/// The one plain table a DML statement targets — no joins, no subqueries, no alias.
fn sole_table(tables: &[TableWithJoins], what: &str) -> Result<Vec<String>> {
    let [only] = tables else {
        bail!("{what} must name exactly one table, found {}", tables.len());
    };
    if !only.joins.is_empty() {
        bail!("{what} with a JOIN is not supported");
    }
    match &only.relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if alias.is_some() {
                bail!("{what} with a table alias is not supported");
            }
            if args.is_some() {
                bail!("{what} against a table function is not supported");
            }
            Ok(name.0.iter().map(normalize_part).collect())
        }
        _ => bail!("{what} must target a table, not a subquery or table function"),
    }
}

/// Normalize one name part the way DataFusion normalizes identifiers: an unquoted ident folds to
/// lowercase, a quoted one is taken literally. Getting this wrong resolves `Orders` to a
/// different table than `SELECT` would.
fn normalize_part(part: &datafusion::sql::sqlparser::ast::ObjectNamePart) -> String {
    match part.as_ident() {
        Some(ident) if ident.quote_style.is_none() => ident.value.to_lowercase(),
        Some(ident) => ident.value.clone(),
        None => part.to_string(),
    }
}

fn single_ident(name: &ObjectName) -> Result<String> {
    match name.0.as_slice() {
        [part] => Ok(normalize_part(part)),
        _ => bail!("`{name}` is a qualified name; SET assigns a plain column of the target table"),
    }
}

/// Render a parsed expression back to SQL so it can be re-planned against the pinned snapshot.
///
/// Round-tripping through `sqlparser`'s `Display` rather than translating to a DataFusion `Expr`
/// keeps this module out of the business of reimplementing expression planning: whatever
/// DataFusion understands in a `SELECT` it understands here, unchanged.
fn render(expr: SqlExpr) -> String {
    expr.to_string()
}

/// Quote a SQL identifier, doubling any embedded `"`. Same rule as [`crate::catalog`].
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// `true` exactly for the rows the statement matches.
///
/// `COALESCE(p, false)` rather than bare `p` because SQL three-valued logic says a row whose
/// predicate is NULL is *not* matched. Left as `NOT p`, a NULL predicate would make the survivor
/// filter NULL too and quietly delete the row.
fn match_expr(predicate: &str) -> String {
    format!("COALESCE(({predicate}), false)")
}

/// `SELECT count(*)` over the matched rows.
fn count_sql(stmt: &DmlStatement, src: &str) -> String {
    match &stmt.predicate {
        Some(p) => format!("SELECT count(*) FROM {src} WHERE {}", match_expr(p)),
        None => format!("SELECT count(*) FROM {src}"),
    }
}

/// The `SELECT` producing the table's **entire new contents**.
///
/// `columns` must be the target's columns in schema order, so the projection lines up with the
/// Iceberg schema the new data files are written against.
fn rewrite_sql(stmt: &DmlStatement, src: &str, columns: &[String]) -> String {
    match stmt.kind {
        DmlKind::Delete => match &stmt.predicate {
            // Survivors are the complement of the matched set, so the same `COALESCE` guard runs
            // here — negating it, not the raw predicate.
            Some(p) => format!("SELECT * FROM {src} WHERE NOT {}", match_expr(p)),
            None => format!("SELECT * FROM {src} WHERE false"),
        },
        DmlKind::Update => {
            let projected: Vec<String> = columns
                .iter()
                .map(|c| {
                    let q = quote_ident(c);
                    match stmt.assignments.iter().find(|(name, _)| name == c) {
                        // Every right-hand side reads the *source* row, so assignments see
                        // pre-update values — the SQL rule — no matter what order they appear in.
                        Some((_, value)) => match &stmt.predicate {
                            Some(p) => format!(
                                "CASE WHEN {} THEN ({value}) ELSE {q} END AS {q}",
                                match_expr(p)
                            ),
                            None => format!("({value}) AS {q}"),
                        },
                        None => q,
                    }
                })
                .collect();
            format!("SELECT {} FROM {src}", projected.join(", "))
        }
    }
}

/// Run a `DELETE` or `UPDATE` against `lake`.
///
/// Returns `Ok(None)` when `sql` is not a statement this module owns, so a caller can hand
/// everything through here and fall back to `SessionContext::sql` for the rest.
pub async fn execute(lake: &Lakehouse, sql: &str) -> Result<Option<DmlOutcome>> {
    let Some(stmt) = parse(sql)? else {
        return Ok(None);
    };
    Ok(Some(execute_statement(lake, &stmt).await?))
}

/// Run an already-parsed statement, retrying on a lost commit race.
pub async fn execute_statement(lake: &Lakehouse, stmt: &DmlStatement) -> Result<DmlOutcome> {
    let (ns, table_name) = stmt.resolve(lake.catalog_name())?;
    let commit_point = lake.commit_point().with_context(|| {
        format!(
            "{} on {ns}.{table_name} needs a `sql` catalog: the commit has to be a \
             compare-and-swap on the table's metadata pointer, and a `memory` catalog has no \
             such thing. Point the manifest's catalog at Postgres (`backend = {{ kind = \
             \"sql\" }}`).",
            stmt.kind
        )
    })?;

    for attempt in 1..=MAX_ATTEMPTS {
        let table = lake.load_table(&ns, &table_name).await?;
        let base_snapshot_id = table.metadata().current_snapshot_id();

        let rows_changed = count_matches(&table, stmt).await?;
        if rows_changed == 0 {
            // Nothing to record. Rewriting every file to commit a no-op would be an expensive
            // way to say so, and there is no conflict to detect either.
            return Ok(DmlOutcome {
                kind: stmt.kind,
                rows_changed: 0,
                snapshot_id: base_snapshot_id,
                base_snapshot_id,
                attempts: attempt,
            });
        }

        let staged = stage(&table, stmt).await?;
        let pool = commit_point.pool().await?;
        if swap_metadata_pointer(
            pool,
            lake.catalog_name(),
            &ns,
            &table_name,
            &staged.base_location,
            &staged.new_location,
        )
        .await?
        {
            return Ok(DmlOutcome {
                kind: stmt.kind,
                rows_changed,
                snapshot_id: Some(staged.snapshot_id),
                base_snapshot_id,
                attempts: attempt,
            });
        }

        // Lost the race. Everything this attempt wrote is unreferenced by any snapshot, so drop
        // it rather than leave the warehouse littered; then re-plan against the winner's state.
        staged.discard(&table).await;
        tracing::debug!(
            table = %format!("{ns}.{table_name}"),
            attempt,
            "commit conflict on {}; re-planning against the winning snapshot",
            stmt.kind
        );
        tokio::time::sleep(CONFLICT_BACKOFF.backoff(attempt - 1)).await;
    }

    bail!(
        "{} on {ns}.{table_name} lost the commit race {MAX_ATTEMPTS} times: another writer \
         committed a new snapshot between every read and commit. Nothing was applied — retry the \
         statement, or reduce concurrent writers on this table.",
        stmt.kind
    )
}

/// A three-part name that belongs to a *different* catalog than the lakehouse it was offered to.
///
/// A distinct type rather than a message, because a caller has to act on it: the coordinator holds
/// several lakehouses and asks each in turn whose table this is, so "not mine" must be
/// distinguishable from "yours, and it went wrong". Matching on the text of an error to decide
/// control flow means a reworded message silently changes behaviour — and here the failure mode is
/// a real error being swallowed as "wrong catalog" and reported as a missing table.
#[derive(Debug, thiserror::Error)]
#[error("`{statement}` names catalog `{named}`, but this lakehouse is catalog `{lakehouse}`")]
pub struct WrongCatalog {
    /// The catalog the statement named.
    pub named: String,
    /// The catalog of the lakehouse that was asked.
    pub lakehouse: String,
    /// The full table name as written.
    pub statement: String,
}

/// A commit assembled and durably written, waiting only on the pointer swap.
struct Staged {
    /// The metadata location the swap must find in place — the one the plan was built on.
    base_location: String,
    /// Where the new metadata JSON was written.
    new_location: MetadataLocation,
    snapshot_id: i64,
    /// Everything this attempt wrote, newest last. Deleted if the swap loses.
    written: Vec<String>,
}

impl Staged {
    /// Best-effort cleanup of a losing attempt's files. Failures are logged, not propagated: the
    /// statement is about to be retried, and an orphaned file is a tidiness problem while a
    /// spurious error would be a correctness one.
    async fn discard(&self, table: &Table) {
        discard_paths(table, &self.written).await;
    }
}

/// Register the table's pinned snapshot in a private session and count matched rows.
async fn count_matches(table: &Table, stmt: &DmlStatement) -> Result<u64> {
    let (ctx, src) = pinned_session(table).await?;
    let sql = count_sql(stmt, &src);
    let batches = ctx
        .sql(&sql)
        .await
        .with_context(|| format!("planning `{sql}`"))?
        .collect()
        .await
        .with_context(|| format!("counting rows matched by `{sql}`"))?;
    let count = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(0);
    Ok(count.max(0) as u64)
}

/// A `SessionContext` that can see exactly one table: the snapshot `table` is pinned to.
///
/// Pinning is the load-bearing part. A catalog-backed provider reloads from the catalog on every
/// scan, so the rewrite could read a *newer* snapshot than the one the commit will be conditioned
/// on — and then the compare-and-swap would succeed while the new contents silently incorporated
/// someone else's write on top of a base that claimed not to include it. A static provider reads
/// the snapshot we are about to overwrite, and only that one.
///
/// It is registered under the bare table name so a predicate may qualify its columns (`orders.id`
/// as well as `id`).
async fn pinned_session(table: &Table) -> Result<(SessionContext, String)> {
    let provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
        .await
        .context("pinning the table to the snapshot being rewritten")?;
    let name = table.identifier().name().to_string();
    let ctx = SessionContext::new();
    ctx.register_table(
        datafusion::sql::TableReference::bare(name.clone()),
        Arc::new(provider),
    )
    .with_context(|| format!("registering {name} for rewriting"))?;
    Ok((ctx, quote_ident(&name)))
}

/// Compute the new contents, write them, and build + persist the new table metadata.
///
/// Everything here is catalog-agnostic — it only needs `FileIO` — which is what lets it be
/// exercised end to end against a memory catalog with no database in sight.
async fn stage(table: &Table, stmt: &DmlStatement) -> Result<Staged> {
    // `stage_inner` threads everything it writes into `written` as it goes, so a failure *part
    // way through* — an IO error on the manifest list, a metadata write that fails — can still
    // clean up after itself. Without this the orphans are invisible: the statement returns an
    // error, nothing is committed, and the files sit in the warehouse forever. Conflicts already
    // clean up via `Staged::discard`; this closes the other path into the same litter.
    let mut written: Vec<String> = Vec::new();
    match stage_inner(table, stmt, &mut written).await {
        Ok(staged) => Ok(staged),
        Err(e) => {
            discard_paths(table, &written).await;
            Err(e)
        }
    }
}

/// Best-effort deletion, newest first. Failures are logged, never propagated — the caller already
/// has an error worth more than this one.
async fn discard_paths(table: &Table, paths: &[String]) {
    for path in paths.iter().rev() {
        if let Err(e) = table.file_io().delete(path).await {
            tracing::debug!(path, error = %e, "could not remove a staged file");
        }
    }
}

async fn stage_inner(
    table: &Table,
    stmt: &DmlStatement,
    written: &mut Vec<String>,
) -> Result<Staged> {
    let metadata = table.metadata();
    let base_location = table
        .metadata_location_result()
        .context("the table has no committed metadata location to swap")?
        .to_string();

    if metadata.format_version() != FormatVersion::V2 {
        bail!(
            "{} needs a format-version-2 table; {} is v{}. v1 manifests and v3 row lineage both \
             need snapshot bookkeeping this rewrite does not do.",
            stmt.kind,
            table.identifier(),
            metadata.format_version() as u8
        );
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        bail!(
            "{} on partitioned table {} is not supported — the rewrite would have to route rows \
             back to partitions, and writing them all to one is a wrong answer, not a slow one.",
            stmt.kind,
            table.identifier()
        );
    }

    let arrow_schema = schema_to_arrow_schema(metadata.current_schema())
        .context("converting the table's Iceberg schema to Arrow")?;
    let columns: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    // 1. The rows that survive.
    let (ctx, src) = pinned_session(table).await?;
    let sql = rewrite_sql(stmt, &src, &columns);
    let df = ctx
        .sql(&sql)
        .await
        .with_context(|| format!("planning the rewrite `{sql}`"))?;
    // Re-project through an explicit cast: an assignment's expression can widen a column's type
    // (`SET n = n + 1` on an int32 yields int64), and the Parquet writer matches the table's
    // schema exactly. Better a cast error naming the column than a writer error naming a field id.
    let df = df.select(cast_projection(&arrow_schema)).with_context(|| {
        format!(
            "aligning the rewrite of {} to its schema",
            table.identifier()
        )
    })?;
    let mut stream = df.execute_stream().await.context("executing the rewrite")?;

    // 2. Write them as fresh data files.
    let props = metadata.table_properties()?;
    let file_format = DataFileFormat::from_str(&props.write_format_default)
        .with_context(|| format!("table write format `{}`", props.write_format_default))?;
    if file_format != DataFileFormat::Parquet {
        bail!(
            "{} can only rewrite Parquet data files, not {file_format}",
            stmt.kind
        );
    }
    let snapshot_id = fresh_snapshot_id(metadata);
    let parquet =
        ParquetWriterBuilder::from_table_properties(&props, metadata.current_schema().clone())
            // Arrow batches out of DataFusion carry no Iceberg field ids, so fields match by name —
            // the same choice `iceberg-datafusion`'s own write path makes.
            .with_match_mode(FieldMatchMode::Name);
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        props.write_target_file_size_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(metadata).context("resolving the table's data location")?,
        DefaultFileNameGenerator::new(format!("dml-{snapshot_id}"), None, file_format),
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .context("building the data file writer")?;
    while let Some(batch) = stream.next().await {
        writer
            .write(batch.context("reading the rewritten rows")?)
            .await
            .context("writing rewritten rows")?;
    }
    let new_files = writer
        .close()
        .await
        .context("closing the data file writer")?;

    written.extend(new_files.iter().map(|f| f.file_path().to_string()));

    // 3. Assemble the snapshot over those files, replacing every previously live one.
    let old_files = live_data_files(table).await?;
    let manifests = write_manifests(table, snapshot_id, &new_files, &old_files, written).await?;
    let manifest_list = format!(
        "{}/metadata/snap-{snapshot_id}-0.avro",
        metadata.location().trim_end_matches('/')
    );
    write_manifest_list(table, snapshot_id, &manifest_list, manifests).await?;
    written.push(manifest_list.clone());

    let snapshot = build_snapshot(metadata, snapshot_id, manifest_list, &new_files, &old_files);

    // 4. Persist the metadata the pointer will name. Writing it *before* the swap is what makes
    //    the swap a pure pointer move: the loser of a race leaves a metadata file nothing refers
    //    to, which is inert, whereas a pointer to a file that does not exist yet would not be.
    let new_metadata = metadata
        .clone()
        .into_builder(Some(base_location.clone()))
        .set_branch_snapshot(snapshot, MAIN_BRANCH)
        .context("adding the new snapshot to the table metadata")?
        .build()
        .context("building the new table metadata")?
        .metadata;
    let new_location = MetadataLocation::from_str(&base_location)
        .with_context(|| format!("parsing metadata location {base_location}"))?
        .with_next_version()
        .with_new_metadata(&new_metadata);
    new_metadata
        .write_to(table.file_io(), &new_location)
        .await
        .context("writing the new table metadata")?;
    written.push(new_location.to_string());

    Ok(Staged {
        base_location,
        new_location,
        snapshot_id,
        written: std::mem::take(written),
    })
}

/// One `CAST(col AS <schema type>) AS col` per field, in schema order.
fn cast_projection(schema: &ArrowSchema) -> Vec<Expr> {
    schema
        .fields()
        .iter()
        .map(|f| {
            cast(
                Expr::Column(Column::new_unqualified(f.name())),
                f.data_type().clone(),
            )
            .alias(f.name())
        })
        .collect()
}

/// A snapshot id no snapshot in this table already uses.
///
/// Same construction iceberg's own `SnapshotProducer` uses: fold a v4 UUID into 64 bits, take the
/// absolute value, reject a collision.
fn fresh_snapshot_id(metadata: &TableMetadata) -> i64 {
    loop {
        let candidate = random_snapshot_id();
        if !metadata.snapshots().any(|s| s.snapshot_id() == candidate) {
            return candidate;
        }
    }
}

fn random_snapshot_id() -> i64 {
    // `Uuid` is not a direct dependency here, and does not need to be: hashing a process-unique
    // seed gives the same "unlikely to collide, and collisions are checked anyway" property.
    use std::hash::{BuildHasher, Hasher, RandomState};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    );
    hasher.write_u32(std::process::id());
    (hasher.finish() >> 1) as i64
}

/// Every data file the table's current snapshot still counts as live, with the sequence numbers a
/// `DELETED` entry has to preserve.
async fn live_data_files(table: &Table) -> Result<Vec<LiveFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .context("reading the current snapshot's manifest list")?;

    let mut live = Vec::new();
    for entry in manifest_list.entries() {
        let manifest = entry
            .load_manifest(table.file_io())
            .await
            .with_context(|| format!("reading manifest {}", entry.manifest_path))?;
        for e in manifest.entries() {
            if !e.is_alive() {
                continue;
            }
            if e.content_type() != DataContentType::Data {
                bail!(
                    "{} carries {:?} files, which this copy-on-write rewrite does not track. \
                     Rewriting the table would drop them and silently resurrect deleted rows, so \
                     this is an error instead.",
                    table.identifier(),
                    e.content_type()
                );
            }
            // Inherited from the manifest list by `load_manifest`, so these are populated for any
            // entry that was actually committed. A `DELETED` entry must carry the *original*
            // numbers, which is why they are captured rather than regenerated.
            let (Some(sequence_number), Some(file_sequence_number)) =
                (e.sequence_number(), e.file_sequence_number)
            else {
                bail!(
                    "manifest entry for {} has no sequence number; refusing to write a delete \
                     entry that would misrepresent when the file was added",
                    e.file_path()
                );
            };
            live.push(LiveFile {
                data_file: e.data_file().clone(),
                sequence_number,
                file_sequence_number,
            });
        }
    }
    Ok(live)
}

/// A currently-live data file plus what a `DELETED` entry for it must preserve.
struct LiveFile {
    data_file: DataFile,
    sequence_number: i64,
    file_sequence_number: i64,
}

/// Write the new snapshot's manifests: the added files, and the removal of every old one.
///
/// Old manifests are deliberately *not* carried forward. They describe files this snapshot no
/// longer contains, and the crate offers no way to rewrite an entry inside a manifest we did not
/// author; carrying them would keep every replaced file live.
async fn write_manifests(
    table: &Table,
    snapshot_id: i64,
    new_files: &[DataFile],
    old_files: &[LiveFile],
    written: &mut Vec<String>,
) -> Result<Vec<ManifestFile>> {
    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let spec = metadata.default_partition_spec().as_ref().clone();
    let base = metadata.location().trim_end_matches('/');
    let mut manifests = Vec::new();

    if !new_files.is_empty() {
        let path = format!("{base}/metadata/{snapshot_id}-m0.avro");
        let mut w = iceberg::spec::ManifestWriterBuilder::new(
            table.file_io().new_output(&path)?,
            Some(snapshot_id),
            schema.clone(),
            spec.clone(),
        )
        .build_v2_data();
        for f in new_files {
            w.add_file(f.clone(), INHERIT_SEQUENCE_NUMBER)
                .with_context(|| format!("recording added file {}", f.file_path()))?;
        }
        written.push(path);
        manifests.push(
            w.write_manifest_file()
                .await
                .context("writing the added-files manifest")?,
        );
    }

    if !old_files.is_empty() {
        // A removed *data* file is a `DELETED` entry in a data manifest — `build_v2_deletes` is
        // for delete files (positional/equality), which is a different thing entirely.
        let path = format!("{base}/metadata/{snapshot_id}-m1.avro");
        let mut w = iceberg::spec::ManifestWriterBuilder::new(
            table.file_io().new_output(&path)?,
            Some(snapshot_id),
            schema,
            spec,
        )
        .build_v2_data();
        for f in old_files {
            w.add_delete_file(
                f.data_file.clone(),
                f.sequence_number,
                Some(f.file_sequence_number),
            )
            .with_context(|| format!("recording removed file {}", f.data_file.file_path()))?;
        }
        written.push(path);
        manifests.push(
            w.write_manifest_file()
                .await
                .context("writing the removed-files manifest")?,
        );
    }

    Ok(manifests)
}

async fn write_manifest_list(
    table: &Table,
    snapshot_id: i64,
    path: &str,
    manifests: Vec<ManifestFile>,
) -> Result<()> {
    let metadata = table.metadata();
    let mut writer = iceberg::spec::ManifestListWriter::v2(
        table.file_io().new_output(path)?.writer().await?,
        snapshot_id,
        metadata.current_snapshot_id(),
        metadata.next_sequence_number(),
    );
    writer
        .add_manifests(manifests.into_iter())
        .context("adding manifests to the manifest list")?;
    writer.close().await.context("writing the manifest list")?;
    Ok(())
}

fn build_snapshot(
    metadata: &TableMetadata,
    snapshot_id: i64,
    manifest_list: String,
    new_files: &[DataFile],
    old_files: &[LiveFile],
) -> Snapshot {
    let mut collector = SnapshotSummaryCollector::default();
    let schema = metadata.current_schema().clone();
    let spec = metadata.default_partition_spec().clone();
    for f in new_files {
        collector.add_file(f, schema.clone(), spec.clone());
    }
    for f in old_files {
        collector.remove_file(&f.data_file, schema.clone(), spec.clone());
    }
    let mut properties = collector.build();
    properties.extend(totals(new_files));

    Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_manifest_list(manifest_list)
        .with_schema_id(metadata.current_schema_id())
        .with_summary(Summary {
            // `overwrite` when rows were replaced, `delete` when the table only shrank — the same
            // distinction iceberg-java draws, and what a reader of `snapshots` metadata expects.
            operation: if new_files.is_empty() {
                Operation::Delete
            } else {
                Operation::Overwrite
            },
            additional_properties: properties,
        })
        .build()
}

/// The `total-*` summary keys, computed from the snapshot's whole file set rather than by
/// adjusting the previous snapshot's numbers.
///
/// Iceberg's own helper for this (`update_snapshot_summaries`) is `pub(crate)`. Recomputing is
/// not a workaround though — it is strictly more robust: a whole-table rewrite *knows* the exact
/// final state, so the totals cannot drift from a mis-signed delta the way an incremental
/// adjustment can. Delete-file totals are pinned at zero because `live_data_files` refuses a
/// table that has any.
fn totals(new_files: &[DataFile]) -> HashMap<String, String> {
    let records: u64 = new_files.iter().map(|f| f.record_count()).sum();
    let size: u64 = new_files.iter().map(|f| f.file_size_in_bytes()).sum();
    HashMap::from([
        ("total-records".to_string(), records.to_string()),
        ("total-data-files".to_string(), new_files.len().to_string()),
        ("total-files-size".to_string(), size.to_string()),
        ("total-delete-files".to_string(), "0".to_string()),
        ("total-position-deletes".to_string(), "0".to_string()),
        ("total-equality-deletes".to_string(), "0".to_string()),
    ])
}

/// The commit: move `iceberg_tables.metadata_location` from `base` to `new`, **only if** it is
/// still `base`. Returns whether this writer won.
///
/// This is `iceberg-catalog-sql`'s own `update_table` statement, reproduced predicate for
/// predicate — including the `iceberg_type` guard that keeps it from matching a view row. Same
/// row, same condition, so a DML commit and an `INSERT`'s commit serialize against each other
/// rather than through two independent schemes.
///
/// The column names are `iceberg-catalog-sql`'s private statics, so they are spelled out here and
/// pinned by `tests/integration/dml_snapshots.rs`, which asserts the live table still has exactly these
/// columns. A silent rename in that crate would otherwise turn every commit into a runtime error
/// — or, worse, a `WHERE` clause that matches nothing and reports a phantom conflict forever.
async fn swap_metadata_pointer(
    pool: &sqlx::postgres::PgPool,
    catalog_name: &str,
    namespace: &str,
    table: &str,
    base: &str,
    new: &MetadataLocation,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE iceberg_tables \
         SET metadata_location = $1, previous_metadata_location = $2 \
         WHERE catalog_name = $3 \
           AND table_name = $4 \
           AND table_namespace = $5 \
           AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL) \
           AND metadata_location = $6",
    )
    .bind(new.to_string())
    .bind(base)
    .bind(catalog_name)
    .bind(table)
    .bind(namespace)
    .bind(base)
    .execute(pool)
    .await
    .with_context(|| format!("committing {namespace}.{table} in catalog {catalog_name}"))?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(sql: &str) -> DmlStatement {
        parse(sql)
            .expect("statement parses")
            .expect("statement is DML")
    }

    #[test]
    fn non_dml_is_not_claimed() -> Result<()> {
        for sql in [
            "SELECT 1",
            "INSERT INTO sales.orders VALUES (1)",
            "CREATE TABLE t (a INT)",
        ] {
            assert!(parse(sql)?.is_none(), "{sql} must fall through");
        }
        Ok(())
    }

    #[test]
    fn delete_parses_target_and_predicate() {
        let stmt = parsed("DELETE FROM sales.orders WHERE id > 3 AND label IS NULL");
        assert_eq!(stmt.kind(), DmlKind::Delete);
        assert_eq!(stmt.name(), ["sales", "orders"]);
        assert_eq!(stmt.predicate.as_deref(), Some("id > 3 AND label IS NULL"));
        assert!(stmt.assignments.is_empty());
        // No WHERE means every row, which is legal and must not be confused with "no statement".
        assert_eq!(parsed("DELETE FROM sales.orders").predicate, None);
    }

    #[test]
    fn update_parses_assignments_in_order() {
        let stmt = parsed("UPDATE lldb.sales.orders SET label = 'x', qty = qty + 1 WHERE id = 2");
        assert_eq!(stmt.kind(), DmlKind::Update);
        assert_eq!(stmt.name(), ["lldb", "sales", "orders"]);
        assert_eq!(stmt.predicate.as_deref(), Some("id = 2"));
        assert_eq!(
            stmt.assignments,
            vec![
                ("label".to_string(), "'x'".to_string()),
                ("qty".to_string(), "qty + 1".to_string()),
            ]
        );
    }

    #[test]
    fn identifiers_normalize_like_datafusion() {
        // Unquoted folds to lowercase; quoted is taken literally. Getting this backwards targets
        // a table that `SELECT` would not resolve to.
        assert_eq!(
            parsed("DELETE FROM Sales.Orders").name(),
            ["sales", "orders"]
        );
        assert_eq!(
            parsed(r#"DELETE FROM "Sales"."Orders""#).name(),
            ["Sales", "Orders"]
        );
    }

    #[test]
    fn merge_is_rejected_with_a_reason() {
        let err = parse(
            "MERGE INTO sales.orders t USING src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.label = s.label",
        )
        .expect_err("MERGE is not implemented")
        .to_string();
        assert!(err.contains("MERGE is not implemented"), "{err}");
        assert!(err.contains("cardinality"), "{err}");
    }

    #[test]
    fn shapes_we_cannot_execute_are_errors_not_silence() {
        // Each of these would produce a wrong answer if it were quietly narrowed to what the
        // rewrite can do, so each must name its own reason.
        for (sql, needle) in [
            ("DELETE FROM a.t USING a.u WHERE t.id = u.id", "USING"),
            ("DELETE FROM a.t WHERE id = 1 RETURNING id", "RETURNING"),
            ("DELETE FROM a.t ORDER BY id LIMIT 1", "ORDER BY"),
            ("UPDATE a.t SET x = 1 FROM a.u", "FROM"),
            ("UPDATE a.t SET x = 1 WHERE id = 1 RETURNING x", "RETURNING"),
            ("UPDATE a.t SET x = 1 LIMIT 1", "LIMIT"),
            ("UPDATE a.t AS z SET x = 1", "alias"),
            ("UPDATE a.t SET x = 1, x = 2", "more than once"),
        ] {
            let err = parse(sql)
                .expect_err(&format!("`{sql}` must be refused"))
                .to_string();
            assert!(err.contains(needle), "`{sql}` said: {err}");
        }
    }

    #[test]
    fn a_qualified_name_must_match_this_catalog() -> Result<()> {
        let stmt = parsed("DELETE FROM lldb.sales.orders WHERE id = 1");
        assert_eq!(stmt.resolve("lldb")?, ("sales".into(), "orders".into()));
        // Pointing at another catalog is a typo with warehouse-sized consequences.
        let err = stmt
            .resolve("other")
            .expect_err("wrong catalog")
            .to_string();
        assert!(err.contains("lldb"), "{err}");

        // A bare name has no namespace to resolve against.
        let err = parsed("DELETE FROM orders")
            .resolve("lldb")
            .expect_err("bare name")
            .to_string();
        assert!(err.contains("no namespace"), "{err}");
        Ok(())
    }

    #[test]
    fn delete_rewrite_keeps_null_predicate_rows() {
        let stmt = parsed("DELETE FROM s.t WHERE id > 3");
        // `NOT (id > 3)` alone is NULL when id is NULL, which would drop the row. The COALESCE is
        // the whole reason this test exists.
        assert_eq!(
            rewrite_sql(&stmt, "\"t\"", &["id".into()]),
            "SELECT * FROM \"t\" WHERE NOT COALESCE((id > 3), false)"
        );
        assert_eq!(
            count_sql(&stmt, "\"t\""),
            "SELECT count(*) FROM \"t\" WHERE COALESCE((id > 3), false)"
        );
    }

    #[test]
    fn unconditional_delete_keeps_nothing() {
        let stmt = parsed("DELETE FROM s.t");
        assert_eq!(
            rewrite_sql(&stmt, "\"t\"", &["id".into()]),
            "SELECT * FROM \"t\" WHERE false"
        );
        assert_eq!(count_sql(&stmt, "\"t\""), "SELECT count(*) FROM \"t\"");
    }

    #[test]
    fn update_rewrite_projects_every_column_in_schema_order() {
        let stmt = parsed("UPDATE s.t SET label = 'x' WHERE id = 2");
        let columns = vec!["id".to_string(), "label".to_string(), "qty".to_string()];
        // Unassigned columns pass through untouched; the assigned one is conditional; and the
        // order is the schema's, not the SET clause's, so the projection lines up with Parquet.
        assert_eq!(
            rewrite_sql(&stmt, "\"t\"", &columns),
            "SELECT \"id\", CASE WHEN COALESCE((id = 2), false) THEN ('x') ELSE \"label\" END \
             AS \"label\", \"qty\" FROM \"t\""
        );
    }

    #[test]
    fn unconditional_update_needs_no_case() {
        let stmt = parsed("UPDATE s.t SET label = 'x'");
        assert_eq!(
            rewrite_sql(&stmt, "\"t\"", &["id".into(), "label".into()]),
            "SELECT \"id\", ('x') AS \"label\" FROM \"t\""
        );
    }

    #[test]
    fn assignments_read_pre_update_values() {
        // Both right-hand sides reference the source row, so `b = a` sees the *old* `a` even
        // though `a` is assigned in the same statement — the SQL rule, and easy to get wrong.
        let stmt = parsed("UPDATE s.t SET a = a + 1, b = a");
        let sql = rewrite_sql(&stmt, "\"t\"", &["a".into(), "b".into()]);
        assert_eq!(sql, "SELECT (a + 1) AS \"a\", (a) AS \"b\" FROM \"t\"");
    }

    #[test]
    fn odd_column_names_survive_quoting() {
        let stmt = parsed(r#"UPDATE s.t SET "od""d" = 1"#);
        let sql = rewrite_sql(&stmt, "\"t\"", &["od\"d".to_string()]);
        assert_eq!(sql, "SELECT (1) AS \"od\"\"d\" FROM \"t\"");
    }

    #[test]
    fn totals_describe_the_whole_new_state() {
        use iceberg::spec::{DataFileBuilder, Struct};
        let file = |records: u64, size: u64, path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(size)
                .record_count(records)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .build()
                .expect("data file builds")
        };
        let t = totals(&[file(3, 100, "a.parquet"), file(4, 200, "b.parquet")]);
        assert_eq!(t["total-records"], "7");
        assert_eq!(t["total-data-files"], "2");
        assert_eq!(t["total-files-size"], "300");
        // Pinned at zero: `live_data_files` refuses a table carrying delete files at all, so a
        // non-zero value here could only ever be a lie.
        assert_eq!(t["total-delete-files"], "0");

        // An empty table is the `DELETE FROM t` case and must report zeros, not absent keys.
        let empty = totals(&[]);
        assert_eq!(empty["total-records"], "0");
        assert_eq!(empty["total-data-files"], "0");
    }

    #[test]
    fn snapshot_ids_are_positive_and_do_not_repeat() {
        let a = random_snapshot_id();
        let b = random_snapshot_id();
        assert!(a > 0 && b > 0, "{a}, {b}");
        assert_ne!(a, b);
    }

    #[test]
    fn outcome_reports_whether_it_committed() {
        let unchanged = DmlOutcome {
            kind: DmlKind::Delete,
            rows_changed: 0,
            snapshot_id: Some(7),
            base_snapshot_id: Some(7),
            attempts: 1,
        };
        assert!(!unchanged.committed());
        assert!(
            DmlOutcome {
                snapshot_id: Some(8),
                ..unchanged
            }
            .committed()
        );
    }

    /// A memory-catalog lakehouse holding `sales.orders(id bigint not null, label text, qty
    /// bigint)`, seeded through the real `INSERT` path, plus a session that can read it.
    ///
    /// A memory catalog needs no database, which is what lets the rewrite machinery below be
    /// tested by `cargo test` on a laptop. Only the final pointer swap needs Postgres.
    async fn seeded_memory_lakehouse(dir: &std::path::Path) -> Result<(SessionContext, Lakehouse)> {
        use datafusion::arrow::datatypes::{DataType, Field};
        use iceberg::NamespaceIdent;

        let lake = Lakehouse::open_memory("lldb", dir).await?;
        let ns = NamespaceIdent::new("sales".to_string());
        lake.ensure_namespace(&ns).await?;
        lake.create_table_from_arrow(
            &ns,
            "orders",
            &ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("qty", DataType::Int64, true),
            ]),
        )
        .await?;
        let ctx = SessionContext::new();
        lake.register_with(&ctx).await?;
        ctx.sql(
            "INSERT INTO lldb.sales.orders VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, NULL, 30), (4, 'd', NULL)",
        )
        .await?
        .collect()
        .await?;
        Ok((ctx, lake))
    }

    /// Read a staged (but uncommitted) metadata file back as a queryable table, so a test can
    /// assert on the *rows* a commit would publish rather than on the metadata that describes
    /// them.
    async fn staged_rows(
        table: &Table,
        staged: &Staged,
    ) -> Result<Vec<(i64, Option<String>, Option<i64>)>> {
        let static_table = iceberg::table::StaticTable::from_metadata_file(
            &staged.new_location.to_string(),
            table.identifier().clone(),
            table.file_io().clone(),
        )
        .await?;
        let provider =
            IcebergStaticTableProvider::try_new_from_table(static_table.into_table()).await?;
        let ctx = SessionContext::new();
        ctx.register_table("staged", Arc::new(provider))?;
        let batches = ctx
            .sql("SELECT id, label, qty FROM staged ORDER BY id")
            .await?
            .collect()
            .await?;

        use datafusion::arrow::array::{Array, StringArray};
        let mut out = Vec::new();
        for b in batches {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let labels = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let qtys = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    ids.value(i),
                    labels.is_valid(i).then(|| labels.value(i).to_string()),
                    qtys.is_valid(i).then(|| qtys.value(i)),
                ));
            }
        }
        Ok(out)
    }

    #[tokio::test]
    async fn a_staged_delete_publishes_the_surviving_rows() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (_ctx, lake) = seeded_memory_lakehouse(dir.path()).await?;
        let table = lake.load_table("sales", "orders").await?;
        let base_snapshot = table.metadata().current_snapshot_id();

        let staged = stage(&table, &parsed("DELETE FROM sales.orders WHERE id = 2")).await?;

        // The rows first: a delete is only correct if reading the new metadata returns the
        // complement of the predicate, NULL-predicate rows included.
        assert_eq!(
            staged_rows(&table, &staged).await?,
            vec![
                (1, Some("a".into()), Some(10)),
                (3, None, Some(30)),
                (4, Some("d".into()), None),
            ]
        );

        // …then the snapshot bookkeeping a reader depends on: a new id, the old one as parent,
        // and totals describing the state that now exists rather than the one that did.
        let metadata =
            TableMetadata::read_from(table.file_io(), staged.new_location.to_string()).await?;
        let snapshot = metadata.current_snapshot().expect("a staged snapshot");
        assert_eq!(snapshot.snapshot_id(), staged.snapshot_id);
        assert_eq!(snapshot.parent_snapshot_id(), base_snapshot);
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(
            snapshot.summary().additional_properties["total-records"],
            "3"
        );
        assert_eq!(
            snapshot.summary().additional_properties["deleted-records"],
            "4",
            "every previously live file is removed, not just the matching rows"
        );
        // Sequence numbers must advance, or a later reader cannot order the snapshots.
        assert!(snapshot.sequence_number() > table.metadata().last_sequence_number());

        // Staging is not committing: the catalog still points at the old metadata, so nothing a
        // reader can reach has changed yet.
        assert_eq!(
            lake.current_snapshot_id("sales", "orders").await?,
            base_snapshot
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_staged_update_applies_only_to_matched_rows() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (_ctx, lake) = seeded_memory_lakehouse(dir.path()).await?;
        let table = lake.load_table("sales", "orders").await?;

        let staged = stage(
            &table,
            &parsed("UPDATE sales.orders SET label = 'x', qty = qty + 5 WHERE id >= 3"),
        )
        .await?;

        assert_eq!(
            staged_rows(&table, &staged).await?,
            vec![
                // Unmatched rows are untouched…
                (1, Some("a".into()), Some(10)),
                (2, Some("b".into()), Some(20)),
                // …matched ones take both assignments…
                (3, Some("x".into()), Some(35)),
                // …and NULL + 5 stays NULL rather than becoming 5.
                (4, Some("x".into()), None),
            ]
        );
        let metadata =
            TableMetadata::read_from(table.file_io(), staged.new_location.to_string()).await?;
        let snapshot = metadata.current_snapshot().expect("a staged snapshot");
        assert_eq!(
            snapshot.summary().additional_properties["total-records"],
            "4",
            "an UPDATE changes rows, it does not add or drop them"
        );
        Ok(())
    }

    #[tokio::test]
    async fn deleting_everything_stages_an_empty_table_not_a_broken_one() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (_ctx, lake) = seeded_memory_lakehouse(dir.path()).await?;
        let table = lake.load_table("sales", "orders").await?;

        let staged = stage(&table, &parsed("DELETE FROM sales.orders")).await?;
        assert!(staged_rows(&table, &staged).await?.is_empty());

        let metadata =
            TableMetadata::read_from(table.file_io(), staged.new_location.to_string()).await?;
        let snapshot = metadata.current_snapshot().expect("a staged snapshot");
        // Nothing was added, so this is a `delete`, not an `overwrite` — the distinction a reader
        // of the `snapshots` metadata table relies on.
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(
            snapshot.summary().additional_properties["total-records"],
            "0"
        );
        assert_eq!(
            snapshot.summary().additional_properties["total-data-files"],
            "0"
        );
        // The schema survives an empty snapshot, which is what makes the table appendable again.
        assert_eq!(metadata.current_schema(), table.metadata().current_schema());
        Ok(())
    }

    #[tokio::test]
    async fn memory_catalog_dml_says_why_it_cannot_commit() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (_ctx, lake) = seeded_memory_lakehouse(dir.path()).await?;

        // The refusal has to arrive before any file is written: a half-applied DELETE against a
        // catalog that cannot arbitrate is exactly the failure this whole module exists to avoid.
        let err = execute(&lake, "DELETE FROM sales.orders WHERE id = 1")
            .await
            .expect_err("a memory catalog has no commit point")
            .to_string();
        assert!(err.contains("sql"), "{err}");
        assert!(err.contains("compare-and-swap"), "{err}");
        Ok(())
    }
}
