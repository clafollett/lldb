//! Reuse a query's **result** across queries, when nothing it read has changed.
//!
//! # The problem this fixes
//!
//! [`crate::stage_cache`] is an *intra*-query cache: it stops one shuffle producer from running
//! once per consumer inside a single query. It is per-worker, per-process, keyed on plan bytes,
//! and it forgets everything the moment a query ends. So a dashboard that re-runs the same SQL
//! every thirty seconds over a table nobody has written to re-scans the parquet, re-shuffles, and
//! re-aggregates every single time — the fleet burns full compute to recompute a byte-identical
//! answer.
//!
//! This module is that cache's cross-query sibling. A result the engine has already computed is
//! stored in the services database, and a later run of the *same query over the same data* is
//! answered from it — without building a physical plan, without touching a worker, without
//! reading a byte of object storage.
//!
//! # The cache key is the whole design
//!
//! Everything here is subordinate to one property: **a hit must be impossible when any input
//! changed.** A cache that is merely fast is worthless; a cache that returns a stale row is worse
//! than no cache, because the wrongness is silent. So the key is built from every fact that can
//! change the answer:
//!
//! | Component | Why it is in the key |
//! | - | - |
//! | account id | A shared cache keyed only on SQL text is a cross-tenant data leak, not an optimization. |
//! | engine [`BUILD_VERSION`] | A fleet upgrade can change a result (a fixed aggregate, a new cast rule). A row computed by a different build must not be served by this one. |
//! | default catalog + schema | `SELECT * FROM orders` means different things in different sessions; the unqualified name only resolves against these. |
//! | statement fingerprint | What was asked. |
//! | every referenced table, with its Iceberg snapshot id | What it was asked *of*. |
//!
//! [`BUILD_VERSION`]: crate::BUILD_VERSION
//!
//! ## Invalidation is automatic, and it is not a mechanism
//!
//! There is no cache-busting path, no invalidation hook, nothing to remember to call after a
//! write. An Iceberg commit moves the table's current snapshot id; the next run of the same SQL
//! therefore composes a *different* key; that key has never been stored, so it misses and
//! recomputes. The stale row is not deleted, it is simply unreachable — which is stronger than
//! deleting it, because there is no window in which a writer has committed and a deleter has not
//! yet run.
//!
//! ## Normalization: re-render, never rewrite
//!
//! The query contributes **two** renderings to the key, and both must match for a hit:
//!
//! 1. **The statement, re-rendered by the parser** (`Display` on the AST). Parsing throws away
//!    whitespace and keyword case, so `select   1` and `SELECT 1` render identically — that is
//!    the entirety of the normalization, and it is the only kind that is safe. String literals,
//!    numeric literals and identifier casing come back verbatim, which is precisely what a naive
//!    normalizer destroys: lowercasing the SQL text would merge `WHERE n = 'Alice'` with
//!    `WHERE n = 'alice'`, and collapsing whitespace would merge `'a  b'` with `'a b'`. Both are
//!    false hits — wrong answers.
//! 2. **The logical plan, rendered with its schema.** This is the semantics rather than the
//!    spelling: names are fully resolved, columns are typed, and views are expanded. A rendering
//!    of the plan is the closest thing to "what will actually be computed" available before
//!    anything is computed.
//!
//! Note what is *not* used: `{:?}` of the AST. sqlparser's nodes carry source spans and raw token
//! text, so the debug rendering of `select a` and `SELECT   a` differ — it is a transcript, not a
//! fingerprint. The `Display` re-render is what erases position and keyword case.
//!
//! Each rendering is individually lossy in principle — `Display` trusts sqlparser to round-trip,
//! and a plan's display is a summary of the plan. Requiring both to match makes a false hit need
//! a simultaneous collision in two independent renderings of the same query, which is the reason
//! both are in the key rather than whichever one is cheaper.
//!
//! The deliberate cost is missed hits: `FROM Orders` and `FROM orders` are different keys (the
//! statement rendering keeps the case even though the plan resolves both to one table), as are
//! `a AND b` and `b AND a`. That is the right trade. A missed hit costs time; a false hit costs
//! correctness.
//!
//! # What this does NOT cache, and why
//!
//! Not caching is always a legal answer, and every one of these falls through to ordinary
//! execution silently:
//!
//! 1. **Anything that is not a single `SELECT`.** DDL, DML, `EXPLAIN`, `SET`, `COPY`,
//!    `CREATE EXTERNAL TABLE`. They have side effects or describe the engine rather than the data.
//! 2. **Any query naming something we cannot snapshot.** A plain-parquet listing table, a table
//!    function, `information_schema`, an in-memory table — none of them has a snapshot id, so
//!    nothing could tell us their contents had changed. Missing one referenced input is the worst
//!    bug this cache can have, so an input we cannot version means the query is not cached at all.
//! 3. **Queries referencing no snapshottable table** (`SELECT 1`). There is nothing to invalidate
//!    on, so there is no safe key. They are also the queries least worth caching.
//! 4. **Queries mentioning a volatile or session-dependent function** — `now()`, `random()`,
//!    `current_user`, … Their answer changes with no input change at all. The check is a textual
//!    deny-list over the re-rendered statement, and is deliberately over-broad: a column named
//!    `version` will suppress caching. Over-broad costs a miss; under-broad costs correctness.
//! 5. **Results bigger than [`ResultCacheConfig::max_payload_bytes`]**, and keys longer than
//!    [`ResultCacheConfig::max_key_bytes`]. A result cache exists to make small answers instant,
//!    not to become a second copy of the warehouse.
//! 6. **Everything, when no services database is configured.** That is not a degraded mode, it is
//!    the documented single-node mode (`CLAUDE.md`): `cargo run` must never need Postgres.
//!
//! A services-database *failure* is also not a query failure. A lookup or a store that errors is
//! logged and ignored — the query executes normally. The cache can only ever make the engine
//! faster or slower, never wrong and never broken.
//!
//! # Which tables a query references — asked twice, on purpose
//!
//! Getting this set wrong in the "too small" direction is the stale-answer bug, so it is derived
//! two independent ways and **unioned**:
//!
//! - [`SessionState::resolve_table_references`] over the parsed statement — DataFusion's own
//!   resolver, which already knows to exclude CTE names while keeping the tables a CTE reads.
//! - A walk of the logical plan's `TableScan` nodes via `apply_with_subqueries`, which sees
//!   through views (whose bodies never appear in the SQL text) and into scalar/`IN`/`EXISTS`
//!   subqueries (whose plans hang off *expressions*, and which a plain `apply` would walk right
//!   past — that omission alone would be enough to serve a stale answer).
//!
//! The union can only be too large, never too small, and too large just means a missed hit.
//!
//! # Why the lookup needs the logical plan first
//!
//! "Return a hit without planning" is not achievable safely. The set of referenced tables is a
//! property of *name resolution*, not of the SQL string, and string-matching table names is
//! exactly how a cache ends up missing the table hiding in a view or a CTE. So a lookup does pay
//! for parsing and logical planning — both coordinator-local, both cheap, neither touching a
//! worker or object storage. What a hit skips is everything expensive: physical planning, the
//! staging rewrite, fleet dispatch, and the scan itself.
//!
//! # Eviction: TTL plus a per-tenant LRU bound
//!
//! Exactly like [`StageCache`](crate::stage_cache::StageCache), correctness never depends on
//! retention — an evicted entry is recomputed, and recomputing is always right.
//!
//! - **TTL.** Every row carries `expires_at`; reads require `expires_at > now()`. The TTL is a
//!   storage bound, *not* a staleness bound: staleness is already impossible by snapshot. It
//!   exists so a key that will never be asked for again (a one-off ad-hoc query) does not occupy
//!   the table forever.
//! - **Per-account entry cap.** After a store, the tenant's entries beyond
//!   [`ResultCacheConfig::max_entries_per_account`] are deleted least-recently-used first
//!   (`last_hit_at`), and expired rows anywhere are swept. Both run in the same transaction as
//!   the insert's aftermath, so the table is bounded by writes rather than by a background job
//!   nobody remembers to deploy.
//!
//! Two knobs, both on [`ResultCacheConfig`], both visible to a reviewer weighing disk against hit
//! rate.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

use crate::BUILD_VERSION;
use crate::lakehouse::Lakehouse;
use crate::services::ServicesDb;

/// Key-format version. Bumped if the *layout* of the key material ever changes, so entries
/// written by an older layout can never be matched by a newer one even if the bytes coincide.
const KEY_FORMAT: &str = "lldb-result-cache/1";

/// Functions whose answer is not a function of the inputs. A statement whose re-rendered text
/// contains any of these is never cached.
///
/// Matched as plain uppercase substrings, which is deliberately blunt — `NOW(` is written with
/// its paren so it cannot fire on `'KNOWN'`, but `VERSION` will happily suppress caching for a
/// column of that name. A suppressed cache is a slow query; a cached `now()` is a wrong one.
const VOLATILE_MARKERS: &[&str] = &[
    "RANDOM",
    "NOW(",
    "CURRENT_TIMESTAMP",
    "CURRENT_TIME",
    "CURRENT_DATE",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UUID",
    "CURRENT_USER",
    "SESSION_USER",
    "CURRENT_SCHEMA",
    "CURRENT_CATALOG",
    "CURRENT_ROLE",
    "VERSION",
    "NEXTVAL",
];

/// The index expression the lookup and the upsert both name. Kept in one place because a
/// mismatch between them would silently stop `ON CONFLICT` from inferring the unique index.
const KEY_INDEX_EXPR: &str = "(account_id, md5(key_material))";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The cache's two bounds plus its two refusal thresholds. See the module docs on eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultCacheConfig {
    /// How long a stored entry stays *reachable*. Not a staleness bound — snapshots already make
    /// staleness impossible — just a bound on how long an unwanted key occupies the table.
    pub ttl: Duration,
    /// Largest Arrow IPC payload that will be stored. A bigger result is not cached at all;
    /// spilling it to object storage is a follow-up, and a half-done one would be worse than none.
    pub max_payload_bytes: usize,
    /// How many entries one tenant may hold before least-recently-used ones are evicted.
    pub max_entries_per_account: usize,
    /// Longest key material that will be stored. A pathological query (thousands of table
    /// references) is not worth a row.
    pub max_key_bytes: usize,
}

impl ResultCacheConfig {
    /// A day of retention, a mebibyte per result, 256 results per tenant, 64 KiB of key.
    ///
    /// The payload cap is the load-bearing default: a result cache is for the small answers that
    /// dashboards ask for over and over, and one mebibyte of Arrow IPC is already tens of
    /// thousands of rows. Anything larger is cheaper to recompute across a fleet than to push
    /// through a single Postgres connection twice.
    pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
    /// Default [`max_payload_bytes`](Self::max_payload_bytes).
    pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
    /// Default [`max_entries_per_account`](Self::max_entries_per_account).
    pub const DEFAULT_MAX_ENTRIES_PER_ACCOUNT: usize = 256;
    /// Default [`max_key_bytes`](Self::max_key_bytes).
    pub const DEFAULT_MAX_KEY_BYTES: usize = 64 * 1024;
}

impl Default for ResultCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Self::DEFAULT_TTL,
            max_payload_bytes: Self::DEFAULT_MAX_PAYLOAD_BYTES,
            max_entries_per_account: Self::DEFAULT_MAX_ENTRIES_PER_ACCOUNT,
            max_key_bytes: Self::DEFAULT_MAX_KEY_BYTES,
        }
    }
}

/// CLI/env surface for the result cache, flattened into a binary's args.
///
/// Off is expressible (`--no-result-cache`) because an operator debugging a wrong answer wants a
/// way to take the cache out of the picture in one flag, without also unconfiguring the services
/// database everything else needs.
#[derive(Debug, Clone, Args)]
pub struct ResultCacheArgs {
    /// Disable the cross-query result cache entirely. It is also inert with no services database.
    #[arg(long, env = "LLDB_NO_RESULT_CACHE")]
    pub no_result_cache: bool,

    /// How long a cached result stays reachable, in seconds.
    #[arg(long, env = "LLDB_RESULT_CACHE_TTL_SECS", default_value_t = 86_400)]
    pub result_cache_ttl_secs: u64,

    /// Largest result (Arrow IPC bytes) that will be cached. Bigger results run normally.
    #[arg(long, env = "LLDB_RESULT_CACHE_MAX_BYTES", default_value_t = 1_048_576)]
    pub result_cache_max_bytes: usize,

    /// How many cached results one tenant may hold before LRU eviction.
    #[arg(long, env = "LLDB_RESULT_CACHE_MAX_ENTRIES", default_value_t = 256)]
    pub result_cache_max_entries: usize,
}

impl ResultCacheArgs {
    /// The config these args describe.
    pub fn to_config(&self) -> ResultCacheConfig {
        ResultCacheConfig {
            ttl: Duration::from_secs(self.result_cache_ttl_secs),
            max_payload_bytes: self.result_cache_max_bytes,
            max_entries_per_account: self.result_cache_max_entries,
            max_key_bytes: ResultCacheConfig::DEFAULT_MAX_KEY_BYTES,
        }
    }

    /// Build the cache these args ask for, or `None` when it is switched off.
    pub fn build(&self, db: ServicesDb) -> Option<ResultCache> {
        if self.no_result_cache {
            return None;
        }
        Some(ResultCache::new(db, self.to_config()))
    }
}

// ---------------------------------------------------------------------------
// The key
// ---------------------------------------------------------------------------

/// One input table, at the version the query would read it at.
///
/// `snapshot: None` is a real, distinguishable state — a table that exists and has never been
/// written. It is *not* "unknown": a table whose version we cannot determine never becomes a
/// [`TableInput`] at all, it makes the whole query uncacheable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableInput {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub snapshot: Option<i64>,
}

impl std::fmt::Display for TableInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}@", self.catalog, self.namespace, self.table)?;
        match self.snapshot {
            Some(id) => write!(f, "{id}"),
            // A table with no snapshot is empty, which is a version like any other. Spelled as a
            // word so it can never be confused with a snapshot id.
            None => write!(f, "none"),
        }
    }
}

/// The full, verbatim cache key.
///
/// Deliberately not a hash. The digest that makes lookups fast lives in a Postgres index
/// expression; what is *compared* is this string, so no collision can hand one query another
/// query's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultCacheKey {
    account_id: i64,
    material: String,
    normalized_sql: String,
    inputs: String,
}

/// How many expired rows one store may sweep.
///
/// A bound, not a target: stores must stay cheap and predictable, and anything left over is
/// collected by the next store. Unreachable rows are not urgent.
const PRUNE_BATCH: usize = 256;

impl ResultCacheKey {
    /// Compose the key from its parts. Inputs are sorted and de-duplicated so that two runs which
    /// discover the same tables in a different order agree.
    ///
    /// Every field is length-prefixed, and that is load-bearing rather than defensive. The
    /// renderings this key is built from are **not** newline-free: `plan.display_indent_schema()`
    /// is deliberately multi-line, and a `Display`-rendered statement can carry newlines inside a
    /// string literal. A separator-based encoding would therefore let one field's content imitate
    /// a field boundary — two different queries composing to one key, which is the single failure
    /// this cache must never have. Lengths make that impossible without depending on any
    /// formatting property of someone else's crate.
    pub fn new(
        account_id: i64,
        build_version: &str,
        default_catalog: &str,
        default_schema: &str,
        normalized: &NormalizedSql,
        inputs: &[TableInput],
    ) -> Self {
        let mut sorted: Vec<&TableInput> = inputs.iter().collect();
        sorted.sort();
        sorted.dedup();

        let mut material = String::new();
        push_field(&mut material, "format", KEY_FORMAT);
        push_field(&mut material, "account", &account_id.to_string());
        push_field(&mut material, "build", build_version);
        push_field(&mut material, "default_catalog", default_catalog);
        push_field(&mut material, "default_schema", default_schema);
        push_field(&mut material, "statement", &normalized.statement);
        push_field(&mut material, "plan", &normalized.plan);
        for input in &sorted {
            push_field(&mut material, "input", &input.to_string());
        }

        let rendered = sorted
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        Self {
            account_id,
            material,
            normalized_sql: normalized.statement.clone(),
            inputs: rendered,
        }
    }

    /// The tenant this key belongs to. Also inside [`material`](Self::material) — carried
    /// separately because every statement filters on the column as well, so a bug in key
    /// composition still cannot cross a tenant boundary.
    pub fn account_id(&self) -> i64 {
        self.account_id
    }

    /// The exact bytes a lookup compares.
    pub fn material(&self) -> &str {
        &self.material
    }

    /// The statement as the parser re-renders it — stored for operators, never compared.
    pub fn normalized_sql(&self) -> &str {
        &self.normalized_sql
    }

    /// `catalog.namespace.table@snapshot` per line — stored for operators, never compared.
    pub fn inputs(&self) -> &str {
        &self.inputs
    }
}

/// Append `name=<len>:<value>\n`. The length prefix is what makes the concatenation unambiguous.
fn push_field(out: &mut String, name: &str, value: &str) {
    out.push_str(name);
    out.push('=');
    out.push_str(&value.len().to_string());
    out.push(':');
    out.push_str(value);
    out.push('\n');
}

/// A query rendered the two ways the key uses. See the module docs for why both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSql {
    /// The statement re-rendered by the parser: whitespace- and keyword-case-insensitive,
    /// literal-exact.
    pub statement: String,
    /// The logical plan rendered with its schema: names resolved, columns typed, views expanded.
    pub plan: String,
}

/// Render a parsed statement and its plan for the key.
pub fn normalize(statement: &DFStatement, plan: &LogicalPlan) -> NormalizedSql {
    NormalizedSql {
        statement: statement.to_string(),
        plan: plan.display_indent_schema().to_string(),
    }
}

/// Whether this statement is the sort of thing that may be cached at all: exactly one plain
/// `SELECT`, with no volatile or session-dependent function anywhere in it.
///
/// `Statement::Query` covers `SELECT`, `VALUES`, `WITH …` and set operations. Every DataFusion
/// extension statement (`CREATE EXTERNAL TABLE`, `COPY TO`, `EXPLAIN`, `RESET`) and every other
/// sqlparser statement — including anything with a side effect — is rejected here.
///
/// `rendered` is the statement as the parser re-renders it; the volatility deny-list is applied to
/// that rather than to the user's text so that comments and odd spacing cannot hide a `now()`.
pub fn is_cacheable_statement(statement: &DFStatement, rendered: &str) -> bool {
    let DFStatement::Statement(inner) = statement else {
        return false;
    };
    if !matches!(**inner, SqlStatement::Query(_)) {
        return false;
    }
    let upper = rendered.to_uppercase();
    !VOLATILE_MARKERS.iter().any(|m| upper.contains(m))
}

// ---------------------------------------------------------------------------
// Referenced tables
// ---------------------------------------------------------------------------

/// Every table reference the plan could possibly read, resolved against the session defaults.
///
/// The union of what the parser named and what the planner scans — see the module docs for why
/// one source is not enough.
fn referenced_tables(
    named: &[TableReference],
    plan: &LogicalPlan,
    default_catalog: &str,
    default_schema: &str,
) -> Result<BTreeSet<ResolvedTableReference>> {
    let mut refs: BTreeSet<ResolvedTableReference> = named
        .iter()
        .map(|r| r.clone().resolve(default_catalog, default_schema))
        .collect();

    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            refs.insert(
                scan.table_name
                    .clone()
                    .resolve(default_catalog, default_schema),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .context("walking the logical plan for its table scans")?;

    Ok(refs)
}

/// Turn resolved table names into versioned [`TableInput`]s, or `None` if *any* of them cannot be
/// versioned.
///
/// The all-or-nothing return is the point. A partially-versioned input set would produce a key
/// that looks specific and is not, and the query it keyed would go stale the moment the
/// unversioned table was written.
async fn version_inputs(
    lakehouses: &[Lakehouse],
    refs: &BTreeSet<ResolvedTableReference>,
) -> Option<Vec<TableInput>> {
    if refs.is_empty() {
        // Nothing to invalidate on. See "What this does NOT cache".
        return None;
    }
    let mut inputs = Vec::with_capacity(refs.len());
    for r in refs {
        let lake = lakehouses
            .iter()
            .find(|l| l.catalog_name() == &*r.catalog)?;
        // A load failure — the table is not in this catalog, the metadata is unreadable — means
        // we do not know this input's version, which means we do not cache.
        let snapshot = match lake.current_snapshot_id(&r.schema, &r.table).await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                tracing::debug!(table = %r, error = %e, "result cache: input cannot be versioned");
                return None;
            }
        };
        inputs.push(TableInput {
            catalog: r.catalog.to_string(),
            namespace: r.schema.to_string(),
            table: r.table.to_string(),
            snapshot,
        });
    }
    Some(inputs)
}

// ---------------------------------------------------------------------------
// The cache
// ---------------------------------------------------------------------------

/// The cross-query result cache: metadata and small results in the services database.
///
/// Held for a process's lifetime and shared, like [`StageCache`](crate::stage_cache::StageCache)
/// — except that this one's entries survive the process, because the whole point is that a
/// different coordinator, tomorrow, can answer a dashboard's query without touching the fleet.
///
/// The counters exist for the same reason `StageCache`'s do: they are what a test asserts on to
/// prove a hit really skipped execution rather than merely being fast.
#[derive(Debug)]
pub struct ResultCache {
    db: ServicesDb,
    config: ResultCacheConfig,
    hits: AtomicUsize,
    misses: AtomicUsize,
    stores: AtomicUsize,
    skips: AtomicUsize,
    executions: AtomicUsize,
}

impl ResultCache {
    /// A cache over `db` with the given bounds.
    pub fn new(db: ServicesDb, config: ResultCacheConfig) -> Self {
        Self {
            db,
            config,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            stores: AtomicUsize::new(0),
            skips: AtomicUsize::new(0),
            executions: AtomicUsize::new(0),
        }
    }

    /// The bounds this cache runs under.
    pub fn config(&self) -> &ResultCacheConfig {
        &self.config
    }

    /// Lookups answered from the cache.
    pub fn hit_count(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    /// Cacheable lookups that found nothing.
    pub fn miss_count(&self) -> usize {
        self.misses.load(Ordering::SeqCst)
    }

    /// Results actually written to the database (a store refused for size does not count).
    pub fn store_count(&self) -> usize {
        self.stores.load(Ordering::SeqCst)
    }

    /// Queries the cache declined to key at all — see "What this does NOT cache".
    pub fn skip_count(&self) -> usize {
        self.skips.load(Ordering::SeqCst)
    }

    /// How many times the engine actually ran a query through this cache.
    ///
    /// This is what the "a repeat query touches no worker" test asserts: two identical runs over
    /// unchanged tables leave it at `1`. It counts *successful* executions, mirroring
    /// [`StageCache::execution_count`](crate::stage_cache::StageCache::execution_count) — a query
    /// that failed produced nothing to cache and is not an execution anyone can observe.
    pub fn execution_count(&self) -> usize {
        self.executions.load(Ordering::SeqCst)
    }

    /// Look `key` up. `Ok(None)` for a miss; a database error is *also* reported as a miss, after
    /// a warning — the cache must never be able to fail a query.
    ///
    /// The read is an `UPDATE … RETURNING`: it bumps `last_hit_at` (which is what makes the LRU
    /// bound meaningful) and returns the payload in one round trip.
    pub async fn lookup(&self, key: &ResultCacheKey) -> Option<Vec<RecordBatch>> {
        match self.try_lookup(key).await {
            Ok(Some(batches)) => {
                self.hits.fetch_add(1, Ordering::SeqCst);
                Some(batches)
            }
            Ok(None) => {
                self.misses.fetch_add(1, Ordering::SeqCst);
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, "result cache lookup failed; executing the query");
                self.misses.fetch_add(1, Ordering::SeqCst);
                None
            }
        }
    }

    async fn try_lookup(&self, key: &ResultCacheKey) -> Result<Option<Vec<RecordBatch>>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "UPDATE result_cache \
                SET last_hit_at = now(), hit_count = hit_count + 1 \
              WHERE account_id = $1 \
                AND md5(key_material) = md5($2) \
                AND key_material = $2 \
                AND expires_at > now() \
          RETURNING payload",
        )
        .bind(key.account_id)
        .bind(&key.material)
        .fetch_optional(self.db.pool())
        .await
        .context("reading the result cache")?;

        let Some((payload,)) = row else {
            return Ok(None);
        };
        match decode_batches(&payload) {
            Ok(batches) => Ok(Some(batches)),
            Err(e) => {
                // A payload we cannot decode is not a query failure. Drop the row so the next run
                // re-stores a good one, and report a miss.
                tracing::warn!(error = %e, "result cache payload is undecodable; discarding it");
                let _ = sqlx::query(
                    "DELETE FROM result_cache WHERE account_id = $1 AND key_material = $2",
                )
                .bind(key.account_id)
                .bind(&key.material)
                .execute(self.db.pool())
                .await;
                Ok(None)
            }
        }
    }

    /// Store a result. Returns whether it was actually written — `false` for a result or key over
    /// the configured caps, which is a normal outcome, not an error.
    pub async fn store(
        &self,
        key: &ResultCacheKey,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> bool {
        match self.try_store(key, schema, batches).await {
            Ok(stored) => {
                if stored {
                    self.stores.fetch_add(1, Ordering::SeqCst);
                }
                stored
            }
            Err(e) => {
                tracing::warn!(error = %e, "result cache store failed; the answer is still correct");
                false
            }
        }
    }

    async fn try_store(
        &self,
        key: &ResultCacheKey,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<bool> {
        if key.material.len() > self.config.max_key_bytes {
            tracing::debug!(
                key_bytes = key.material.len(),
                cap = self.config.max_key_bytes,
                "result cache: key too large to store"
            );
            return Ok(false);
        }
        let payload = encode_batches(schema, batches)?;
        if payload.len() > self.config.max_payload_bytes {
            tracing::debug!(
                payload_bytes = payload.len(),
                cap = self.config.max_payload_bytes,
                "result cache: result too large to store"
            );
            return Ok(false);
        }
        let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        sqlx::query(&format!(
            "INSERT INTO result_cache \
                 (account_id, key_material, build_version, normalized_sql, inputs, row_count, \
                  payload, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8)) \
             ON CONFLICT {KEY_INDEX_EXPR} DO UPDATE SET \
                 key_material   = EXCLUDED.key_material, \
                 build_version  = EXCLUDED.build_version, \
                 normalized_sql = EXCLUDED.normalized_sql, \
                 inputs         = EXCLUDED.inputs, \
                 row_count      = EXCLUDED.row_count, \
                 payload        = EXCLUDED.payload, \
                 created_at     = now(), \
                 expires_at     = EXCLUDED.expires_at, \
                 last_hit_at    = now(), \
                 hit_count      = 0"
        ))
        .bind(key.account_id)
        .bind(&key.material)
        .bind(BUILD_VERSION)
        .bind(&key.normalized_sql)
        .bind(&key.inputs)
        .bind(row_count)
        .bind(&payload)
        .bind(self.config.ttl.as_secs_f64())
        .execute(self.db.pool())
        .await
        .context("writing the result cache")?;

        self.prune(key.account_id).await?;
        Ok(true)
    }

    /// Apply both bounds: sweep expired rows anywhere, then evict this tenant's least-recently-used
    /// entries beyond the cap. Runs after a store, so the table is bounded by writes rather than by
    /// a background job.
    async fn prune(&self, account_id: i64) -> Result<()> {
        // Scoped to this account and bounded, because this runs on **every** successful store.
        // An unscoped `DELETE ... WHERE expires_at <= now()` makes each store proportional to the
        // whole table: as the cache warms, one tenant's write pays to sweep every other tenant's
        // garbage, and busy tenants contend on rows they will never read. Neither is necessary —
        // expiry is only a storage bound, so sweeping lazily and locally is entirely sufficient.
        //
        // The consequence, stated rather than hidden: a tenant that stops writing keeps its
        // expired rows. They are unreachable (every read requires `expires_at > now()`), and their
        // number is capped by the same per-account LRU bound below, so the cost is bounded storage
        // for a dormant tenant — not unbounded growth, and never a wrong answer.
        sqlx::query(
            "DELETE FROM result_cache WHERE id IN ( \
                 SELECT id FROM result_cache \
                  WHERE account_id = $1 AND expires_at <= now() LIMIT $2)",
        )
        .bind(account_id)
        .bind(PRUNE_BATCH as i64)
        .execute(self.db.pool())
        .await
        .context("sweeping expired result-cache rows")?;

        sqlx::query(
            "DELETE FROM result_cache WHERE id IN ( \
                 SELECT id FROM result_cache WHERE account_id = $1 \
                  ORDER BY last_hit_at DESC, id DESC OFFSET $2)",
        )
        .bind(account_id)
        .bind(self.config.max_entries_per_account as i64)
        .execute(self.db.pool())
        .await
        .context("evicting least-recently-used result-cache rows")?;
        Ok(())
    }

    /// Delete every entry for one tenant. Not part of invalidation — snapshots do that — this is
    /// for an operator who wants a clean slate, and for tests that clean up after themselves.
    pub async fn purge_account(&self, account_id: i64) -> Result<u64> {
        let done = sqlx::query("DELETE FROM result_cache WHERE account_id = $1")
            .bind(account_id)
            .execute(self.db.pool())
            .await
            .context("purging a tenant's result cache")?;
        Ok(done.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// Arrow IPC payloads
// ---------------------------------------------------------------------------

/// Encode a result as an Arrow IPC **stream**.
///
/// The schema is written even when there are no batches, so an empty result round-trips as an
/// empty result with the right columns rather than as nothing at all. Arrow IPC comes with the
/// `arrow` crate the engine already pins tree-wide, so caching costs no new dependency and no
/// second serialization format at the Flight boundary.
fn encode_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, schema).context("opening an Arrow IPC stream writer")?;
    for batch in batches {
        writer
            .write(batch)
            .context("writing a batch to the Arrow IPC stream")?;
    }
    writer.finish().context("finishing the Arrow IPC stream")?;
    drop(writer);
    Ok(buf)
}

/// Decode a payload written by [`encode_batches`].
fn decode_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .context("reading an Arrow IPC stream")?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("decoding a batch from the Arrow IPC stream")?);
    }
    Ok(batches)
}

// ---------------------------------------------------------------------------
// The entry point
// ---------------------------------------------------------------------------

/// Plan `sql`, answer it from the cache if the inputs are unchanged, otherwise run `execute` and
/// cache what it produced.
///
/// This is the coordinator's query entry point. `execute` receives the logical plan and owns
/// everything expensive — physical planning, the staging rewrite, fleet dispatch — so a cache hit
/// is visibly the absence of all of it.
///
/// Every "no" answer here falls through to `execute` unchanged: no cache configured, no account
/// resolved, an uncacheable statement, an input we cannot version, a database that will not
/// answer. The engine's behaviour without a services database is bit-for-bit what it was before
/// this module existed.
pub async fn execute_cached<F, Fut>(
    ctx: &SessionContext,
    cache: Option<&ResultCache>,
    lakehouses: &[Lakehouse],
    account_id: Option<i64>,
    sql: &str,
    execute: F,
) -> Result<Vec<RecordBatch>>
where
    F: FnOnce(LogicalPlan) -> Fut,
    Fut: Future<Output = Result<Vec<RecordBatch>>>,
{
    let state = ctx.state();
    let dialect = state.config_options().sql_parser.dialect;
    let statement = state
        .sql_to_statement(sql, &dialect)
        .context("parsing the query")?;
    // DataFusion's own reference resolver, run before planning so it sees the statement exactly as
    // written (CTE names excluded, the tables a CTE reads included).
    let named = state
        .resolve_table_references(&statement)
        .context("resolving the query's table references")?;
    let plan = state
        .statement_to_plan(statement.clone())
        .await
        .context("planning the query")?;

    let Some((cache, account_id)) = cache.zip(account_id) else {
        return execute(plan).await;
    };

    let Some(key) = cache_key_for(&state, lakehouses, account_id, &statement, &named, &plan).await
    else {
        cache.skips.fetch_add(1, Ordering::SeqCst);
        tracing::debug!("result cache: query is not cacheable; executing normally");
        return execute(plan).await;
    };

    if let Some(batches) = cache.lookup(&key).await {
        tracing::info!(
            inputs = %key.inputs(),
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "result cache hit; no plan built and no worker touched"
        );
        return Ok(batches);
    }

    // Captured before `execute` consumes the plan: it is the only schema available for a result
    // that comes back with no batches at all.
    let plan_schema: SchemaRef = std::sync::Arc::new(plan.schema().as_arrow().clone());
    let batches = execute(plan).await?;
    cache.executions.fetch_add(1, Ordering::SeqCst);
    // Prefer the *executed* schema. A logical schema and a physical one can disagree on details
    // like nullability, and Arrow IPC rejects a batch that does not match its stream's schema —
    // which would quietly reduce this cache to "never stores anything".
    let schema = batches.first().map(|b| b.schema()).unwrap_or(plan_schema);
    cache.store(&key, &schema, &batches).await;
    Ok(batches)
}

/// The key for this query, or `None` if it must not be cached.
async fn cache_key_for(
    state: &datafusion::execution::SessionState,
    lakehouses: &[Lakehouse],
    account_id: i64,
    statement: &DFStatement,
    named: &[TableReference],
    plan: &LogicalPlan,
) -> Option<ResultCacheKey> {
    let normalized = normalize(statement, plan);
    if !is_cacheable_statement(statement, &normalized.statement) {
        return None;
    }
    let options = state.config_options();
    let default_catalog = options.catalog.default_catalog.clone();
    let default_schema = options.catalog.default_schema.clone();

    let refs = referenced_tables(named, plan, &default_catalog, &default_schema).ok()?;
    let inputs = version_inputs(lakehouses, &refs).await?;

    Some(ResultCacheKey::new(
        account_id,
        BUILD_VERSION,
        &default_catalog,
        &default_schema,
        &normalized,
        &inputs,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::Dialect;

    /// A session with two small in-memory tables, so the key tests can build real logical plans
    /// with no database, no files and no catalog server.
    fn session() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("n", DataType::Utf8, true),
        ]));
        let t = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["Alice", "alice", "a b"])),
            ],
        )
        .expect("t");
        ctx.register_batch("t", t).expect("register t");

        let u_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let u = RecordBatch::try_new(u_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
            .expect("u");
        ctx.register_batch("u", u).expect("register u");
        ctx
    }

    fn parse(sql: &str) -> DFStatement {
        SessionContext::new()
            .state()
            .sql_to_statement(sql, &Dialect::Generic)
            .expect("parses")
    }

    /// Parse + plan `sql` against [`session`] and compose a key, exactly as the engine would —
    /// only the inputs are supplied by hand, because a unit test has no Iceberg catalog to
    /// version them against.
    async fn key(account: i64, sql: &str, inputs: &[TableInput]) -> ResultCacheKey {
        key_in(&session(), account, sql, "lldb", "sales", inputs).await
    }

    async fn key_in(
        ctx: &SessionContext,
        account: i64,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
        inputs: &[TableInput],
    ) -> ResultCacheKey {
        let state = ctx.state();
        let statement = state
            .sql_to_statement(sql, &Dialect::Generic)
            .expect("parses");
        let plan = state
            .statement_to_plan(statement.clone())
            .await
            .expect("plans");
        let normalized = normalize(&statement, &plan);
        ResultCacheKey::new(
            account,
            BUILD_VERSION,
            default_catalog,
            default_schema,
            &normalized,
            inputs,
        )
    }

    fn input(table: &str, snapshot: Option<i64>) -> TableInput {
        TableInput {
            catalog: "lldb".to_string(),
            namespace: "sales".to_string(),
            table: table.to_string(),
            snapshot,
        }
    }

    // ---- Normalization -------------------------------------------------------------------

    #[tokio::test]
    async fn whitespace_and_keyword_case_do_not_change_the_key() {
        let a = key(1, "SELECT a FROM t WHERE a > 1", &[input("t", Some(7))]).await;
        let b = key(1, "select   a\n  from t\twhere a>1", &[input("t", Some(7))]).await;
        assert_eq!(
            a.material(),
            b.material(),
            "formatting is not part of the question"
        );
    }

    #[tokio::test]
    async fn string_literals_are_never_folded_together() {
        // The false hit a "just lowercase it" normalizer would produce. These are different
        // questions and must be different keys.
        let a = key(
            1,
            "SELECT * FROM t WHERE n = 'Alice'",
            &[input("t", Some(1))],
        )
        .await;
        let b = key(
            1,
            "SELECT * FROM t WHERE n = 'alice'",
            &[input("t", Some(1))],
        )
        .await;
        assert_ne!(a.material(), b.material());

        // …and the whitespace *inside* a literal is part of the literal.
        let c = key(
            1,
            "SELECT * FROM t WHERE n = 'a  b'",
            &[input("t", Some(1))],
        )
        .await;
        let d = key(1, "SELECT * FROM t WHERE n = 'a b'", &[input("t", Some(1))]).await;
        assert_ne!(c.material(), d.material());
    }

    #[tokio::test]
    async fn different_queries_never_collide() {
        let inputs = [input("t", Some(3))];
        let mut seen = std::collections::HashSet::new();
        for sql in [
            "SELECT a FROM t",
            "SELECT b FROM t",
            "SELECT a, b FROM t",
            "SELECT a FROM t WHERE a > 1",
            "SELECT a FROM t WHERE a >= 1",
            "SELECT a FROM t WHERE b > 1",
            "SELECT a FROM t ORDER BY a",
            "SELECT a FROM t ORDER BY a DESC",
            "SELECT a FROM t LIMIT 10",
            "SELECT a FROM t LIMIT 11",
            "SELECT count(*) FROM t",
            "SELECT count(a) FROM t",
            "SELECT sum(a) FROM t",
            "SELECT a FROM t GROUP BY a",
            "SELECT * FROM t JOIN u ON t.a = u.a",
            "SELECT * FROM t LEFT JOIN u ON t.a = u.a",
        ] {
            assert!(
                seen.insert(key(1, sql, &inputs).await.material().to_string()),
                "two different queries produced one key, at `{sql}`"
            );
        }
    }

    #[tokio::test]
    async fn a_subquery_and_a_cte_are_not_the_same_query() {
        // Both may well compute the same answer; keying them together would still be a guess
        // about semantics, and this cache does not guess.
        let inputs = [input("t", Some(1)), input("u", Some(1))];
        let sub = key(1, "SELECT a FROM t WHERE a IN (SELECT a FROM u)", &inputs).await;
        let cte = key(
            1,
            "WITH c AS (SELECT a FROM u) SELECT a FROM t WHERE a IN (SELECT a FROM c)",
            &inputs,
        )
        .await;
        assert_ne!(sub.material(), cte.material());
    }

    // ---- Inputs --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_changed_snapshot_changes_the_key() {
        let sql = "SELECT count(*) FROM t";
        let before = key(1, sql, &[input("t", Some(100))]).await;
        let after = key(1, sql, &[input("t", Some(101))]).await;
        assert_ne!(
            before.material(),
            after.material(),
            "a write moved the snapshot; the old key must not match"
        );
        // "never written" is its own version, distinct from every snapshot id.
        let empty = key(1, sql, &[input("t", None)]).await;
        assert_ne!(empty.material(), before.material());
        assert_ne!(empty.material(), after.material());
    }

    #[tokio::test]
    async fn every_input_participates_and_order_does_not() {
        let sql = "SELECT * FROM t JOIN u ON t.a = u.a";
        let both = key(1, sql, &[input("t", Some(1)), input("u", Some(2))]).await;
        // The second table moving invalidates, even though the first did not.
        let moved = key(1, sql, &[input("t", Some(1)), input("u", Some(3))]).await;
        assert_ne!(both.material(), moved.material());
        // Discovery order is not part of the question.
        let reordered = key(1, sql, &[input("u", Some(2)), input("t", Some(1))]).await;
        assert_eq!(both.material(), reordered.material());
    }

    #[tokio::test]
    async fn same_table_name_in_a_different_namespace_is_a_different_input() {
        let sql = "SELECT count(*) FROM t";
        let a = key(1, sql, &[input("t", Some(1))]).await;
        let mut other = input("t", Some(1));
        other.namespace = "hr".to_string();
        let b = key(1, sql, &[other]).await;
        assert_ne!(a.material(), b.material());
    }

    #[tokio::test]
    async fn field_lengths_keep_concatenation_unambiguous() {
        // Without length prefixes, a value that happened to render the next field's name could
        // forge another key's material. Assert the framing directly.
        let k = key(1, "SELECT a FROM t", &[input("t", Some(1))]).await;
        assert!(k.material().contains("format=19:lldb-result-cache/1\n"));
        assert!(k.material().contains("account=1:1\n"));
        assert!(k.material().contains("input=14:lldb.sales.t@1\n"));
    }

    // ---- Tenancy, build and scope --------------------------------------------------------

    #[tokio::test]
    async fn different_accounts_get_different_keys() {
        let sql = "SELECT count(*) FROM t";
        let inputs = [input("t", Some(5))];
        assert_ne!(
            key(1, sql, &inputs).await.material(),
            key(2, sql, &inputs).await.material(),
            "a cache keyed only on SQL text is a cross-tenant leak"
        );
    }

    #[tokio::test]
    async fn the_engine_build_is_part_of_the_key() {
        let ctx = session();
        let state = ctx.state();
        let statement = state
            .sql_to_statement("SELECT count(*) FROM t", &Dialect::Generic)
            .expect("parses");
        let plan = state
            .statement_to_plan(statement.clone())
            .await
            .expect("plans");
        let n = normalize(&statement, &plan);
        let inputs = [input("t", Some(5))];
        let a = ResultCacheKey::new(1, "0.1.0+aaaaaaaa", "lldb", "sales", &n, &inputs);
        let b = ResultCacheKey::new(1, "0.1.0+bbbbbbbb", "lldb", "sales", &n, &inputs);
        assert_ne!(
            a.material(),
            b.material(),
            "a fleet upgrade must not serve results computed by the old build"
        );
    }

    #[tokio::test]
    async fn the_resolution_scope_is_part_of_the_key() {
        // `FROM t` means different tables under different defaults, so the defaults are in the key.
        let ctx = session();
        let inputs = [input("t", Some(5))];
        let a = key_in(&ctx, 1, "SELECT count(*) FROM t", "lldb", "sales", &inputs).await;
        let b = key_in(&ctx, 1, "SELECT count(*) FROM t", "lldb", "hr", &inputs).await;
        let c = key_in(&ctx, 1, "SELECT count(*) FROM t", "other", "sales", &inputs).await;
        assert_ne!(a.material(), b.material());
        assert_ne!(a.material(), c.material());
    }

    // ---- What is refused -----------------------------------------------------------------

    #[test]
    fn only_a_plain_select_is_cacheable() {
        for sql in [
            "SELECT a FROM t",
            "WITH c AS (SELECT a FROM t) SELECT * FROM c",
            "SELECT a FROM t UNION ALL SELECT a FROM u",
            "VALUES (1), (2)",
        ] {
            let s = parse(sql);
            assert!(
                is_cacheable_statement(&s, &s.to_string()),
                "should be cacheable: {sql}"
            );
        }
        // Side effects, engine introspection, and DataFusion's own statement extensions.
        for sql in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "EXPLAIN SELECT a FROM t",
            "DELETE FROM t",
            "UPDATE t SET a = 1",
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/tmp/x'",
            "SET datafusion.execution.target_partitions = 4",
        ] {
            let s = parse(sql);
            assert!(
                !is_cacheable_statement(&s, &s.to_string()),
                "must not be cacheable: {sql}"
            );
        }
    }

    #[test]
    fn volatile_functions_are_refused() {
        for sql in [
            "SELECT now()",
            "SELECT random() FROM t",
            "SELECT * FROM t WHERE d < current_date",
            "SELECT current_timestamp",
            "SELECT current_user",
            // A comment cannot hide it: the deny-list runs over the re-rendered statement.
            "SELECT /* harmless */ now() AS x",
        ] {
            let s = parse(sql);
            assert!(
                !is_cacheable_statement(&s, &s.to_string()),
                "a query whose answer changes on its own must not be cached: {sql}"
            );
        }
        // The deny-list is a substring match and therefore over-broad on purpose: this ordinary
        // query is refused because a column happens to be called `version`. A miss, not a bug.
        let s = parse("SELECT version FROM t");
        assert!(!is_cacheable_statement(&s, &s.to_string()));
        // …but a value that merely contains a marker's letters does not fire.
        let known = parse("SELECT a FROM t WHERE n = 'KNOWN'");
        assert!(is_cacheable_statement(&known, &known.to_string()));
    }

    #[tokio::test]
    async fn a_query_with_no_versionable_input_is_not_cacheable() {
        // No lakehouses at all: nothing can be versioned, so nothing may be cached.
        let refs: BTreeSet<ResolvedTableReference> =
            [TableReference::from("lldb.sales.t").resolve("lldb", "sales")]
                .into_iter()
                .collect();
        assert!(version_inputs(&[], &refs).await.is_none());
        // …and a query touching no table has nothing to invalidate on.
        assert!(version_inputs(&[], &BTreeSet::new()).await.is_none());
    }

    #[tokio::test]
    async fn referenced_tables_sees_through_subqueries_and_past_ctes() {
        // The load-bearing case: a table named only inside a scalar subquery hangs off an
        // *expression*, which a plain plan walk does not visit. Missing it would let a write to
        // `u` go unnoticed and a stale answer be served.
        let ctx = session();
        let state = ctx.state();
        let sql = "WITH c AS (SELECT a FROM u) \
                   SELECT a FROM t WHERE a > (SELECT max(a) FROM c)";
        let statement = state
            .sql_to_statement(sql, &Dialect::Generic)
            .expect("parses");
        let named = state
            .resolve_table_references(&statement)
            .expect("resolves");
        let plan = state.statement_to_plan(statement).await.expect("plans");

        let refs = referenced_tables(&named, &plan, "datafusion", "public").expect("walks");
        let names: BTreeSet<String> = refs.iter().map(|r| r.table.to_string()).collect();
        assert!(names.contains("t"), "{names:?}");
        assert!(names.contains("u"), "{names:?}");
        // `c` is a CTE, not a table — it must not appear as an input to be versioned.
        assert!(!names.contains("c"), "{names:?}");
    }

    // ---- Payloads ------------------------------------------------------------------------

    fn payload_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(payload_schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    #[test]
    fn arrow_ipc_round_trips_a_result() -> Result<()> {
        let batches = vec![batch(vec![1, 2, 3]), batch(vec![4])];
        let decoded = decode_batches(&encode_batches(&payload_schema(), &batches)?)?;
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        assert_eq!(decoded[0].schema(), payload_schema());
        Ok(())
    }

    #[test]
    fn an_empty_result_round_trips_with_its_schema() -> Result<()> {
        // A zero-row answer is a real answer, and the schema has to survive so the caller can
        // still format columns.
        let decoded = decode_batches(&encode_batches(&payload_schema(), &[])?)?;
        assert!(decoded.is_empty());
        Ok(())
    }

    #[test]
    fn a_corrupt_payload_is_an_error_not_a_panic() {
        assert!(decode_batches(b"not an arrow stream").is_err());
    }

    // ---- Config --------------------------------------------------------------------------

    #[test]
    fn args_map_onto_the_config() {
        let args = ResultCacheArgs {
            no_result_cache: false,
            result_cache_ttl_secs: 60,
            result_cache_max_bytes: 4096,
            result_cache_max_entries: 8,
        };
        let config = args.to_config();
        assert_eq!(config.ttl, Duration::from_secs(60));
        assert_eq!(config.max_payload_bytes, 4096);
        assert_eq!(config.max_entries_per_account, 8);

        // Defaults are the documented ones.
        let default = ResultCacheConfig::default();
        assert_eq!(default.ttl, ResultCacheConfig::DEFAULT_TTL);
        assert_eq!(
            default.max_payload_bytes,
            ResultCacheConfig::DEFAULT_MAX_PAYLOAD_BYTES
        );
    }
}
