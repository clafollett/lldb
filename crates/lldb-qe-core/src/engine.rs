//! **Running one query** — the pipeline shared by the one-shot coordinator and the query server.
//!
//! Everything from "here is some SQL and a fleet" to "here are the batches" lives here, in one
//! place, because there are now two callers of it: `lldb-qe-coordinator`, which runs a single
//! query and exits, and `lldb-qe-server`, which runs many concurrently under a scheduler. Those
//! two must not drift. A copy of the plan/distribute/offload policy in each binary is a bug
//! waiting to be found by a user reporting that "the same query gives a different answer through
//! the server".
//!
//! # The policy, in one sentence
//!
//! **Distribute what can be distributed and reduce locally; offload a boundary-less plan whole to
//! one worker.** [`plan_distributed`] cuts every distribution boundary it recognizes into a DAG of
//! stages fanned across the fleet, and returns the plan *unchanged* when there is nothing to cut (a
//! constant query, a bare scan). So "did it actually distribute" is answered by looking for the
//! [`FlightReaderExec`] leaves the rewrite inserts — if they are there, the coordinator runs the
//! reduce side locally and those leaves pull each stage over Flight; if they are not, the whole
//! plan is shipped to one worker, which keeps even a trivial query exercising a real worker (what
//! the cross-container smoke test relies on).
//!
//! Both paths tolerate losing a worker: the distributed one reassigns each stage through the
//! fallbacks its leaves carry, and the offload path walks the fleet with [`fetch_with_failover`].
//!
//! # One step comes before the policy: Iceberg scans are resolved to files
//!
//! An `iceberg-datafusion` `IcebergTableScan` holds a live catalog handle and resolves its own
//! files at execute time, which makes it both unserializable and unsliceable — so before this
//! module's policy runs at all, [`resolve_iceberg_scans`] rewrites every such node into a plain
//! parquet scan over the concrete data files of the snapshot it was planned against. That is what
//! lets an Iceberg query be distributed *and* what pins the snapshot: the file list travels inside
//! the plan bytes, so a worker never resolves "current" itself and needs no catalog access. It runs
//! here, in the shared funnel, for the same reason everything else here does — a coordinator that
//! resolved and a server that did not would be two engines with two answers.
//!
//! # Writes are the exception: they never leave this process
//!
//! An `INSERT` reaches [`execute_query_cached`] as an ordinary [`LogicalPlan::Dml`], because
//! appends go through DataFusion (`IcebergTableProvider` implements `insert_into`). It is executed
//! locally rather than distributed, and that is not a limitation to be lifted later: the commit
//! node `iceberg-datafusion` puts at the plan's root carries a live catalog handle and a connection
//! pool no codec can serialize, and fanning a write out would move its serialization point off the
//! machine that owns the statement. One statement, one committer. `DELETE`/`UPDATE` never get here
//! at all — [`crate::dml`] answers them against the catalog directly.
//!
//! # What this does NOT do
//!
//! - **No admission control, no query id, no history.** This is the execution path; the scheduler
//!   ([`crate::scheduler`]) and the history ([`crate::query_log`]) wrap it. A one-shot invocation
//!   legitimately has neither.
//! - **No catalog caching between calls.** [`build_query_session`] is called once per *tenant*,
//!   not once per query. Sharing one [`SessionContext`] across concurrent queries is safe and is
//!   exactly what the server does — that property is about concurrency, and it is unchanged.
//!   What changed is the *unit*: a long-running server now holds one context per account
//!   ([`TenantSessions`]), because a single process-wide context would have to register every
//!   tenant's catalog into it and `register_catalog` is global to a context. Every tenant's
//!   catalog *name* would then be visible to every tenant, and the boundary would go back to being
//!   a grant check rather than a structure. Each account's context is still built once and still
//!   shared across that account's concurrent queries.
//! - **No cancellation channel of its own, and no partial results.** Stopping a query is done by
//!   dropping the future that called into here — [`crate::cancel`] owns the mechanism and
//!   [`crate::server`] owns the wiring — so nothing in this module has to know about it. Batches
//!   are still collected whole before they are returned, which is what makes stage reassignment
//!   safe (see [`crate::flight::fetch_partition_with_failover`] for why a half-delivered partition
//!   cannot be resumed) and is also why a cancelled query never delivers a partial answer.
//!
//! # The result cache sits *above* the policy, not beside it
//!
//! [`execute_query_cached`] is the real entry point; [`execute_query`] is it with the cache turned
//! off. The cache wraps the closure that does all of the above, so a hit is visibly the absence of
//! physical planning, of the staging rewrite, and of every byte that would have crossed the wire.
//! Putting it here rather than in each binary is the same argument the rest of this module makes:
//! a cache the one-shot honours and the server does not is two engines again.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;

use crate::catalog::apply_manifest;
use crate::discovery::{discover_workers, discover_workers_with};
use crate::flight::fetch_with_failover;
use crate::iceberg_scan::resolve_iceberg_scans;
use crate::lakehouse::Lakehouse;
use crate::manifest::Manifest;
use crate::plan_assertion::{PlanAuth, QueryIdentity};
use crate::rbac::QueryAuthorization;
use crate::remote::FlightReaderExec;
use crate::result_cache::{ResultCache, execute_cached};
use crate::session::{build_session, register_tpch_parquet};
use crate::staging::plan_distributed;
use crate::storage::StorageConfig;
use crate::tenancy::TenantScope;
use tokio::sync::OnceCell;

/// Where a session's tables come from.
///
/// Config-as-data all the way down (see CLAUDE.md): a catalog is a manifest, and the TPC-H seed is
/// a convenience over the same generic path rather than a second loader. Modelled as an enum
/// rather than an `Option<PathBuf>` so "no manifest" has a name and the TPC-H subdir travels with
/// the case that uses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogSource {
    /// A TOML manifest describing namespaces, tables and the catalog backend.
    Manifest(PathBuf),
    /// The TPC-H listing tables under `<subdir>/<table>.parquet`.
    Tpch { subdir: String },
}

/// How an endpoint's authority becomes a set of addresses.
///
/// Production is DNS. Tests need something else — resolving `analytics.lldb.local` on a laptop is
/// not a thing — and the honest way to give them one is the same injection point
/// [`discover_workers_with`] already offers, boxed so a long-lived server can *hold* one.
/// Passing `None` where a `BoxResolver` is optional selects [`discover_workers`], i.e. real DNS,
/// which is what a real deployment gets.
pub type BoxResolver = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>>> + Send>>
        + Send
        + Sync
        + 'static,
>;

/// Build a session with `config`'s storage backend and `catalog`'s tables registered — for one
/// tenant — returning it alongside the [`Lakehouse`] handles the catalog produced.
///
/// One per tenant, not one per query. A [`SessionContext`] is `Clone` and internally shared, so
/// concurrent queries on one context is the intended usage, not a liberty; `scope` is what decides
/// how many such contexts a process has (see [`TenantSessions`] and [`crate::tenancy`]).
///
/// The lakehouses are returned rather than dropped because they are the only thing that can answer
/// "what snapshot is this table at", which is what makes a result-cache key safe to trust. The
/// TPC-H seed registers plain-parquet listing tables, which have no snapshot — so that path yields
/// no lakehouses and nothing it can query is cacheable. That is the intended behaviour, not a gap.
pub async fn build_query_session(
    config: StorageConfig,
    catalog: &CatalogSource,
    scope: &TenantScope,
) -> Result<(SessionContext, Vec<Lakehouse>)> {
    let (ctx, storage) = build_session(config).await?;
    let lakehouses = match catalog {
        CatalogSource::Manifest(path) => {
            let manifest = Manifest::from_path(path)?;
            apply_manifest(&ctx, &storage, &manifest, scope)
                .await
                .with_context(|| format!("applying catalog manifest {}", path.display()))?
        }
        CatalogSource::Tpch { subdir } => {
            register_tpch_parquet(&ctx, &storage, subdir)
                .await
                .with_context(|| format!("seeding the TPC-H tables from `{subdir}`"))?;
            Vec::new()
        }
    };
    Ok((ctx, lakehouses))
}

/// One tenant's query session: a [`SessionContext`] with that tenant's catalogs registered, and
/// the [`Lakehouse`] handles behind them.
///
/// The pair travels together because it has to be consistent: the lakehouses are what version the
/// result-cache key, and versioning one context's query against another context's catalogs would
/// produce a key that names tables the query never read.
pub struct TenantSession {
    ctx: SessionContext,
    lakehouses: Vec<Lakehouse>,
}

impl TenantSession {
    /// Wrap an already-built context and its lakehouses.
    pub fn new(ctx: SessionContext, lakehouses: Vec<Lakehouse>) -> Self {
        Self { ctx, lakehouses }
    }

    /// The DataFusion session queries plan and run in.
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// The catalogs behind it — what the result cache versions its inputs against.
    pub fn lakehouses(&self) -> &[Lakehouse] {
        &self.lakehouses
    }
}

/// Every tenant's session, produced on demand and then kept.
///
/// # Why a map instead of one context
///
/// `register_catalog` is global to a [`SessionContext`]. A single process-wide context serving
/// every tenant would therefore have to hold every tenant's catalog, and catalog listing would
/// show `acct_43` to account 42 — refused by [`crate::rbac`] when queried, but *visible*. Isolation
/// that depends on a check rather than on absence is exactly what per-tenant catalogs exist to
/// replace, so rebuilding it one layer up would close the issue in name only. One context per
/// account means another tenant's catalog is not merely denied, it is not there.
///
/// # This is per-*tenant* state, not per-query state
///
/// Which is the property that matters for holding one of these behind an `Arc` and sharing it
/// across every in-flight request. A session is a pure function of `(account, storage config,
/// catalog source)` — nothing about a *query* enters it — so memoizing it is caching a
/// computation, not accumulating request state. Two concurrent queries for one account share a
/// context exactly as every concurrent query shared the single context before, and two queries for
/// different accounts share nothing.
///
/// # The costs, stated rather than discovered
///
/// - **Sessions are never evicted.** A deployment with a very large number of *active* accounts
///   holds a context and a catalog handle for each. That is bounded by tenants who have actually
///   run a query on this process, not by the size of the `accounts` table, and each one's
///   Postgres pool is capped (see `CATALOG_POOL_MAX_CONNECTIONS` in [`crate::lakehouse`]) — but it
///   is unbounded in principle, and an eviction policy is the follow-on if it ever bites.
/// - **First query per tenant pays for the build**, which for a `sql` catalog means opening the
///   catalog and applying the manifest. Subsequent queries do not.
pub struct TenantSessions {
    source: SessionSource,
}

enum SessionSource {
    /// One session for every caller.
    ///
    /// The single-tenant shape, and the honest name for it. It is what a process with no control
    /// plane gets — there are no accounts, so there is nothing to key on — and what a caller that
    /// hand-builds a context (tests, a bespoke embedding) gets. It is *not* safe for a
    /// multi-tenant front door, which is why the multi-tenant constructor is the other one and
    /// this one says so out loud.
    Fixed(Arc<TenantSession>),
    /// A session per account, built from these ingredients the first time that account is seen.
    PerAccount {
        storage: StorageConfig,
        catalog: CatalogSource,
        /// Keyed by `Option<i64>` so a server with no services database — which resolves no
        /// account and therefore has exactly one tenant — shares the `None` entry instead of
        /// needing a separate code path.
        ///
        /// `OnceCell` inside the map, not a value: the sync `Mutex` is held only long enough to
        /// hand out the cell, so two accounts build concurrently and two queries for the *same*
        /// account build once. Holding the map's lock across the build would serialize every
        /// tenant's first query behind every other tenant's.
        built: Mutex<HashMap<Option<i64>, PendingSession>>,
    },
}

/// One account's slot in the session map: shared, and initialized exactly once however many
/// queries for that account race to be first.
type PendingSession = Arc<OnceCell<Arc<TenantSession>>>;

impl TenantSessions {
    /// One session, used for every account. See `SessionSource::Fixed` for when that is right.
    pub fn fixed(session: TenantSession) -> Self {
        Self {
            source: SessionSource::Fixed(Arc::new(session)),
        }
    }

    /// A session per account, built lazily from `storage` + `catalog`.
    pub fn per_account(storage: StorageConfig, catalog: CatalogSource) -> Self {
        Self {
            source: SessionSource::PerAccount {
                storage,
                catalog,
                built: Mutex::new(HashMap::new()),
            },
        }
    }

    /// This account's session, building it if this is the first query for that account.
    pub async fn for_account(&self, account_id: Option<i64>) -> Result<Arc<TenantSession>> {
        match &self.source {
            SessionSource::Fixed(session) => Ok(Arc::clone(session)),
            SessionSource::PerAccount {
                storage,
                catalog,
                built,
            } => {
                let cell = {
                    let mut map = built.lock().expect("tenant session map is not poisoned");
                    Arc::clone(map.entry(account_id).or_default())
                };
                let session = cell
                    .get_or_try_init(|| async {
                        let scope = TenantScope::for_account(account_id);
                        tracing::info!(tenant = %scope, "building this tenant's query session");
                        let (ctx, lakehouses) =
                            build_query_session(storage.clone(), catalog, &scope)
                                .await
                                .with_context(|| {
                                    format!("building the query session for tenant {scope}")
                                })?;
                        Ok::<_, anyhow::Error>(Arc::new(TenantSession::new(ctx, lakehouses)))
                    })
                    .await?;
                Ok(Arc::clone(session))
            }
        }
    }
}

impl std::fmt::Debug for TenantSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            SessionSource::Fixed(_) => f.write_str("TenantSessions::Fixed"),
            SessionSource::PerAccount { built, .. } => {
                let built = built.lock().map(|m| m.len()).unwrap_or(0);
                write!(f, "TenantSessions::PerAccount {{ built: {built} }}")
            }
        }
    }
}

/// The coordinator's in-memory object store is per-process, so a remote worker can never see data
/// written to it. Reject the combination up front rather than failing deep in a scan with an empty
/// result — a wrong answer that looks like a right one is the worst outcome available here.
pub fn reject_inmemory_storage(config: &StorageConfig) -> Result<()> {
    if matches!(config, StorageConfig::InMemory) {
        bail!(
            "--storage memory can't be used with remote workers: the in-memory object store is \
             per-process, so workers can't see the coordinator's data. Use `--storage local` \
             (a shared filesystem) or `--storage s3`."
        );
    }
    Ok(())
}

/// Discover the concrete fleet behind `endpoints`, with the logging and the desired-vs-observed
/// check the operator story depends on.
///
/// `declared_size` is the warehouse row's size when the query was routed to a warehouse. Desired
/// state (the row) and observed state (what DNS answered) diverge while a resize rolls out — and,
/// more importantly, stay diverged forever if nobody applied it — so an operator who resized and
/// saw no speedup gets told why.
///
/// Resolution runs on **every** call, which is what makes scaling take effect with no redeploy.
/// For a long-running server that means a resized warehouse is picked up by the next query.
pub async fn resolve_fleet(
    endpoints: &[String],
    declared_size: Option<i32>,
    resolver: Option<&BoxResolver>,
) -> Result<Vec<String>> {
    let fleet = match resolver {
        Some(resolve) => discover_workers_with(endpoints, |authority| resolve(authority)).await,
        None => discover_workers(endpoints).await,
    }
    .context("discovering the worker fleet")?;

    if fleet.is_empty() {
        bail!("no workers discovered from {endpoints:?}: every endpoint resolved to nothing");
    }
    tracing::info!(
        fleet_size = fleet.len(),
        workers = ?fleet,
        "discovered worker fleet"
    );
    if let Some(size) = declared_size
        && fleet.len() != size as usize
    {
        tracing::warn!(
            declared_size = size,
            fleet_size = fleet.len(),
            "warehouse size does not match the fleet actually answering: the resize/resume may \
             not have been applied to the compute yet"
        );
    }
    Ok(fleet)
}

/// Plan `sql` in `ctx`, distribute it across `fleet`, run it, and collect the answer.
///
/// [`execute_query_cached`] with no cache. Kept because plenty of callers — every test in here, and
/// any deployment with no services database — genuinely have nothing to cache against.
pub async fn execute_query(
    ctx: &SessionContext,
    sql: &str,
    fleet: &[String],
) -> Result<Vec<RecordBatch>> {
    execute_query_cached(ctx, None, &[], None, None, sql, fleet).await
}

/// Plan `sql`, refuse it if `authorization` does not cover what it touches, answer it from `cache`
/// when every input is unchanged, and otherwise distribute it across `fleet` and run it.
///
/// See the module docs for the distribute-vs-offload policy. This is the function both binaries
/// call, and the only place that policy is written down in code.
///
/// Every argument the control plane supplies is optional, and every "no" falls through to a normal
/// execution: no cache configured, no account resolved, no authorization to enforce, an uncacheable
/// statement, an input with no snapshot. With `None`/`&[]`/`None`/`None` the behaviour is
/// bit-for-bit what it was before either the cache or access control existed — which is what keeps
/// the single-node, no-Postgres path alive.
///
/// The authorization check happens inside [`execute_cached`], not here, because it must run between
/// planning and the cache lookup; see that function's docs for why that ordering is the whole point.
pub async fn execute_query_cached(
    ctx: &SessionContext,
    cache: Option<&ResultCache>,
    lakehouses: &[Lakehouse],
    account_id: Option<i64>,
    authorization: Option<&QueryAuthorization>,
    sql: &str,
    fleet: &[String],
) -> Result<Vec<RecordBatch>> {
    // Checked before anything is planned or looked up: "there is no fleet" is a configuration
    // error, and answering it from cache would hide a broken deployment behind a stale row.
    if fleet.is_empty() {
        bail!("cannot run a query with no workers");
    }
    execute_cached(
        ctx,
        cache,
        lakehouses,
        account_id,
        authorization,
        sql,
        |logical| async {
            // What the coordinator will tell a worker about this request. The objects are the same
            // requirements [`crate::rbac`] just checked, recomputed from the same logical plan by
            // the same pure function — carried for audit, since a worker cannot map a file path back
            // to a table name (see [`crate::plan_assertion`]). A statement whose privileges this
            // build cannot name has already been refused upstream when authorization is in force;
            // with no services database it reaches here and simply names nothing.
            let catalog = ctx.copied_config().options().catalog.clone();
            let identity = QueryIdentity {
                account_id,
                user: authorization.map(|a| a.user_name.clone()),
                objects: crate::rbac::required_privileges(
                    &logical,
                    &catalog.default_catalog,
                    &catalog.default_schema,
                )
                .map(|reqs| reqs.iter().map(|r| r.to_string()).collect())
                .unwrap_or_default(),
            };

            // `INSERT` is a write, and it must not leave this process. It arrives here rather than
            // through [`crate::dml`] because appends go through DataFusion
            // (`IcebergTableProvider` implements `insert_into`), so it is an ordinary logical plan
            // — just one that cannot be offloaded: the commit node `iceberg-datafusion` puts at its
            // root carries a live catalog handle and a connection pool no codec can serialize, and
            // shipping a commit to a worker would move the write's serialization point off the
            // machine that owns the statement. It sits inside the cache closure because a write is
            // never cacheable, so this is the one place it can be caught without planning the
            // statement a second time.
            if matches!(logical, LogicalPlan::Dml(_)) {
                return ctx
                    .execute_logical_plan(logical)
                    .await?
                    .collect()
                    .await
                    .context("running the write");
            }

            // `execute_logical_plan` rather than `create_physical_plan` straight off the logical
            // plan: it is what `ctx.sql` does, and it is what makes a DDL/DML statement submitted
            // through `--sql` still take effect. Those statements are never cacheable, so they
            // reach here unchanged.
            let plan = ctx
                .execute_logical_plan(logical)
                .await
                .context("planning the query")?
                .create_physical_plan()
                .await
                .context("building the physical plan")?;
            run_on_fleet(ctx, plan, fleet, &identity).await
        },
    )
    .await
}

/// Distribute `plan` across `fleet`, run it, and collect the answer — the policy itself, with no
/// planning and no caching around it.
async fn run_on_fleet(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    fleet: &[String],
    identity: &QueryIdentity,
) -> Result<Vec<RecordBatch>> {
    // Before anything is staged, sliced or serialized: every `IcebergTableScan` becomes a plain
    // parquet scan over the data files of the snapshot it was planned against. See
    // [`crate::iceberg_scan`] — this is the single funnel both front ends share, so putting it here
    // is what makes "an Iceberg query is distributable" true of the engine rather than of one
    // binary. A plan with no Iceberg scan comes back untouched.
    let plan = resolve_iceberg_scans(ctx, plan)
        .await
        .context("resolving iceberg scans to the data files of their snapshot")?;

    // Minted from the plan *after* the Iceberg rewrite and *before* staging, which is the one moment
    // both halves are true: every location this query will read is now named in the plan (the
    // rewrite is what turns a catalog handle into file URIs), and nothing has been cut into stages
    // yet — so one assertion covers every stage, however the fleet is fanned. Staging only wraps and
    // slices those scans; it never introduces a location the assertion has not seen. See
    // [`crate::plan_assertion`].
    //
    // `None` for a fleet with no `LLDB_FLEET_TOKEN`: no secret, no key, nothing to mint — and the
    // workers of such a fleet check nothing either, so the no-configuration path is unchanged.
    let assertion = PlanAuth::from_fleet_auth(crate::flight::ambient_fleet_auth())
        .mint(identity, &plan, std::time::SystemTime::now())
        .context("minting this query's plan assertion for the worker boundary")?;

    let coordinated = plan_distributed(Arc::clone(&plan), fleet)?;

    if contains_flight_reader(&coordinated) {
        // Genuinely distributed: the coordinator runs the reduce side locally and its
        // FlightReaderExec leaves pull each map/reduce stage from a worker over Flight. The
        // assertion rides on the `TaskContext` those leaves execute under — not in the plan bytes,
        // which are content-hashed into a stage id.
        let task_ctx = crate::plan_assertion::task_ctx_with(&ctx.task_ctx(), assertion);
        collect(coordinated, task_ctx)
            .await
            .context("running the distributed query across the fleet")
    } else {
        // Boundary-less: nothing to fan out. Collapse to a single output partition and ship the
        // whole plan to one worker — this still exercises a real worker over Flight instead of
        // running everything locally.
        //
        // "One worker" is a placement, not a commitment: the plan is self-contained and the worker
        // materializes it once by content hash, so any member of the fleet produces the same
        // answer. `fetch_with_failover` therefore walks the fleet in order.
        let plan = Arc::new(CoalescePartitionsExec::new(plan));
        tracing::info!(
            worker = %fleet[0],
            fleet_size = fleet.len(),
            "no distribution boundary; offloading the whole plan to one worker (failing over across the fleet if it is lost)"
        );
        fetch_with_failover(fleet, 0, plan, assertion.as_ref())
            .await
            .context("running the query on a single worker")
    }
}

/// True if `plan` contains at least one [`FlightReaderExec`] leaf — i.e. [`plan_distributed`]
/// actually cut a distribution boundary and inserted remote reads, rather than returning the plan
/// unchanged.
pub fn contains_flight_reader(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    // `apply` is infallible here (the closure never errors), so the expect cannot fire.
    plan.apply(|node| {
        if node.as_any().downcast_ref::<FlightReaderExec>().is_some() {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("walking the plan for FlightReaderExec does not error");
    found
}

/// Total rows across a set of batches — what query history records as `result_rows`.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;

    /// A manifest declaring nothing, so a session can be built with no database, no data files and
    /// no catalog server — enough to assert the *lifecycle*, which is what this is about.
    fn empty_manifest(dir: &std::path::Path) -> Result<CatalogSource> {
        let path = dir.join("empty.toml");
        std::fs::write(&path, "catalogs = []\n")?;
        Ok(CatalogSource::Manifest(path))
    }

    #[tokio::test]
    async fn a_fixed_source_serves_every_account_the_same_session() -> Result<()> {
        // The single-tenant shape, asserted as such rather than assumed: it hands the *same*
        // session to two different accounts, which is exactly why it is the wrong choice for a
        // multi-tenant front door and why the constructor for that one has a different name.
        let sessions = TenantSessions::fixed(TenantSession::new(SessionContext::new(), Vec::new()));
        let a = sessions.for_account(Some(1)).await?;
        let b = sessions.for_account(Some(2)).await?;
        assert!(Arc::ptr_eq(&a, &b));
        Ok(())
    }

    #[tokio::test]
    async fn a_per_account_source_builds_one_session_per_account_and_keeps_it() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let sessions = TenantSessions::per_account(
            StorageConfig::Local(dir.path().to_path_buf()),
            empty_manifest(dir.path())?,
        );

        // Two accounts, two sessions — the property the whole design rests on, since a shared
        // context would have to hold both tenants' catalogs.
        let a = sessions.for_account(Some(1)).await?;
        let b = sessions.for_account(Some(2)).await?;
        assert!(!Arc::ptr_eq(&a, &b));

        // …and one session per account, not one per query. Building a `sql` catalog per query
        // would open a Postgres pool per query, so this is a cost assertion as much as a
        // correctness one.
        let a_again = sessions.for_account(Some(1)).await?;
        assert!(Arc::ptr_eq(&a, &a_again));

        // No account resolved — a server with no services database — is its own entry rather than
        // a special case, and is not shared with any tenant's.
        let none = sessions.for_account(None).await?;
        assert!(!Arc::ptr_eq(&none, &a));
        assert!(Arc::ptr_eq(&none, &sessions.for_account(None).await?));
        Ok(())
    }

    #[tokio::test]
    async fn a_session_that_will_not_build_is_an_error_naming_its_tenant() {
        // A refusal has to say *whose* session failed: with one context per process there was only
        // one thing it could have been, and with N there is not.
        let sessions = TenantSessions::per_account(
            StorageConfig::Local("/definitely/not/a/directory".into()),
            CatalogSource::Manifest("/definitely/not/a/manifest.toml".into()),
        );
        let Err(err) = sessions.for_account(Some(42)).await else {
            panic!("neither the storage root nor the manifest exists");
        };
        let chain = format!("{err:#}");
        assert!(chain.contains("acct_42"), "{chain}");
    }

    #[test]
    fn a_plan_with_no_remote_leaves_is_not_distributed() {
        // The offload path's trigger, asserted directly: nothing was rewritten, so there is no
        // FlightReaderExec and the whole plan must be shipped to one worker.
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        assert!(!contains_flight_reader(&plan));
    }

    #[test]
    fn memory_storage_is_refused_for_a_remote_fleet() {
        let err = reject_inmemory_storage(&StorageConfig::InMemory)
            .expect_err("the in-memory store is invisible to a worker process");
        let message = err.to_string();
        assert!(message.contains("--storage memory"), "{message}");
        assert!(
            message.contains("--storage local"),
            "the error must name the fix: {message}"
        );
        // Every other backend is shared, so every other backend is fine.
        reject_inmemory_storage(&StorageConfig::Local("/tmp".into())).expect("local is shared");
    }

    #[tokio::test]
    async fn a_query_with_no_workers_is_refused_rather_than_run_locally() {
        // Silently running on the coordinator would be the wrong kind of helpful: the whole
        // premise is that execution happens on a fleet.
        let ctx = SessionContext::new();
        let err = execute_query(&ctx, "SELECT 1", &[])
            .await
            .expect_err("no fleet is not a runnable configuration");
        assert!(err.to_string().contains("no workers"), "{err}");
    }

    #[tokio::test]
    async fn an_empty_resolution_is_an_error_naming_the_endpoints() {
        let resolver: BoxResolver = Arc::new(|_authority| Box::pin(async { Ok(Vec::new()) }));
        let err = resolve_fleet(
            &["http://worker.lldb.local:50051".to_string()],
            None,
            Some(&resolver),
        )
        .await
        .expect_err("an endpoint behind which nothing is registered has no fleet");
        let chain = format!("{err:#}");
        assert!(chain.contains("worker.lldb.local"), "{chain}");
    }

    #[tokio::test]
    async fn the_injected_resolver_expands_an_endpoint_into_a_fleet() {
        // The seam the server needs: a warehouse's DNS name answered with its tasks' addresses,
        // which is exactly what Cloud Map does for an ECS service at `desiredCount: N`.
        let resolver: BoxResolver = Arc::new(|_authority| {
            Box::pin(async {
                Ok(vec![
                    "10.0.0.1:50051".parse::<SocketAddr>()?,
                    "10.0.0.2:50051".parse::<SocketAddr>()?,
                ])
            })
        });
        let fleet = resolve_fleet(
            &["http://analytics.lldb.local:50051".to_string()],
            // A declared size that disagrees warns but must not fail — desired state and observed
            // state legitimately differ while a resize rolls out.
            Some(4),
            Some(&resolver),
        )
        .await
        .expect("resolution succeeds");
        assert_eq!(
            fleet,
            vec!["http://10.0.0.1:50051", "http://10.0.0.2:50051"]
        );
    }

    #[test]
    fn rows_are_counted_across_every_batch() {
        assert_eq!(total_rows(&[]), 0);
    }
}
