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
//! one worker.** [`plan_distributed`](crate::plan_distributed) cuts every distribution boundary it
//! recognizes into a DAG of stages fanned across the fleet, and returns the plan *unchanged* when
//! there is nothing to cut (a constant query, a bare scan). So "did it actually distribute" is
//! answered by looking for the [`FlightReaderExec`] leaves the rewrite inserts — if they are
//! there, the coordinator runs the reduce side locally and those leaves pull each stage over
//! Flight; if they are not, the whole plan is shipped to one worker, which keeps even a trivial
//! query exercising a real worker (what the cross-container smoke test relies on).
//!
//! Both paths tolerate losing a worker: the distributed one reassigns each stage through the
//! fallbacks its leaves carry, and the offload path walks the fleet with
//! [`fetch_with_failover`](crate::fetch_with_failover).
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
//! - **No catalog caching between calls.** [`build_query_session`] is called once per process, not
//!   once per query. Sharing one [`SessionContext`] across concurrent queries is safe and is
//!   exactly what the server does.
//! - **No cancellation, no partial results.** Batches are collected whole before they are
//!   returned, which is what makes stage reassignment safe (see
//!   [`crate::flight::fetch_partition_with_failover`] for why a half-delivered partition cannot be
//!   resumed).
//!
//! # The result cache sits *above* the policy, not beside it
//!
//! [`execute_query_cached`] is the real entry point; [`execute_query`] is it with the cache turned
//! off. The cache wraps the closure that does all of the above, so a hit is visibly the absence of
//! physical planning, of the staging rewrite, and of every byte that would have crossed the wire.
//! Putting it here rather than in each binary is the same argument the rest of this module makes:
//! a cache the one-shot honours and the server does not is two engines again.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

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
use crate::lakehouse::Lakehouse;
use crate::manifest::Manifest;
use crate::rbac::QueryAuthorization;
use crate::remote::FlightReaderExec;
use crate::result_cache::{ResultCache, execute_cached};
use crate::session::{build_session, register_tpch_parquet};
use crate::staging::plan_distributed;
use crate::storage::StorageConfig;

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

/// Build a session with `config`'s storage backend and `catalog`'s tables registered, returning it
/// alongside the [`Lakehouse`] handles the catalog produced.
///
/// One per process. A [`SessionContext`] is `Clone` and internally shared, so concurrent queries
/// on one context is the intended usage, not a liberty.
///
/// The lakehouses are returned rather than dropped because they are the only thing that can answer
/// "what snapshot is this table at", which is what makes a result-cache key safe to trust. The
/// TPC-H seed registers plain-parquet listing tables, which have no snapshot — so that path yields
/// no lakehouses and nothing it can query is cacheable. That is the intended behaviour, not a gap.
pub async fn build_query_session(
    config: StorageConfig,
    catalog: &CatalogSource,
) -> Result<(SessionContext, Vec<Lakehouse>)> {
    let (ctx, storage) = build_session(config).await?;
    let lakehouses = match catalog {
        CatalogSource::Manifest(path) => {
            let manifest = Manifest::from_path(path)?;
            apply_manifest(&ctx, &storage, &manifest)
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
            run_on_fleet(ctx, plan, fleet).await
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
) -> Result<Vec<RecordBatch>> {
    let coordinated = plan_distributed(Arc::clone(&plan), fleet)?;

    if contains_flight_reader(&coordinated) {
        // Genuinely distributed: the coordinator runs the reduce side locally and its
        // FlightReaderExec leaves pull each map/reduce stage from a worker over Flight.
        collect(coordinated, ctx.task_ctx())
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
        fetch_with_failover(fleet, 0, plan)
            .await
            .context("running the query on a single worker")
    }
}

/// True if `plan` contains at least one [`FlightReaderExec`] leaf — i.e.
/// [`plan_distributed`](crate::plan_distributed) actually cut a distribution boundary and inserted
/// remote reads, rather than returning the plan unchanged.
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
