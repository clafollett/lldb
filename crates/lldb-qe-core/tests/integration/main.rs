//! Every integration test that is safe to share one process, compiled as **one** binary.
//!
//! Each file under `tests/` used to be its own integration-test target, and each of those targets
//! statically links the whole dependency graph — DataFusion, Arrow, Parquet, Iceberg, tonic. That
//! made 24 binaries of roughly 300 MB apiece: measured at 89.2 s of the warm cycle's 189.7 s of
//! compile CPU (47%) and 7.6 GB of an 11 GB target directory. Collapsing them into one target
//! links once instead of twenty-three times. `docs/build-performance.md` holds the before/after
//! numbers and the method; issue #44 is the parent.
//!
//! Nothing about the tests themselves changed. They are the same files, asserting the same things,
//! moved down one directory and declared here as modules.
//!
//! ## What changes, and what does not
//!
//! Selecting one former file is now a **filter** rather than a target:
//!
//! ```text
//! cargo test -p lldb-qe-core --test integration services_db     # was: --test services_db
//! ```
//!
//! Test names are module-qualified (`services_db::services_database_migrates_and_scopes_a_warehouse`),
//! so two former files may each keep a test of the same name without becoming ambiguous.
//!
//! Peak parallelism does **not** change: libtest bounds concurrent tests by the CPU count within a
//! binary, and Cargo already ran the binaries one after another, so the number of tests — and
//! therefore the number of throwaway Postgres containers — that can be in flight at once is what
//! it always was. Environment gating is untouched: `LLDB_TEST_POSTGRES_URL` / `LLDB_DOCKER` are
//! read exactly where they were, and a machine with neither still prints the skips and passes.
//!
//! ## Why `distributed_cluster` is still its own binary
//!
//! It is the one file deliberately left out, and the reason is *not* linking: it depends on
//! nothing but `std`, so its binary is 1.1 MB and merging it would save nothing measurable. What
//! merging would cost is real, though. It shells out to `docker compose up --build` for the whole
//! repository and tears the project down in a `Drop` guard; in its own target it is the only thing
//! in its process, whereas merged it would run under `LLDB_DOCKER=1` *concurrently* with the
//! database-gated tests, whose containers have a 60 s readiness budget that a multi-minute,
//! all-cores image build can plausibly blow. Zero benefit against a real flakiness risk, so it
//! stays a separate target — which is also what CI invokes by name, with its own `LLDB_IMAGE`.
//!
//! ## The rule for adding a file here
//!
//! **Separate binaries are separate processes; one binary is not.** A file belongs in this list
//! only if it is indifferent to sharing process-global state with every other file. The audit that
//! preceded this consolidation found exactly three pieces of such state, and each is safe for a
//! reason worth writing down rather than by luck:
//!
//! - [`lldb_qe_core::flight::ambient_fleet_auth`] is a `OnceLock`, so the *first* caller in the
//!   process decides its value for everyone. That is harmless only because its initializer,
//!   `FleetAuth::from_env`, is a pure function of `LLDB_FLEET_TOKEN` — which nothing mutates. The
//!   library and `auth_rbac` both go out of their way to pass a `FleetAuth` explicitly
//!   (`serve_worker_with_auth`, `fetch_stream_with_auth`) precisely because `std::env::set_var` is
//!   `unsafe` in edition 2024 and would race the rest of the binary. **A test that sets or removes
//!   an environment variable would break that reasoning and does not belong in this binary.**
//! - `stage_reassignment::show_retry_logs` installs a `tracing` subscriber with `try_init`, which
//!   was already idempotent across the several `#[tokio::test]`s in that file. Merged, the winner
//!   sets the filter for the whole binary's log output. No test asserts on log output, so this
//!   costs nothing but noise under `--nocapture`.
//! - `lldb_qe_core::server`'s `ABANDONED_CLOSED` / `ABANDONED_UNCLOSED` counters are process-wide
//!   and now accumulate across former files. `query_scheduler` reads them, but only inside the
//!   failure message of a `with_context`, never in an assertion — the test asserts on the database
//!   row. Anything that starts asserting on an absolute count of these would be order-dependent
//!   and must move out.
//!
//! Everything else these tests touch is per-instance: sessions, catalogs, stage caches and
//! warehouses are constructed per test, every server binds `127.0.0.1:0`, and every file that
//! writes to disk writes into its own `tempfile::tempdir()`. The database-gated files name their
//! rows with a pid + nanosecond suffix, which was always required to survive a shared CI server
//! and is unaffected by the process count.

mod support;

mod auth_rbac;
mod cache_grant_ordering;
mod catalog_generic;
mod coordinator_liveness;
mod distributed_iceberg;
mod distributed_join;
mod distributed_operators;
mod distributed_shuffle;
mod dml_snapshots;
mod first_light;
mod fleet_discovery;
mod flight_transport;
mod iceberg_roundtrip;
mod manifest_examples;
mod query_scheduler;
mod result_cache_db;
mod scan_slicing;
mod services_db;
mod shared_sql_catalog;
mod shuffle_materialization;
mod stage_reassignment;
mod tenant_catalogs;
mod tpch_baseline;
mod warehouse_lifecycle;
mod warehouse_routing;
mod worker_to_worker;
