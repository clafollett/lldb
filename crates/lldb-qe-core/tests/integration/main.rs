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
//! read exactly where they were, and a machine with neither still reports the skips and passes —
//! *reports* rather than *prints* since issue #112, because libtest's output capture threw the
//! printing away for every test that passed. [`support::gates`] is that fix and
//! `LLDB_TEST_REQUIRE_GATED` is the switch that turns those skips into failures instead.
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
//!   `worker_plan_assertion` (issue #34) is the second thing to live under that constraint and the
//!   place it bites hardest: a worker dialling another worker presents the *process's* ambient fleet
//!   token, so two fully-closed workers here could never authenticate to each other. It uses
//!   `serve_worker_with_postures` — documented in `flight.rs` as exactly this seam — rather than
//!   reaching for `set_var`.
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
//! Issue #33 (TLS) added two more, and both were checked against the rule above rather than
//! assumed to be fine:
//!
//! - **rustls's process-default crypto provider.** rustls 0.23 keeps exactly one, and installing a
//!   second is an error rather than a replacement. `lldb_qe_core::tls::install_crypto_provider`
//!   therefore owns it — a `OnceLock` whose `install_default` failure is ignored, called from every
//!   entry point that touches TLS. Nothing in `tests/` installs one, so there is no ordering to get
//!   wrong: whichever test reaches TLS first installs `ring`, and every later caller finds it there.
//!   A test that installed a *different* provider would break this and does not belong here.
//! - **`lldb_qe_core::tls`'s ambient client trust** (`install_client_trust`), which `flight_tls`
//!   sets. It is mutable global state, which normally would be exactly the thing that forces a
//!   separate target — except that it is consulted **only for `https://` URLs**. Every other test in
//!   this binary dials `http://`, so installing a CA is genuinely inert for them, and
//!   `flight_tls::with_no_certificates_at_all_the_plaintext_path_is_unchanged` asserts that rather
//!   than assuming it. All TLS tests share one CA (`support::certs::shared`), so concurrent installs
//!   agree on a value and the race has no losing side. A test that installed a *different* trust
//!   would reintroduce one.
//!
//! Issue #112 added the sixth, and it is the one whose whole purpose is to be process-wide:
//!
//! - **[`support::gates`]'s `REPORTED` set**, which makes the skip report one report per *run*
//!   rather than one per test. A `Mutex<BTreeSet<String>>` is exactly the mutable global the rule
//!   above is suspicious of, so: it is **append-only**, keyed by the exact line, read by nothing but
//!   the duplicate check, and asserted on by no test. What it ends up holding is a pure function of
//!   which prerequisites are absent — and those are read from the environment, which nothing in this
//!   binary mutates, so a prerequisite absent for one test is absent for all of them. Test order
//!   therefore permutes the *order* of the report's lines and nothing else about it, which is why
//!   this one is safe in a way a counter would not be. The switch beside it
//!   ([`support::gates::REQUIRE_GATED_ENV`]) is **read** and never written, so it lives under the
//!   first bullet's rule rather than breaking it; the policy it feeds is a pure function precisely
//!   so that testing it needs no `set_var` ([`support::gates::is_fatal`]).
//!
//! Issue #121 added the seventh, and it is the counter the bullet above says would not be safe —
//! so the difference is the whole justification:
//!
//! - **`support::CONTAINER_SEQ`**, which distinguishes two throwaway Postgres containers started in
//!   the same microsecond. `ABANDONED_CLOSED` is dangerous because `query_scheduler` *reads* it;
//!   this one is consumed only to build a container name and is read by no test, no assertion and
//!   no failure message. Test order therefore permutes which container gets which integer, which is
//!   not a fact anything can observe. The state has to be process-global to do its job at all: the
//!   collision it removes is between two threads of *this* process, which is exactly the scope one
//!   binary made possible.
//!
//! Everything else these tests touch is per-instance: sessions, catalogs, stage caches and
//! warehouses are constructed per test, every server binds `127.0.0.1:0`, and every file that
//! writes to disk writes into its own `tempfile::tempdir()`. The database-gated files name their
//! rows with a pid + nanosecond suffix, which was always required to survive a shared CI server
//! and is unaffected by the process count.
//!
//! ## Teardown is a `Drop` guard, and that is a consequence of this file
//!
//! One binary is one process, so anything a test does not give back is held for the whole run
//! rather than for a second or two. Two things used not to be given back — a spawned Flight server
//! (a dropped `JoinHandle` *detaches* its task, and every server here is spawned with a
//! `pending()` shutdown that never resolves) and, on a *failing* run, the rows a test wrote (a
//! `cleanup().await?` at the end of a body is unwound past by the assertion that failed). Neither
//! was a bug when it was written: separate binaries meant process exit reclaimed the socket, and
//! failure-path cleanup was ceremony. Issue #48 is that premise changing, and the fix is two guards
//! in [`support`] — [`support::Servers`] and [`support::DbCleanup`]. **A new test that starts a
//! server or writes a row uses them; it does not hand-roll a `cleanup()`.**

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
mod flight_tls;
mod flight_transport;
mod iceberg_roundtrip;
mod manifest_examples;
mod query_cancel;
mod query_reaper;
mod query_scheduler;
mod remote_eq_properties;
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
mod worker_plan_assertion;
mod worker_to_worker;
