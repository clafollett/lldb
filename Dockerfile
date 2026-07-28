# syntax=docker/dockerfile:1
#
# One image, both binaries. The coordinator and worker MUST be the identical build — serialized
# DataFusion physical plans are not cross-version compatible — so we build them together and
# select the role via the container `command`.
#
# Build layout uses cargo-chef so the (huge) dependency compile lands in its own layer keyed only
# by Cargo.toml/Cargo.lock. That layer is cache-hit on every build where dependencies are
# unchanged — locally via Docker's layer cache, and in CI via the buildx GitHub Actions cache
# (`cache-from/to type=gha`) — so only our own crates recompile when source changes.

# ---- Chef: pin the toolchain + install cargo-chef once (its own cached layer) --------------
FROM rust:1.97.1-bookworm AS chef
# object_store's S3 backend + TLS need these at build time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /build

# Build-speed knobs, defaulting to the real `[profile.release]` in Cargo.toml.
#
# That profile is tuned for benchmark honesty — `lto = "thin"` plus `codegen-units = 1`. Thin LTO
# is a *whole-program* link pass, so it runs across the entire DataFusion/Arrow/Iceberg graph even
# when every dependency is already compiled and cached, single-threaded, on every build. Touching
# one line of our own code costs the full pass.
#
# Callers that only need working binaries (the CI cluster smoke test, the compose demo) override
# these to trade runtime speed for build speed. A plain `docker build` still produces the
# optimized image. Set before `cargo chef cook` so the dependency layer and the final build agree
# — otherwise cargo considers the cooked deps stale and recompiles them.
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
ENV CARGO_PROFILE_RELEASE_LTO=$CARGO_PROFILE_RELEASE_LTO \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=$CARGO_PROFILE_RELEASE_CODEGEN_UNITS

# ---- Planner: distill the dependency graph into a recipe (invalidated only by manifests) ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Builder: cook dependencies from the recipe, THEN build our crates ---------------------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
# Compile every dependency. This layer is reused as long as recipe.json (i.e. the dependency
# set) is unchanged, no matter how our own source churns.
RUN cargo chef cook --release --recipe-path recipe.json

# Now the real sources; only our crates compile past this point.
COPY . .
# Stamp the build with its commit so the binaries report `version+sha` (the .git dir is not in
# the build context, so the SHA must be injected). Defaults keep local `docker build` working.
ARG GIT_SHA=unknown
ENV LLDB_GIT_SHA=$GIT_SHA
# The coordinator package also builds the control-plane one-shots: `lldb-qe-migrate` (applies
# services-DB migrations), `lldb-qe-warehouse` (create/list/resize/suspend/resume a virtual
# warehouse) and `lldb-qe-reap` (resolve query-history rows stranded by a dead coordinator), plus
# `lldb-qe-server`, the long-running query scheduler. They ship in the same image on purpose —
# migrations are embedded in the binary at compile time, the build that writes a warehouse row must
# be the build that reads it, and a coordinator (one-shot or serving) must be the identical build to
# every worker it ships a plan to.
RUN cargo build --release -p lldb-qe-coordinator -p lldb-qe-worker \
    && cp target/release/lldb-qe-coordinator target/release/lldb-qe-migrate \
          target/release/lldb-qe-warehouse target/release/lldb-qe-reap \
          target/release/lldb-qe-server \
          target/release/lldb-qe-worker /usr/local/bin/

# ---- Runtime -------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates for S3 TLS; libssl for the dynamically-linked TLS stack; netcat lets
# compose/orchestrators health-check the worker's gRPC port (`nc -z`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/lldb-qe-coordinator /usr/local/bin/lldb-qe-coordinator
COPY --from=builder /usr/local/bin/lldb-qe-migrate /usr/local/bin/lldb-qe-migrate
COPY --from=builder /usr/local/bin/lldb-qe-warehouse /usr/local/bin/lldb-qe-warehouse
COPY --from=builder /usr/local/bin/lldb-qe-reap /usr/local/bin/lldb-qe-reap
COPY --from=builder /usr/local/bin/lldb-qe-server /usr/local/bin/lldb-qe-server
COPY --from=builder /usr/local/bin/lldb-qe-worker /usr/local/bin/lldb-qe-worker

# Run as non-root.
RUN useradd --system --uid 10001 lldb
USER lldb

# Default to a worker bound on all interfaces; compose/ECS override `command` per role.
ENV LLDB_WORKER_BIND=0.0.0.0:50051
EXPOSE 50051
CMD ["lldb-qe-worker"]
