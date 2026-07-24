# syntax=docker/dockerfile:1
#
# One image, both binaries. The coordinator and worker MUST be the identical build — serialized
# DataFusion physical plans are not cross-version compatible — so we build them together and
# select the role via the container `command`.

# ---- Builder ---------------------------------------------------------------
FROM rust:1.97-bookworm AS builder

# object_store's S3 backend + TLS need these at build time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the whole workspace (Cargo.lock is committed) and build release binaries. The
# rust-toolchain.toml pins the exact toolchain; the base image already matches it.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p lldb-qe-coordinator -p lldb-qe-worker \
    && cp target/release/lldb-qe-coordinator target/release/lldb-qe-worker /usr/local/bin/

# ---- Runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates for S3 TLS; libssl for the dynamically-linked TLS stack; netcat lets
# compose/orchestrators health-check the worker's gRPC port (`nc -z`).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/lldb-qe-coordinator /usr/local/bin/lldb-qe-coordinator
COPY --from=builder /usr/local/bin/lldb-qe-worker /usr/local/bin/lldb-qe-worker

# Run as non-root.
RUN useradd --system --uid 10001 lldb
USER lldb

# Default to a worker bound on all interfaces; compose/ECS override `command` per role.
ENV LLDB_WORKER_BIND=0.0.0.0:50051
EXPOSE 50051
CMD ["lldb-qe-worker"]
