//! `lldb-qe-worker` — a stateless Arrow Flight server that executes sub-plans shipped by a
//! coordinator.
//!
//! Configured entirely by flags/env (so it drops cleanly into a container):
//!   `--bind` (`LLDB_WORKER_BIND`) and the shared `--storage …` group. For an S3/MinIO
//!   deployment the worker registers the object store so serialized plans whose scans use
//!   `s3://…` resolve locally; for the default `local` backend nothing needs registering
//!   (DataFusion's built-in `file://` store handles the embedded absolute paths).
//!
//! # Who may talk to this port
//!
//! A worker executes whatever physical plan it is handed, with whatever storage credentials it
//! holds. `LLDB_FLEET_TOKEN` is what stops that being available to anyone who can route a packet
//! here: set it to the same value on every coordinator and every worker and this port requires it,
//! constant-time compared. Leave it unset and the port is open — which is what makes `cargo run -p
//! lldb-qe-worker` work with no configuration, and which is warned about, loudly, on every startup.
//! See [`lldb_qe_core::auth::FleetAuth`] for the scope of the claim a shared secret makes (it
//! proves membership of a deployment, not the identity of a user).

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::prelude::SessionContext;
use lldb_qe_core::{StorageArgs, StorageConfig, init_tracing, serve_worker};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-worker",
    about = "Stateless Arrow Flight worker for lldb",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Address to bind the Flight server to.
    #[arg(long, env = "LLDB_WORKER_BIND", default_value = "127.0.0.1:50051")]
    bind: SocketAddr,

    #[command(flatten)]
    storage: StorageArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_core::BUILD_VERSION,
        "starting lldb-qe-worker"
    );

    let ctx = SessionContext::new();
    // Register non-local backends so `s3://` / `memory://` scans in shipped plans resolve.
    // Local rides the built-in `file://` store and needs no data dir to exist to serve.
    let storage_cfg = cli.storage.to_config()?;
    if !matches!(storage_cfg, StorageConfig::Local(_)) {
        storage_cfg
            .build()
            .context("building worker storage")?
            .register_on(&ctx)?;
    }

    let listener = TcpListener::bind(cli.bind)
        .await
        .context("binding worker")?;
    tracing::info!(addr = %listener.local_addr()?, "lldb-qe-worker listening");
    serve_worker(listener, ctx).await
}
