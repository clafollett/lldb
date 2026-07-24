//! `lldb-qe-worker` — a stateless Arrow Flight server that executes sub-plans shipped by a
//! coordinator.
//!
//! Configured entirely by flags/env (so it drops cleanly into a container):
//!   `--bind` (`LLDB_WORKER_BIND`) and the shared `--storage …` group. For an S3/MinIO
//!   deployment the worker registers the object store so serialized plans whose scans use
//!   `s3://…` resolve locally; for the default `local` backend nothing needs registering
//!   (DataFusion's built-in `file://` store handles the embedded absolute paths).

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::prelude::SessionContext;
use lldb_qe_core::{StorageArgs, StorageConfig, init_tracing, serve_worker};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-worker",
    about = "Stateless Arrow Flight worker for lldb"
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
