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
//!
//! # …and over what
//!
//! `--tls-cert` / `--tls-key` make this port TLS. The fleet secret above used to cross it in the
//! clear, readable and replayable by anyone on the path, so the two are tied together by one rule:
//! **a worker that requires `LLDB_FLEET_TOKEN` refuses to bind a plaintext port** unless
//! `--allow-plaintext` says the operator meant it. A worker with no fleet token has no secret to
//! expose and binds plaintext with no configuration at all, exactly as it always did — that is what
//! keeps `cargo run -p lldb-qe-worker` a one-liner. The rule, and why it keys on the credential
//! rather than on the port, is [`lldb_qe_core::tls`].
//!
//! `--tls-ca` is the other half and is about *dialing*: a worker pulls its map stages from other
//! workers (the worker-to-worker shuffle), so it is a Flight client too, and a fleet whose workers
//! serve TLS is a fleet whose worker URLs say `https://`.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::Parser;
use datafusion::prelude::SessionContext;
use lldb_qe_core::flight::{ambient_fleet_auth, serve_worker_with};
use lldb_qe_core::stage_cache::StageCache;
use lldb_qe_core::{
    CredentialCheck, Storage, StorageArgs, StorageConfig, TlsArgs, init_tracing,
    install_client_trust,
};
use std::sync::Arc;
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

    #[command(flatten)]
    tls: TlsArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_core::BUILD_VERSION,
        "starting lldb-qe-worker"
    );

    // Resolve the transport posture *before* anything else that can fail slowly, and certainly
    // before the port is bound: a worker that is about to check a fleet secret over an unencrypted
    // port must stop here rather than come up and be quietly insecure. The credential half of the
    // answer is the ambient fleet auth, which is the same value `serve_worker_with` will enforce.
    let fleet_auth = ambient_fleet_auth();
    let server_tls = cli
        .tls
        .resolve_server(CredentialCheck::from_bool(fleet_auth.is_required()))?;
    // What this worker presents when it dials *another* worker for a shuffle stage. Installed from
    // this process's own flags rather than re-read from the environment by the library, because the
    // dial happens inside a plan that was serialized somewhere else and can carry nothing with it.
    install_client_trust(cli.tls.to_trust()?);

    let ctx = SessionContext::new();
    // Register non-local backends so `s3://` / `memory://` scans in shipped plans resolve.
    // Local rides the built-in `file://` store and needs no data dir to exist to serve.
    let storage_cfg = cli.storage.to_config()?;
    if !matches!(storage_cfg, StorageConfig::Local(_)) {
        Storage::from_config(&storage_cfg)
            .context("building worker storage")?
            .register_on(&ctx)?;
    }

    let listener = TcpListener::bind(cli.bind)
        .await
        .context("binding worker")?;
    tracing::info!(
        addr = %listener.local_addr()?,
        tls = server_tls.is_tls(),
        "lldb-qe-worker listening"
    );
    serve_worker_with(
        listener,
        ctx,
        Arc::new(StageCache::new()),
        fleet_auth.clone(),
        server_tls,
    )
    .await
}
