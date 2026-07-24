//! `lldb-qe-worker` — a stateless Arrow Flight server that executes sub-plans shipped by a
//! coordinator.
//!
//! Usage: `lldb-qe-worker [bind_addr]` (default `127.0.0.1:50051`).

use std::net::SocketAddr;

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use lldb_qe_core::serve_worker;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:50051".to_string())
        .parse()
        .context("parsing bind address")?;

    let listener = TcpListener::bind(addr).await.context("binding worker")?;
    println!(
        "lldb-qe-worker listening on http://{}",
        listener.local_addr()?
    );
    serve_worker(listener, SessionContext::new()).await
}
