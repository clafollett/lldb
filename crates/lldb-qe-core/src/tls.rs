//! **Transport security** — TLS on both Flight boundaries, and the rule that stops a plaintext
//! one being bound by accident.
//!
//! Issue #19 put credentials on both of this engine's Flight ports: an API key on the
//! coordinator's front door (`authorization: Bearer <token>`, see [`crate::auth`]) and a shared
//! fleet secret on a worker's ([`crate::auth::FleetAuth`]). Both crossed the wire in the clear. A
//! bearer token on an unencrypted channel is a token anyone on the path can read and *replay* —
//! indefinitely, as that tenant, with that tenant's grants, until somebody revokes a key they have
//! no reason to suspect. This module closes that.
//!
//! # In-process TLS, not a terminating proxy
//!
//! [`tonic::transport::ServerTlsConfig`] / [`tonic::transport::ClientTlsConfig`], configured by
//! the binaries themselves. Not an ALB, not a service mesh, not a sidecar — and that is a decision
//! about *this* system's traffic rather than a preference. The whole transport is gRPC, and
//! workers talk to **each other** directly: a reduce stage pulls its map stages worker-to-worker
//! (see [`crate::remote`]), so the fleet is a mesh, not a fan-in behind one door. A single
//! terminating front door would encrypt the client→coordinator hop and leave every
//! coordinator→worker and worker→worker hop — the ones carrying the fleet secret and the plan
//! bytes — in the clear. There is therefore no "or terminate in front" mode here, and adding one
//! would be adding a way to be half-encrypted.
//!
//! # Certificates are mounted files
//!
//! `--tls-cert` / `--tls-key` (server identity) and `--tls-ca` (what this process trusts when it
//! dials), each a path, each with the `LLDB_TLS_*` env fallback every other option in this repo
//! has. **The engine generates nothing and issues nothing**: no private-CA machinery, no
//! self-signed fallback, no ACME. A binary that could mint its own certificate could also mint one
//! an operator did not mean to trust, and "TLS is on" would stop implying "someone decided who the
//! peers are". Certificate lifecycle belongs to the deployment (Secrets Manager, a mounted volume,
//! cert-manager); this reads two files.
//!
//! # The rule: a plaintext port is opt-in exactly when a credential is checked on it
//!
//! [`crate::server`]'s `--allow-anonymous` is the idiom this copies — default secure, explicit
//! opt-in to insecure, a loud warning on every startup for as long as it is set. But the *analogue*
//! of that idiom is not "TLS is on unless you said otherwise": that would make `cargo run` require
//! certificates, which CLAUDE.md forbids in the same breath it forbids `cargo run` requiring
//! Postgres. Nor is it "warn and carry on", which is what the disclosure in [`crate::server`]'s
//! module docs already was.
//!
//! The rule this implements is narrower and, I think, exactly right:
//!
//! > **Binding a plaintext port while a credential is actually being checked on it requires
//! > `--allow-plaintext`.**
//!
//! Because that — and only that — is the configuration in which a real secret crosses a real
//! network in the clear. It is [`CredentialCheck`], and each binary answers it from what it
//! already knows:
//!
//! | Binary | A credential is checked when… |
//! | - | - |
//! | `lldb-qe-server` | a services database is configured — that is where accounts, keys and grants live, so it is exactly when a `Bearer` token is verified ([`crate::auth`]) |
//! | `lldb-qe-worker` | `LLDB_FLEET_TOKEN` is set — the fleet secret is the credential on that port |
//! | `lldb-qe-coordinator` | never: it binds no port. It is a client, and its TLS surface is `--tls-ca` |
//!
//! So a single-node `cargo run` — no services database, no fleet token — has no credential to leak
//! and keeps working untouched: no flag, no certificate, not even a warning. Add a control plane
//! (or a fleet secret) without certificates and the process **refuses to start**, naming the flags.
//! That refusal is the point of the issue: the insecure posture has to be *chosen*, and the
//! dangerous configuration is precisely the one nobody would otherwise notice they were in.
//!
//! # What this closes, and what it does not
//!
//! Closed: **eavesdropping and replay of a credential in transit**, on both boundaries. A token or
//! fleet secret on the wire is now inside a TLS session, and a passive observer on the path learns
//! neither it nor the plan bytes and rows beside it.
//!
//! Not closed, and deliberately out of scope:
//!
//! - **Which fleet member you are.** This is *server-authenticated* TLS: a client verifies the
//!   server's certificate; the server does not verify the client's. mTLS at the worker boundary —
//!   whether a client certificate should *replace* [`crate::auth::FleetAuth`]'s shared secret — is
//!   its own decision and is settled elsewhere (issue #34). Nothing here makes `LLDB_FLEET_TOKEN`
//!   redundant: it is unchanged, still required when set, and still the only thing proving a caller
//!   belongs to this deployment.
//! - **Per-request identity at the worker boundary.** A worker still authenticates the *fleet*, not
//!   the user, and TLS does not change what claim a shared secret makes. See [`crate::auth`].
//! - **A downgrade a client chooses.** Trust flows from the URL: `https://` dials TLS, `http://`
//!   does not. That is not a hole, because the *server* is what refuses — a TLS-serving worker
//!   fails a plaintext client's handshake rather than answering it — but it does mean the operator
//!   who turns on certificates must also change the `--workers` URLs to `https://`, and a
//!   half-converted fleet fails loudly rather than quietly falling back.
//!
//! # Three details worth keeping straight
//!
//! **`--tls-domain` is not decorative, because discovery rewrites worker URLs.**
//! [`crate::discovery`] expands one endpoint into one URL *per task IP* — that is the whole point of
//! it, and it is what makes scaling a warehouse change a query's fan-out with no redeploy. So
//! `https://analytics.lldb.local:50051` becomes `https://10.0.1.7:50051` before anything dials, and
//! the name a client would verify is an **IP**, not the name the certificate was issued for. Two
//! ways out, and a deployment must pick one: issue worker certificates with IP SANs, or set
//! `--tls-domain` to the name they *do* carry. Getting it wrong fails the handshake rather than
//! silently accepting the wrong peer, which is the right failure — but it fails on every query, so
//! it is worth knowing before the certificates are minted rather than after.
//!
//! **There is no trust store compiled in.** `tls-native-roots` / `tls-webpki-roots` are
//! deliberately off (see the workspace `Cargo.toml`): this fleet verifies against the CA file the
//! deployment mounts, so a bundled root store would be a dependency bought for nothing — and,
//! worse, would make "I forgot `--tls-ca`" succeed against any publicly-signed certificate instead
//! of failing. Dialing `https://` with no `--tls-ca` is therefore an error that says so.
//!
//! **The crypto provider is installed, not inferred.** rustls 0.23 resolves its provider from crate
//! features when no process default has been installed, which works today only because `ring` is
//! the sole provider in the tree. If some future dependency adds `aws-lc-rs`, that inference
//! becomes ambiguous and `ServerConfig::builder()` **panics** — inside a handshake, at runtime, in
//! production. [`install_crypto_provider`] makes the choice explicit and idempotent, and every
//! entry point here calls it before touching rustls.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{Context, Result, bail};
use clap::Args;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server, ServerTlsConfig};

/// Install this process's rustls crypto provider, exactly once, with an explicit choice of `ring`.
///
/// Idempotent by construction — a `Once` around an `install_default` whose `Err` (somebody else got
/// there first) is deliberately ignored, because rustls guarantees exactly one winner and every
/// caller here only needs *a* provider, not *theirs*.
///
/// Why call it at all when rustls can infer one from crate features: the inference is only
/// unambiguous while `ring` is the single provider in the dependency tree, and it fails by
/// **panicking** rather than by erroring. See the module docs.
pub fn install_crypto_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

// ---------------------------------------------------------------------------
// CLI surface
// ---------------------------------------------------------------------------

/// What this process trusts when it *dials* — the client half, shared by every role.
///
/// Split out from [`TlsArgs`] so the one-shot `lldb-qe-coordinator`, which binds no port and can
/// therefore never be the thing that serves plaintext, takes only the flags that mean something to
/// it. The env-var names are the same in both, so one environment block configures every role in a
/// compose file or a task definition.
#[derive(Debug, Clone, Args)]
pub struct TlsClientArgs {
    /// PEM CA bundle used to verify the certificate of any `https://` peer this process dials.
    ///
    /// Required to dial `https://` at all: no system trust store is compiled in, on purpose (see
    /// the module docs), so without this there is nothing to verify against and the dial errors
    /// rather than silently trusting whoever answers.
    #[arg(long, env = "LLDB_TLS_CA", value_name = "PATH")]
    pub tls_ca: Option<PathBuf>,

    /// Hostname to verify a dialed peer's certificate against, when it is not the URL's host.
    ///
    /// The escape hatch for the case where the address and the name genuinely differ — a fleet
    /// addressed by IP, or a certificate issued for a service name. Unset (the normal case) the
    /// URL's host is the name, which is what you want.
    #[arg(long, env = "LLDB_TLS_DOMAIN", value_name = "HOST")]
    pub tls_domain: Option<String>,
}

impl TlsClientArgs {
    /// Load the CA bundle, if one is configured.
    pub fn to_trust(&self) -> Result<ClientTrust> {
        let ca = match &self.tls_ca {
            None => None,
            Some(path) => Some(read_pem(path, "--tls-ca (LLDB_TLS_CA)")?),
        };
        Ok(ClientTrust {
            ca_pem: ca.map(Arc::new),
            domain: self.tls_domain.clone(),
        })
    }
}

/// The full TLS surface of a binary that **binds** a Flight port.
#[derive(Debug, Clone, Args)]
pub struct TlsArgs {
    /// PEM certificate chain this process serves. Set with `--tls-key`; either alone is an error.
    #[arg(long, env = "LLDB_TLS_CERT", value_name = "PATH")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key for `--tls-cert`.
    #[arg(long, env = "LLDB_TLS_KEY", value_name = "PATH")]
    pub tls_key: Option<PathBuf>,

    /// Serve a **plaintext** port even though a credential is checked on it.
    ///
    /// The escape hatch, `false` by default for the same reason `--allow-anonymous` is: security
    /// that has to be turned on is security that is off. It exists because a demo cluster, a
    /// laptop, and a deployment that has a control plane before it has certificates are all real —
    /// and it costs a warning on every startup for as long as it is set. It has no effect when no
    /// credential is checked (there is nothing to protect) and none when a certificate is
    /// configured (nothing is being downgraded).
    #[arg(long, env = "LLDB_ALLOW_PLAINTEXT")]
    pub allow_plaintext: bool,

    #[command(flatten)]
    pub client: TlsClientArgs,
}

impl TlsArgs {
    /// What this process trusts when it dials — see [`TlsClientArgs::to_trust`].
    pub fn to_trust(&self) -> Result<ClientTrust> {
        self.client.to_trust()
    }

    /// Decide what to serve on a port, refusing an *accidentally* plaintext one.
    ///
    /// This is the whole of the rule stated in the module docs, in one function, so that both
    /// serving binaries get the identical behaviour and so that it is testable without a socket:
    ///
    /// - a cert **and** a key → TLS (whatever else is set);
    /// - one of them alone → an error naming the missing one, because it is always a mistake and
    ///   never a posture;
    /// - neither, and nothing on this port checks a credential → plaintext, silently legal;
    /// - neither, a credential **is** checked, `--allow-plaintext` → plaintext, warned about;
    /// - neither, a credential **is** checked, no opt-in → **error**, naming both ways out.
    pub fn resolve_server(&self, credential: CredentialCheck) -> Result<ServerTls> {
        match (&self.tls_cert, &self.tls_key) {
            (Some(cert), Some(key)) => {
                if self.allow_plaintext {
                    tracing::info!(
                        "--allow-plaintext is set but a certificate is configured; serving TLS \
                         (the flag only ever permits a plaintext port, it never downgrades one)"
                    );
                }
                let identity = Identity::from_pem(
                    read_pem(cert, "--tls-cert (LLDB_TLS_CERT)")?,
                    read_pem(key, "--tls-key (LLDB_TLS_KEY)")?,
                );
                install_crypto_provider();
                Ok(ServerTls::Tls(Box::new(
                    ServerTlsConfig::new().identity(identity),
                )))
            }
            (Some(_), None) => bail!(
                "--tls-cert is set without --tls-key (LLDB_TLS_KEY): a certificate cannot be \
                 served without its private key"
            ),
            (None, Some(_)) => bail!(
                "--tls-key is set without --tls-cert (LLDB_TLS_CERT): a private key names no \
                 certificate to serve"
            ),
            (None, None) => match (credential, self.allow_plaintext) {
                (CredentialCheck::None, _) => {
                    Ok(ServerTls::Plaintext(PlaintextReason::NoCredential))
                }
                (CredentialCheck::Enforced, true) => {
                    Ok(ServerTls::Plaintext(PlaintextReason::OptedIn))
                }
                (CredentialCheck::Enforced, false) => bail!(
                    "refusing to serve a PLAINTEXT port while a credential is checked on it: the \
                     secret callers present would cross the network in the clear, where anyone on \
                     the path can read and replay it. Configure --tls-cert (LLDB_TLS_CERT) and \
                     --tls-key (LLDB_TLS_KEY) to serve TLS, or set --allow-plaintext \
                     (LLDB_ALLOW_PLAINTEXT) to accept that risk deliberately."
                ),
            },
        }
    }
}

/// Whether the port about to be bound actually verifies a credential.
///
/// Deliberately a named two-state type rather than a `bool` at the call site: the whole rule turns
/// on this answer, and `resolve_server(&self, true)` reads as though it might mean anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialCheck {
    /// Something on this port is verified — an API key (`lldb-qe-server` with a services database)
    /// or the fleet secret (`lldb-qe-worker` with `LLDB_FLEET_TOKEN`).
    Enforced,
    /// Nothing on this port is verified, so no secret crosses it and there is nothing to protect.
    None,
}

impl CredentialCheck {
    /// `Enforced` when `checked`, `None` otherwise — for a binary that already has the answer as a
    /// boolean (`db.is_some()`, `auth.is_required()`).
    pub fn from_bool(checked: bool) -> Self {
        if checked { Self::Enforced } else { Self::None }
    }
}

/// Why a port is being served in plaintext — the difference between "there is nothing to protect"
/// and "an operator accepted the risk", which is the difference between an `info` and a `warn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaintextReason {
    /// No credential is checked on this port.
    NoCredential,
    /// A credential is checked and `--allow-plaintext` was passed.
    OptedIn,
}

/// What a Flight port serves.
#[derive(Debug)]
pub enum ServerTls {
    /// Plaintext gRPC, for the stated reason.
    Plaintext(PlaintextReason),
    /// TLS with a configured identity. Boxed because `ServerTlsConfig` is much larger than the
    /// other variant and this enum is passed by value into the serving functions.
    Tls(Box<ServerTlsConfig>),
}

impl ServerTls {
    /// The no-configuration posture: plaintext, because nothing is checked.
    pub fn plaintext() -> Self {
        Self::Plaintext(PlaintextReason::NoCredential)
    }

    /// Whether this port is encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Apply this posture to a tonic server builder.
    ///
    /// Fails here rather than at the first connection when the certificate or key does not parse:
    /// `Server::tls_config` builds the rustls acceptor eagerly, so a bad PEM is a startup error
    /// naming the file, not a handshake failure on somebody's query.
    ///
    /// The provider is installed *here* and not only in [`TlsArgs::resolve_server`], even though
    /// every caller in this repo reaches a `Tls` variant through the resolver. `ServerTls` is
    /// public and so is its variant, so nothing stops a caller building one directly — and this
    /// line, not the resolver, is where rustls is actually touched. Resting the invariant on
    /// "every caller happens to go through the resolver" is precisely the inference this module
    /// exists to stop relying on; the call is a `OnceLock` and runs once per process, on a path
    /// taken once per server start.
    pub fn configure(&self, server: Server) -> Result<Server> {
        match self {
            Self::Plaintext(_) => Ok(server),
            Self::Tls(config) => {
                install_crypto_provider();
                server
                    .tls_config((**config).clone())
                    .context("building the TLS acceptor from --tls-cert / --tls-key")
            }
        }
    }

    /// Say what this port is, at startup, every time.
    ///
    /// Logged from inside the serving functions rather than from each binary for the same reason
    /// [`crate::auth::FleetAuth::log_posture`] is: an in-process test worker and the real
    /// `lldb-qe-worker` must report the identical line, and a warning that only one of them prints
    /// is a warning nobody trusts.
    pub fn log_posture(&self, port: &str) {
        match self {
            Self::Tls(_) => tracing::info!(port, "TLS is ON: this Flight port serves TLS"),
            Self::Plaintext(PlaintextReason::NoCredential) => tracing::info!(
                port,
                "this Flight port serves PLAINTEXT; nothing on it checks a credential, so there \
                 is none to expose. Configure --tls-cert/--tls-key before putting a credential on \
                 it"
            ),
            Self::Plaintext(PlaintextReason::OptedIn) => tracing::warn!(
                port,
                "SECURITY: this Flight port serves PLAINTEXT while checking a credential, because \
                 --allow-plaintext (LLDB_ALLOW_PLAINTEXT) is set. Every token presented to it \
                 crosses the network in the clear and can be read and replayed by anyone on the \
                 path. Configure --tls-cert (LLDB_TLS_CERT) and --tls-key (LLDB_TLS_KEY) and drop \
                 the flag"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// What this process trusts when it dials a peer, and how it dials one.
///
/// [`ClientTrust::default`] trusts nothing, which is the correct default here rather than a
/// pessimistic one: no system trust store is compiled in, so "trust nothing" and "trust whatever
/// the platform trusts" are not both available, and the fleet's CA is a file the deployment mounts.
#[derive(Debug, Clone, Default)]
pub struct ClientTrust {
    ca_pem: Option<Arc<Vec<u8>>>,
    domain: Option<String>,
}

impl ClientTrust {
    /// Trust the CA in `ca_pem`, verifying the peer's name against `domain` (or the URL's host).
    pub fn from_pem(ca_pem: impl Into<Vec<u8>>, domain: Option<String>) -> Self {
        Self {
            ca_pem: Some(Arc::new(ca_pem.into())),
            domain,
        }
    }

    /// Whether a CA is configured at all — i.e. whether an `https://` dial can succeed.
    pub fn has_ca(&self) -> bool {
        self.ca_pem.is_some()
    }

    fn client_config(&self, url: &str) -> Result<ClientTlsConfig> {
        let Some(ca) = &self.ca_pem else {
            bail!(
                "cannot dial {url} over TLS: no CA is configured, so there is nothing to verify \
                 the peer's certificate against. Set --tls-ca (LLDB_TLS_CA) to the PEM bundle that \
                 signed the fleet's certificates. (No system trust store is compiled in, on \
                 purpose — see the tls module docs.)"
            );
        };
        install_crypto_provider();
        let mut config =
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca.as_slice()));
        if let Some(domain) = &self.domain {
            config = config.domain_name(domain.clone());
        }
        Ok(config)
    }

    /// Open a channel to `url`, encrypted iff the URL says `https://`.
    ///
    /// The scheme is the switch, and nothing else is: there is no "try TLS, fall back to
    /// plaintext", because a fallback is a downgrade an attacker can trigger.
    ///
    /// The scheme test below is **not** what makes a plaintext dial plaintext — tonic's connector
    /// applies a TLS config only to an `https://` URI regardless of what we hand it. What the test
    /// buys is the error: reaching `client_config` early turns "you configured no `--tls-ca`" into a
    /// message naming the flag, raised before a socket is opened, instead of tonic's
    /// `Connecting to HTTPS without TLS enabled` surfacing from inside a connect attempt. Both
    /// refuse; only one tells the operator what to do.
    pub async fn dial(&self, url: &str) -> Result<Channel> {
        // `Channel::from_shared`, not `Endpoint::from_shared`, and the difference is load-bearing
        // rather than stylistic: the former hands back `http::uri::InvalidUri` while the latter
        // wraps it in a `tonic::transport::Error`, which [`crate::retry::classify`] reads as
        // *transport loss* and would therefore replay a malformed URL against every worker in the
        // fleet. See the caveat in that module. (It returns an `Endpoint` either way.)
        let endpoint =
            Channel::from_shared(url.to_string()).with_context(|| format!("invalid url {url}"))?;
        let endpoint = if endpoint.uri().scheme_str() == Some("https") {
            endpoint
                .tls_config(self.client_config(url)?)
                .with_context(|| format!("configuring TLS for {url}"))?
        } else {
            endpoint
        };
        endpoint
            .connect()
            .await
            .with_context(|| format!("connecting to {url}"))
    }
}

/// This process's dialing trust, shared by every outgoing Flight call.
///
/// Ambient for exactly the reason [`crate::flight::ambient_fleet_auth`] is, and the argument is
/// worth repeating because it is the one that rules out the obvious alternative: a
/// [`FlightReaderExec`] is **serialized into a plan** and re-executed on a worker for
/// worker-to-worker exchange, so a per-call TLS config would either have to travel inside those
/// plan bytes — where it would be content-hashed into a stage id and cached — or be absent exactly
/// where worker-to-worker pulls need it. A process-wide setting configured from that process's own
/// flags is what it actually is.
///
/// It is a `RwLock` rather than a `OnceLock` because the binaries *install* it from parsed flags at
/// startup instead of the library re-reading the environment, and because that makes it settable
/// from a test. Mutating it is safe to do in a shared test binary for a reason worth stating
/// rather than assuming: **it is consulted only for `https://` URLs**, so installing a CA changes
/// nothing for any caller dialing `http://`. See `tests/integration/main.rs`.
///
/// [`FlightReaderExec`]: crate::remote::FlightReaderExec
fn ambient() -> &'static RwLock<ClientTrust> {
    static AMBIENT: OnceLock<RwLock<ClientTrust>> = OnceLock::new();
    AMBIENT.get_or_init(|| RwLock::new(ClientTrust::default()))
}

/// Install this process's dialing trust. Called once at startup by each binary, from its own
/// `--tls-ca`; last writer wins, and nothing in the library writes it.
pub fn install_client_trust(trust: ClientTrust) {
    *ambient().write().expect("client trust lock poisoned") = trust;
}

/// This process's dialing trust, cloned (an `Arc` and an `Option<String>` — cheap enough for a
/// path that is about to open a socket).
pub fn client_trust() -> ClientTrust {
    ambient()
        .read()
        .expect("client trust lock poisoned")
        .clone()
}

/// Dial `url` with this process's ambient trust. The one call every outgoing Flight path uses.
pub async fn dial(url: &str) -> Result<Channel> {
    client_trust().dial(url).await
}

/// Read a PEM file, saying which flag named it when it cannot be read.
fn read_pem(path: &Path, flag: &str) -> Result<Vec<u8>> {
    std::fs::read(path)
        .with_context(|| format!("reading {} from {}", flag, path.display()))
        .and_then(|bytes| {
            if bytes.is_empty() {
                bail!("{} names an empty file: {}", flag, path.display());
            }
            Ok(bytes)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> TlsArgs {
        TlsArgs {
            tls_cert: None,
            tls_key: None,
            allow_plaintext: false,
            client: TlsClientArgs {
                tls_ca: None,
                tls_domain: None,
            },
        }
    }

    /// The single-node bargain, as an assertion: no services database, no fleet token, no
    /// certificate and no flag — and the process still binds. CLAUDE.md's rule that `cargo run`
    /// needs no Postgres has a twin here, that it needs no certificate either.
    #[test]
    fn no_credential_and_no_certificate_is_plaintext_and_legal() {
        let resolved = args()
            .resolve_server(CredentialCheck::None)
            .expect("nothing is checked, so nothing is exposed");
        assert!(matches!(
            resolved,
            ServerTls::Plaintext(PlaintextReason::NoCredential)
        ));
    }

    /// The load-bearing one. A credential is checked, the port would be plaintext, nobody said so:
    /// refuse, and name the way out.
    #[test]
    fn a_checked_credential_on_a_plaintext_port_refuses_to_start() {
        let error = args()
            .resolve_server(CredentialCheck::Enforced)
            .expect_err("a credential in the clear must not be the default");
        let message = format!("{error:#}");
        assert!(
            message.contains("--allow-plaintext"),
            "the refusal must name the opt-in, got: {message}"
        );
        assert!(
            message.contains("--tls-cert"),
            "the refusal must name the fix, got: {message}"
        );
    }

    /// …and with the opt-in it starts, and says loudly what it is.
    #[test]
    fn the_opt_in_permits_it_and_is_reported_as_deliberate() {
        let mut args = args();
        args.allow_plaintext = true;
        let resolved = args
            .resolve_server(CredentialCheck::Enforced)
            .expect("explicitly opted in");
        assert!(
            matches!(resolved, ServerTls::Plaintext(PlaintextReason::OptedIn)),
            "an opted-in plaintext port must be distinguishable from a harmless one — that is what \
             makes the startup warning fire"
        );
        assert!(!resolved.is_tls());
    }

    /// A half-configured identity is always a mistake, never a posture, so it is an error in both
    /// directions rather than a silent fall-through to plaintext — which is the failure mode that
    /// would turn a typo into an unencrypted production port.
    #[test]
    fn half_an_identity_is_an_error_either_way() {
        let mut cert_only = args();
        cert_only.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
        let error = cert_only
            .resolve_server(CredentialCheck::None)
            .expect_err("a certificate with no key cannot be served");
        assert!(format!("{error:#}").contains("--tls-key"), "got: {error:#}");

        let mut key_only = args();
        key_only.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));
        let error = key_only
            .resolve_server(CredentialCheck::None)
            .expect_err("a key with no certificate names nothing to serve");
        assert!(
            format!("{error:#}").contains("--tls-cert"),
            "got: {error:#}"
        );
    }

    /// The opt-in permits a plaintext port; it must never *downgrade* a configured one.
    #[test]
    fn the_opt_in_never_downgrades_a_configured_certificate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, "-- not a real certificate --").expect("write cert");
        std::fs::write(&key, "-- not a real key --").expect("write key");

        let mut args = args();
        args.tls_cert = Some(cert);
        args.tls_key = Some(key);
        args.allow_plaintext = true;
        let resolved = args
            .resolve_server(CredentialCheck::Enforced)
            .expect("a configured identity is served regardless of the opt-in");
        assert!(resolved.is_tls());
        // …and the garbage PEM above still fails, at *startup*, when the acceptor is built —
        // never later, on somebody's query.
        assert!(
            resolved.configure(Server::builder()).is_err(),
            "an unparseable certificate must fail while the server is being built"
        );
    }

    /// A missing file names the flag that pointed at it, because "No such file or directory" on
    /// its own tells an operator nothing about which of four paths they mistyped.
    #[test]
    fn a_missing_certificate_file_names_its_flag() {
        let mut args = args();
        args.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
        args.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));
        let error = args
            .resolve_server(CredentialCheck::None)
            .expect_err("the file does not exist");
        assert!(
            format!("{error:#}").contains("--tls-cert"),
            "got: {error:#}"
        );
    }

    #[test]
    fn credential_check_reads_a_boolean_the_obvious_way() {
        assert_eq!(CredentialCheck::from_bool(true), CredentialCheck::Enforced);
        assert_eq!(CredentialCheck::from_bool(false), CredentialCheck::None);
    }

    /// `http://` never consults the trust, which is what makes installing one in a shared test
    /// binary — or in a process that also talks to a plaintext peer — inert.
    #[tokio::test]
    async fn a_plaintext_url_ignores_the_trust_entirely() {
        // Nothing is listening, so this fails to *connect* — the point is that it gets that far
        // with no CA configured, rather than being refused for having none.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let error = ClientTrust::default()
            .dial(&format!("http://{addr}"))
            .await
            .expect_err("nothing is listening");
        let message = format!("{error:#}");
        assert!(
            message.contains("connecting to"),
            "a plaintext dial must fail on the connection, not on the trust: {message}"
        );
        assert!(!message.contains("--tls-ca"), "got: {message}");
    }

    /// …and `https://` with no CA is refused *before* a socket is opened, rather than trusting
    /// whoever answers.
    #[tokio::test]
    async fn an_https_url_with_no_ca_is_refused_rather_than_trusted() {
        let error = ClientTrust::default()
            .dial("https://example.invalid:50051")
            .await
            .expect_err("there is nothing to verify against");
        assert!(format!("{error:#}").contains("--tls-ca"), "got: {error:#}");
    }

    /// Installing is a `Once`; calling it twice must not panic, because every entry point calls it
    /// and several may run concurrently.
    #[test]
    fn installing_the_crypto_provider_twice_is_a_no_op() {
        install_crypto_provider();
        install_crypto_provider();
    }
}
