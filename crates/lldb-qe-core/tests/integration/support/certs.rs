//! One throwaway certificate authority for the whole test binary, one deliberately unrelated root
//! beside it, and the things the TLS tests do with them.
//!
//! # Why the certificate is minted rather than committed
//!
//! A checked-in PEM private key is a private key in git: flagged by every secret scanner, liable to
//! be refused outright by GitHub push protection, and — the part that actually matters — a
//! standing invitation for someone to reuse "the test key" somewhere it is not a test. Shelling out
//! to `openssl` was the other option and it would make `cargo test` depend on a binary that is not
//! in the toolchain and is absent from several CI images. `rcgen` is a dev-dependency that adds no
//! duplicate crate version (see the workspace manifest, and `cargo tree -d`), so the suite mints its
//! own CA on first use and nothing is ever written to the repository.
//!
//! # Why exactly one CA is *installed*, in a `OnceLock`
//!
//! Everything under `tests/integration` is one binary, therefore one process, therefore one
//! [`lldb_qe_core::tls::install_client_trust`] — see `main.rs`'s list of process-global state.
//! Two TLS tests that minted two CAs and installed both would race to set two different trusts and
//! whichever lost would fail intermittently. [`shared`] is the one every install names, which makes
//! every install idempotent *in value*, so the race stops existing rather than being managed.
//!
//! [`unrelated_root`] is a second root and does **not** break that rule, because nothing installs
//! it: it is only ever handed to [`bundle_trust`], which returns a [`ClientTrust`] the caller dials
//! with directly. A trust that is never ambient is a trust no other test can observe. Building one
//! per dial rather than installing it is the whole reason a suite about *what a client trusts* can
//! live in the shared binary at all.
//!
//! Generation is not free (a key pair apiece), which is the other reason each is minted once.

use std::sync::OnceLock;

use anyhow::{Context, Result};
use lldb_qe_core::tls::{ClientTrust, ServerTls, TlsArgs, TlsClientArgs, install_client_trust};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

/// The name the leaf certificate is issued for, and the name a client verifies against.
///
/// Servers in these tests bind `127.0.0.1:0` and are dialed by IP, so the address and the name
/// differ on purpose: that is exactly the shape a real deployment has once
/// `lldb_qe_core::discovery` has expanded a DNS endpoint into per-task IP URLs, and it is what
/// `--tls-domain` exists for. Verifying it here means the flag is exercised rather than assumed.
pub const TEST_DOMAIN: &str = "localhost";

/// A CA, and one leaf certificate it signed.
pub struct TestCerts {
    /// PEM of the CA certificate — what a client trusts (`--tls-ca`).
    pub ca_pem: String,
    /// PEM of the server's certificate (`--tls-cert`).
    pub cert_pem: String,
    /// PEM of the server's private key (`--tls-key`).
    pub key_pem: String,
}

/// This process's test CA, minted on first use.
pub fn shared() -> &'static TestCerts {
    static CERTS: OnceLock<TestCerts> = OnceLock::new();
    CERTS.get_or_init(|| generate().expect("minting a test CA cannot fail"))
}

/// The PEM of a second root, **independent of [`shared`]'s and signing nothing this suite serves**.
///
/// It exists so a test can build a real trust *bundle* — two roots, one of which is irrelevant to
/// the certificate on the wire — and assert both halves of what `--tls-ca` / `--tls-ca-pem` being a
/// bundle means: the signing root still anchors the chain when it is not alone, and a root that
/// signed nothing anchors nothing. That property is what `infra/README.md`'s *Rotating* runbook
/// spends three restarts on, and it is inherited from rustls by way of tonic rather than
/// implemented here — so nothing but a test holds it (issue #137).
///
/// Never installed as this process's trust: it is not value-identical to [`install_test_trust`]'s,
/// and the module docs above say why that matters. Use [`bundle_trust`].
pub fn unrelated_root() -> &'static str {
    static ROOT: OnceLock<String> = OnceLock::new();
    ROOT.get_or_init(|| {
        mint_ca("lldb integration-test UNRELATED CA")
            .expect("minting a second root cannot fail")
            .1
    })
}

/// A self-signed certificate authority: the issuer that can sign a leaf, and the PEM a client
/// trusts.
///
/// A real CA, not a self-signed leaf. rustls builds a `RootCertStore` from `--tls-ca` and webpki
/// will refuse a trust anchor that is not marked as a certificate authority, so "generate one
/// self-signed cert and trust it" fails with an opaque `UnknownIssuer`.
fn mint_ca(common_name: &str) -> Result<(Issuer<'static, KeyPair>, String)> {
    let key = KeyPair::generate().context("generating a CA key")?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    let pem = params.self_signed(&key)?.pem();
    Ok((Issuer::new(params, key), pem))
}

fn generate() -> Result<TestCerts> {
    let (issuer, ca_pem) = mint_ca("lldb integration-test CA")?;

    let leaf_key = KeyPair::generate().context("generating the server key")?;
    let mut leaf_params = CertificateParams::new(vec![TEST_DOMAIN.to_string()])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, TEST_DOMAIN);
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer)?;

    Ok(TestCerts {
        ca_pem,
        cert_pem: leaf_cert.pem(),
        key_pem: leaf_key.serialize_pem(),
    })
}

/// Install this process's dialing trust: the test CA, verifying peers as [`TEST_DOMAIN`].
///
/// Safe to call from any test, in any order, and safe for the tests that never touch TLS — the
/// trust is consulted only for `https://` URLs, so installing one changes nothing for a caller
/// dialing `http://`. That is the property `main.rs` records; see [`lldb_qe_core::tls`].
pub fn install_test_trust() {
    install_client_trust(ClientTrust::from_pem(
        shared().ca_pem.clone().into_bytes(),
        Some(TEST_DOMAIN.to_string()),
    ));
}

/// The posture a process with **no TLS configuration at all** resolves to.
///
/// Built by asking [`TlsArgs`] with every field unset, rather than by naming
/// [`ServerTls::plaintext`] directly, because that is the claim under test: a checkout with no
/// certificates, no flags and no services database must *resolve* to a plaintext port, not merely
/// be capable of one. Naming the variant would still pass if the rule had been rewritten as "TLS
/// unless you said otherwise", which is exactly the mistake worth catching.
pub fn no_tls_configured() -> Result<ServerTls> {
    TlsArgs {
        tls_cert: None,
        tls_cert_pem: None,
        tls_key: None,
        tls_key_pem: None,
        allow_plaintext: false,
        client: TlsClientArgs {
            tls_ca: None,
            tls_ca_pem: None,
            tls_domain: None,
        },
    }
    .resolve_server(lldb_qe_core::tls::CredentialCheck::None)
}

/// A [`ServerTls`] for the test certificate, built **through the real file-reading path**.
///
/// Deliberately not a shortcut constructor: an operator points `--tls-cert` / `--tls-key` at files,
/// and a test that bypassed that would leave the reading, the error wrapping and the
/// half-configured guard covered only by unit tests. The PEMs are read into memory eagerly, so the
/// caller may drop `dir` immediately.
pub fn server_tls(dir: &std::path::Path) -> Result<ServerTls> {
    let certs = shared();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, &certs.cert_pem).context("writing the test certificate")?;
    std::fs::write(&key_path, &certs.key_pem).context("writing the test key")?;

    TlsArgs {
        tls_cert: Some(cert_path),
        tls_cert_pem: None,
        tls_key: Some(key_path),
        tls_key_pem: None,
        allow_plaintext: false,
        client: TlsClientArgs {
            tls_ca: None,
            tls_ca_pem: None,
            tls_domain: None,
        },
    }
    // `CredentialCheck::None`: these servers hold no credential of their own, and the refusal rule
    // is a *unit* test in `lldb_qe_core::tls` because it needs no socket. What is under test here
    // is the transport.
    .resolve_server(lldb_qe_core::tls::CredentialCheck::None)
}

/// The same identity, supplied **inline** — no file, no directory, no path anywhere.
///
/// This is the shape ECS Fargate forces (issue #73): a Secrets Manager value reaches a task as an
/// environment variable and by no other means, so `LLDB_TLS_CERT_PEM` / `LLDB_TLS_KEY_PEM` are how
/// a fleet there gets an identity at all. Kept beside [`server_tls`] rather than replacing it,
/// because the file path is still what compose and a laptop use and both must keep working.
pub fn server_tls_inline() -> Result<ServerTls> {
    let certs = shared();
    TlsArgs {
        tls_cert: None,
        tls_cert_pem: Some(certs.cert_pem.clone()),
        tls_key: None,
        tls_key_pem: Some(certs.key_pem.clone()),
        allow_plaintext: false,
        client: TlsClientArgs {
            tls_ca: None,
            tls_ca_pem: None,
            tls_domain: None,
        },
    }
    .resolve_server(lldb_qe_core::tls::CredentialCheck::None)
}

/// The dialing half of the same story: a trust built from `--tls-ca-pem` rather than `--tls-ca`.
///
/// Returned rather than installed, because the ambient trust is process-global and this binary
/// shares one — see [`install_test_trust`] and `main.rs`.
pub fn inline_trust() -> Result<ClientTrust> {
    TlsClientArgs {
        tls_ca: None,
        tls_ca_pem: Some(shared().ca_pem.clone()),
        tls_domain: Some(TEST_DOMAIN.to_string()),
    }
    .to_trust()
}

/// A dialing trust over `roots` **concatenated**, verifying peers as [`TEST_DOMAIN`].
///
/// Plain concatenation and not a join, because that is literally what the runbook does:
/// `cat ./fleet-tls-ca/ca.crt ./fleet-tls-ca-new/ca.crt > ./both-roots.pem`. Every block rcgen emits
/// already ends in a newline, so this is byte-for-byte the file an operator produces.
///
/// Returned, never installed — see the module docs. That is what lets a test dial with a trust of
/// its own without any other test in this binary being able to observe it.
pub fn bundle_trust(roots: &[&str]) -> ClientTrust {
    ClientTrust::from_pem(roots.concat().into_bytes(), Some(TEST_DOMAIN.to_string()))
}
