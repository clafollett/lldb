//! One throwaway certificate authority for the whole test binary, and the two things the TLS
//! tests do with it.
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
//! # Why it is *one* CA, in a `OnceLock`
//!
//! Everything under `tests/integration` is one binary, therefore one process, therefore one
//! [`lldb_qe_core::tls::install_client_trust`] — see `main.rs`'s list of process-global state.
//! Two TLS tests that minted two CAs would race to install two different trusts and whichever lost
//! would fail intermittently. Sharing one makes every install idempotent *in value*, so the race
//! stops existing rather than being managed.
//!
//! Generation is not free (two key pairs), which is the other reason to do it once.

use std::sync::OnceLock;

use anyhow::{Context, Result};
use lldb_qe_core::tls::{ClientTrust, ServerTls, TlsArgs, TlsClientArgs, install_client_trust};

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

fn generate() -> Result<TestCerts> {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };

    // A real CA, not a self-signed leaf. rustls builds a `RootCertStore` from `--tls-ca` and
    // webpki will refuse a trust anchor that is not marked as a certificate authority, so
    // "generate one self-signed cert and trust it" fails with an opaque `UnknownIssuer`.
    let ca_key = KeyPair::generate().context("generating the CA key")?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "lldb integration-test CA");
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);

    let leaf_key = KeyPair::generate().context("generating the server key")?;
    let mut leaf_params = CertificateParams::new(vec![TEST_DOMAIN.to_string()])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, TEST_DOMAIN);
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer)?;

    Ok(TestCerts {
        ca_pem: ca_cert.pem(),
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
        tls_key: None,
        allow_plaintext: false,
        client: TlsClientArgs {
            tls_ca: None,
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
        tls_key: Some(key_path),
        allow_plaintext: false,
        client: TlsClientArgs {
            tls_ca: None,
            tls_domain: None,
        },
    }
    // `CredentialCheck::None`: these servers hold no credential of their own, and the refusal rule
    // is a *unit* test in `lldb_qe_core::tls` because it needs no socket. What is under test here
    // is the transport.
    .resolve_server(lldb_qe_core::tls::CredentialCheck::None)
}
