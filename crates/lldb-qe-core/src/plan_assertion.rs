//! **Per-request identity at the worker boundary** — a short-lived, MAC'd statement that travels
//! *beside* a plan and says who authorized it and which bytes it may read.
//!
//! [`crate::rbac`] checks grants on the coordinator, at plan time. A worker sees none of that: it
//! receives a serialized *physical* plan and executes it, and [`crate::auth::FleetAuth`] proves only
//! "you are part of this deployment" — never "you are user X and X may read this table". Since
//! [`crate::iceberg_scan`] resolves an Iceberg scan into a parquet scan naming warehouse data files
//! by absolute URI, that gap stopped being theoretical: a worker is no longer shuffling opaque
//! intermediate batches, it is reading warehouse data at rest with its own storage credentials.
//! Per-tenant catalogs ([`crate::tenancy`]) separate *layout*, not access — any worker that will
//! execute tenant A's plan will execute one naming tenant B's files if handed it.
//!
//! This module is the assertion that closes that. The coordinator, having authenticated a request
//! and authorized its logical plan, mints one; a worker verifies it and then checks that **what the
//! plan actually touches is inside what the assertion covers**. A worker that verified a signature
//! and ignored the plan's contents would have built a second fleet token, not per-request identity:
//! the covering check is the point.
//!
//! # Where it travels: gRPC metadata, never the plan
//!
//! [`PLAN_ASSERTION_HEADER`], beside the fleet token in [`crate::auth::AUTHORIZATION_HEADER`]. Both
//! must be present and both are checked. It is **not** in the ticket, and that is a correctness
//! requirement rather than hygiene: a stage id is `stage_cache::stage_id_of(plan_bytes)`, a plain
//! content hash of the plan bytes, and it is the [`crate::stage_cache::StageCache`] key. A
//! per-request value inside those bytes would give every request a different stage id, so every
//! request would be a cache miss — silently destroying the materialize-once shuffle that
//! `shuffle_materialization::a_producer_pulled_by_many_consumers_executes_once` exists to prove. The
//! ticket also gets logged and cached, which is the same reason [`crate::auth`] keeps the API key
//! out of it.
//!
//! # How a worker forwards it: the `TaskContext`, for the same reason
//!
//! A worker that decodes a [`FlightReaderExec`] leaf dials another worker itself, and that outbound
//! call needs the assertion too — otherwise stage reassignment and every worker-to-worker shuffle
//! break the moment the boundary is closed. The plan cannot carry it (above), and a
//! `tokio::task_local!` cannot either: DataFusion's `collect_partitioned` drives each partition in
//! a `JoinSet::spawn`, and a task-local does not cross a spawn.
//!
//! What *does* reach every operator by construction is the [`TaskContext`] — every `execute` call
//! passes it to its children. So [`task_ctx_with`] hangs the verified header value on the context as
//! a `SessionConfig` extension, and [`FlightReaderExec::execute`](crate::remote::FlightReaderExec)
//! reads it back with [`forwarded`]. Per-request state, per-request channel, and not one byte of it
//! in the plan.
//!
//! # The key: an HMAC derived from the fleet secret, and what that does and does not buy
//!
//! `LLDB_FLEET_TOKEN` → [`AssertionKey::derive`] → HMAC-SHA256. No key distribution, no new
//! infrastructure, and it degrades exactly the way everything else here does: **no fleet token means
//! no key means nothing to verify**, so `cargo run` and every single-node path are untouched. It
//! gates identically to [`crate::tls`]'s plaintext rule — the assertion is required precisely when a
//! credential is already being checked on that port.
//!
//! Be honest about the ceiling. The key is **symmetric**, so a worker can mint as well as verify: an
//! assertion proves *"someone in this fleet authorized this plan"*, not *"the coordinator authorized
//! it"*. A compromised worker can still forge one for any files it likes. That is a large
//! improvement on the previous state — where a plan needed no authorization at all, and anything
//! holding the fleet secret could have any plan executed — and it is not the end state. What it does
//! buy, precisely:
//!
//! - **A plan is no longer self-authorizing.** Presenting the fleet token is no longer enough; the
//!   request must also carry a live assertion whose coverage contains every file the plan names, so
//!   a captured assertion cannot be paired with a different plan (another tenant's warehouse root,
//!   `file:///etc/`) and replayed.
//! - **A bounded window.** [`DEFAULT_TTL`] is measured in a query's expected duration, so a captured
//!   assertion stops working in minutes rather than never.
//! - **Attribution.** The account and user reach the worker, so a worker's logs can say who a stage
//!   was run for. Nothing before this could.
//!
//! Asymmetric signing (coordinators hold a private key, workers only a public one) is what makes a
//! compromised worker unable to mint, and **the mechanism for it now exists here**: [`SigningKey`],
//! [`VerifyingKey`], [`KeyId`], and the format-version-2 payload that carries the key id inside the
//! signed bytes. Read what that does and does not yet mean:
//!
//! - **Nothing configures it yet.** No binary reads a signing key, [`PlanAuth`] still derives from
//!   the fleet secret, and every deployment mints and checks exactly the v1 assertion it did before.
//!   #127 lands in three stages and this is the first; the second wires the coordinator's private
//!   key and the worker's *set* of accepted public keys, which is where the posture rule changes.
//! - **The ceiling moves but does not disappear.** Asymmetric signing relocates the trust boundary
//!   from *any fleet member* to *any coordinator*. A compromised **coordinator** still mints an
//!   assertion for any tenant naming any location. That is a large improvement — workers are the
//!   numerous, plan-executing half — and it is not "the coordinator proved it" in a sense that
//!   survives a compromised coordinator.
//! - **A worker accepting signatures does not fall back to the MAC.** [`SignedAssertion::verify_signed`]
//!   refuses a v1 payload by name rather than checking it the old way, because a verifier that can
//!   be talked into the weaker of two schemes is a downgrade attack wearing a migration —
//!   [`crate::tls`]'s "the scheme is the switch and there is no fallback", one layer up.
//!
//! # What the covering check verifies — and what it cannot
//!
//! A physical plan has **no table names**. One optimizer pass after the grant check, `lldb.sales.orders`
//! is a list of file paths, which is exactly why [`crate::rbac`] runs on the *logical* plan. So the
//! assertion carries two kinds of statement and they are not equally strong:
//!
//! | Field | Checked against the plan? |
//! | - | - |
//! | [`PlanAssertion::prefixes`] — object-store URI prefixes | **Yes.** Every file the plan names must sit under one. |
//! | [`PlanAssertion::objects`] — `SELECT on table lldb.sales.orders` | **No.** Carried for audit and for the worker's logs; a worker cannot map a file path back to a table without the catalog it deliberately does not have. |
//! | [`PlanAssertion::account_id`] / [`PlanAssertion::user`] | **No.** A worker has no accounts table. Carried for attribution. |
//!
//! The verifiable granularity is therefore a **directory**, not a file: prefixes are minted as the
//! parent directory of each file the plan reads, so an assertion for `…/orders/data/x.parquet` also
//! covers `…/orders/data/y.parquet` — a sibling file of the same table, possibly from another
//! snapshot. Widening it to exact file URIs was the alternative and does not fit: a table of a
//! thousand files would not fit in a gRPC header ([`MAX_ASSERTION_HEADER_BYTES`]). Binding the
//! assertion to a digest of the plan bytes instead was the other, and is worse than it looks — a
//! worker re-serializes a `FlightReaderExec`'s inner plan after a proto round trip, and nothing
//! guarantees those bytes equal the coordinator's, so the check would fail on legitimate
//! worker-to-worker traffic.
//!
//! Two more limits, stated rather than discovered:
//!
//! - **Only file scans are enumerated.** [`plan_reads`] finds every `DataSourceExec` over a
//!   `FileScanConfig` — the one way this engine reads bytes at rest — and descends into
//!   `FlightReaderExec::inner`, which `children()` does not report. A future node that reads storage
//!   by another route would be invisible here and must be added to that walk.
//! - **Rotating the *symmetric* key needs a restart, and always will.** It is derived from
//!   `LLDB_FLEET_TOKEN`, which [`crate::flight::ambient_fleet_auth`] reads once per process, and
//!   exactly one key is accepted. That is why the asymmetric half takes a **set** of verifying keys
//!   instead: widen the set, move the signer, narrow the set — three passes, each rolling-safe
//!   alone, because at every moment every verifier accepts the key every signer is using. It is the
//!   same shape as a multi-root TLS trust bundle and it is that shape for the same reason;
//!   `infra/README.md`'s *Rotating* documents the procedure. Accepting a set of
//!   keys (a key id in the payload, a previous-secret variable) is what would make rotation
//!   hitless; it is not built here, and pretending the current design supports it would be worse
//!   than saying so.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use base64::Engine as _;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use hmac::{Hmac, Mac};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq;

use crate::auth::FleetAuth;
use crate::remote::FlightReaderExec;
use crate::scan_split::file_scan_config;

type HmacSha256 = Hmac<Sha256>;

/// gRPC metadata key the assertion travels in — beside
/// [`AUTHORIZATION_HEADER`](crate::auth::AUTHORIZATION_HEADER), never inside the ticket.
///
/// Lowercase because gRPC metadata keys are lowercase on the wire, and named after
/// [`QUERY_ID_HEADER`](crate::server::QUERY_ID_HEADER), which is the existing idiom for a custom
/// header here.
pub const PLAN_ASSERTION_HEADER: &str = "lldb-plan-assertion";

/// Payload format version. Bumped when the encoding changes; a worker refuses anything else rather
/// than guessing, because coordinator and worker already must be the identical build.
const FORMAT_VERSION: u8 = 1;

/// Domain separator for deriving the MAC key from the fleet secret, so the key that signs
/// assertions is never the fleet token itself and cannot collide with a future second use of it.
const KEY_DOMAIN: &[u8] = b"lldb/plan-assertion/v1/key";

/// Domain separator prefixed to every MAC input, for the same reason.
const MAC_DOMAIN: &[u8] = b"lldb/plan-assertion/v1/mac";

/// Payload format version for the **signed** assertion: Ed25519 rather than HMAC (#127).
///
/// A v2 payload is `2 || key_id[4] || <the same body a v1 payload carries>`. The body is byte-for-
/// byte identical on purpose — what changes between the versions is who can produce the signature,
/// not what is being asserted, and keeping the body one shape is what lets [`PlanAssertion::decode`]
/// dispatch on the leading byte and then run one parser.
const FORMAT_VERSION_SIGNED: u8 = 2;

/// Domain separator prefixed to every signature input.
///
/// Distinct from [`MAC_DOMAIN`] so that no byte string is ever both a valid v1 MAC input and a valid
/// v2 signature input, even if some future deployment held both a fleet secret and a signing key.
const SIGN_DOMAIN: &[u8] = b"lldb/plan-assertion/v2/sign";

/// Bytes of `SHA-256(public key)` that name a key.
///
/// Four, because a key id is a *lookup* into a set an operator configured, not a security boundary:
/// the signature is what authorizes, and a collision here costs one extra verification attempt
/// rather than an accepted forgery. Four bytes keep the header small while making an accidental
/// collision between two keys a fleet actually holds vanishingly unlikely.
const KEY_ID_BYTES: usize = 4;

/// How long a minted assertion is good for.
///
/// A query's expected duration, not a session's: fifteen minutes is long enough that ordinary
/// analytical queries never notice and short enough that a captured assertion is worth little.
/// The cost is stated in the module docs — a query whose stages are still being pulled after this
/// long fails at the worker, because nothing re-mints mid-query.
pub const DEFAULT_TTL: Duration = Duration::from_secs(900);

/// Slack allowed on the expiry check, for fleets whose clocks are not perfectly aligned. Only ever
/// applied to *expiry* — an assertion from the future is not treated specially, because a skewed
/// issuer is already covered by this window on the other end.
pub const CLOCK_SKEW_ALLOWANCE: Duration = Duration::from_secs(60);

/// Upper bound on the encoded header value.
///
/// gRPC implementations bound the header list (tonic/hyper default to tens of kilobytes, and
/// intermediaries are stingier), so an assertion that grows without limit would fail as a transport
/// error at some unpredictable size. Bounding it here turns that into one legible refusal on the
/// coordinator, naming the plan that was too wide.
pub const MAX_ASSERTION_HEADER_BYTES: usize = 6 * 1024;

// ---------------------------------------------------------------------------
// The key
// ---------------------------------------------------------------------------

/// The symmetric key that signs and verifies assertions, derived from the fleet secret.
///
/// Not the fleet token itself: `HMAC(fleet_secret, KEY_DOMAIN)`, so the value that signs assertions
/// is a distinct 32 bytes and a future second use of the secret cannot produce a colliding MAC.
#[derive(Clone, PartialEq, Eq)]
pub struct AssertionKey([u8; 32]);

impl AssertionKey {
    /// Derive the key from a fleet secret.
    pub fn derive(fleet_secret: &str) -> Self {
        let mut mac = HmacSha256::new_from_slice(fleet_secret.as_bytes())
            .expect("HMAC accepts a key of any length");
        mac.update(KEY_DOMAIN);
        let bytes = mac.finalize().into_bytes();
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Self(key)
    }

    /// The MAC over a payload, with its domain separator.
    fn mac(&self, payload: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(&self.0).expect("a 32-byte key is accepted");
        mac.update(MAC_DOMAIN);
        mac.update(payload);
        let bytes = mac.finalize().into_bytes();
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        out
    }
}

impl fmt::Debug for AssertionKey {
    /// Never prints the key. This type ends up inside [`PlanAuth`], which ends up inside config
    /// structs that binaries log at startup — the same rule [`crate::auth::FleetAuth`] follows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AssertionKey(****)")
    }
}

// ---------------------------------------------------------------------------
// The asymmetric key pair — what a compromised worker cannot mint with (#127)
// ---------------------------------------------------------------------------

/// Names which key signed an assertion: the first four bytes of `SHA-256(public key)`.
///
/// **Derived, never registered.** A registry would be one more thing to keep in sync across a
/// fleet, and the whole point of rotation being three passes is that a worker's accepted set is the
/// only state that has to change. Deriving the id from the key means adding a key to that set is
/// self-describing: nothing has to agree in advance on what to call it.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId([u8; KEY_ID_BYTES]);

impl KeyId {
    /// The id of an Ed25519 public key.
    pub fn of_public_key(public_key: &[u8]) -> Self {
        let digest = Sha256::digest(public_key);
        let mut id = [0u8; KEY_ID_BYTES];
        id.copy_from_slice(&digest[..KEY_ID_BYTES]);
        Self(id)
    }
}

impl fmt::Debug for KeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyId({self})")
    }
}

impl fmt::Display for KeyId {
    /// Hex, because a key id is the one part of this module that belongs in a log line: an operator
    /// diagnosing a refused assertion needs to see *which* key was named and compare it to the set
    /// they configured. It identifies a public key, so it is not a secret.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The private half: what a **coordinator** holds, and what a worker must not.
///
/// This is the whole of #127 in one type. With [`AssertionKey`] a worker could mint as well as
/// verify, so an assertion proved only *"someone in this fleet authorized this"*; a worker that
/// holds no `SigningKey` cannot produce one at all.
///
/// Not `Clone`, because `ring`'s key pair is not, and that is a property worth keeping rather than
/// working around: one process, one signing key, no copies to lose track of.
pub struct SigningKey {
    pair: Ed25519KeyPair,
    id: KeyId,
}

impl SigningKey {
    /// Load a key from its PKCS#8 v2 document — the format `ring` itself generates.
    ///
    /// PKCS#8 rather than a bare 32-byte seed because it carries the public key alongside the
    /// private one, so `ring` verifies on load that the two halves agree. A bare seed cannot be
    /// checked against anything, and the failure it hides is a fleet configured with a key whose id
    /// nobody accepts — which surfaces as every query being refused, far from the mistake.
    pub fn from_pkcs8(pkcs8: &[u8]) -> Result<Self, AssertionError> {
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|e| {
            AssertionError::Malformed(format!("signing key is not a PKCS#8 Ed25519 document: {e}"))
        })?;
        let id = KeyId::of_public_key(pair.public_key().as_ref());
        Ok(Self { pair, id })
    }

    /// This key's id, which every assertion it signs carries.
    pub fn id(&self) -> KeyId {
        self.id
    }

    /// The public half, to hand a worker.
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey::from_public_key(self.pair.public_key().as_ref().to_vec())
    }

    fn sign(&self, payload: &[u8]) -> [u8; 64] {
        let mut input = Vec::with_capacity(SIGN_DOMAIN.len() + payload.len());
        input.extend_from_slice(SIGN_DOMAIN);
        input.extend_from_slice(payload);
        let signature = self.pair.sign(&input);
        let mut out = [0u8; 64];
        out.copy_from_slice(signature.as_ref());
        out
    }
}

impl fmt::Debug for SigningKey {
    /// Never prints the key material. The id is safe and is the useful half — it is what an operator
    /// matches against a worker's accepted set.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SigningKey(**** id={})", self.id)
    }
}

/// The public half: what a **worker** holds, one per key it accepts.
///
/// A worker holds a *set* of these, and that is what makes rotation hitless — widen the set, move
/// the coordinator to the new key, narrow the set, each pass rolling-safe on its own. It is the same
/// shape as a multi-root TLS trust bundle, for the same reason, and `infra/README.md`'s *Rotating*
/// documents that shape already.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifyingKey {
    public_key: Vec<u8>,
    id: KeyId,
}

impl VerifyingKey {
    /// Wrap a raw 32-byte Ed25519 public key.
    ///
    /// Nothing is validated here beyond the id derivation, because `ring` validates the point during
    /// verification and a public key is not secret — a malformed one costs a refused assertion
    /// naming its id, which is exactly the diagnosis an operator needs.
    pub fn from_public_key(public_key: Vec<u8>) -> Self {
        let id = KeyId::of_public_key(&public_key);
        Self { public_key, id }
    }

    /// This key's id, which is how an assertion names it.
    pub fn id(&self) -> KeyId {
        self.id
    }

    fn verify(&self, payload: &[u8], signature: &[u8]) -> bool {
        let mut input = Vec::with_capacity(SIGN_DOMAIN.len() + payload.len());
        input.extend_from_slice(SIGN_DOMAIN);
        input.extend_from_slice(payload);
        UnparsedPublicKey::new(&ED25519, &self.public_key)
            .verify(&input, signature)
            .is_ok()
    }
}

impl fmt::Debug for VerifyingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VerifyingKey(id={})", self.id)
    }
}

// ---------------------------------------------------------------------------
// The posture
// ---------------------------------------------------------------------------

/// Whether this process signs and checks plan assertions, and with what key.
///
/// Derived from [`FleetAuth`] and from nothing else, which is what makes the two postures impossible
/// to configure inconsistently: a worker that requires the fleet secret requires an assertion, and
/// one that does not require neither. `cargo run -p lldb-qe-worker` and the compose demo keep
/// working with no configuration at all.
#[derive(Clone)]
pub enum PlanAuth {
    /// No fleet secret, so no key: assertions are neither minted nor checked.
    Disabled,
    /// Mint with, and verify against, this key.
    Required(AssertionKey),
}

impl PlanAuth {
    /// The posture implied by a fleet credential.
    pub fn from_fleet_auth(auth: &FleetAuth) -> Self {
        match auth.token() {
            None => Self::Disabled,
            Some(secret) => Self::Required(AssertionKey::derive(secret)),
        }
    }

    /// Whether an assertion is required on this boundary.
    pub fn is_required(&self) -> bool {
        matches!(self, PlanAuth::Required(_))
    }

    /// Sign `assertion`, or `None` when this posture mints nothing.
    pub fn sign(&self, assertion: &PlanAssertion) -> Result<Option<SignedAssertion>> {
        let PlanAuth::Required(key) = self else {
            return Ok(None);
        };
        Ok(Some(assertion.sign(key)?))
    }

    /// Mint an assertion for `plan`, covering every location it reads.
    ///
    /// `None` when this posture mints nothing, which is what keeps a fleet with no secret behaving
    /// exactly as it did. Errors only if the plan reads more distinct directories than a header can
    /// carry — see [`MAX_ASSERTION_HEADER_BYTES`].
    pub fn mint(
        &self,
        identity: &QueryIdentity,
        plan: &Arc<dyn ExecutionPlan>,
        now: SystemTime,
    ) -> Result<Option<SignedAssertion>> {
        if !self.is_required() {
            return Ok(None);
        }
        let assertion = PlanAssertion::for_plan(identity, plan, now, DEFAULT_TTL);
        self.sign(&assertion)
    }

    /// Verify a presented header value.
    ///
    /// `Ok(None)` means "nothing to check here": a [`PlanAuth::Disabled`] boundary ignores the header
    /// entirely, exactly as [`FleetAuth::Open`] ignores a presented token. Otherwise the header must
    /// be present, well-formed, correctly signed and unexpired.
    pub fn verify(
        &self,
        presented: Option<&str>,
        now: SystemTime,
    ) -> Result<Option<VerifiedAssertion>, AssertionError> {
        let PlanAuth::Required(key) = self else {
            return Ok(None);
        };
        let Some(presented) = presented else {
            return Err(AssertionError::Missing);
        };
        let signed = SignedAssertion(presented.to_string());
        let assertion = signed.verify(key, now)?;
        Ok(Some(VerifiedAssertion { assertion, signed }))
    }

    /// Check that every location in `reads` is covered by `verified`.
    ///
    /// The other half of the door, and the half that makes this per-request identity rather than a
    /// second fleet token. `None` is only ever produced by a disabled posture, so it passes.
    pub fn check_cover(
        &self,
        verified: Option<&VerifiedAssertion>,
        reads: &[String],
    ) -> Result<(), AssertionError> {
        match verified {
            None => Ok(()),
            Some(verified) => verified.assertion.covers_all(reads),
        }
    }
}

impl fmt::Debug for PlanAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanAuth::Disabled => f.write_str("PlanAuth::Disabled"),
            PlanAuth::Required(_) => f.write_str("PlanAuth::Required(****)"),
        }
    }
}

// ---------------------------------------------------------------------------
// What a coordinator knows about the caller
// ---------------------------------------------------------------------------

/// The identity half of an assertion: what the coordinator proved about the caller, and what its
/// grant check named.
///
/// Every field is optional in effect, because a deployment with no services database has no accounts
/// and no users and must keep working (CLAUDE.md's standing rule). An assertion minted for such a
/// deployment names nobody and still covers the plan's locations, which is the part a worker can
/// actually check.
#[derive(Debug, Clone, Default)]
pub struct QueryIdentity {
    /// The tenant the query runs as, derived from the credential — never claimed by the caller.
    pub account_id: Option<i64>,
    /// The user the credential named.
    pub user: Option<String>,
    /// The objects the query's *logical* plan was authorized against, rendered as
    /// `SELECT on table lldb.sales.orders`. Carried for audit; a worker cannot check it (see the
    /// module docs).
    pub objects: Vec<String>,
}

// ---------------------------------------------------------------------------
// The assertion itself
// ---------------------------------------------------------------------------

/// The payload: who, what, and until when.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlanAssertion {
    /// `None` when no services database resolved a tenant.
    pub account_id: Option<i64>,
    /// `None` when the request was anonymous.
    pub user: Option<String>,
    /// Unix seconds when this was minted.
    pub issued_at: i64,
    /// Unix seconds after which it is refused (plus [`CLOCK_SKEW_ALLOWANCE`]).
    pub expires_at: i64,
    /// Object-store URI prefixes this assertion covers, each ending in `/`. **The verifiable half.**
    pub prefixes: Vec<String>,
    /// Rendered requirements the coordinator's grant check passed. **Not verifiable by a worker.**
    pub objects: Vec<String>,
}

impl PlanAssertion {
    /// Build an assertion covering everything `plan` reads, valid for `ttl` from `now`.
    pub fn for_plan(
        identity: &QueryIdentity,
        plan: &Arc<dyn ExecutionPlan>,
        now: SystemTime,
        ttl: Duration,
    ) -> Self {
        let issued_at = unix_seconds(now);
        Self {
            account_id: identity.account_id,
            user: identity.user.clone(),
            issued_at,
            expires_at: issued_at + ttl.as_secs() as i64,
            prefixes: covering_prefixes(&plan_reads(plan)),
            objects: identity.objects.clone(),
        }
    }

    /// Sign this payload with the fleet secret's symmetric key, producing the header value.
    ///
    /// Unchanged by #127, deliberately: a v1 assertion means exactly what it always meant, and the
    /// existing tests are the assertion that this held.
    pub fn sign(&self, key: &AssertionKey) -> Result<SignedAssertion> {
        let payload = self.encode();
        let mac = key.mac(&payload);
        self.assemble(&payload, &mac)
    }

    /// Sign this payload with a coordinator's **private** key (#127).
    ///
    /// The difference that matters is not the algorithm, it is who can produce the result: a worker
    /// holding only the public half can verify this and cannot mint it, so an assertion stops
    /// proving *"someone in this fleet authorized this"* and starts proving *"a holder of this
    /// signing key did"*.
    ///
    /// The key's id goes **inside** the signed payload rather than beside it, so it cannot be
    /// rewritten to point a verifier at a different key than the one that actually signed.
    pub fn sign_with(&self, key: &SigningKey) -> Result<SignedAssertion> {
        let payload = self.encode_as(Some(key.id()));
        let signature = key.sign(&payload);
        self.assemble(&payload, &signature)
    }

    /// `base64(payload).base64(signature)`, bounded.
    ///
    /// Shared by both signing paths so the bound cannot be enforced on one and forgotten on the
    /// other — and it is checked against the **assembled header**, not estimated from the payload,
    /// which is what makes it correct for either signature size. An Ed25519 signature is 64 bytes
    /// where a MAC is 32, so a v2 header is about 48 characters longer for the same plan; measuring
    /// the real string means that difference needs no arithmetic here to stay right.
    fn assemble(&self, payload: &[u8], signature: &[u8]) -> Result<SignedAssertion> {
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = format!("{}.{}", b64.encode(payload), b64.encode(signature));
        if header.len() > MAX_ASSERTION_HEADER_BYTES {
            bail!(
                "this query's plan reads {} distinct location(s), which does not fit in a {}-byte \
                 plan assertion ({} bytes). A plan assertion names the directories a worker may \
                 read; a query spanning this many of them cannot be authorized at the worker \
                 boundary. Compact the table, or narrow the query.",
                self.prefixes.len(),
                MAX_ASSERTION_HEADER_BYTES,
                header.len()
            );
        }
        Ok(SignedAssertion(header))
    }

    /// Whether every location in `reads` sits under one of this assertion's prefixes.
    ///
    /// A read set that is empty passes trivially — a stage that touches no storage (a constant, a
    /// reduce over batches already in flight) has nothing to authorize.
    pub fn covers_all(&self, reads: &[String]) -> Result<(), AssertionError> {
        for uri in reads {
            // A relative segment would let a prefix match while the read escapes it. `object_store`
            // normalizes paths and rejects these, so this is belt-and-braces — and belt-and-braces
            // is right where the failure mode is reading another tenant's files.
            if uri.contains("/../") || uri.ends_with("/..") || uri.contains("/./") {
                return Err(AssertionError::Traversal(uri.clone()));
            }
            if !self.prefixes.iter().any(|prefix| uri.starts_with(prefix)) {
                return Err(AssertionError::NotCovered {
                    uri: uri.clone(),
                    covered: self.prefixes.len(),
                });
            }
        }
        Ok(())
    }

    /// The canonical byte encoding the MAC is computed over.
    ///
    /// Hand-rolled with `u32` length prefixes for the same reason [`crate::flight`]'s ticket and
    /// [`crate::server`]'s are: the shape is small, the format is readable at a glance, and a
    /// serialization dependency here would have to be canonical to be safe to MAC.
    fn encode(&self) -> Vec<u8> {
        self.encode_as(None)
    }

    /// [`Self::encode`], but naming the key that will sign it.
    ///
    /// `Some(id)` produces a v2 payload — `2 || key_id[4] || body` — and `None` the v1 payload,
    /// byte-for-byte what this module has always produced. The **body is identical either way**:
    /// the version changes who can produce the signature over these bytes, not what they assert. The
    /// key id lives inside the signed payload rather than beside it precisely so it cannot be
    /// rewritten to point a verifier at a different key.
    fn encode_as(&self, key_id: Option<KeyId>) -> Vec<u8> {
        let mut buf = Vec::new();
        match key_id {
            None => buf.push(FORMAT_VERSION),
            Some(id) => {
                buf.push(FORMAT_VERSION_SIGNED);
                buf.extend_from_slice(&id.0);
            }
        }
        // `0` and `""` are the "absent" spellings rather than a flags byte: an account id is a
        // Postgres identity column and a user name is non-empty by `validate_identifier`, so neither
        // value is representable and the encoding stays one shape.
        put_i64(&mut buf, self.account_id.unwrap_or(0));
        put_str(&mut buf, self.user.as_deref().unwrap_or(""));
        put_i64(&mut buf, self.issued_at);
        put_i64(&mut buf, self.expires_at);
        put_strs(&mut buf, &self.prefixes);
        put_strs(&mut buf, &self.objects);
        buf
    }

    /// Inverse of [`Self::encode`], for either version.
    fn decode(buf: &[u8]) -> Result<Self, AssertionError> {
        Ok(Self::decode_versioned(buf)?.0)
    }

    /// [`Self::decode`], also reporting which key the payload named.
    ///
    /// `None` is a v1 payload, which names no key because an HMAC has only one. Callers that must
    /// *find* a key before they can verify need this; callers that already have one do not.
    fn decode_versioned(buf: &[u8]) -> Result<(Self, Option<KeyId>), AssertionError> {
        let (version, rest) = take_u8(buf)?;
        let (key_id, rest) = match version {
            FORMAT_VERSION => (None, rest),
            FORMAT_VERSION_SIGNED => {
                if rest.len() < KEY_ID_BYTES {
                    return Err(truncated("key id", KEY_ID_BYTES, rest.len()));
                }
                let mut id = [0u8; KEY_ID_BYTES];
                id.copy_from_slice(&rest[..KEY_ID_BYTES]);
                (Some(KeyId(id)), &rest[KEY_ID_BYTES..])
            }
            other => {
                return Err(AssertionError::Malformed(format!(
                    "plan assertion format version {other}, but this build speaks \
                     {FORMAT_VERSION} and {FORMAT_VERSION_SIGNED} (every coordinator and worker \
                     must run the identical build)"
                )));
            }
        };
        let (account_id, rest) = take_i64(rest)?;
        let (user, rest) = take_str(rest)?;
        let (issued_at, rest) = take_i64(rest)?;
        let (expires_at, rest) = take_i64(rest)?;
        let (prefixes, rest) = take_strs(rest)?;
        let (objects, rest) = take_strs(rest)?;
        if !rest.is_empty() {
            return Err(AssertionError::Malformed(format!(
                "plan assertion has {} trailing byte(s)",
                rest.len()
            )));
        }
        Ok((
            Self {
                account_id: (account_id != 0).then_some(account_id),
                user: (!user.is_empty()).then_some(user),
                issued_at,
                expires_at,
                prefixes,
                objects,
            },
            key_id,
        ))
    }
}

impl fmt::Display for PlanAssertion {
    /// What a worker logs. Never the MAC, and never more than the identity — the prefixes are a
    /// count, because a stage's log line should not be a directory listing.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@account {} ({} covered prefix(es), expires {})",
            self.user.as_deref().unwrap_or("anonymous"),
            self.account_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".to_string()),
            self.prefixes.len(),
            self.expires_at
        )
    }
}

/// A signed assertion, as it appears on the wire.
///
/// Opaque on purpose: it is the thing a worker forwards verbatim on its own outbound calls, so
/// anything that re-encoded it would risk changing the bytes the MAC was computed over.
#[derive(Clone, PartialEq, Eq)]
pub struct SignedAssertion(String);

impl SignedAssertion {
    /// The header value to put in [`PLAN_ASSERTION_HEADER`].
    pub fn as_header_value(&self) -> &str {
        &self.0
    }

    /// Verify signature and expiry, yielding the payload.
    ///
    /// Order matters: the MAC is checked **before** the payload is trusted for anything, including
    /// its own expiry, so a forged payload never influences a decision.
    pub fn verify(
        &self,
        key: &AssertionKey,
        now: SystemTime,
    ) -> Result<PlanAssertion, AssertionError> {
        let (payload, presented_mac) = self.split()?;

        // Constant-time, following [`crate::auth::verify_token`]'s precedent exactly: a byte-at-a-time
        // comparison tells a forger how many leading bytes of their guess were right, which is what
        // turns 2^256 into 2x32. `ct_eq` short-circuits on length only, and every MAC is 32 bytes.
        let expected = key.mac(&payload);
        if !bool::from(presented_mac.ct_eq(&expected)) {
            return Err(AssertionError::BadSignature);
        }

        let assertion = PlanAssertion::decode(&payload)?;
        Self::check_expiry(assertion, now)
    }

    /// Verify an **Ed25519** signature against a set of accepted keys, yielding the payload (#127).
    ///
    /// `accepted` is a set rather than a value, and that is the whole rotation story: widen it to
    /// both keys, move the coordinator to the new one, narrow it again — three passes, each
    /// rolling-safe on its own, because at every moment every verifier accepts the key every signer
    /// is using. It is the same shape as a multi-root TLS trust bundle and it is that shape for the
    /// same reason; `infra/README.md`'s *Rotating* documents the procedure.
    ///
    /// Order matters exactly as it does above: **nothing in the payload is trusted until the
    /// signature verifies**, including its own expiry. The key id is the one exception and it is not
    /// an exception to that rule — it is read only to *choose* a key, and choosing the wrong one can
    /// only make verification fail.
    pub fn verify_signed(
        &self,
        accepted: &[VerifyingKey],
        now: SystemTime,
    ) -> Result<PlanAssertion, AssertionError> {
        let (payload, signature) = self.split()?;
        let (assertion, key_id) = PlanAssertion::decode_versioned(&payload)?;
        let Some(key_id) = key_id else {
            // A v1 payload carries no key id because an HMAC has only one key. Reaching here means
            // a caller expecting signatures was handed a MAC'd assertion, which is a *downgrade*
            // and is refused rather than quietly checked the old way.
            return Err(AssertionError::Malformed(
                "this assertion is MAC'd (format version 1), but this boundary accepts only                  signed assertions (format version 2). A worker configured with signing keys does                  not fall back to the fleet secret — see the rotation procedure for how to move a                  fleet across."
                    .to_string(),
            ));
        };
        let Some(key) = accepted.iter().find(|key| key.id() == key_id) else {
            // Naming the id is the whole diagnosis: it tells an operator whether they are missing a
            // key from this worker's set or the coordinator is signing with one nobody was told
            // about. It identifies a public key, so it is safe to say out loud.
            return Err(AssertionError::Malformed(format!(
                "assertion names signing key {key_id}, which is not in this boundary's accepted                  set ({}). Either this worker's accepted keys are stale, or the coordinator signed                  with a key that was never distributed.",
                if accepted.is_empty() {
                    "empty".to_string()
                } else {
                    accepted
                        .iter()
                        .map(|key| key.id().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )));
        };
        if !key.verify(&payload, &signature) {
            return Err(AssertionError::BadSignature);
        }
        Self::check_expiry(assertion, now)
    }

    /// `<payload>.<signature>`, both base64url-decoded.
    fn split(&self) -> Result<(Vec<u8>, Vec<u8>), AssertionError> {
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let (payload_b64, sig_b64) = self
            .0
            .split_once('.')
            .ok_or_else(|| AssertionError::Malformed("expected `<payload>.<mac>`".to_string()))?;
        let payload = b64
            .decode(payload_b64)
            .map_err(|e| AssertionError::Malformed(format!("payload is not base64url: {e}")))?;
        let signature = b64
            .decode(sig_b64)
            .map_err(|e| AssertionError::Malformed(format!("mac is not base64url: {e}")))?;
        Ok((payload, signature))
    }

    /// The one expiry rule, shared by both verification paths so they cannot drift apart.
    fn check_expiry(
        assertion: PlanAssertion,
        now: SystemTime,
    ) -> Result<PlanAssertion, AssertionError> {
        let now = unix_seconds(now);
        if now > assertion.expires_at + CLOCK_SKEW_ALLOWANCE.as_secs() as i64 {
            return Err(AssertionError::Expired {
                expired_at: assertion.expires_at,
                now,
            });
        }
        Ok(assertion)
    }
}

impl fmt::Debug for SignedAssertion {
    /// Elides the value. It is authorization-bearing for as long as it is valid, so it belongs in
    /// no log line — the same rule [`crate::auth::NewToken`] and [`FleetAuth`] follow.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SignedAssertion(**** {} bytes)", self.0.len())
    }
}

/// A verified assertion: the payload a worker may act on, plus the exact bytes it forwards.
#[derive(Debug, Clone)]
pub struct VerifiedAssertion {
    pub assertion: PlanAssertion,
    pub signed: SignedAssertion,
}

/// Why an assertion was not accepted.
#[derive(Debug, thiserror::Error)]
pub enum AssertionError {
    #[error(
        "unauthenticated: this worker requires a plan assertion. Every request must carry \
         `{PLAN_ASSERTION_HEADER}`, which a coordinator mints from the same {} the fleet secret \
         comes from — a plan on its own is not authorization to read anything.",
        crate::auth::FLEET_TOKEN_ENV
    )]
    Missing,
    #[error("unauthenticated: the plan assertion is malformed ({0})")]
    Malformed(String),
    #[error(
        "unauthenticated: the plan assertion's signature does not verify. Every coordinator and \
         worker must share the same {} value.",
        crate::auth::FLEET_TOKEN_ENV
    )]
    BadSignature,
    #[error(
        "unauthenticated: the plan assertion expired at {expired_at} (now {now}). An assertion is \
         minted per query and is deliberately short-lived; resubmit the query."
    )]
    Expired { expired_at: i64, now: i64 },
    #[error(
        "permission denied: this plan reads `{uri}`, which none of the assertion's {covered} \
         covered location(s) contains. A worker executes only what the coordinator authorized."
    )]
    NotCovered { uri: String, covered: usize },
    #[error(
        "permission denied: this plan names `{0}`, which contains a relative path segment. A \
         location that can escape its own prefix cannot be authorized."
    )]
    Traversal(String),
}

// ---------------------------------------------------------------------------
// What a plan reads
// ---------------------------------------------------------------------------

/// Every object-store location `plan` will read, fully qualified and deduplicated.
///
/// Two things it does that a plain [`TreeNode::apply`](datafusion::common::tree_node::TreeNode)
/// would not, and both matter:
///
/// - It descends into [`FlightReaderExec::inner`], which is deliberately *not* a child (the sub-plan
///   runs elsewhere). Without that, a coordinator would mint an assertion covering only the stages
///   it runs itself, and every worker-to-worker pull would be refused.
/// - It returns a sorted, deduplicated set, so an assertion minted from one plan and a check run
///   against another are comparing canonical values.
///
/// This is the same notion of "a file scan" [`crate::scan_split`] and [`crate::iceberg_scan`] use —
/// one definition, so what is sliced, what is reported and what is authorized cannot drift.
pub fn plan_reads(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut found = BTreeSet::new();
    collect_reads(plan, &mut found);
    found.into_iter().collect()
}

fn collect_reads(node: &Arc<dyn ExecutionPlan>, out: &mut BTreeSet<String>) {
    if let Some(config) = file_scan_config(node) {
        let base = config.object_store_url.as_str();
        for group in &config.file_groups {
            for file in group.iter() {
                out.insert(format!("{base}{}", file.object_meta.location));
            }
        }
    }
    if let Some(reader) = node.as_any().downcast_ref::<FlightReaderExec>() {
        collect_reads(reader.inner(), out);
    }
    for child in node.children() {
        collect_reads(child, out);
    }
}

/// The covering prefixes for a set of locations: each file's parent directory, deduplicated.
///
/// Directory granularity is a deliberate trade and the module docs state its cost. A location with
/// no `/` after its scheme cannot be reduced to a directory, so it is covered exactly.
pub fn covering_prefixes(reads: &[String]) -> Vec<String> {
    let mut prefixes = BTreeSet::new();
    for uri in reads {
        match uri.rfind('/') {
            Some(cut) => prefixes.insert(uri[..=cut].to_string()),
            None => prefixes.insert(uri.clone()),
        };
    }
    prefixes.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Carrying it through execution
// ---------------------------------------------------------------------------

/// The `SessionConfig` extension a verified assertion rides in. A newtype so the `TypeId` keyed
/// lookup cannot collide with anything else's extension.
struct Forwarded(SignedAssertion);

/// A [`TaskContext`] identical to `base` but carrying `assertion` for every operator beneath it.
///
/// `None` returns the context unchanged, so a fleet with no secret builds nothing and pays nothing.
/// See the module docs for why this is the channel rather than the plan or a task-local.
pub fn task_ctx_with(
    base: &Arc<TaskContext>,
    assertion: Option<SignedAssertion>,
) -> Arc<TaskContext> {
    let Some(assertion) = assertion else {
        return Arc::clone(base);
    };
    let config = base
        .session_config()
        .clone()
        .with_extension(Arc::new(Forwarded(assertion)));
    // `TaskContext` is not `Clone`, so it is rebuilt from its own accessors. Per query on the
    // coordinator and per stage materialization on a worker — never per batch.
    Arc::new(TaskContext::new(
        base.task_id(),
        base.session_id(),
        config,
        base.scalar_functions().clone(),
        base.aggregate_functions().clone(),
        base.window_functions().clone(),
        base.runtime_env(),
    ))
}

/// The assertion a [`TaskContext`] carries, if any — what
/// [`FlightReaderExec::execute`](crate::remote::FlightReaderExec) puts on its outbound request.
pub fn forwarded(ctx: &TaskContext) -> Option<SignedAssertion> {
    ctx.session_config()
        .get_extension::<Forwarded>()
        .map(|forwarded| forwarded.0.clone())
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn unix_seconds(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        // A clock before the epoch is not a thing a fleet recovers from by guessing; 0 makes every
        // assertion it mints instantly expired, which is the safe direction.
        .unwrap_or(0)
}

fn put_i64(buf: &mut Vec<u8>, value: i64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn put_str(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

fn put_strs(buf: &mut Vec<u8>, values: &[String]) {
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        put_str(buf, value);
    }
}

fn truncated(what: &str, wanted: usize, had: usize) -> AssertionError {
    AssertionError::Malformed(format!(
        "truncated {what}: wanted {wanted} bytes, had {had}"
    ))
}

fn take_u8(buf: &[u8]) -> Result<(u8, &[u8]), AssertionError> {
    match buf.split_first() {
        Some((first, rest)) => Ok((*first, rest)),
        None => Err(truncated("version", 1, 0)),
    }
}

fn take_u32(buf: &[u8]) -> Result<(u32, &[u8]), AssertionError> {
    if buf.len() < 4 {
        return Err(truncated("length prefix", 4, buf.len()));
    }
    let (head, rest) = buf.split_at(4);
    Ok((u32::from_le_bytes(head.try_into().expect("4 bytes")), rest))
}

fn take_i64(buf: &[u8]) -> Result<(i64, &[u8]), AssertionError> {
    if buf.len() < 8 {
        return Err(truncated("integer", 8, buf.len()));
    }
    let (head, rest) = buf.split_at(8);
    Ok((i64::from_le_bytes(head.try_into().expect("8 bytes")), rest))
}

fn take_str(buf: &[u8]) -> Result<(String, &[u8]), AssertionError> {
    let (len, rest) = take_u32(buf)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(truncated("string", len, rest.len()));
    }
    let (bytes, rest) = rest.split_at(len);
    let value = std::str::from_utf8(bytes)
        .map_err(|e| AssertionError::Malformed(format!("string is not utf-8: {e}")))?;
    Ok((value.to_string(), rest))
}

fn take_strs(buf: &[u8]) -> Result<(Vec<String>, &[u8]), AssertionError> {
    let (count, mut rest) = take_u32(buf)?;
    // The count is read before the strings are, so a corrupt buffer could otherwise ask for four
    // billion allocations. Every string costs at least its own 4-byte length prefix, so the
    // remaining buffer bounds the count exactly. (The MAC has already verified, so this is
    // defence against a bug rather than an attacker — which is the right way round.)
    if count as usize > rest.len() / 4 {
        return Err(AssertionError::Malformed(format!(
            "plan assertion claims {count} strings but only {} bytes remain",
            rest.len()
        )));
    }
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (value, tail) = take_str(rest)?;
        values.push(value);
        rest = tail;
    }
    Ok((values, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

    fn key() -> AssertionKey {
        AssertionKey::derive("fleet-secret")
    }

    fn sample(now: SystemTime) -> PlanAssertion {
        PlanAssertion {
            account_id: Some(7),
            user: Some("alice".to_string()),
            issued_at: unix_seconds(now),
            expires_at: unix_seconds(now) + DEFAULT_TTL.as_secs() as i64,
            prefixes: vec!["file:///wh/acct_7__lldb/sales/orders/data/".to_string()],
            objects: vec!["SELECT on table lldb.sales.orders".to_string()],
        }
    }

    /// A deterministic-per-call Ed25519 key pair. `ring` generates PKCS#8, which is what
    /// `SigningKey::from_pkcs8` takes and what an operator will actually be handed.
    fn signing_key() -> SigningKey {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate");
        SigningKey::from_pkcs8(pkcs8.as_ref()).expect("load the key just generated")
    }

    /// **The property #127 exists for**: a holder of the public half can verify, and cannot mint.
    ///
    /// Expressed as a round trip plus the key-set lookup, because those are the two things a worker
    /// actually does — and asserting the payload survives intact is what makes the signature check
    /// meaningful rather than a bare boolean.
    #[test]
    fn a_signed_assertion_verifies_against_the_public_half_alone() {
        let now = SystemTime::now();
        let key = signing_key();
        let signed = sample(now).sign_with(&key).expect("sign");

        let public = key.verifying_key();
        // A worker holds only this: 32 bytes of public key and the id derived from them.
        let verified = signed
            .verify_signed(std::slice::from_ref(&public), now)
            .expect("the public half must verify what the private half signed");
        assert_eq!(verified, sample(now));
        // The id is derived identically on both sides, so no registry has to agree in advance.
        assert_eq!(public.id(), key.id());
    }

    /// A key that did not sign it must not verify it — otherwise the test above passes for a
    /// verifier that checks nothing.
    #[test]
    fn another_key_does_not_verify_it_and_the_refusal_names_the_id_it_wanted() {
        let now = SystemTime::now();
        let signer = signing_key();
        let stranger = signing_key();
        let signed = sample(now).sign_with(&signer).expect("sign");

        let error = signed
            .verify_signed(&[stranger.verifying_key()], now)
            .expect_err("a key that signed nothing must not verify it");
        let rendered = error.to_string();
        // The id it *wanted* and the ids it *has* — the whole diagnosis for an operator staring at
        // a stale accepted set.
        assert!(rendered.contains(&signer.id().to_string()), "{rendered}");
        assert!(rendered.contains(&stranger.id().to_string()), "{rendered}");

        // An empty set is the same refusal, not a pass. This is the fail-closed case.
        assert!(signed.verify_signed(&[], now).is_err());
    }

    /// A **set**, because that is what makes rotation three rolling-safe passes rather than an
    /// outage: during the window both keys are accepted, and an assertion signed by either verifies.
    #[test]
    fn a_key_set_accepts_every_key_in_it_which_is_what_makes_rotation_rolling() {
        let now = SystemTime::now();
        let old = signing_key();
        let new = signing_key();
        let during_rotation = [old.verifying_key(), new.verifying_key()];

        for key in [&old, &new] {
            let signed = sample(now).sign_with(key).expect("sign");
            signed
                .verify_signed(&during_rotation, now)
                .unwrap_or_else(|e| panic!("key {} must verify during the window: {e}", key.id()));
        }

        // …and narrowing the set is what retires a key: pass 3 of the procedure.
        let after = [new.verifying_key()];
        let by_old = sample(now).sign_with(&old).expect("sign");
        assert!(
            by_old.verify_signed(&after, now).is_err(),
            "a retired key must stop verifying once it leaves the set"
        );
    }

    /// **No downgrade.** A boundary that accepts signatures must not fall back to the MAC, and the
    /// converse: a v1 verifier must not be talked into accepting a v2 payload it cannot check.
    #[test]
    fn the_two_formats_do_not_verify_each_other() {
        let now = SystemTime::now();
        let signing = signing_key();
        let macd = sample(now).sign(&key()).expect("mac");
        let signed = sample(now).sign_with(&signing).expect("sign");

        // A MAC'd assertion presented where signatures are required: refused, and the message says
        // why rather than leaving an operator to guess.
        let error = macd
            .verify_signed(&[signing.verifying_key()], now)
            .expect_err("a MAC must not satisfy a signature check");
        assert!(error.to_string().contains("format version 1"), "{error}");

        // And the signed one presented to the symmetric verifier: also refused. The MAC is computed
        // over the whole payload, so the v2 version byte and key id are inside what it covers.
        assert!(
            signed.verify(&key(), now).is_err(),
            "a signature must not satisfy a MAC check"
        );
    }

    /// **The test the rest of this file cannot substitute for: a correct key id and a bad
    /// signature.**
    ///
    /// Every other negative case here refuses at the key-id *lookup*, which means none of them
    /// would notice if the signature were never checked at all — a suite that passes with
    /// `verify_signed`'s verification step deleted is a suite asserting nothing about signatures.
    /// This was caught by deleting exactly that step and watching every test stay green, so the
    /// test exists because the omission was real rather than hypothetical.
    ///
    /// Tampering with the **body** rather than the key id is what makes it bite: the id still
    /// matches, so the lookup succeeds and the signature is the only thing left that can refuse it.
    #[test]
    fn a_correct_key_id_with_a_bad_signature_is_refused_by_the_signature() {
        let now = SystemTime::now();
        let key = signing_key();
        let signed = sample(now).sign_with(&key).expect("sign");
        let (mut payload, signature) = signed.split().expect("split");

        // Flip a bit well past the version byte and the key id, inside the body.
        let body_byte = 1 + KEY_ID_BYTES + 2;
        payload[body_byte] ^= 0x01;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let forged = SignedAssertion(format!(
            "{}.{}",
            b64.encode(&payload),
            b64.encode(&signature)
        ));

        // The lookup must SUCCEED here — otherwise this test degenerates into the one above.
        let (_, named) = PlanAssertion::decode_versioned(&payload).expect("still decodes");
        assert_eq!(
            named,
            Some(key.id()),
            "the tampered payload must still name the real key, or this proves nothing"
        );

        assert!(
            matches!(
                forged.verify_signed(&[key.verifying_key()], now),
                Err(AssertionError::BadSignature)
            ),
            "a payload the signature does not cover must be refused by the signature check"
        );
    }

    /// The signature covers the key id, so it cannot be rewritten to point a verifier elsewhere.
    #[test]
    fn tampering_with_the_key_id_invalidates_the_signature() {
        let now = SystemTime::now();
        let key = signing_key();
        let signed = sample(now).sign_with(&key).expect("sign");
        let (mut payload, signature) = signed.split().expect("split");

        // Flip a bit in the key id — byte 1, immediately after the version byte.
        payload[1] ^= 0x01;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let forged = SignedAssertion(format!(
            "{}.{}",
            b64.encode(&payload),
            b64.encode(&signature)
        ));

        // It now names a key nobody has, so it is refused at the lookup…
        assert!(forged.verify_signed(&[key.verifying_key()], now).is_err());

        // …and even if that id happened to be in the set, the signature is over the id too. Prove
        // that directly rather than relying on the lookup: verify the tampered payload against the
        // real key, which is what the lookup would have selected.
        assert!(
            !key.verifying_key().verify(&payload, &signature),
            "the signature must cover the key id, not merely sit beside it"
        );
    }

    /// Expiry is checked **after** the signature, for both formats — a forged payload must never
    /// influence a decision, including its own.
    #[test]
    fn a_signed_assertion_expires_like_a_macd_one() {
        let now = SystemTime::now();
        let key = signing_key();
        let signed = sample(now).sign_with(&key).expect("sign");
        let long_after = now + DEFAULT_TTL + CLOCK_SKEW_ALLOWANCE + Duration::from_secs(1);
        assert!(matches!(
            signed.verify_signed(&[key.verifying_key()], long_after),
            Err(AssertionError::Expired { .. })
        ));
    }

    /// Neither key type may print its material, because both end up inside config a binary logs.
    #[test]
    fn a_signing_key_never_renders_its_material() {
        let key = signing_key();
        let rendered = format!("{key:?}");
        assert!(!rendered.contains("SigningKey(ed"), "{rendered}");
        assert!(rendered.contains("****"), "{rendered}");
        // The id is safe and is the useful half — it is what an operator matches against a set.
        assert!(rendered.contains(&key.id().to_string()), "{rendered}");
    }

    #[test]
    fn an_assertion_round_trips_through_its_own_encoding() {
        let now = SystemTime::now();
        let assertion = sample(now);
        let decoded = PlanAssertion::decode(&assertion.encode()).expect("round trip");
        assert_eq!(decoded, assertion);

        // The absent spellings survive as absences rather than as `0` / `""`.
        let anonymous = PlanAssertion {
            account_id: None,
            user: None,
            objects: Vec::new(),
            ..assertion
        };
        let decoded = PlanAssertion::decode(&anonymous.encode()).expect("round trip");
        assert_eq!(decoded.account_id, None);
        assert_eq!(decoded.user, None);
        assert!(decoded.objects.is_empty());
    }

    #[test]
    fn a_signed_assertion_verifies_and_nothing_else_does() {
        let now = SystemTime::now();
        let signed = sample(now).sign(&key()).expect("sign");
        let back = signed.verify(&key(), now).expect("verifies");
        assert_eq!(back, sample(now));

        // A different fleet secret is a different key, and its MAC does not verify. This is also the
        // assertion that key derivation actually depends on the secret.
        let other = AssertionKey::derive("another-fleet");
        assert!(matches!(
            signed.verify(&other, now),
            Err(AssertionError::BadSignature)
        ));
        assert_ne!(key(), other);
    }

    #[test]
    fn tampering_with_the_payload_is_refused_before_it_is_trusted() {
        let now = SystemTime::now();
        let key = key();
        let signed = sample(now).sign(&key).expect("sign");

        // Widen the coverage to another tenant's root and re-encode, keeping the original MAC —
        // the exact forgery the MAC exists to stop.
        let mut forged = sample(now);
        forged
            .prefixes
            .push("file:///wh/acct_99__lldb/sales/orders/data/".to_string());
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let mac = signed.0.split_once('.').expect("shape").1;
        let tampered = SignedAssertion(format!("{}.{mac}", b64.encode(forged.encode())));
        assert!(matches!(
            tampered.verify(&key, now),
            Err(AssertionError::BadSignature)
        ));

        // …and so is anything that is not `<payload>.<mac>` at all.
        for bad in ["", "no-dot", "!!!.!!!", "."] {
            assert!(
                SignedAssertion(bad.to_string()).verify(&key, now).is_err(),
                "`{bad}` must be refused"
            );
        }
    }

    #[test]
    fn an_expired_assertion_is_refused_but_a_minute_of_skew_is_tolerated() {
        let now = SystemTime::now();
        let key = key();
        let short = PlanAssertion {
            expires_at: unix_seconds(now),
            ..sample(now)
        };
        let signed = short.sign(&key).expect("sign");

        // Inside the skew allowance: still good.
        signed
            .verify(&key, now + CLOCK_SKEW_ALLOWANCE - Duration::from_secs(5))
            .expect("a fleet whose clocks differ by seconds still works");

        // Past it: refused, and the error says when.
        let error = signed
            .verify(&key, now + CLOCK_SKEW_ALLOWANCE + Duration::from_secs(120))
            .expect_err("an expired assertion is not a credential");
        assert!(matches!(error, AssertionError::Expired { .. }), "{error}");
        assert!(error.to_string().contains("expired"), "{error}");
    }

    #[test]
    fn covering_is_by_directory_and_respects_segment_boundaries() {
        let assertion = PlanAssertion {
            prefixes: vec!["file:///wh/sales/".to_string()],
            ..PlanAssertion::default()
        };

        assertion
            .covers_all(&["file:///wh/sales/a.parquet".to_string()])
            .expect("a file in the covered directory");
        assertion
            .covers_all(&[])
            .expect("a stage that reads nothing is trivially covered");

        // The bug a naive `starts_with` on an unterminated prefix would ship: a sibling directory
        // whose name merely begins with the covered one.
        let error = assertion
            .covers_all(&["file:///wh/salesforce/a.parquet".to_string()])
            .expect_err("a different directory must not be covered");
        assert!(
            matches!(error, AssertionError::NotCovered { .. }),
            "{error}"
        );

        // Another store entirely, and another tenant's root.
        assert!(
            assertion
                .covers_all(&["s3://bucket/wh/sales/a.parquet".to_string()])
                .is_err()
        );
        assert!(
            assertion
                .covers_all(&["file:///wh/other/a.parquet".to_string()])
                .is_err()
        );

        // A relative segment could escape a prefix it matches, so it is refused outright.
        let error = assertion
            .covers_all(&["file:///wh/sales/../other/a.parquet".to_string()])
            .expect_err("traversal is refused");
        assert!(matches!(error, AssertionError::Traversal(_)), "{error}");
    }

    #[test]
    fn prefixes_are_the_parent_directory_deduplicated() {
        let reads = vec![
            "file:///wh/sales/orders/data/b.parquet".to_string(),
            "file:///wh/sales/orders/data/a.parquet".to_string(),
            "file:///wh/sales/lineitem/data/a.parquet".to_string(),
        ];
        assert_eq!(
            covering_prefixes(&reads),
            vec![
                "file:///wh/sales/lineitem/data/".to_string(),
                "file:///wh/sales/orders/data/".to_string(),
            ]
        );
    }

    #[test]
    fn a_plan_that_reads_more_than_a_header_can_carry_is_refused_by_name() {
        let assertion = PlanAssertion {
            prefixes: (0..500)
                .map(|i| format!("file:///wh/a-rather-long-directory-name-number-{i}/"))
                .collect(),
            ..PlanAssertion::default()
        };
        let error = assertion
            .sign(&key())
            .expect_err("a header has a size a fleet's transport will actually carry");
        let message = format!("{error:#}");
        assert!(message.contains("500 distinct location"), "{message}");
    }

    #[test]
    fn the_posture_follows_the_fleet_secret_and_nothing_else() {
        assert!(!PlanAuth::from_fleet_auth(&FleetAuth::Open).is_required());
        assert!(PlanAuth::from_fleet_auth(&FleetAuth::Required("s3cret".into())).is_required());

        // Disabled ignores whatever is presented, exactly as an open worker ignores a token — which
        // is what keeps `cargo run -p lldb-qe-worker` working with no configuration.
        let open = PlanAuth::from_fleet_auth(&FleetAuth::Open);
        assert!(
            open.verify(None, SystemTime::now())
                .expect("open")
                .is_none()
        );
        assert!(
            open.verify(Some("nonsense"), SystemTime::now())
                .expect("open")
                .is_none()
        );
        assert!(
            open.check_cover(None, &["file:///anything".to_string()])
                .is_ok()
        );
        assert!(
            open.sign(&PlanAssertion::default())
                .expect("open")
                .is_none()
        );

        // Required refuses an absent one as `Missing`, which maps to UNAUTHENTICATED.
        let closed = PlanAuth::from_fleet_auth(&FleetAuth::Required("s3cret".into()));
        assert!(matches!(
            closed.verify(None, SystemTime::now()),
            Err(AssertionError::Missing)
        ));
    }

    #[test]
    fn debug_never_prints_the_key_or_the_assertion() {
        let auth = PlanAuth::from_fleet_auth(&FleetAuth::Required("hunter2".into()));
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");

        let signed = sample(SystemTime::now()).sign(&key()).expect("sign");
        let rendered = format!("{signed:?}");
        assert!(!rendered.contains(signed.as_header_value()), "{rendered}");
        assert!(rendered.contains("****"), "{rendered}");
    }

    /// Write one parquet file **in its own directory** and plan a scan of it, so the walk runs
    /// against a real `DataSourceExec` rather than a hand-built one.
    ///
    /// One directory per table is how both an Iceberg warehouse and this engine's listing tables lay
    /// data out, and it is the layout the covering check's granularity assumes — see
    /// [`covering_prefixes`] and the module docs.
    async fn scan_plan(dir: &std::path::Path, name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let table_dir = dir.join(name);
        std::fs::create_dir_all(&table_dir).expect("table dir");
        let path = table_dir.join(format!("{name}.parquet"));
        let file = std::fs::File::create(&path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let ctx = SessionContext::new();
        ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .expect("register");
        ctx.sql(&format!("SELECT n FROM {name}"))
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical")
    }

    #[tokio::test]
    async fn the_read_walk_descends_into_a_remote_stage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let local = scan_plan(dir.path(), "local").await;
        let remote = scan_plan(dir.path(), "remote").await;

        // A `FlightReaderExec` reports no children on purpose — its sub-plan runs elsewhere — so a
        // plain tree walk would miss the files a worker is about to be asked to read. If this ever
        // regresses, every worker-to-worker pull is refused as uncovered.
        let staged: Arc<dyn ExecutionPlan> = Arc::new(
            FlightReaderExec::new("http://w:50051", 0, Arc::clone(&remote)).expect("stage leaf"),
        );
        assert!(staged.children().is_empty(), "test premise");

        let reads = plan_reads(&staged);
        assert_eq!(reads.len(), 1, "{reads:?}");
        assert!(reads[0].ends_with("remote.parquet"), "{reads:?}");

        // And both halves of a plan that mixes local scans with remote stages.
        let combined = datafusion::physical_plan::union::UnionExec::try_new(vec![local, staged])
            .expect("union");
        let reads = plan_reads(&combined);
        assert_eq!(reads.len(), 2, "{reads:?}");
    }

    #[tokio::test]
    async fn an_assertion_minted_for_a_plan_covers_exactly_that_plan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plan = scan_plan(dir.path(), "orders").await;
        let auth = PlanAuth::from_fleet_auth(&FleetAuth::Required("s3cret".into()));
        let now = SystemTime::now();

        let identity = QueryIdentity {
            account_id: Some(7),
            user: Some("alice".to_string()),
            objects: vec!["SELECT on table lldb.sales.orders".to_string()],
        };
        let signed = auth
            .mint(&identity, &plan, now)
            .expect("mint")
            .expect("a closed fleet mints one");
        let verified = auth
            .verify(Some(signed.as_header_value()), now)
            .expect("verifies")
            .expect("a closed fleet checks one");

        assert_eq!(verified.assertion.account_id, Some(7));
        assert_eq!(verified.assertion.user.as_deref(), Some("alice"));
        assert_eq!(verified.assertion.objects, identity.objects);
        auth.check_cover(Some(&verified), &plan_reads(&plan))
            .expect("the plan it was minted for is covered");

        // Another plan over another table's directory is not — which is the whole point: an
        // assertion is not a bearer token for arbitrary plans.
        let other = scan_plan(dir.path(), "payroll").await;
        let error = auth
            .check_cover(Some(&verified), &plan_reads(&other))
            .expect_err("a plan reading somewhere else must be refused");
        assert!(
            matches!(error, AssertionError::NotCovered { .. }),
            "{error}"
        );

        // And the granularity, asserted rather than left to the docs: a *sibling file in the same
        // directory* IS covered. That is what "the verifiable unit is a directory, not a file" costs,
        // and a reader deciding whether it is enough for their deployment should see it here.
        let sibling = plan_reads(&plan)[0].replace("orders.parquet", "orders-2.parquet");
        auth.check_cover(Some(&verified), &[sibling])
            .expect("directory granularity covers a sibling file");
    }

    #[tokio::test]
    async fn a_task_context_carries_the_assertion_and_an_absent_one_changes_nothing() {
        let ctx = SessionContext::new();
        let base = ctx.task_ctx();
        assert!(forwarded(&base).is_none(), "nothing is carried by default");

        let unchanged = task_ctx_with(&base, None);
        assert!(Arc::ptr_eq(&base, &unchanged), "None must cost nothing");

        let signed = sample(SystemTime::now()).sign(&key()).expect("sign");
        let carrying = task_ctx_with(&base, Some(signed.clone()));
        assert_eq!(
            forwarded(&carrying).expect("carried").as_header_value(),
            signed.as_header_value()
        );
        // The rebuilt context is otherwise the same one, or an operator's UDF lookup would fail
        // under a closed fleet and work under an open one.
        assert_eq!(carrying.session_id(), base.session_id());
        assert_eq!(
            carrying.scalar_functions().len(),
            base.scalar_functions().len()
        );
    }
}
