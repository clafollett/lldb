#!/usr/bin/env bash
#
# mint-fleet-tls.sh — mint the lldb fleet's TLS material and put it in Secrets Manager.
#
# This is the step `cdk deploy -c tls=fleet` deliberately does NOT do. The stack *imports* three
# secrets by name rather than creating them, so that CDK never holds a private key — a key CDK
# holds is a key that can reach `cdk.out`, and `cdk.out` is an artifact that gets copied around.
# Run this first; then deploy.
#
# What it makes, and why it is shaped this way:
#
#   a CA        self-signed, `basicConstraints=CA:TRUE`. rustls builds its RootCertStore from the
#               CA bundle and webpki refuses a trust anchor that is not marked as a CA, so
#               "one self-signed leaf, trusted directly" fails with an opaque UnknownIssuer.
#
#   one leaf    shared by every worker in every warehouse, carrying `DNS:<--domain>` (default
#               `fleet.lldb.local`). Not one per warehouse, and not IP SANs, and neither is a
#               shortcut:
#                 * a Fargate task's IP is allocated at task start and changes on every
#                   replacement and every scale event — which is exactly the elasticity
#                   `discovery.rs` exists to deliver — so no certificate minted in advance can
#                   name them, and the engine builds its TLS acceptor once at startup anyway;
#                 * the engine's dialing trust is process-global (a FlightReaderExec is serialized
#                   into a plan and can carry nothing per-call) while one coordinator dials several
#                   warehouses, so there is exactly one name available to verify against.
#               The stack sets LLDB_TLS_DOMAIN to that name: the URL's IP connects, the
#               certificate's own SAN verifies. `--domain` and the stack's `-c tlsDomain` are two
#               halves of ONE setting and must be given the same value — see the note this script
#               prints when they would differ.
#
# The **CA private key is never uploaded**. It stays in the output directory so it can re-issue a
# leaf later; keep it offline. Discard it and the next mint replaces the trust root — which is a
# phased rotation or a query outage, never a plain roll: see "Rotating" in infra/README.md.
#
# Usage:
#   ./scripts/mint-fleet-tls.sh                                   # default prefix, ./fleet-tls-ca
#   ./scripts/mint-fleet-tls.sh --prefix team/lldb-prod --days 90
#   ./scripts/mint-fleet-tls.sh --ca-dir ~/keys/lldb-ca           # re-issue from an existing CA
#   ./scripts/mint-fleet-tls.sh --domain fleet.example.com        # then deploy with the SAME name:
#     cd infra && npx cdk deploy -c imageTag=… -c tls=fleet -c tlsDomain=fleet.example.com
#
# Needs: openssl, and an AWS CLI already authenticated to the target account/region.

set -euo pipefail

PREFIX="lldb/fleet-tls"
# Matches the CDK stack's DEFAULT_FLEET_TLS_DOMAIN, so the two agree when neither is told otherwise
# — that is what every client is told to verify. Change it here and change it there (`-c tlsDomain`).
DEFAULT_DOMAIN="fleet.lldb.local"
DOMAIN="$DEFAULT_DOMAIN"
DAYS=825           # the CA/Browser Forum's cap on leaf lifetime; a sane habit even privately.
CA_DIR="./fleet-tls-ca"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --domain) DOMAIN="$2"; shift 2 ;;
    --days) DAYS="$2"; shift 2 ;;
    --ca-dir) CA_DIR="$2"; shift 2 ;;
    # The header block, which ends at the `Needs:` line — keep this range in step with it.
    -h|--help) sed -n '2,42p' "$0"; exit 0 ;;
    *) echo "ERROR: unknown argument '$1' (try --help)" >&2; exit 2 ;;
  esac
done

for tool in openssl aws; do
  command -v "$tool" >/dev/null 2>&1 || { echo "ERROR: $tool is not on PATH" >&2; exit 1; }
done

# A SAN list is comma-separated and a DNS name has no spaces, so either would corrupt the
# extension rather than fail — and a certificate with a mangled SAN fails at handshake time on a
# deployed fleet, which is the most expensive place to find out.
if [[ -z "$DOMAIN" || "$DOMAIN" =~ [[:space:],] ]]; then
  echo "ERROR: --domain must be a single DNS name with no spaces or commas (got '$DOMAIN')" >&2
  exit 2
fi

# The wildcard is DERIVED from whatever --domain ended up being, never hard-coded: a fixed
# `*.lldb.local` on a certificate issued for some other domain is an unrelated SAN nobody asked
# for, and it would be an unrelated name this CA is now vouching for.
#
# `${DOMAIN#*.}` drops the first label — `fleet.lldb.local` -> `lldb.local`. A single-label domain
# has no parent to wildcard, so it gets the exact name alone rather than a nonsense `*.fleet`.
if [[ "$DOMAIN" == *.* ]]; then
  SAN="DNS:$DOMAIN,DNS:*.${DOMAIN#*.}"
else
  SAN="DNS:$DOMAIN"
fi

WORK="$(mktemp -d)"
# The leaf's key exists only inside this directory and only for as long as the upload takes.
trap 'rm -rf "$WORK"' EXIT
umask 077
mkdir -p "$CA_DIR"

# ---------------------------------------------------------------------------
# 1. The CA — reused if one is already in --ca-dir, so a re-issued leaf does not
#    invalidate a fleet that is already trusting this root.
# ---------------------------------------------------------------------------
if [[ -f "$CA_DIR/ca.key" && -f "$CA_DIR/ca.crt" ]]; then
  echo "==> [1/3] Reusing the CA in $CA_DIR (delete it to start a new trust root)"
else
  echo "==> [1/3] Minting a CA into $CA_DIR"
  openssl req -x509 -newkey rsa:4096 -nodes -sha256 \
    -days $((DAYS * 2)) \
    -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.crt" \
    -subj "/CN=lldb fleet CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
  chmod 600 "$CA_DIR/ca.key"
fi

# ---------------------------------------------------------------------------
# 2. The leaf every worker serves.
# ---------------------------------------------------------------------------
echo "==> [2/3] Issuing a fleet certificate for $DOMAIN (SAN: $SAN, valid ${DAYS}d)"
openssl req -newkey rsa:2048 -nodes -sha256 \
  -keyout "$WORK/fleet.key" -out "$WORK/fleet.csr" \
  -subj "/CN=$DOMAIN" 2>/dev/null

# `extendedKeyUsage=serverAuth` because that is what a TLS server certificate is for, and rustls
# checks it. The wildcard is there so a client that dials by Cloud Map name — compose, or anything
# that skips discovery's IP expansion — validates against the same leaf.
openssl x509 -req -in "$WORK/fleet.csr" -sha256 -days "$DAYS" \
  -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" -CAcreateserial \
  -out "$WORK/fleet.crt" \
  -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n' \
    "$SAN") 2>/dev/null

# ---------------------------------------------------------------------------
# 3. Upload. Three whole-string secrets, not one with JSON keys: `ecs.Secret` grants read on a
#    whole secret, so only separate secrets keep the coordinator's execution role off the key.
# ---------------------------------------------------------------------------
echo "==> [3/3] Writing $PREFIX-{ca,cert,key} to Secrets Manager"
put_secret() {
  local name="$1" file="$2" description="$3"
  # `--secret-string file://` so the PEM never becomes a process argument, where it would be
  # visible in `ps` to anything else on the machine.
  if aws secretsmanager describe-secret --secret-id "$name" >/dev/null 2>&1; then
    aws secretsmanager put-secret-value --secret-id "$name" \
      --secret-string "file://$file" >/dev/null
    echo "    updated $name"
  else
    aws secretsmanager create-secret --name "$name" --description "$description" \
      --secret-string "file://$file" >/dev/null
    echo "    created $name"
  fi
}

put_secret "$PREFIX-ca"   "$CA_DIR/ca.crt"   "lldb fleet CA — what every role verifies peers against"
put_secret "$PREFIX-cert" "$WORK/fleet.crt"  "lldb fleet certificate — served by every worker"
put_secret "$PREFIX-key"  "$WORK/fleet.key"  "lldb fleet private key — workers only, never the coordinator"

# Deleted HERE rather than left to the EXIT trap, so that the claim printed below is true at the
# moment it is printed. The trap still runs and still cleans the rest up; this is not a substitute
# for it, it is the difference between a guarantee and an intention.
rm -f "$WORK/fleet.key"

cat <<EOF

Done. The fleet's private key was deleted from this machine after upload — it now exists only in
Secrets Manager. The CA private key is a separate thing and IS still here, in $CA_DIR/ca.key: it
was never uploaded. Keep it offline, or delete it and accept that the next mint replaces the trust
root — which is a phased rotation or a query outage, never a plain roll. See "Rotating" in
infra/README.md before you do that.

Next:
  cd infra && npx cdk deploy -c imageTag=<version+sha> -c tls=fleet$([[ "$PREFIX" != "lldb/fleet-tls" ]] && echo " -c tlsSecretPrefix=$PREFIX")$([[ "$DOMAIN" != "$DEFAULT_DOMAIN" ]] && echo " -c tlsDomain=$DOMAIN")

Rotating the leaf later — same CA, because this script reuses $CA_DIR: re-run it, then force a new
deployment so tasks pick the new material up (the engine reads its certificate once, at startup).
That roll is safe on its own; every task trusts the same root throughout.
  aws ecs update-service --cluster <ClusterName> --service <each WarehouseServices entry> \\
    --force-new-deployment
EOF

if [[ "$DOMAIN" != "$DEFAULT_DOMAIN" ]]; then
  cat >&2 <<EOF

NOTE: this certificate is issued for '$DOMAIN', not the default '$DEFAULT_DOMAIN'.
Every client verifies the name in LLDB_TLS_DOMAIN, so the deploy must carry the SAME name:

  -c tlsDomain=$DOMAIN

Deploying \`-c tls=fleet\` WITHOUT it leaves every client verifying '$DEFAULT_DOMAIN' against this
certificate, and every handshake in the fleet fails — coordinator to worker and worker to worker
alike, at handshake time, on a deployed fleet. Anywhere that is not the CDK stack (compose, a
hand-rolled task definition), set LLDB_TLS_DOMAIN=$DOMAIN yourself instead.
EOF
fi
