#!/usr/bin/env bash
# Fail if a binary target in this workspace neither calls `check_fleet_posture_from_env()` nor says,
# in its own source, why it does not. Also fail if `infra/fleet-posture.json` — the committed copy of
# that classification, which the CDK tests read — is not what this run just derived.
#
# `crates/lldb-qe-control/src/auth.rs` makes `LLDB_REQUIRE_FLEET_TOKEN` a deployment's assertion that
# its worker fleet is closed, and the assertion is only worth anything on a process that checks it.
# The check is three lines in `main`. Omitting it compiles, lints, tests and runs — the deployment's
# claim is simply not evaluated on that process, and the evidence of that is nothing at all. #90's
# stale module references were the same class of defect (a convention, failing silently), and got
# `scripts/check-path-refs.sh`; this is that script for this convention.
#
# The enumeration is INVERTED on purpose, and that inversion is the whole design. A sweep for the
# binaries known to receive the token goes stale the moment a fourth one is added, which is the
# defect rather than the fix. So the source of truth is `cargo metadata` — every `[[bin]]` cargo
# builds, including the `src/bin/*.rs` targets nobody declares by hand — and the default for a new
# binary is FAILING. Joining the fleet means calling the check; not joining it means writing down
# that you are not, in the diff that adds the binary.
#
# What this can and cannot see, stated because a check whose reach is overclaimed is worse than a
# narrow one: it greps the target's OWN root source file for a call-shaped occurrence of the guard,
# and for the opt-out marker. It does not follow `mod` declarations and does not resolve calls, so a
# binary reaching the guard through a helper reads as missing it — deliberately, because the
# alternative is scanning a whole package, and `lldb-qe-coordinator` ships two binaries where one
# calling it would then cover the other. It also cannot tell a call from a comment quoting one. That
# is the honest bound: this catches the omission, not a determined author.
#
# ---- `--json`, and why the roster is committed rather than derived where it is read (#126) --------
#
# The above guards the CODE. It cannot guard the DEPLOY: `infra/lib/lldb-stack.ts` injects
# `LLDB_REQUIRE_FLEET_TOKEN` into task definitions, and a container that gets the variable while
# running an opted-out binary deploys clean and asserts nothing. The jest test that closes that gap
# needs the very classification computed above — and hard-coding the checking binaries there is
# exactly the staleness this script was built to design away, one repository over.
#
# So `--json` emits the classification, it is committed as `$ROSTER`, and the run below re-derives it
# and fails when the committed copy disagrees. That puts the freshness check in the one CI job that
# already has cargo and jq (`Fleet posture`), and leaves `npm test` reading a static file — the
# `cdk synth • test` job has node and nothing else, and a jest test that shelled out to cargo would
# either fail there or be written to skip when the tools are missing, which reinvents the silence
# #116 was filed about.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Call-shaped, with the open paren: `use lldb_qe_core::auth::check_fleet_posture_from_env;` alone
# imports the guard without running it, and an import is not a check.
CALL='check_fleet_posture_from_env('

# A binary that does not join the fleet opts out with `fleet-posture-allow: <reason>` anywhere in its
# own source — `scripts/check-path-refs.sh`'s `path-refs-allow:` idiom, and the same argument for
# putting it in the file rather than in a list here: an opt-out that travels with the binary cannot
# outlive it, and a reviewer meets the claim while reading the binary that makes it. The reason is
# required and must be on the marker's line, because an opt-out nobody had to justify is a list of
# names, and a list of names is what a reviewer skims past.
MARKER='fleet-posture-allow:'

# The committed classification. Read by `infra/test/fleet-posture.test.ts`; written by nothing but
# this script.
ROSTER='infra/fleet-posture.json'
SELF='scripts/check-fleet-posture.sh'

# Written into the artifact itself, because a generated file that does not say so gets hand-edited.
GENERATED_NOTE="GENERATED — do not hand-edit. Written by \`$SELF --json\`, which classifies every \
binary target \`cargo metadata\` reports; that script's normal run re-derives this and fails when \
the committed copy disagrees, so an edit here is reverted rather than believed. Regenerate with: \
$SELF --json > $ROSTER. Read by infra/test/fleet-posture.test.ts, which requires every container \
handed LLDB_REQUIRE_FLEET_TOKEN to run a binary listed here with posture \"checks\" — a binary \
absent from this list FAILS that test rather than being skipped as unknown."

usage() {
    cat >&2 <<EOF
usage: $SELF [--json]

  (no argument)  classify every binary target, then verify $ROSTER matches that
                 classification. An unclassifiable binary and a stale roster are both failures.
  --json         write that classification to stdout, in $ROSTER's exact bytes, and
                 nothing else. Regenerate the roster with:

                     $SELF --json > $ROSTER
EOF
}

emit_json=0
case ${1-} in
    --json) emit_json=1 ;;
    '') ;;
    *)
        usage
        exit 2
        ;;
esac
if [ "$#" -gt 1 ]; then
    usage
    exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required (cargo metadata is JSON, and hand-parsing it is how a check starts lying)" >&2
    exit 2
fi

# Capture cargo separately and check ITS status, `scripts/check-dep-dupes.sh`'s rule: piping straight
# into jq would turn a cargo failure into an empty target list, and an empty list passes — the check
# would report loudest exactly when it stopped working.
#
# `--no-deps` because only workspace members can carry a binary of ours, and it also means this
# resolves nothing and touches no network.
if ! meta=$(cargo metadata --format-version 1 --no-deps 2>&1); then
    echo "cargo metadata failed:" >&2
    printf '%s\n' "$meta" >&2
    exit 2
fi

targets=$(printf '%s' "$meta" | jq -r '
    .packages[] | .name as $pkg | .targets[]
    | select(.kind | index("bin"))
    | "\($pkg)\t\(.name)\t\(.src_path)"
' | sort)

if [ -z "$targets" ]; then
    echo "cargo metadata reported no binary targets, which cannot be true here" >&2
    exit 2
fi

fail=0
total=0
checked=0
excused=0
root=$PWD
# The human report, accumulated rather than streamed so `--json` can keep stdout to itself. The
# bytes, and their order, are what they were before `--json` existed.
report=
# One compact JSON object per target, in the same (sorted) order — `jq -s` slurps them below.
entries=

while IFS=$'\t' read -r pkg bin src; do
    total=$((total + 1))
    rel=${src#"$root/"}

    calls=0
    if grep -qF -- "$CALL" "$src"; then
        calls=1
    fi

    marked=0
    if grep -qF -- "$MARKER" "$src"; then
        marked=1
    fi
    # Not `| head -n 1`: under `pipefail` a second marker line would let head close the pipe while
    # sed still had output, and the resulting SIGPIPE aborts the whole run under `set -e` instead of
    # reporting anything. Take the first line with parameter expansion, which cannot fail.
    reasons=$(sed -n "s/.*${MARKER}[[:space:]]*//p" "$src")
    reason=${reasons%%$'\n'*}

    posture=
    if [ "$calls" -eq 1 ] && [ "$marked" -eq 1 ]; then
        line=$(printf '%s:1: %s calls the posture check AND claims an exemption from it' "$rel" "$bin")
        fail=1
    elif [ "$calls" -eq 1 ]; then
        checked=$((checked + 1))
        posture=checks
        line=$(printf '  %-22s checks the fleet posture' "$bin")
    elif [ "$marked" -eq 1 ] && [ -n "$reason" ]; then
        excused=$((excused + 1))
        posture=opted-out
        line=$(printf '  %-22s opted out: %s' "$bin" "$reason")
    elif [ "$marked" -eq 1 ]; then
        line=$(printf '%s:1: %s opts out with no reason on the marker line' "$rel" "$bin")
        fail=1
    else
        line=$(printf '%s:1: %s (package %s) neither calls the posture check nor opts out' \
            "$rel" "$bin" "$pkg")
        fail=1
    fi
    report+="$line"$'\n'

    # `--arg` for every value: a reason is free-form prose from a source comment, and building JSON
    # by concatenation is the same bug class as hand-parsing `cargo metadata`.
    entry=$(jq -nc \
        --arg name "$bin" --arg package "$pkg" --arg source "$rel" \
        --arg posture "$posture" --arg reason "$reason" \
        '{name: $name, package: $package, source: $source, posture: $posture,
          reason: (if $posture == "checks" then null else $reason end)}')
    entries+="$entry"$'\n'
done <<EOF
$targets
EOF

if [ "$fail" -ne 0 ]; then
    # In `--json` mode this goes to stderr with the rest: stdout is the artifact, and a workspace
    # this script cannot classify has no roster to write.
    if [ "$emit_json" -eq 1 ]; then
        printf '%s' "$report" >&2
    else
        printf '%s' "$report"
    fi
    cat >&2 <<EOF

Every binary this workspace builds must either check the fleet posture or say why it does not.

If the binary can receive LLDB_FLEET_TOKEN, call it from main() before anything binds a port:

    use lldb_qe_core::auth::check_fleet_posture_from_env;   // or lldb_qe_control::auth::…
    check_fleet_posture_from_env()?;

With LLDB_REQUIRE_FLEET_TOKEN absent it is a no-op, so this costs a single-node run nothing.

If it genuinely does not join the fleet — an operator one-shot that binds no port and is handed no
fleet secret — put the marker in its source with the reason on the same line:

    //! ${MARKER} <why this binary never receives the fleet token>

Do not add the marker to make this pass. The variable exists because a deployment asserted that its
fleet is closed; a process that skips the check answers that assertion with silence, which is the
one failure mode nobody notices. See crates/lldb-qe-control/src/auth.rs.
EOF
    exit 1
fi

derived=$(printf '%s' "$entries" | jq -s \
    --arg generated "$GENERATED_NOTE" --arg generator "$SELF --json" \
    '{_generated: $generated, generator: $generator, binaries: .}')

if [ "$emit_json" -eq 1 ]; then
    printf '%s\n' "$derived"
    exit 0
fi

printf '%s' "$report"

# ---- The roster is a derivation, so it is verified rather than trusted ----------------------------
if [ ! -f "$ROSTER" ]; then
    cat >&2 <<EOF

$ROSTER does not exist, and infra/test/fleet-posture.test.ts cannot classify a
deployed container's binary without it. Write it:

    $SELF --json > $ROSTER
EOF
    exit 1
fi

# `cmp` on the bytes rather than a string compare of two command substitutions: those strip trailing
# newlines, so a hand-edit that only added blank lines at the end would compare equal. The file is a
# generated artifact — equality means byte equality.
if ! printf '%s\n' "$derived" | cmp -s - "$ROSTER"; then
    {
        printf '\n%s is stale — it is not what this workspace classifies to.\n\n' "$ROSTER"
        # Options before operands: BSD diff (macOS) does not accept them after.
        diff -u -L "$ROSTER (committed)" -L "$SELF --json (derived)" \
            "$ROSTER" <(printf '%s\n' "$derived") || true
        cat <<EOF

Regenerate it and commit the result:

    $SELF --json > $ROSTER

Do not hand-edit it to match. It is a generated artifact, and the assertion it carries into
infra/test/fleet-posture.test.ts — that a container handed LLDB_REQUIRE_FLEET_TOKEN runs a binary
that actually checks it — is worth exactly as much as its derivation.
EOF
    } >&2
    exit 1
fi

printf 'fleet posture OK — %s binary targets, %s check it, %s opted out\n' \
    "$total" "$checked" "$excused"
