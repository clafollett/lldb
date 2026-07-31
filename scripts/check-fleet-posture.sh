#!/usr/bin/env bash
# Fail if a binary target in this workspace neither calls `check_fleet_posture_from_env()` nor says,
# in its own source, why it does not.
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

    if [ "$calls" -eq 1 ] && [ "$marked" -eq 1 ]; then
        printf '%s:1: %s calls the posture check AND claims an exemption from it\n' "$rel" "$bin"
        fail=1
    elif [ "$calls" -eq 1 ]; then
        checked=$((checked + 1))
        printf '  %-22s checks the fleet posture\n' "$bin"
    elif [ "$marked" -eq 1 ] && [ -n "$reason" ]; then
        excused=$((excused + 1))
        printf '  %-22s opted out: %s\n' "$bin" "$reason"
    elif [ "$marked" -eq 1 ]; then
        printf '%s:1: %s opts out with no reason on the marker line\n' "$rel" "$bin"
        fail=1
    else
        printf '%s:1: %s (package %s) neither calls the posture check nor opts out\n' \
            "$rel" "$bin" "$pkg"
        fail=1
    fi
done <<EOF
$targets
EOF

if [ "$fail" -ne 0 ]; then
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

printf 'fleet posture OK — %s binary targets, %s check it, %s opted out\n' \
    "$total" "$checked" "$excused"
