#!/usr/bin/env bash
# Fail if the dependency tree carries more than one version of a pinned crate.
#
# CLAUDE.md makes "one arrow / object_store / datafusion tree-wide" a standing constraint, because
# a serialized DataFusion plan is not compatible across versions — coordinator and workers must
# agree exactly. `iceberg-datafusion` is what pins the whole chain.
#
# This asserts the constraint itself rather than a duplicate TOTAL. A total drifts with every
# unrelated crate and goes stale within a month (#78: a comment claiming 48 sat next to a tree
# reporting 59), and it cannot even express the rule — a contributor can add a duplicate of one
# pinned crate while the total stays flat because something else deduped.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

PINNED='arrow|datafusion|object_store|iceberg'

# Capture cargo separately and check ITS status. Piping straight into `grep ... || true` would
# turn a cargo failure into an empty match and report success — the check would pass loudest
# exactly when it stopped working.
if ! tree=$(cargo tree -d --workspace 2>&1); then
    echo "cargo tree failed:" >&2
    printf '%s\n' "$tree" >&2
    exit 2
fi

dupes=$(printf '%s\n' "$tree" | grep -E "^($PINNED)[a-z0-9_-]* v" || true)

if [ -n "$dupes" ]; then
    echo "Duplicate versions of pinned crates:" >&2
    printf '%s\n' "$dupes" >&2
    cat >&2 <<'EOF'

Coordinator and workers must run the identical build — a serialized DataFusion plan is not
compatible across versions. These pins move together or not at all; see CLAUDE.md.
EOF
    exit 1
fi

echo "no duplicate versions of pinned crates"
