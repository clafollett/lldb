#!/usr/bin/env bash
# Fail if any tracked file mentions a `crates/<pkg>/…` path that does not exist.
#
# Prose in this repo points at the module carrying the reasoning behind a setting — see the module
# docs referenced from docker-compose.yml, Cargo.toml and the migrations. `git mv` renames files and
# leaves those strings behind, so the pointers rot silently every time a crate is split.
#
# This deliberately derives nothing from a hardcoded list of module names, crates or file
# extensions. The sweep originally proposed in #90 had all three, and missed three references in
# `.sql` migrations because `.sql` was not among its `--include` globs.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# A reference can be deliberately historical — docs/build-performance.md records commands run at a
# named commit, and repointing those would misreport what was measured. Such a file opts a specific
# path out with `path-refs-allow: <path>` anywhere in it. Naming the path rather than suppressing a
# line keeps the opt-out from silently covering a genuinely rotten reference added later.
allowed() {
    grep -qF "path-refs-allow: $2" -- "$1" 2>/dev/null
}

fail=0
while IFS= read -r hit; do
    file=${hit%%:*}
    rest=${hit#*:}
    line=${rest%%:*}
    ref=${rest#*:}

    # Trailing punctuation from prose ("…/tls.rs." / "…/auth.rs)") is not part of the path. A
    # trailing "/" marks a directory reference and survives; no real path ends in any other
    # non-path character, so stripping until one appears is safe.
    while [ -n "$ref" ]; do
        case ${ref#"${ref%?}"} in
            [A-Za-z0-9_/-]) break ;;
            *) ref=${ref%?} ;;
        esac
    done

    # Placeholders and globs are not paths to resolve.
    case $ref in
        *"<"* | *">"* | *"*"*) continue ;;
    esac

    [ -e "$ref" ] && continue
    allowed "$file" "$ref" && continue
    printf '%s:%s: no such path: %s\n' "$file" "$line" "$ref"
    fail=1
done < <(
    git grep -InoE --full-name 'crates/[a-z0-9][a-z0-9-]*/[A-Za-z0-9_./-]+' -- \
        ':!target' ':!*.lock' \
    | sort -u
)

if [ "$fail" -ne 0 ]; then
    cat >&2 <<'EOF'

Each path above is referenced in a tracked file but does not exist.

If a module moved between crates, repoint the reference — do not delete it. A pointer to a missing
file is worse than no pointer: the reader concludes the reference is stale and stops trusting the
ones that are still good.
EOF
    exit 1
fi

echo "path references OK"
