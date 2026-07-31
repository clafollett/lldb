#!/usr/bin/env bash
# Remove agent worktrees whose PR has already merged. Dry run unless given --apply.
#
# Each agent worktree is a full independent checkout **with its own `target/`** — that is what
# worktree isolation is for, and it is why they run 3-7 GB each rather than a few MB. Nothing
# removed them when the PR merged, and eight of them had quietly accumulated 36 GB (#107) — more
# than the entire disk budget the build-size work was optimising against, and about fifty times what
# gating the benches recovered (#97).
#
# Merged-ness comes from `gh pr list --state merged`, NOT from `git branch --merged`. Branches here
# land by SQUASH merge, which gives the merge commit a different identity, so `git branch --merged`
# calls a demonstrably merged branch unmerged. Trusting it would make this script refuse to clean up
# exactly the branches it exists for.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

apply=false
[ "${1:-}" = "--apply" ] && apply=true

# `git worktree list --porcelain` emits a blank-line-separated record per worktree. The main
# worktree is the first record and has no `branch` of its own to reclaim; `locked` marks one an
# agent is still using.
path="" branch="" locked=false
reclaimed_kb=0
found=0

consider() {
    [ -n "$path" ] || return 0
    [ "$path" != "$(pwd)" ] || return 0          # never the main worktree
    [ -n "$branch" ] || return 0

    local short=${branch#refs/heads/}

    if [ "$locked" = true ]; then
        printf 'skip  %-44s locked — an agent is still using it\n' "$short"
        return 0
    fi

    local dirty
    dirty=$(git -C "$path" status --porcelain 2>/dev/null | wc -l | tr -d ' ')
    if [ "$dirty" != "0" ]; then
        printf 'skip  %-44s %s uncommitted change(s)\n' "$short" "$dirty"
        return 0
    fi

    local merged
    merged=$(gh pr list --head "$short" --state merged --json number --jq '.[0].number' 2>/dev/null || true)
    if [ -z "$merged" ]; then
        printf 'skip  %-44s no merged PR\n' "$short"
        return 0
    fi

    local kb
    kb=$(du -sk "$path" 2>/dev/null | cut -f1)
    reclaimed_kb=$((reclaimed_kb + kb))
    found=$((found + 1))

    if [ "$apply" = true ]; then
        git worktree remove "$path"
        git branch -D "$short" >/dev/null 2>&1 || true
        printf 'removed %-42s PR #%s  %s MiB\n' "$short" "$merged" "$((kb / 1024))"
    else
        printf 'would remove %-37s PR #%s  %s MiB\n' "$short" "$merged" "$((kb / 1024))"
    fi
}

while IFS= read -r line; do
    case "$line" in
        worktree\ *) consider; path=${line#worktree }; branch=""; locked=false ;;
        branch\ *)   branch=${line#branch } ;;
        locked*)     locked=true ;;
    esac
done < <(git worktree list --porcelain)
consider

if [ "$found" -eq 0 ]; then
    echo "nothing to reclaim"
    exit 0
fi

if [ "$apply" = true ]; then
    git worktree prune
    printf '\nreclaimed %s MiB across %s worktree(s)\n' "$((reclaimed_kb / 1024))" "$found"
else
    printf '\n%s MiB across %s worktree(s) — re-run with --apply to remove\n' \
        "$((reclaimed_kb / 1024))" "$found"
fi
