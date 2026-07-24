#!/usr/bin/env bash
#
# bootstrap.sh — get a fresh clone of lldb ready to build and test.
#
# Idempotent: safe to run repeatedly. Each step skips work that is already done
# and says so. Honors SCALE_FACTOR (default 1) and generates TPC-H data into
# data/sf${SCALE_FACTOR}.
#
# Usage:
#   ./scripts/bootstrap.sh                 # scale factor 1 -> data/sf1
#   SCALE_FACTOR=10 ./scripts/bootstrap.sh # scale factor 10 -> data/sf10
#
set -euo pipefail

# Resolve the repo root from this script's own location so it works from any CWD.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SCALE_FACTOR="${SCALE_FACTOR:-1}"
DATA_DIR="data/sf${SCALE_FACTOR}"
LINEITEM="${DATA_DIR}/lineitem.parquet"

# ---------------------------------------------------------------------------
# 1. Toolchain check
# ---------------------------------------------------------------------------
echo "==> [1/4] Checking Rust toolchain (cargo, rustc)..."
if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then
  echo "ERROR: cargo and/or rustc were not found on your PATH." >&2
  echo "       Install the Rust toolchain via rustup: https://rustup.rs" >&2
  exit 1
fi
# rust-toolchain.toml pins the exact channel automatically, so no manual version
# juggling is needed — rustup selects the pinned toolchain on first build.
echo "    Found: $(cargo --version) / $(rustc --version)"
echo "    (rust-toolchain.toml pins the version automatically — nothing to juggle.)"

# ---------------------------------------------------------------------------
# 2. Install tpchgen-cli
# ---------------------------------------------------------------------------
echo "==> [2/4] Ensuring tpchgen-cli is installed..."
if command -v tpchgen-cli >/dev/null 2>&1; then
  echo "    Skipping: tpchgen-cli already on PATH ($(command -v tpchgen-cli))."
else
  echo "    Not found — running 'cargo install tpchgen-cli'..."
  cargo install tpchgen-cli
  if ! command -v tpchgen-cli >/dev/null 2>&1; then
    echo "ERROR: tpchgen-cli was installed but is still not on your PATH." >&2
    echo "       cargo installs binaries to ~/.cargo/bin — add it to your PATH, e.g.:" >&2
    echo '           export PATH="$HOME/.cargo/bin:$PATH"' >&2
    exit 1
  fi
  echo "    Installed: $(command -v tpchgen-cli)"
fi

# ---------------------------------------------------------------------------
# 3. Generate TPC-H data
# ---------------------------------------------------------------------------
echo "==> [3/4] Generating TPC-H data (scale factor ${SCALE_FACTOR}) into ${DATA_DIR}..."
if [ -f "$LINEITEM" ]; then
  echo "    Skipping: ${LINEITEM} already exists."
else
  tpchgen-cli -s "${SCALE_FACTOR}" --format=parquet --output-dir "${DATA_DIR}"
  echo "    Generated data into ${DATA_DIR}."
fi

# ---------------------------------------------------------------------------
# 4. Done
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Bootstrap complete. You're ready to build and test."
echo ""
echo "      cargo test"
echo ""
echo "============================================================"
