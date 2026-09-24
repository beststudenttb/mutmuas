#!/usr/bin/env bash
# Run the whole test suite (unit + in-process multi-node E2E + failure tests), then the multi-process E2E.
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/pytest -q "$@"
scripts/e2e-local.sh
