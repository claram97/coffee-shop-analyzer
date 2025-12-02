#!/usr/bin/env bash
set -euo pipefail
BASE="$(cd "$(dirname "$0")/.." && pwd)/results_finisher_state"
if [[ -d "$BASE" ]]; then
  echo "Cleaning finisher state under $BASE" >&2
  find "$BASE" -mindepth 1 -maxdepth 1 -type d -name 'finisher-*' -exec rm -rf {} +
else
  echo "No finisher state directory at $BASE" >&2
fi
