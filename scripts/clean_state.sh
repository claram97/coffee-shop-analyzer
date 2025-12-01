#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

clean_dir() {
  local dir="$1"
  if [[ -d "$dir" ]]; then
    echo "Cleaning $dir" >&2
    find "$dir" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
  else
    echo "Skipping $dir (not found)" >&2
  fi
}

clean_dir "$ROOT/results_finisher_state"
clean_dir "$ROOT/filter_router_state"
clean_dir "$ROOT/joiner_state"
clean_dir "$ROOT/joiner_router_state"
