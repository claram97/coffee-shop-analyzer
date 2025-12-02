#!/usr/bin/env bash
set -euo pipefail

CLIENT_ID="${1:-}"

if [[ -z "$CLIENT_ID" ]]; then
    exit 0
fi

python3 scripts/validate_client_results.py "client_runs/${CLIENT_ID}"
