#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}

case "$mode" in
  partial)
    rm -rf .data
    cp -r .partial_data .data
    ;;
  full)
    rm -rf .data
    cp -r .full_data .data
    ;;
  *)
    echo "Usage: $0 partial|full" >&2
    exit 1
    ;;
 esac
