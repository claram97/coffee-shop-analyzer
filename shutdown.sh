#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <component_type> <role> [index]"
    echo "Sends a SIGINT signal for graceful shutdown to a specific container or group of containers."
    echo ""
    echo "Arguments:"
    echo "  component_type: The type of the component (e.g., 'joiner')."
    echo "  role:           The role of the component ('worker' or 'router')."
    echo "  index:          (Optional) The specific index of the container. If omitted, signals all containers of that type."
    echo ""
    echo "Example (signal a specific worker):"
    echo "  $ ./shutdown.sh joiner worker 0"
    echo ""
    echo "Example (signal all joiner routers):"
    echo "  $ ./shutdown.sh joiner router"
    exit 1
}

if [[ $# -lt 2 ]]; then
    echo "Error: Missing required arguments." >&2
    usage
fi

COMPONENT="$1"
ROLE="$2"
INDEX="${3:-}"

if [[ "$COMPONENT" != "joiner" ]] && [[ "$COMPONENT" != "filter" ]]; then
    echo "Error: Invalid component type '$COMPONENT'. Must be 'joiner' or 'filter'." >&2
    exit 1
fi

if [[ "$ROLE" != "worker" ]] && [[ "$ROLE" != "router" ]]; then
    echo "Error: Invalid role '$ROLE'. Must be 'worker' or 'router'." >&2
    exit 1
fi

if [[ -n "$INDEX" ]]; then
    CONTAINER_NAME="${COMPONENT}-${ROLE}-${INDEX}"
    echo "🚀 Sending SIGINT to container: $CONTAINER_NAME"
    
    if ! docker kill --signal=SIGINT "$CONTAINER_NAME"; then
        echo "⚠️  Failed to send signal. Is the container '$CONTAINER_NAME' running?" >&2
        exit 1
    fi
else
    PATTERN="${COMPONENT}-${ROLE}-"
    echo "Searching for containers matching '${PATTERN}*' to send SIGINT..."
    
    TARGET_CONTAINERS=$(docker ps --filter "name=${PATTERN}" --format '{{.Names}}')

    if [[ -z "$TARGET_CONTAINERS" ]]; then
        echo "No running containers found matching the pattern."
        exit 0
    fi

    for container in $TARGET_CONTAINERS; do
        echo "  - Sending SIGINT to container: $container"
        docker kill --signal=SIGINT "$container"
    done
fi

echo "✅ Signal(s) sent successfully."
