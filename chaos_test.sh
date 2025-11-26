#!/bin/bash

# Chaos testing script - randomly stops containers and lets the leader revive them
# Usage: ./chaos_test.sh [number_of_events]

set -e

COMPOSE_FILE="docker-compose-dev.yaml"
COMPOSE_PROJECT="coffee-shop-analyzer"

# Component patterns to target
COMPONENTS=(
    "filter-router-"
    "filter-worker-"
    "joiner-router-"
    "joiner-worker-"    
    "aggregator-"
    "results-router-"
    "results-finisher-"
)

# Number of times to stop containers (default: 10)
NUM_EVENTS=${1:-10}

echo "=== Chaos Testing Script ==="
echo "Will perform $NUM_EVENTS container stops"
echo "Max wait between stops: 20 seconds"
echo ""

# Function to get all running containers matching a pattern
get_containers() {
    local pattern=$1
    docker ps --filter "name=${pattern}" --format "{{.Names}}" | grep -E "${pattern}[0-9]+"
}

# Function to pick a random container from available components
pick_random_container() {
    local all_containers=()
    
    # Collect all containers from all components
    for component in "${COMPONENTS[@]}"; do
        while IFS= read -r container; do
            if [[ -n "$container" ]]; then
                all_containers+=("$container")
            fi
        done < <(get_containers "$component")
    done
    
    if [[ ${#all_containers[@]} -eq 0 ]]; then
        echo ""
        return 1
    fi
    
    # Pick a random container
    local random_index=$((RANDOM % ${#all_containers[@]}))
    echo "${all_containers[$random_index]}"
}

# Function to stop a container and let the leader revive it
chaos_stop_container() {
    local container=$1
    
    echo "[$(date +'%H:%M:%S')] Stopping container: $container"
    docker kill "$container" 2>/dev/null || true
}

# Main chaos loop
for i in $(seq 1 "$NUM_EVENTS"); do
    echo ""
    echo "=== Chaos Event $i/$NUM_EVENTS ==="
    
    # Pick a random container
    target_container=$(pick_random_container)
    
    if [[ -z "$target_container" ]]; then
        echo "No containers found to stop. Exiting."
        break
    fi
    
    # Stop container and let leader handle recovery
    chaos_stop_container "$target_container"
    
    # Wait random time before next stop event (0-10 seconds)
    if [[ $i -lt $NUM_EVENTS ]]; then
        wait_time=$((RANDOM % 20 + 10))
        echo "[$(date +'%H:%M:%S')] Waiting $wait_time seconds before next chaos event..."
        sleep "$wait_time"
    fi
done

echo ""
echo "=== Chaos testing complete ==="
echo "[$(date +'%H:%M:%S')] Containers should have been revived by their leaders"
