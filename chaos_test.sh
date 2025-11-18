#!/bin/bash

# Chaos testing script - randomly kills and restarts containers
# Usage: ./chaos_test.sh [number_of_kills]

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

# Number of times to kill containers (default: 10)
NUM_KILLS=${1:-10}

echo "=== Chaos Testing Script ==="
echo "Will perform $NUM_KILLS container kills"
echo "Max wait between kills: 20 seconds"
echo "Max wait before restart: 10 seconds"
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

# Function to kill and restart a container
chaos_kill_restart() {
    local container=$1
    
    echo "[$(date +'%H:%M:%S')] Killing container: $container"
    docker kill "$container" 2>/dev/null || true
    
    # Random wait before restart (0-10 seconds)
    local restart_wait=$((RANDOM % 11))
    echo "[$(date +'%H:%M:%S')] Will restart in $restart_wait seconds..."
    
    # Start restart in background
    (
        sleep "$restart_wait"
        echo "[$(date +'%H:%M:%S')] Restarting container: $container"
        docker start "$container" 2>/dev/null || true
        echo "[$(date +'%H:%M:%S')] Container $container restarted"
    ) &
}

# Main chaos loop
for i in $(seq 1 "$NUM_KILLS"); do
    echo ""
    echo "=== Chaos Event $i/$NUM_KILLS ==="
    
    # Pick a random container
    target_container=$(pick_random_container)
    
    if [[ -z "$target_container" ]]; then
        echo "No containers found to kill. Exiting."
        break
    fi
    
    # Kill and schedule restart
    chaos_kill_restart "$target_container"
    
    # Wait random time before next kill (0-20 seconds)
    if [[ $i -lt $NUM_KILLS ]]; then
        wait_time=$((RANDOM % 21))
        echo "[$(date +'%H:%M:%S')] Waiting $wait_time seconds before next chaos event..."
        sleep "$wait_time"
    fi
done

echo ""
echo "=== Chaos testing complete ==="
echo "Waiting for all background restart tasks to complete..."
wait
echo "[$(date +'%H:%M:%S')] All containers have been restarted"
