#!/bin/bash

# Script to run middleware tests using Docker Compose

set -e  # Exit immediately if a command exits with a non-zero status

# Define the Docker Compose file path
COMPOSE_FILE="/home/claram97/Escritorio/coffee-shop-analyzer/middleware/docker-compose.yml"

# Step 1: Build the Docker images (if not already built)
echo "Building Docker images..."
docker-compose -f "$COMPOSE_FILE" build

# Step 2: Run the tests
echo "Running tests..."
docker-compose -f "$COMPOSE_FILE" up --abort-on-container-exit

# Step 3: Capture the exit code of the test container
CONTAINER_ID=$(docker-compose -f "$COMPOSE_FILE" ps -q middleware-tests)
if [ -z "$CONTAINER_ID" ]; then
    echo "Error: Test container 'middleware-tests' not found or not running."
    docker-compose -f "$COMPOSE_FILE" down
    exit 1
fi

TEST_EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "$CONTAINER_ID")

# Step 4: Clean up the containers
echo "Cleaning up..."
docker-compose -f "$COMPOSE_FILE" down

# Step 5: Exit with the test container's exit code
exit $TEST_EXIT_CODE