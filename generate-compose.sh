#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./generate-compose.sh -c ./config/config.ini -o docker-compose-dev.yaml
#
# Requisitos:
#   - python3 disponible
#   - config_subscript.py en el mismo repo (o en PATH)
#   - config.ini con las secciones acordadas

INI_PATH="./app_config/config.ini"
OUT_PATH="docker-compose-dev.yaml"

while getopts "c:o:" opt; do
  case "$opt" in
    c) INI_PATH="$OPTARG" ;;
    o) OUT_PATH="$OPTARG" ;;
    *) echo "Uso: $0 [-c config.ini] [-o docker-compose-dev.yaml]" >&2; exit 2 ;;
  esac
done

read FILTERS AGGS JOINERS FINISHERS ORCHESTRATORS < <(python3 ./app_config/config_subscript.py -c "$INI_PATH" workers --format=plain)
read FR_ROUTERS J_ROUTERS RESULTS_ROUTERS < <(python3 ./app_config/config_subscript.py -c "$INI_PATH" routers --format=plain)

eval "$(python3 ./app_config/config_subscript.py -c "$INI_PATH" broker --format=env)"
# Read election ports from config
eval "$(python3 ./app_config/config_subscript.py -c "$INI_PATH" election_ports --format=env)"

# Defaults por si faltan en INI
: "${RABBIT_HOST:=rabbitmq}"
: "${RABBIT_PORT:=5672}"
: "${RABBIT_MGMT_PORT:=15672}"
: "${RABBIT_USER:=guest}"
: "${RABBIT_PASS:=guest}"
: "${RABBIT_VHOST:=/}"

# Election port defaults (100 ports per component group)
: "${ELECTION_PORT_FILTER_WORKERS:=9100}"
: "${ELECTION_PORT_FILTER_ROUTERS:=9200}"
: "${ELECTION_PORT_AGGREGATORS:=9300}"
: "${ELECTION_PORT_JOINER_WORKERS:=9400}"
: "${ELECTION_PORT_JOINER_ROUTERS:=9500}"
: "${ELECTION_PORT_RESULTS_WORKERS:=9600}"
: "${ELECTION_PORT_RESULTS_ROUTERS:=9700}"

: "${HEARTBEAT_INTERVAL_SECONDS:=2.0}"
: "${HEARTBEAT_TIMEOUT_SECONDS:=1.0}"
: "${HEARTBEAT_MAX_MISSES:=3}"
: "${HEARTBEAT_STARTUP_GRACE_SECONDS:=4.0}"
: "${HEARTBEAT_ELECTION_COOLDOWN_SECONDS:=5.0}"
: "${HEARTBEAT_COOLDOWN_JITTER_SECONDS:=1.0}"
: "${FOLLOWER_DOWN_TIMEOUT_SECONDS:=15.0}"
: "${FOLLOWER_RESTART_COOLDOWN_SECONDS:=30.0}"
: "${FOLLOWER_RECOVERY_GRACE_SECONDS:=10.0}"

cat > "$OUT_PATH" <<YAML
version: '3.8'
services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:3-management
    ports:
      - "${RABBIT_PORT}:5672"
      - "${RABBIT_MGMT_PORT}:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=${RABBIT_USER}
      - RABBITMQ_DEFAULT_PASS=${RABBIT_PASS}
      - RABBITMQ_DEFAULT_VHOST=${RABBIT_VHOST}
    networks:
      - testing_net
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
    ulimits:
      nofile:
        soft: 65536
        hard: 65536

  orchestrator:
    container_name: orchestrator
    build:
      context: .
      dockerfile: orchestrator/Dockerfile
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS_AMOUNT=1
      - RABBITMQ_HOST=rabbitmq
      - ORCH_TO_FR_EXCHANGE=fr.ex
      - ORCH_TO_FR_RK_FMT=fr.{pid:02d}
      - FILTER_ROUTER_COUNT=${FR_ROUTERS}
      - ORCH_PROCESS_COUNT=${ORCHESTRATORS}
      - ORCH_PROCESS_QUEUE_SIZE=256
      - ORCH_PROCESS_QUEUE_TIMEOUT=10.0
    networks:
      - testing_net
    volumes:
      - ./orchestrator/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
YAML

# --- filter routers ---
for i in $(seq 0 $((FR_ROUTERS-1))); do
cat >> "$OUT_PATH" <<YAML

  filter-router-${i}:
    container_name: filter-router-${i}
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_ROUTER_INDEX=${i}
      - ELECTION_PORT=$((ELECTION_PORT_FILTER_ROUTERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
      - FOLLOWER_DOWN_TIMEOUT_SECONDS=${FOLLOWER_DOWN_TIMEOUT_SECONDS}
      - FOLLOWER_RESTART_COOLDOWN_SECONDS=${FOLLOWER_RESTART_COOLDOWN_SECONDS}
      - FOLLOWER_RECOVERY_GRACE_SECONDS=${FOLLOWER_RECOVERY_GRACE_SECONDS}
    networks:
      - testing_net
    volumes:
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
YAML
done

# --- filter workers ---
for i in $(seq 0 $((FILTERS-1))); do
cat >> "$OUT_PATH" <<YAML

  filter-worker-${i}:
    container_name: filter-worker-${i}
    build:
      context: .
      dockerfile: filter/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - FILTER_WORKER_INDEX=${i}
      - ELECTION_PORT=$((ELECTION_PORT_FILTER_WORKERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started
YAML
done

# --- aggregators (shardeados) ---
for i in $(seq 0 $((AGGS-1))); do
cat >> "$OUT_PATH" <<YAML

  aggregator-${i}:
    container_name: aggregator-${i}
    build:
      context: .
      dockerfile: aggregator/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - CONFIG_PATH=/config/config.ini
      - LOG_LEVEL=INFO
      - AGGREGATOR_ID=${i}
      - ELECTION_PORT=$((ELECTION_PORT_AGGREGATORS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router-0:
        condition: service_started
YAML
done

# --- joiner-router ---
for i in $(seq 0 $((J_ROUTERS-1))); do
cat >> "$OUT_PATH" <<YAML

  joiner-router-${i}:
    container_name: joiner-router-${i}
    build:
      context: .
      dockerfile: joiner/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - CONFIG_PATH=/config/config.ini
      - JOINER_ROUTER_INDEX=${i}
      - ELECTION_PORT=$((ELECTION_PORT_JOINER_ROUTERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      aggregator-0:
        condition: service_started
YAML
done

# --- joiner workers (shardeados) ---
for i in $(seq 0 $((JOINERS-1))); do
cat >> "$OUT_PATH" <<YAML

  joiner-worker-${i}:
    container_name: joiner-worker-${i}
    build:
      context: .
      dockerfile: joiner/Dockerfile.worker
    environment:
      - PYTHONUNBUFFERED=1
      - LOG_LEVEL=INFO
      - JOINER_WORKER_INDEX=${i}
      - CONFIG_PATH=/config/config.ini
      - ELECTION_PORT=$((ELECTION_PORT_JOINER_WORKERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      joiner-router-0:
        condition: service_started
YAML
done

FINISHER_QUEUES=$(printf "finisher_input_queue_%s," $(seq 0 $((FINISHERS-1))))
FINISHER_QUEUES=${FINISHER_QUEUES%,}

# --- results-router ---
for i in $(seq 0 $((RESULTS_ROUTERS-1))); do
cat >> "$OUT_PATH" <<YAML

  results-router-${i}:
    container_name: results-router-${i}
    build:
      context: .
      dockerfile: results-finisher/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - INPUT_QUEUE=results.controller.in
      - OUTPUT_QUEUES=${FINISHER_QUEUES}
      - RESULTS_ROUTER_INDEX=${i}
      - ELECTION_PORT=$((ELECTION_PORT_RESULTS_ROUTERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
YAML
done

# --- results-finishers (multiple instances) ---
for i in $(seq 0 $((FINISHERS-1))); do
cat >> "$OUT_PATH" <<YAML

  results-finisher-${i}:
    container_name: results-finisher-${i}
    build:
      context: .
      dockerfile: results-finisher/Dockerfile
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
      - STRATEGY_MODE=append_only  # Can be 'append_only' or 'incremental'
      - INPUT_QUEUE=finisher_input_queue_${i}
      - OUTPUT_QUEUE=orchestrator_results_queue
      - RESULTS_FINISHER_INDEX=${i}
      - ELECTION_PORT=$((ELECTION_PORT_RESULTS_WORKERS + i))
      - HEARTBEAT_INTERVAL_SECONDS=${HEARTBEAT_INTERVAL_SECONDS}
      - HEARTBEAT_TIMEOUT_SECONDS=${HEARTBEAT_TIMEOUT_SECONDS}
      - HEARTBEAT_MAX_MISSES=${HEARTBEAT_MAX_MISSES}
      - HEARTBEAT_STARTUP_GRACE_SECONDS=${HEARTBEAT_STARTUP_GRACE_SECONDS}
      - HEARTBEAT_ELECTION_COOLDOWN_SECONDS=${HEARTBEAT_ELECTION_COOLDOWN_SECONDS}
      - HEARTBEAT_COOLDOWN_JITTER_SECONDS=${HEARTBEAT_COOLDOWN_JITTER_SECONDS}
    networks:
      - testing_net
    volumes:
      - ./results-finisher:/app/results-finisher:ro
      - $INI_PATH:/config/config.ini:ro
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
      results-router-0:
        condition: service_started
YAML
done

# --- networks ---
cat >> "$OUT_PATH" <<'YAML'

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
YAML

echo "✅ Generado: $OUT_PATH"
