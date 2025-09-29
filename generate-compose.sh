#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./generate-compose.sh -c ./config/config.ini -o docker-compose-dev.yaml
#
# Requisitos:
#   - python3 disponible
#   - config_subscript.py en el mismo repo (o en PATH)
#   - config.ini con las secciones acordadas

INI_PATH="config/config.ini"
OUT_PATH="docker-compose-dev.yaml"

while getopts "c:o:" opt; do
  case "$opt" in
    c) INI_PATH="$OPTARG" ;;
    o) OUT_PATH="$OPTARG" ;;
    *) echo "Uso: $0 [-c config.ini] [-o docker-compose-dev.yaml]" >&2; exit 2 ;;
  esac
done

read FILTERS AGGS JOINERS < <(python3 ./config/config_subscript.py -c "$INI_PATH" workers --format=plain)

eval "$(python3 ./config/config_subscript.py -c "$INI_PATH" broker --format=env)"

# Defaults por si faltan en INI
: "${RABBIT_HOST:=rabbitmq}"
: "${RABBIT_PORT:=5672}"
: "${RABBIT_MGMT_PORT:=15672}"
: "${RABBIT_USER:=guest}"
: "${RABBIT_PASS:=guest}"
: "${RABBIT_VHOST:=/}"

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

  orchestrator:
    container_name: orchestrator
    build:
      context: .
      dockerfile: orchestrator/Dockerfile
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS_AMOUNT=1
    networks:
      - testing_net
    volumes:
      - ./orchestrator/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy

  client1:
    container_name: client1
    build:
      context: .
      dockerfile: client/Dockerfile
    entrypoint: /client
    environment:
      - CLI_ID=1
    networks:
      - testing_net
    depends_on:
      - orchestrator
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./.data:/data:ro

  filter-router:
    container_name: filter-router
    build:
      context: .
      dockerfile: filter/Dockerfile.router
    environment:
      - PYTHONUNBUFFERED=1
      - RABBITMQ_HOST=rabbitmq
      - LOG_LEVEL=INFO
    networks:
      - testing_net
    volumes:
      - ./filter/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
YAML

# --- filter workers ---
for i in $(seq 1 "$FILTERS"); do
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
    networks:
      - testing_net
    volumes:
      - ./filter/config.ini:/app/config.ini:ro
    depends_on:
      rabbitmq:
        condition: service_healthy
      filter-router:
        condition: service_started
YAML
done

# --- aggregators (shardeados) ---
# for i in $(seq 1 "$AGGS"); do
# cat >> "$OUT_PATH" <<YAML
#
#   aggregator-${i}:
#     container_name: aggregator-${i}
#     build:
#       context: .
#       dockerfile: aggregator/Dockerfile.worker
#     environment:
#       - PYTHONUNBUFFERED=1
#       - RABBITMQ_HOST=rabbitmq
#       - LOG_LEVEL=INFO
#       - AGG_WORKER_INDEX=${i}
#     networks:
#       - testing_net
#     volumes:
#       - ./aggregator/config.ini:/app/config.ini:ro
#     depends_on:
#       rabbitmq:
#         condition: service_healthy
#       filter-router:
#         condition: service_started
# YAML
# done
#
# # --- joiner-router (único) ---
# cat >> "$OUT_PATH" <<YAML
#
#   joiner-router:
#     container_name: joiner-router
#     build:
#       context: .
#       dockerfile: joiner/Dockerfile.router
#     environment:
#       - PYTHONUNBUFFERED=1
#       - RABBITMQ_HOST=rabbitmq
#       - LOG_LEVEL=INFO
#     networks:
#       - testing_net
#     volumes:
#       - ./joiner/config.ini:/app/config.ini:ro
#     depends_on:
#       rabbitmq:
#         condition: service_healthy
#       aggregator-1:
#         condition: service_started
# YAML
#
# # --- joiner workers (shardeados) ---
# for i in $(seq 1 "$JOINERS"); do
# cat >> "$OUT_PATH" <<YAML
#
#   joiner-worker-${i}:
#     container_name: joiner-worker-${i}
#     build:
#       context: .
#       dockerfile: joiner/Dockerfile.worker
#     environment:
#       - PYTHONUNBUFFERED=1
#       - RABBITMQ_HOST=rabbitmq
#       - LOG_LEVEL=INFO
#       - JOINER_WORKER_INDEX=${i}
#     networks:
#       - testing_net
#     volumes:
#       - ./joiner/config.ini:/app/config.ini:ro
#     depends_on:
#       rabbitmq:
#         condition: service_healthy
#       joiner-router:
#         condition: service_started
# YAML
# done

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
