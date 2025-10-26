#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./generate-client-compose.sh -n 3 -o docker-compose-client.yaml
# Defaults:
#   -n 1
#   -o docker-compose-client.yaml

CLIENT_COUNT=1
OUT_PATH="docker-compose-client.yaml"

usage() {
  echo "Uso: $0 [-n cantidad_clientes] [-o archivo_salida]" >&2
  exit 2
}

while getopts "n:o:" opt; do
  case "$opt" in
    n) CLIENT_COUNT="$OPTARG" ;;
    o) OUT_PATH="$OPTARG" ;;
    *) usage ;;
  esac
done

shift $((OPTIND - 1))
if [ "$#" -gt 0 ]; then
  if [ "$#" -gt 1 ]; then
    usage
  fi
  CLIENT_COUNT="$1"
fi

if ! [[ "$CLIENT_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "Cantidad de clientes inválida: $CLIENT_COUNT" >&2
  exit 1
fi

cat > "$OUT_PATH" <<'YAML'
version: "3.8"
services:
YAML

for i in $(seq 1 "$CLIENT_COUNT"); do
  cat >> "$OUT_PATH" <<YAML
  client${i}:
    container_name: client${i}
    build:
      context: .
      dockerfile: client/Dockerfile
    entrypoint: /client
    environment:
      - CLI_ID=${i}
    networks:
      - testing_net
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./.data:/data:ro
      - ./client_runs:/client_runs

YAML
done

cat >> "$OUT_PATH" <<'YAML'
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
YAML
