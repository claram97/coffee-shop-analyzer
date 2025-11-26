#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./generate-client-compose.sh -n 3 -o docker-compose-client.yaml
# Defaults:
#   -n 1
#   -o docker-compose-client.yaml

CLIENT_COUNT=1
OUT_PATH="docker-compose-client.yaml"
MAKE_PATH="Makefile"

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

cat > "$MAKE_PATH" <<'MAKE'
SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./orchestrator/Dockerfile -t "orchestrator:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-up-client: docker-image
	docker compose -f docker-compose-client.yaml up -d --build
.PHONY: docker-compose-up-client

docker-compose-down-client:
	docker compose -f docker-compose-client.yaml stop -t 1
	docker compose -f docker-compose-client.yaml down
.PHONY: docker-compose-down-client

docker-compose-logs-client:
	docker compose -f docker-compose-client.yaml logs -f
.PHONY: docker-compose-logs-client

MAKE

for i in $(seq 1 "$CLIENT_COUNT"); do
  {
    printf 'docker-compose-up-client%u: docker-image\n' "$i"
    printf '\tdocker compose -f docker-compose-client.yaml up -d --build client%u\n' "$i"
    printf '.PHONY: docker-compose-up-client%u\n\n' "$i"
    printf 'docker-compose-down-client%u:\n' "$i"
    printf '\tdocker compose -f docker-compose-client.yaml stop -t 1 client%u\n' "$i"
    printf '\tdocker compose -f docker-compose-client.yaml rm -f client%u\n' "$i"
    printf '.PHONY: docker-compose-down-client%u\n\n' "$i"
  } >> "$MAKE_PATH"
done
