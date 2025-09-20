#!/bin/bash
echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"

touch $1

echo "name: tp0
services:
  orchestrator:
    container_name: orchestrator
    image: orchestrator:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS_AMOUNT=$2
      # - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net
    volumes:
      - ./orchestrator/config.ini:/config.ini:ro" > $1

for i in $(seq 1 $2); do
    echo "
  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$i
      # - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - orchestrator
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./.data/agency-5.csv:/bets.csv:ro" >> $1
done

echo "
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> $1
