#!/bin/bash

server_ip=$(awk -F "=" '/SERVER_IP/ {print $2}' ./server/config.ini)
server_port=$(awk -F "=" '/SERVER_PORT/ {print $2}' ./server/config.ini)
timestamp=$(date +%s)
docker_compose_file="docker-compose-test-$timestamp.yaml"
dockerfile_dir="./dockerfile-dir-test-$timestamp"
dockerfile="./$dockerfile_dir/Dockerfile"

touch $docker_compose_file
echo "
name: tp0
services:
  tester:
    container_name: tester
    image: tester:latest
    environment:
      - CLI_ID=1
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" > $docker_compose_file

mkdir $dockerfile_dir
touch $dockerfile
echo "
FROM alpine:latest
RUN apk add --no-cache netcat-openbsd
CMD [\"sleep\", \"infinity\"]" > $dockerfile

docker build -f $dockerfile -t "tester:latest" . > /dev/null 2>&1
docker compose -f $docker_compose_file up -d --build > /dev/null 2>&1

respuesta=$(docker exec -i tester sh -c "echo 'test_msg_$timestamp' | timeout 10 nc $server_ip $server_port")

docker compose -f $docker_compose_file stop -t 1 > /dev/null 2>&1
docker compose -f $docker_compose_file down > /dev/null 2>&1

rm -rf ./$dockerfile_dir
rm $docker_compose_file

if [ "$respuesta" = "test_msg_$timestamp" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
