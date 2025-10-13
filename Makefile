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

docker-compose-up-client1: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client1
.PHONY: docker-compose-up-client1

docker-compose-down-client1:
	docker compose -f docker-compose-client.yaml stop -t 1 client1
	docker compose -f docker-compose-client.yaml rm -f client1
.PHONY: docker-compose-down-client1

docker-compose-up-client2: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client2
.PHONY: docker-compose-up-client2

docker-compose-down-client2:
	docker compose -f docker-compose-client.yaml stop -t 1 client2
	docker compose -f docker-compose-client.yaml rm -f client2
.PHONY: docker-compose-down-client2

docker-compose-up-client3: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client3
.PHONY: docker-compose-up-client3

docker-compose-down-client3:
	docker compose -f docker-compose-client.yaml stop -t 1 client3
	docker compose -f docker-compose-client.yaml rm -f client3
.PHONY: docker-compose-down-client3