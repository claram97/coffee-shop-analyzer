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

docker-compose-up-client4: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client4
.PHONY: docker-compose-up-client4

docker-compose-down-client4:
	docker compose -f docker-compose-client.yaml stop -t 1 client4
	docker compose -f docker-compose-client.yaml rm -f client4
.PHONY: docker-compose-down-client4

docker-compose-up-client5: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client5
.PHONY: docker-compose-up-client5

docker-compose-down-client5:
	docker compose -f docker-compose-client.yaml stop -t 1 client5
	docker compose -f docker-compose-client.yaml rm -f client5
.PHONY: docker-compose-down-client5

docker-compose-up-client6: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client6
.PHONY: docker-compose-up-client6

docker-compose-down-client6:
	docker compose -f docker-compose-client.yaml stop -t 1 client6
	docker compose -f docker-compose-client.yaml rm -f client6
.PHONY: docker-compose-down-client6

docker-compose-up-client7: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client7
.PHONY: docker-compose-up-client7

docker-compose-down-client7:
	docker compose -f docker-compose-client.yaml stop -t 1 client7
	docker compose -f docker-compose-client.yaml rm -f client7
.PHONY: docker-compose-down-client7

docker-compose-up-client8: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client8
.PHONY: docker-compose-up-client8

docker-compose-down-client8:
	docker compose -f docker-compose-client.yaml stop -t 1 client8
	docker compose -f docker-compose-client.yaml rm -f client8
.PHONY: docker-compose-down-client8

docker-compose-up-client9: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client9
.PHONY: docker-compose-up-client9

docker-compose-down-client9:
	docker compose -f docker-compose-client.yaml stop -t 1 client9
	docker compose -f docker-compose-client.yaml rm -f client9
.PHONY: docker-compose-down-client9

docker-compose-up-client10: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client10
.PHONY: docker-compose-up-client10

docker-compose-down-client10:
	docker compose -f docker-compose-client.yaml stop -t 1 client10
	docker compose -f docker-compose-client.yaml rm -f client10
.PHONY: docker-compose-down-client10

docker-compose-up-client11: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client11
.PHONY: docker-compose-up-client11

docker-compose-down-client11:
	docker compose -f docker-compose-client.yaml stop -t 1 client11
	docker compose -f docker-compose-client.yaml rm -f client11
.PHONY: docker-compose-down-client11

docker-compose-up-client12: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client12
.PHONY: docker-compose-up-client12

docker-compose-down-client12:
	docker compose -f docker-compose-client.yaml stop -t 1 client12
	docker compose -f docker-compose-client.yaml rm -f client12
.PHONY: docker-compose-down-client12

docker-compose-up-client13: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client13
.PHONY: docker-compose-up-client13

docker-compose-down-client13:
	docker compose -f docker-compose-client.yaml stop -t 1 client13
	docker compose -f docker-compose-client.yaml rm -f client13
.PHONY: docker-compose-down-client13

docker-compose-up-client14: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client14
.PHONY: docker-compose-up-client14

docker-compose-down-client14:
	docker compose -f docker-compose-client.yaml stop -t 1 client14
	docker compose -f docker-compose-client.yaml rm -f client14
.PHONY: docker-compose-down-client14

docker-compose-up-client15: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client15
.PHONY: docker-compose-up-client15

docker-compose-down-client15:
	docker compose -f docker-compose-client.yaml stop -t 1 client15
	docker compose -f docker-compose-client.yaml rm -f client15
.PHONY: docker-compose-down-client15

docker-compose-up-client16: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client16
.PHONY: docker-compose-up-client16

docker-compose-down-client16:
	docker compose -f docker-compose-client.yaml stop -t 1 client16
	docker compose -f docker-compose-client.yaml rm -f client16
.PHONY: docker-compose-down-client16

docker-compose-up-client17: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client17
.PHONY: docker-compose-up-client17

docker-compose-down-client17:
	docker compose -f docker-compose-client.yaml stop -t 1 client17
	docker compose -f docker-compose-client.yaml rm -f client17
.PHONY: docker-compose-down-client17

docker-compose-up-client18: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client18
.PHONY: docker-compose-up-client18

docker-compose-down-client18:
	docker compose -f docker-compose-client.yaml stop -t 1 client18
	docker compose -f docker-compose-client.yaml rm -f client18
.PHONY: docker-compose-down-client18

docker-compose-up-client19: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client19
.PHONY: docker-compose-up-client19

docker-compose-down-client19:
	docker compose -f docker-compose-client.yaml stop -t 1 client19
	docker compose -f docker-compose-client.yaml rm -f client19
.PHONY: docker-compose-down-client19

docker-compose-up-client20: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client20
.PHONY: docker-compose-up-client20

docker-compose-down-client20:
	docker compose -f docker-compose-client.yaml stop -t 1 client20
	docker compose -f docker-compose-client.yaml rm -f client20
.PHONY: docker-compose-down-client20

docker-compose-up-client21: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client21
.PHONY: docker-compose-up-client21

docker-compose-down-client21:
	docker compose -f docker-compose-client.yaml stop -t 1 client21
	docker compose -f docker-compose-client.yaml rm -f client21
.PHONY: docker-compose-down-client21

docker-compose-up-client22: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client22
.PHONY: docker-compose-up-client22

docker-compose-down-client22:
	docker compose -f docker-compose-client.yaml stop -t 1 client22
	docker compose -f docker-compose-client.yaml rm -f client22
.PHONY: docker-compose-down-client22

docker-compose-up-client23: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client23
.PHONY: docker-compose-up-client23

docker-compose-down-client23:
	docker compose -f docker-compose-client.yaml stop -t 1 client23
	docker compose -f docker-compose-client.yaml rm -f client23
.PHONY: docker-compose-down-client23

docker-compose-up-client24: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client24
.PHONY: docker-compose-up-client24

docker-compose-down-client24:
	docker compose -f docker-compose-client.yaml stop -t 1 client24
	docker compose -f docker-compose-client.yaml rm -f client24
.PHONY: docker-compose-down-client24

docker-compose-up-client25: docker-image
	docker compose -f docker-compose-client.yaml up -d --build client25
.PHONY: docker-compose-up-client25

docker-compose-down-client25:
	docker compose -f docker-compose-client.yaml stop -t 1 client25
	docker compose -f docker-compose-client.yaml rm -f client25
.PHONY: docker-compose-down-client25

