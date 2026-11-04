PROTO_INCLUDE ?= /usr/include

.PHONY: generate test integration build up down demo

generate:
	PATH="$$(go env GOPATH)/bin:$$PATH" protoc -I api -I $(PROTO_INCLUDE) --go_out=. --go_opt=module=github.com/hhoshii/flight-booking-homework-3 --go-grpc_out=. --go-grpc_opt=module=github.com/hhoshii/flight-booking-homework-3 api/flight/v1/flight.proto

test:
	go test -race ./...
	go vet ./...

integration:
	docker compose --profile test run --rm test

build:
	go build -o bin/flight-service ./cmd/flight-service
	go build -o bin/booking-service ./cmd/booking-service

up:
	docker compose up --build -d --wait

down:
	docker compose down

demo:
	python3 scripts/demo.py
