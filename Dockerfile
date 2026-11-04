FROM golang:1.27.3-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN GODEBUG=http2client=0 go mod download -x
RUN GODEBUG=http2client=0 go install -v google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
RUN GODEBUG=http2client=0 go install -v google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
COPY . .
RUN make generate && CGO_ENABLED=0 go build -o /out/flight-service ./cmd/flight-service && CGO_ENABLED=0 go build -o /out/booking-service ./cmd/booking-service

FROM alpine:3.22 AS flight-service
RUN adduser -D app
USER app
COPY --from=build /out/flight-service /app/flight-service
EXPOSE 50051
ENTRYPOINT ["/app/flight-service"]

FROM alpine:3.22 AS booking-service
RUN adduser -D app
USER app
COPY --from=build /out/booking-service /app/booking-service
EXPOSE 8080
ENTRYPOINT ["/app/booking-service"]
