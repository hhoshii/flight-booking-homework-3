package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/hhoshii/flight-booking-homework-3/internal/flight"
	"github.com/hhoshii/flight-booking-homework-3/internal/platform"
	"github.com/hhoshii/flight-booking-homework-3/internal/rpc"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	platform.LogSetup()
	if err := run(); err != nil {
		slog.Error("flight service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	key := os.Getenv("GRPC_API_KEY")
	if key == "" {
		return fmt.Errorf("GRPC_API_KEY is required")
	}
	if len(os.Args) > 1 && os.Args[1] == "health" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cc, err := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(rpc.ClientAuth(key)))
		if err != nil {
			return err
		}
		defer cc.Close()
		result, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if result.Status != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("flight is not serving")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := platform.OpenDB(ctx, "flight")
	cancel()
	if err != nil {
		return err
	}
	defer pool.Close()
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    platform.Env("REDIS_MASTER_NAME", "flight-cache"),
		SentinelAddrs: strings.Split(platform.Env("REDIS_SENTINELS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"), ","),
		DialTimeout:   500 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, MaxRetries: 1,
	})
	defer rdb.Close()
	server := grpc.NewServer(grpc.UnaryInterceptor(rpc.ServerAuth(key)), grpc.StreamInterceptor(rpc.StreamAuth(key)))
	pb.RegisterFlightServiceServer(server, flight.New(pool, flight.NewCache(rdb)))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	listener, err := net.Listen("tcp", platform.Env("GRPC_ADDR", ":50051"))
	if err != nil {
		return err
	}
	stopping, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopping.Done()
		healthServer.Shutdown()
		timer := time.AfterFunc(5*time.Second, server.Stop)
		server.GracefulStop()
		timer.Stop()
	}()
	slog.Info("flight service listening", "address", listener.Addr().String())
	return server.Serve(listener)
}
