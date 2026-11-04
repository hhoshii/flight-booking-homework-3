package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/hhoshii/flight-booking-homework-3/internal/booking"
	"github.com/hhoshii/flight-booking-homework-3/internal/platform"
	"github.com/hhoshii/flight-booking-homework-3/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	platform.LogSetup()
	if err := run(); err != nil {
		slog.Error("booking service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "health" {
		client := &http.Client{Timeout: 2 * time.Second}
		res, err := client.Get("http://127.0.0.1:8080/health")
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("booking is not serving")
		}
		return nil
	}
	key := os.Getenv("GRPC_API_KEY")
	if key == "" {
		return fmt.Errorf("GRPC_API_KEY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := platform.OpenDB(ctx, "booking")
	cancel()
	if err != nil {
		return err
	}
	defer pool.Close()
	breaker := rpc.NewBreaker(platform.PositiveInt("CB_FAILURE_THRESHOLD", "5"), platform.Duration("CB_OPEN_TIMEOUT", "10s"), platform.Duration("CB_FAILURE_WINDOW", "30s"))
	cc, err := grpc.NewClient(platform.Env("FLIGHT_GRPC_ADDR", "flight-service:50051"), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithChainUnaryInterceptor(breaker.Unary, rpc.Retry(platform.Duration("GRPC_ATTEMPT_TIMEOUT", "1s")), rpc.ClientAuth(key)))
	if err != nil {
		return err
	}
	defer cc.Close()
	server := &http.Server{Addr: platform.Env("HTTP_ADDR", ":8080"), Handler: booking.New(pool, pb.NewFlightServiceClient(cc)).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	stopping, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopping.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	slog.Info("booking service listening", "address", server.Addr)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
