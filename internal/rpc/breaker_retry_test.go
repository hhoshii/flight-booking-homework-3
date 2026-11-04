package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type failingHealth struct {
	healthpb.UnimplementedHealthServer
	code  codes.Code
	calls atomic.Int32
}

func (s *failingHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.calls.Add(1)
	return nil, status.Error(s.code, "injected server failure")
}

func healthClient(t *testing.T, b *Breaker, code codes.Code) (healthpb.HealthClient, *failingHealth) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	failure := &failingHealth{code: code}
	healthpb.RegisterHealthServer(server, failure)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	cc, err := grpc.NewClient("passthrough:///breaker-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDisableRetry(),
		grpc.WithChainUnaryInterceptor(b.Unary, Retry(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return healthpb.NewHealthClient(cc), failure
}

func TestHalfOpenMakesOnePhysicalAttempt(t *testing.T) {
	b := NewBreaker(1, 20*time.Millisecond, time.Second)
	client, server := healthClient(t, b, codes.Unavailable)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable || server.calls.Load() != 3 {
		t.Fatalf("CLOSED: err=%v attempts=%d, want 3", err, server.calls.Load())
	}
	// Make the existing OPEN interval expire without depending on scheduler sleeps.
	b.mu.Lock()
	b.openedAt = time.Now().Add(-time.Second)
	b.mu.Unlock()
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable || server.calls.Load() != 4 {
		t.Fatalf("HALF_OPEN: err=%v total attempts=%d, want 4 (one probe)", err, server.calls.Load())
	}
}

func TestCircuitCountsServerFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.Internal, codes.Unknown, codes.DataLoss} {
		t.Run(code.String(), func(t *testing.T) {
			client, server := healthClient(t, NewBreaker(2, time.Second, time.Second), code)
			for i := 0; i < 2; i++ {
				_, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
				if status.Code(err) != code {
					t.Fatal(err)
				}
			}
			_, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
			if status.Code(err) != codes.Unavailable || server.calls.Load() != 2 {
				t.Fatalf("circuit did not open: err=%v server calls=%d", err, server.calls.Load())
			}
		})
	}
}
