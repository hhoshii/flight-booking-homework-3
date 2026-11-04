package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRetryTransientOnlyAndMaxThree(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want int
	}{
		{codes.Unavailable, 3}, {codes.DeadlineExceeded, 3},
		{codes.InvalidArgument, 1}, {codes.NotFound, 1}, {codes.ResourceExhausted, 1}, {codes.Unauthenticated, 1},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			calls := 0
			start := time.Now()
			err := Retry(time.Second)(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(tc.code, "failure")
			})
			if status.Code(err) != tc.code || calls != tc.want {
				t.Fatalf("code=%v calls=%d, want %v/%d", status.Code(err), calls, tc.code, tc.want)
			}
			if tc.want == 3 && time.Since(start) < 295*time.Millisecond {
				t.Fatal("missing exponential backoff 100ms + 200ms")
			}
		})
	}
}

func TestRetryRecoversAndHonorsCancellation(t *testing.T) {
	calls := 0
	err := Retry(time.Second)(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls++
		if calls < 3 {
			return status.Error(codes.Unavailable, "temporary")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v attempts=%d", err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = Retry(time.Second)(ctx, "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		t.Fatal("cancelled request reached invoker")
		return nil
	})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("got %v", err)
	}
}

func TestCircuitOpensThenAllowsOnlyOneProbe(t *testing.T) {
	b := NewBreaker(2, 10*time.Millisecond, time.Second)
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "offline")
	}
	for i := 0; i < 2; i++ {
		_ = b.Unary(context.Background(), "/test", nil, nil, nil, fail)
	}
	calls := 0
	err := b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls++
		return nil
	})
	if status.Code(err) != codes.Unavailable || calls != 0 {
		t.Fatalf("open circuit called server: %v calls=%d", err, calls)
	}
	time.Sleep(15 * time.Millisecond)
	started, finish := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	var allowed atomic.Int32
	for i := 0; i < 10; i++ {
		err = b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			allowed.Add(1)
			return nil
		})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("extra probe: %v", err)
		}
	}
	close(finish)
	wg.Wait()
	if allowed.Load() != 0 {
		t.Fatal("concurrent probes reached server")
	}
	if err = b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }); err != nil {
		t.Fatal("did not close", err)
	}
}

func TestCircuitBusinessErrorsDoNotOpen(t *testing.T) {
	b := NewBreaker(1, time.Second, time.Second)
	for i := 0; i < 3; i++ {
		err := b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.NotFound, "missing")
		})
		if status.Code(err) != codes.NotFound {
			t.Fatal(err)
		}
	}
}

func TestAPIKeyRequired(t *testing.T) {
	for _, key := range []string{"", "wrong", "secret"} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", key))
		_, err := ServerAuth("secret")(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return "ok", nil })
		if key == "secret" && err != nil {
			t.Fatal(err)
		}
		if key != "secret" && status.Code(err) != codes.Unauthenticated {
			t.Fatalf("key %q: %v", key, err)
		}
	}
}

type testStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s testStream) Context() context.Context { return s.ctx }

func TestStreamingHealthRequiresAPIKey(t *testing.T) {
	for _, key := range []string{"", "wrong", "secret"} {
		called := false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", key))
		err := StreamAuth("secret")(nil, testStream{ctx: ctx}, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { called = true; return nil })
		if key == "secret" {
			if err != nil || !called {
				t.Fatalf("valid key rejected: %v", err)
			}
		} else if status.Code(err) != codes.Unauthenticated || called {
			t.Fatalf("unauthenticated stream reached handler: %v", err)
		}
	}
}

func TestRetryTimeoutsEachAttempt(t *testing.T) {
	calls := 0
	err := Retry(5*time.Millisecond)(context.Background(), "/test", nil, nil, nil, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		calls++
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	})
	if calls != 3 || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("attempts=%d err=%v", calls, err)
	}
}

func TestFailedProbeReopensCircuit(t *testing.T) {
	b := NewBreaker(1, 5*time.Millisecond, time.Second)
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "offline")
	}
	_ = b.Unary(context.Background(), "/test", nil, nil, nil, fail)
	time.Sleep(10 * time.Millisecond)
	if e := b.Unary(context.Background(), "/test", nil, nil, nil, fail); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	called := false
	_ = b.Unary(context.Background(), "/test", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("failed HALF_OPEN probe did not reopen circuit")
	}
}
