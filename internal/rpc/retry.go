package rpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func transient(err error) bool {
	return status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}

// Retry makes at most three attempts, with 100ms and 200ms delays.
// A HALF_OPEN probe gets exactly one physical attempt with the same timeout.
func Retry(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var err error
		attempts := 3
		if probe, _ := ctx.Value(probeContextKey{}).(bool); probe {
			attempts = 1
		}
		for attempt := 0; attempt < attempts; attempt++ {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			callCtx, cancel := context.WithTimeout(ctx, timeout)
			err = invoke(callCtx, method, req, reply, cc, opts...)
			cancel()
			if !transient(err) || attempt == attempts-1 {
				return err
			}
			delay := 100 * time.Millisecond * time.Duration(1<<attempt)
			slog.Warn("grpc retry", "method", method, "attempt", attempt+2, "delay", delay, "code", status.Code(err).String())
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return status.FromContextError(ctx.Err()).Err()
			case <-timer.C:
			}
		}
		return err
	}
}
