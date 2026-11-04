package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Breaker struct {
	mu                     sync.Mutex
	state                  string
	failures, threshold    int
	openFor, window        time.Duration
	openedAt, firstFailure time.Time
	generation             uint64
}

type probeContextKey struct{}

func circuitFailure(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.DataLoss:
		return true
	default:
		return false
	}
}

func NewBreaker(threshold int, openFor, window time.Duration) *Breaker {
	return &Breaker{state: "CLOSED", threshold: threshold, openFor: openFor, window: window}
}

// change is called with mu held. A generation ignores results of older in-flight calls.
func (b *Breaker) change(next string) {
	slog.Info("circuit transition", "from", b.state, "to", next)
	b.state = next
	b.generation++
}

func (b *Breaker) Unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	b.mu.Lock()
	if b.state == "OPEN" && time.Since(b.openedAt) >= b.openFor {
		b.change("HALF_OPEN")
		ctx = context.WithValue(ctx, probeContextKey{}, true)
	} else if b.state != "CLOSED" {
		b.mu.Unlock()
		return status.Error(codes.Unavailable, "flight service circuit breaker is open; retry later")
	}
	generation := b.generation
	b.mu.Unlock()

	err := invoke(ctx, method, req, reply, cc, opts...)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.generation != generation {
		return err
	}
	// Caller cancellation says nothing about Flight's health.
	if status.Code(err) == codes.Canceled {
		if b.state == "HALF_OPEN" {
			b.openedAt = time.Now()
			b.change("OPEN")
		}
		return err
	}
	if !circuitFailure(err) {
		b.failures = 0
		if b.state == "HALF_OPEN" {
			b.change("CLOSED")
		}
		return err
	}
	now := time.Now()
	if b.state == "HALF_OPEN" {
		b.openedAt = now
		b.change("OPEN")
		return err
	}
	if b.failures == 0 || now.Sub(b.firstFailure) > b.window {
		b.failures = 0
		b.firstFailure = now
	}
	b.failures++
	if b.failures >= b.threshold {
		b.openedAt = now
		b.change("OPEN")
	}
	return err
}
