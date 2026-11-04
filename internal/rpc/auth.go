package rpc

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func ServerAuth(key string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := checkKey(ctx, key); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// Health Watch is streaming; protect it with the same metadata policy.
func StreamAuth(key string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := checkKey(stream.Context(), key); err != nil {
			return err
		}
		return next(srv, stream)
	}
}

func checkKey(ctx context.Context, key string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-api-key")
	if key == "" || len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(key)) != 1 {
		return status.Error(codes.Unauthenticated, "valid x-api-key metadata is required")
	}
	return nil
}

func ClientAuth(key string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return next(metadata.AppendToOutgoingContext(ctx, "x-api-key", key), method, req, reply, cc, opts...)
	}
}
