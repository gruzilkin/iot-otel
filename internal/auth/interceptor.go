package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type deviceIDKey struct{}

// WithDeviceID injects an authenticated device id into the context. The
// interceptor calls it after validating the token; it's also useful for tests.
func WithDeviceID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, deviceIDKey{}, id)
}

// DeviceIDFromContext returns the authenticated device id injected by the
// interceptor, if present.
func DeviceIDFromContext(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(deviceIDKey{}).(int64)
	return id, ok
}

// StreamAuthInterceptor validates the bearer token in the stream's metadata and
// injects the resolved device id into the context. All failures map to
// Unauthenticated so the client can distinguish them from transport errors.
func StreamAuthInterceptor(ts *TokenStore) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		token, err := bearerToken(ss.Context())
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}
		deviceID, err := ts.Lookup(ss.Context(), token)
		if err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithDeviceID(ss.Context(), deviceID)})
	}
}

func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoToken
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", ErrNoToken
	}
	parts := strings.SplitN(vals[0], " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", ErrNoToken
	}
	return parts[1], nil
}

// wrappedStream overrides Context() so the handler sees the injected device id.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
