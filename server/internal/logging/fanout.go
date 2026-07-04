// Package logging provides slog helpers shared by the server. The fan-out
// handler lets the app write to stdout and export to OTLP simultaneously.
package logging

import (
	"context"
	"log/slog"
)

// fanout is a slog.Handler that forwards each record to several child handlers,
// so a single logger can write to stdout and an OTLP exporter at once. Each
// child decides independently whether it is enabled for a given level.
type fanout struct {
	handlers []slog.Handler
}

// Fanout returns a slog.Handler that dispatches to all of handlers. With a
// single handler it returns that handler unchanged.
func Fanout(handlers ...slog.Handler) slog.Handler {
	if len(handlers) == 1 {
		return handlers[0]
	}
	return &fanout{handlers: handlers}
}

func (f *fanout) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f *fanout) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range f.handlers {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		// Clone per child: Handle may retain or mutate the record.
		if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (f *fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &fanout{handlers: next}
}

func (f *fanout) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithGroup(name)
	}
	return &fanout{handlers: next}
}
