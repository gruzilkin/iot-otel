package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestFanoutDispatchesToAllHandlers(t *testing.T) {
	var a, b bytes.Buffer
	log := slog.New(Fanout(
		slog.NewTextHandler(&a, nil),
		slog.NewTextHandler(&b, nil),
	))

	log.Info("hello", "k", "v")

	for name, buf := range map[string]*bytes.Buffer{"a": &a, "b": &b} {
		if !strings.Contains(buf.String(), "hello") || !strings.Contains(buf.String(), "k=v") {
			t.Errorf("handler %s missing record: %q", name, buf.String())
		}
	}
}

func TestFanoutWithAttrsAndGroupPropagate(t *testing.T) {
	var a, b bytes.Buffer
	base := Fanout(slog.NewTextHandler(&a, nil), slog.NewTextHandler(&b, nil))
	log := slog.New(base).With("svc", "iotd").WithGroup("req")

	log.Info("done", "code", 200)

	for name, buf := range map[string]*bytes.Buffer{"a": &a, "b": &b} {
		s := buf.String()
		if !strings.Contains(s, "svc=iotd") || !strings.Contains(s, "req.code=200") {
			t.Errorf("handler %s missing attr/group: %q", name, s)
		}
	}
}

func TestFanoutEnabledIfAnyChildEnabled(t *testing.T) {
	// One child only logs Warn+, the other logs everything.
	warnOnly := slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	all := slog.NewTextHandler(&bytes.Buffer{}, nil)
	h := Fanout(warnOnly, all)

	if !h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Enabled(Info) = false; want true because one child accepts Info")
	}
}

func TestFanoutHandleRespectsPerChildLevel(t *testing.T) {
	var quiet, loud bytes.Buffer
	log := slog.New(Fanout(
		slog.NewTextHandler(&quiet, &slog.HandlerOptions{Level: slog.LevelWarn}),
		slog.NewTextHandler(&loud, nil),
	))

	log.Info("info-line")

	if quiet.Len() != 0 {
		t.Errorf("warn-only handler received an Info record: %q", quiet.String())
	}
	if !strings.Contains(loud.String(), "info-line") {
		t.Errorf("all-level handler missing Info record: %q", loud.String())
	}
}

func TestFanoutSingleHandlerReturnedDirectly(t *testing.T) {
	h := slog.NewTextHandler(&bytes.Buffer{}, nil)
	if got := Fanout(h); got != slog.Handler(h) {
		t.Error("Fanout with one handler should return it unchanged")
	}
}
