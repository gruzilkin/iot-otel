package devices_test

import (
	"context"
	"testing"

	"github.com/gruzilkin/iot-otel/server/internal/devices"
)

// fakeStore implements devices.Store with in-memory maps.
type fakeStore struct {
	devs     map[int64]devices.Device // keyed by device id
	tokens   map[int64][]devices.Token
	inserted []devices.Token
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		devs:   map[int64]devices.Device{1: {ID: 1, UserID: 10, Name: "owned"}},
		tokens: map[int64][]devices.Token{1: {{Token: "t1", DeviceID: 1}}},
	}
}

func (f *fakeStore) FindAllByUserID(_ context.Context, userID int64) ([]devices.Device, error) {
	var out []devices.Device
	for _, d := range f.devs {
		if d.UserID == userID {
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeStore) FindByIDAndUserID(_ context.Context, id, userID int64) (devices.Device, error) {
	d, ok := f.devs[id]
	if !ok || d.UserID != userID {
		return devices.Device{}, devices.ErrNotFound
	}
	return d, nil
}

func (f *fakeStore) Insert(_ context.Context, userID int64, name string) (devices.Device, error) {
	id := int64(len(f.devs) + 1)
	d := devices.Device{ID: id, UserID: userID, Name: name}
	f.devs[id] = d
	return d, nil
}

func (f *fakeStore) DeleteByIDAndUserID(_ context.Context, id, userID int64) error {
	if d, ok := f.devs[id]; !ok || d.UserID != userID {
		return devices.ErrNotFound
	}
	delete(f.devs, id)
	return nil
}

func (f *fakeStore) ListTokens(_ context.Context, deviceID int64) ([]devices.Token, error) {
	return f.tokens[deviceID], nil
}

func (f *fakeStore) InsertToken(_ context.Context, t devices.Token) error {
	f.inserted = append(f.inserted, t)
	return nil
}

func (f *fakeStore) DeleteToken(_ context.Context, token string, deviceID int64) error {
	return nil
}

func TestCanAccess(t *testing.T) {
	svc := devices.NewService(newFakeStore())
	cases := []struct {
		userID, deviceID int64
		want             bool
	}{
		{10, 1, true},   // owner
		{11, 1, false},  // not owner
		{10, 99, false}, // missing
	}
	for _, c := range cases {
		got, err := svc.CanAccess(context.Background(), c.userID, c.deviceID)
		if err != nil {
			t.Fatalf("CanAccess(%d,%d): %v", c.userID, c.deviceID, err)
		}
		if got != c.want {
			t.Fatalf("CanAccess(%d,%d) = %v, want %v", c.userID, c.deviceID, got, c.want)
		}
	}
}

func TestAddTokenEnforcesOwnership(t *testing.T) {
	store := newFakeStore()
	svc := devices.NewService(store)

	if _, err := svc.AddToken(context.Background(), 11, 1); err != devices.ErrNotFound {
		t.Fatalf("non-owner AddToken: want ErrNotFound, got %v", err)
	}
	if len(store.inserted) != 0 {
		t.Fatal("non-owner AddToken should not insert a token")
	}

	tok, err := svc.AddToken(context.Background(), 10, 1)
	if err != nil {
		t.Fatalf("owner AddToken: %v", err)
	}
	if len(tok.Token) != 32 {
		t.Fatalf("token should be 32 hex chars, got %d", len(tok.Token))
	}
	if !tok.ValidUntil.After(tok.CreatedAt) {
		t.Fatal("ValidUntil should be after CreatedAt")
	}
	if len(store.inserted) != 1 {
		t.Fatalf("want 1 inserted token, got %d", len(store.inserted))
	}
}

func TestGetReturnsDeviceAndTokens(t *testing.T) {
	svc := devices.NewService(newFakeStore())
	d, toks, err := svc.Get(context.Background(), 10, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d.ID != 1 || len(toks) != 1 {
		t.Fatalf("unexpected device/tokens: %+v %+v", d, toks)
	}
	if _, _, err := svc.Get(context.Background(), 11, 1); err != devices.ErrNotFound {
		t.Fatalf("non-owner Get: want ErrNotFound, got %v", err)
	}
}
