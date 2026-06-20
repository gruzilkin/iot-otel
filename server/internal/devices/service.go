package devices

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

const tokenValidity = 365 * 24 * time.Hour

// Store is the persistence surface the service needs (satisfied by *Repo).
type Store interface {
	FindAllByUserID(ctx context.Context, userID int64) ([]Device, error)
	FindByIDAndUserID(ctx context.Context, id, userID int64) (Device, error)
	Insert(ctx context.Context, userID int64, name string) (Device, error)
	DeleteByIDAndUserID(ctx context.Context, id, userID int64) error
	ListTokens(ctx context.Context, deviceID int64) ([]Token, error)
	InsertToken(ctx context.Context, t Token) error
	DeleteToken(ctx context.Context, token string, deviceID int64) error
}

type Service struct {
	store Store
	now   func() time.Time
}

func NewService(store Store) *Service { return &Service{store: store, now: time.Now} }

// CanAccess reports whether the user owns the device.
func (s *Service) CanAccess(ctx context.Context, userID, deviceID int64) (bool, error) {
	_, err := s.store.FindByIDAndUserID(ctx, deviceID, userID)
	switch {
	case err == nil:
		return true, nil
	case isNotFound(err):
		return false, nil
	default:
		return false, err
	}
}

func (s *Service) List(ctx context.Context, userID int64) ([]Device, error) {
	return s.store.FindAllByUserID(ctx, userID)
}

func (s *Service) Create(ctx context.Context, userID int64, name string) (Device, error) {
	return s.store.Insert(ctx, userID, name)
}

func (s *Service) Delete(ctx context.Context, userID, deviceID int64) error {
	return s.store.DeleteByIDAndUserID(ctx, deviceID, userID)
}

// Get returns a device and its tokens, enforcing ownership.
func (s *Service) Get(ctx context.Context, userID, deviceID int64) (Device, []Token, error) {
	d, err := s.store.FindByIDAndUserID(ctx, deviceID, userID)
	if err != nil {
		return Device{}, nil, err
	}
	toks, err := s.store.ListTokens(ctx, deviceID)
	return d, toks, err
}

// AddToken generates a new access token for an owned device.
func (s *Service) AddToken(ctx context.Context, userID, deviceID int64) (Token, error) {
	if _, err := s.store.FindByIDAndUserID(ctx, deviceID, userID); err != nil {
		return Token{}, err
	}
	now := s.now()
	t := Token{Token: newToken(), DeviceID: deviceID, CreatedAt: now, ValidUntil: now.Add(tokenValidity)}
	if err := s.store.InsertToken(ctx, t); err != nil {
		return Token{}, err
	}
	return t, nil
}

func (s *Service) DeleteToken(ctx context.Context, userID, deviceID int64, token string) error {
	if _, err := s.store.FindByIDAndUserID(ctx, deviceID, userID); err != nil {
		return err
	}
	return s.store.DeleteToken(ctx, token, deviceID)
}

func isNotFound(err error) bool { return err == ErrNotFound }

// newToken returns a 32-character hex token (matching access_tokens.token).
func newToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
