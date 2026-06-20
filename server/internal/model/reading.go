// Package model holds the core domain types shared across the data plane.
// Keeping them here lets storage, ingest, and hub depend on the types without
// depending on each other.
package model

import "time"

// Reading is a single sensor measurement after the device id has been resolved
// from the bearer token. ObservedAt is the device-side timestamp (UTC).
type Reading struct {
	DeviceID   int64
	SensorName string
	Value      float64
	ObservedAt time.Time
}
