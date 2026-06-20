// Package sensors holds sensor-reading read/query helpers shared by the realtime
// and chart paths.
package sensors

import "math"

// Floor3 rounds toward negative infinity to 3 decimal places, matching the
// legacy BigDecimal setScale(3, RoundingMode.FLOOR) used for chart/realtime
// values (handles negative temperatures correctly).
func Floor3(v float64) float64 {
	return math.Floor(v*1000) / 1000
}
