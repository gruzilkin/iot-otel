package sensors

import "strconv"

// Point is one chart sample. It marshals to a [epochMillis, value] array to
// match the legacy chart JSON (the timestamp stays an integer, not 1.7e12).
type Point struct {
	TimestampMillis int64
	Value           float64
}

func (p Point) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 28)
	b = append(b, '[')
	b = strconv.AppendInt(b, p.TimestampMillis, 10)
	b = append(b, ',')
	b = strconv.AppendFloat(b, p.Value, 'f', -1, 64)
	b = append(b, ']')
	return b, nil
}
