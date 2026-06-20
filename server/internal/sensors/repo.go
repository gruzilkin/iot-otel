package sensors

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Querier is the subset of pgxpool.Pool the repo needs.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// smartFindSQL is ported verbatim from the legacy CustomSensorDataRepositoryImpl:
// the first point in range, the top-N points by Douglas–Peucker weight, and the
// last point in range. UNION (not UNION ALL) deduplicates identical rows.
const smartFindSQL = `(SELECT sensor_value, received_at FROM sensor_data
 WHERE device_id = $1 AND sensor_name = $2 AND received_at >= $3 AND received_at <= $4
 ORDER BY id ASC LIMIT 1)
UNION
(SELECT sensor_value, received_at FROM sensor_data JOIN sensor_data_weights USING (id)
 WHERE device_id = $1 AND sensor_name = $2 AND received_at >= $3 AND received_at <= $4
 ORDER BY weight DESC LIMIT $5)
UNION
(SELECT sensor_value, received_at FROM sensor_data
 WHERE device_id = $1 AND sensor_name = $2 AND received_at >= $3 AND received_at <= $4
 ORDER BY id DESC LIMIT 1)`

type pgxRepo struct{ q Querier }

func NewPgxRepo(q Querier) SensorRepo { return &pgxRepo{q: q} }

func (r *pgxRepo) SmartFind(ctx context.Context, deviceID int64, sensorName string, start, end time.Time, limit int) ([]Point, error) {
	rows, err := r.q.Query(ctx, smartFindSQL, deviceID, sensorName, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pts []Point
	for rows.Next() {
		var v float64
		var ts time.Time
		if err := rows.Scan(&v, &ts); err != nil {
			return nil, err
		}
		pts = append(pts, Point{TimestampMillis: ts.UnixMilli(), Value: Floor3(v)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// UNION does not guarantee order; the chart expects ascending time.
	sort.Slice(pts, func(i, j int) bool { return pts[i].TimestampMillis < pts[j].TimestampMillis })
	return pts, nil
}
