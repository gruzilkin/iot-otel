package sensors

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// SensorRepo fetches a single downsampled series.
type SensorRepo interface {
	SmartFind(ctx context.Context, deviceID int64, sensorName string, start, end time.Time, limit int) ([]Point, error)
}

type Service struct {
	repo SensorRepo
}

func NewService(repo SensorRepo) *Service { return &Service{repo: repo} }

// ReadData fetches the series for each sensor concurrently (one query per
// sensor), mirroring the legacy coroutine fan-out.
func (s *Service) ReadData(ctx context.Context, deviceID int64, names []string, start, end time.Time, limit int) (map[string][]Point, error) {
	results := make([][]Point, len(names))
	g, ctx := errgroup.WithContext(ctx)
	for i, name := range names {
		g.Go(func() error {
			pts, err := s.repo.SmartFind(ctx, deviceID, name, start, end, limit)
			if err != nil {
				return err
			}
			results[i] = pts
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := make(map[string][]Point, len(names))
	for i, name := range names {
		out[name] = results[i]
	}
	return out, nil
}
