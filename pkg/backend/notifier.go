package backend

import (
	"context"
	"time"
)

// Notifier is the source of change events the watch layer tails. It is the seam
// that lets the default polling implementation be swapped for a Spanner Change
// Streams implementation without touching watch fan-out or any other path.
// Implementations return events in ascending revision order, may return zero
// events, and the watch layer waits Interval() between calls.
type Notifier interface {
	Poll(ctx context.Context, cursor int64) ([]*Event, error)
	Interval() time.Duration
}

// pollNotifier is the default Notifier: it tails the global log by querying
// After(cursor) on a fixed interval. Watch latency is ~Interval; it adds no
// extra moving parts and works against any Backend.
type pollNotifier struct {
	be       Backend
	interval time.Duration
}

// NewPollNotifier returns the default polling Notifier.
func NewPollNotifier(be Backend, interval time.Duration) Notifier {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &pollNotifier{be: be, interval: interval}
}

func (p *pollNotifier) Poll(ctx context.Context, cursor int64) ([]*Event, error) {
	return p.be.After(ctx, "", "", cursor, 0)
}

func (p *pollNotifier) Interval() time.Duration { return p.interval }
