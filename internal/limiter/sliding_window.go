package limiter

import (
	"context"
	"sync"
	"time"
)

type SlidingWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	states map[string][]time.Time
	now    func() time.Time
}

func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:  limit,
		window: window,
		states: make(map[string][]time.Time),
		now:    time.Now,
	}
}

func (l *SlidingWindow) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	cutoff := now.Add(-l.window)
	old := l.states[key]

	// Remove expired timestamps.
	first := 0
	for first < len(old) && !old[first].After(cutoff) {
		first++
	}

	active := old[first:]

	if len(active) >= l.limit {
		retry := active[0].Add(l.window).Sub(now)
		l.states[key] = active
		return false, retry, nil
	}

	active = append(active, now)
	l.states[key] = active
	return true, 0, nil
}
