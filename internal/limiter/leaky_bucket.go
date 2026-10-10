package limiter

import (
	"context"
	"sync"
	"time"
)

type LeakyBucket struct {
	mu     sync.Mutex
	next   map[string]time.Time
	window time.Duration
	limit  int
	now    func() time.Time
}

func NewLeakyBucket(limit int, window time.Duration) *LeakyBucket {
	return &LeakyBucket{
		next:   make(map[string]time.Time),
		window: window,
		limit:  limit,
		now:    time.Now,
	}
}

func (l *LeakyBucket) Allow(
	_ context.Context,
	key string,
) (bool, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	next := l.next[key]

	if !now.Before(next) {
		l.next[key] = now.Add(l.window / time.Duration(l.limit))
		return true, 0, nil
	}

	return false, next.Sub(now), nil
}
