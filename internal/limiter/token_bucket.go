package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	refillRate float64
	buckets    map[string]bucket
	now        func() time.Time
}

func NewTokenBucket(limit int, windowSize time.Duration, burst int) *TokenBucket {
	return &TokenBucket{
		capacity:   float64(burst),
		refillRate: float64(limit) / windowSize.Seconds(),
		buckets:    make(map[string]bucket),
		now:        time.Now,
	}
}

func (l *TokenBucket) Allow(
	_ context.Context,
	key string,
) (bool, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	bkt, exists := l.buckets[key]

	if !exists {
		bkt = bucket{
			tokens:   l.capacity,
			lastSeen: now,
		}
	} else {
		elapsed := now.Sub(bkt.lastSeen).Seconds()
		if elapsed > 0 {
			bkt.tokens = math.Min(
				l.capacity,
				bkt.tokens+elapsed*l.refillRate,
			)
		}
		bkt.lastSeen = now
	}

	if bkt.tokens < 1 {
		retry := time.Duration(
			math.Ceil((1 - bkt.tokens) / l.refillRate * float64(time.Second)),
		)
		l.buckets[key] = bkt
		return false, retry, nil
	}

	bkt.tokens--
	l.buckets[key] = bkt
	return true, 0, nil
}
