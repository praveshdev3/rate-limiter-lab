package limiter

import (
	"context"
	"sync"
	"time"
)

type windowState struct {
	startTime    time.Time
	requestCount int
}

type FixedWindow struct {
	mu         sync.Mutex
	windowSize time.Duration
	limit      int
	states     map[string]windowState
	now        func() time.Time
}

func NewFixedWindow(limit int, windowSize time.Duration) *FixedWindow {
	return &FixedWindow{
		windowSize: windowSize,
		limit:      limit,
		states:     make(map[string]windowState),
		now:        time.Now,
	}
}

func (l *FixedWindow) Allow(
	_ context.Context,
	key string,
) (bool, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	state := l.states[key]

	if state.startTime.IsZero() || !now.Before(state.startTime.Add(l.windowSize)) {
		state = windowState{startTime: now}
	}

	if state.requestCount >= l.limit {
		l.states[key] = state
		return false, state.startTime.Add(l.windowSize).Sub(now), nil
	}

	state.requestCount++
	l.states[key] = state
	return true, 0, nil
}
