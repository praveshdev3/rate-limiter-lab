package limiter

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// fixedWindowScript atomically handles the fixed window counter.
// ARGV[1]: limit, ARGV[2]: window_in_milliseconds
var fixedWindowScript = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

local current = redis.call("INCR", key)

if current == 1 then
    redis.call("PEXPIRE", key, window_ms)
end

if current > limit then
    local ttl = redis.call("PTTL", key)
    if ttl < 0 then
        ttl = window_ms
    end
    return {0, ttl}
else
    return {1, 0}
end
`)

type windowState struct {
	startTime    time.Time
	requestCount int
}

type FixedWindow struct {
	windowSize time.Duration
	limit      int
	mu         sync.Mutex
	states     map[string]windowState
	now        func() time.Time
	client     *redis.Client
}

func NewFixedWindow(limit int, windowSize time.Duration, client *redis.Client) *FixedWindow {
	return &FixedWindow{
		windowSize: windowSize,
		limit:      limit,
		states:     make(map[string]windowState),
		now:        time.Now,
		client:     client,
	}
}

func (l *FixedWindow) Allow(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	if l.client != nil {
		return l.allowRedis(ctx, key)
	}
	return l.allowMemory(ctx, key)
}

func (l *FixedWindow) allowMemory(
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

func (l *FixedWindow) allowRedis(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	windowMs := l.windowSize.Milliseconds()
	redisKey := "rl:fw:" + key

	res, err := fixedWindowScript.Run(ctx, l.client, []string{redisKey}, l.limit, windowMs).Slice()
	if err != nil {
		return false, 0, err
	}

	allowed := res[0].(int64) == 1
	retryAfterMs := res[1].(int64)

	var retryAfter time.Duration
	if retryAfterMs > 0 {
		retryAfter = time.Duration(retryAfterMs) * time.Millisecond
	}

	return allowed, retryAfter, nil
}
