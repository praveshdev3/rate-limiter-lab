package limiter

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// slidingWindowScript implements the sliding window log using a Redis Sorted Set (ZSET).
// KEYS[1]: rate limiter key
// ARGV[1]: now_ms (current timestamp in milliseconds)
// ARGV[2]: window_ms (window duration in milliseconds)
// ARGV[3]: limit (maximum allowed requests)
// ARGV[4]: ttl_sec (key safety TTL)
var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local cutoff = now - window

-- 1. Remove expired timestamps outside the current window
redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff)

-- 2. Count timestamps currently inside the window
local count = redis.call("ZCARD", key)

if count >= limit then
    -- Oldest timestamp in the active window determines retry_after
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local oldest_time = tonumber(oldest[2])
    local retry_after_ms = (oldest_time + window) - now
    if retry_after_ms < 0 then
        retry_after_ms = 0
    end
    redis.call("EXPIRE", key, ttl)
    return {0, retry_after_ms}
else
    -- Add current timestamp as both score and unique member
    -- Concatenate now with a random/sequence suffix if multiple requests arrive within the same ms
    local member = tostring(now) .. ":" .. tostring(redis.call("INCR", key .. ":seq"))
    redis.call("ZADD", key, now, member)
    redis.call("EXPIRE", key, ttl)
    redis.call("EXPIRE", key .. ":seq", ttl)
    return {1, 0}
end
`)

type SlidingWindow struct {
	limit  int
	window time.Duration
	mu     sync.Mutex
	states map[string][]time.Time
	now    func() time.Time
	client *redis.Client
}

func NewSlidingWindow(limit int, window time.Duration, client *redis.Client) *SlidingWindow {
	return &SlidingWindow{
		limit:  limit,
		window: window,
		states: make(map[string][]time.Time),
		now:    time.Now,
		client: client,
	}
}

func (l *SlidingWindow) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	if l.client != nil {
		return l.allowRedis(ctx, key)
	}
	return l.allowMemory(ctx, key)
}

func (l *SlidingWindow) allowMemory(_ context.Context, key string) (bool, time.Duration, error) {
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

func (l *SlidingWindow) allowRedis(ctx context.Context, key string) (bool, time.Duration, error) {
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)
	windowMs := l.window.Milliseconds()
	ttlSec := int(math.Ceil(l.window.Seconds())) + 5
	redisKey := "rl:sw:" + key

	res, err := slidingWindowScript.Run(ctx, l.client, []string{redisKey},
		nowMs,
		windowMs,
		l.limit,
		ttlSec,
	).Slice()
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
