package limiter

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// leakyBucketScript models the Generic Cell Rate Algorithm (GCRA) / Leaky Bucket.
// ARGV[1]: emission_interval_ms (window_ms / limit)
// ARGV[2]: now_ms
// ARGV[3]: ttl_seconds
var leakyBucketScript = redis.NewScript(`
local key = KEYS[1]
local emission_interval = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local next_allowed = tonumber(redis.call("GET", key))

if not next_allowed or now >= next_allowed then
    local new_next = now + emission_interval
    redis.call("SET", key, new_next, "EX", ttl)
    return {1, 0}
else
    local retry_after_ms = next_allowed - now
    return {0, retry_after_ms}
end
`)

type LeakyBucket struct {
	window time.Duration
	limit  int
	mu     sync.Mutex
	next   map[string]time.Time
	now    func() time.Time
	client *redis.Client
}

func NewLeakyBucket(limit int, window time.Duration, client *redis.Client) *LeakyBucket {
	return &LeakyBucket{
		limit:  limit,
		window: window,
		next:   make(map[string]time.Time),
		now:    time.Now,
		client: client,
	}
}

func (l *LeakyBucket) Allow(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	if l.client != nil {
		return l.allowRedis(ctx, key)
	}
	return l.allowMemory(ctx, key)
}

func (l *LeakyBucket) allowMemory(
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

func (l *LeakyBucket) allowRedis(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	emissionIntervalMs := (l.window / time.Duration(l.limit)).Milliseconds()
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)
	ttlSec := int(math.Ceil(l.window.Seconds())) + 5

	redisKey := "rl:lb:" + key

	res, err := leakyBucketScript.Run(ctx, l.client, []string{redisKey},
		emissionIntervalMs,
		nowMs,
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
