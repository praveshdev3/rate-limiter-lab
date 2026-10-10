package limiter

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript atomically computes token regeneration and consumption.
// ARGV[1]: capacity (float)
// ARGV[2]: refill_rate (float tokens per second)
// ARGV[3]: cost (float, typically 1.0)
// ARGV[4]: now (float Unix epoch seconds)
// ARGV[5]: ttl_sec (integer key expiration seconds)
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call("HMGET", key, "tokens", "last_seen")
local tokens = tonumber(data[1])
local last_seen = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_seen = now
else
    local elapsed = math.max(0, now - last_seen)
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_seen = now
end

if tokens < cost then
    local missing = cost - tokens
    local retry_after_sec = math.ceil(missing / refill_rate)
    redis.call("HMSET", key, "tokens", tokens, "last_seen", last_seen)
    redis.call("EXPIRE", key, ttl)
    return {0, retry_after_sec}
else
    tokens = tokens - cost
    redis.call("HMSET", key, "tokens", tokens, "last_seen", last_seen)
    redis.call("EXPIRE", key, ttl)
    return {1, 0}
end
`)

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

type TokenBucket struct {
	capacity   float64
	refillRate float64
	mu         sync.Mutex
	buckets    map[string]bucket
	now        func() time.Time
	client     *redis.Client
}

func NewTokenBucket(limit int, windowSize time.Duration, burst int, client *redis.Client) *TokenBucket {
	return &TokenBucket{
		capacity:   float64(burst),
		refillRate: float64(limit) / windowSize.Seconds(),
		buckets:    make(map[string]bucket),
		now:        time.Now,
		client:     client,
	}
}

func (l *TokenBucket) Allow(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	if l.client != nil {
		return l.allowRedis(ctx, key)
	}
	return l.allowMemory(ctx, key)
}

func (l *TokenBucket) allowMemory(
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

func (l *TokenBucket) allowRedis(
	ctx context.Context,
	key string,
) (bool, time.Duration, error) {
	nowSec := float64(time.Now().UnixNano()) / 1e9
	ttlSec := int(math.Ceil(l.capacity/l.refillRate)) + 60
	redisKey := "rl:tb:" + key

	res, err := tokenBucketScript.Run(ctx, l.client, []string{redisKey},
		l.capacity,
		l.refillRate,
		1.0, // cost
		nowSec,
		ttlSec,
	).Slice()
	if err != nil {
		return false, 0, err
	}

	allowed := res[0].(int64) == 1
	retryAfterSec := res[1].(int64)

	var retryAfter time.Duration
	if retryAfterSec > 0 {
		retryAfter = time.Duration(retryAfterSec) * time.Second
	}

	return allowed, retryAfter, nil
}
