package limiter

import (
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/praveshdev3/rate-limiter-lab/internal/config"
	"github.com/praveshdev3/rate-limiter-lab/internal/shards"
)

// createBaseLimiter instantiates a single base limiter implementation.
func createBaseLimiter(cfg config.Config, rdb *redis.Client) (Limiter, error) {
	switch cfg.Algorithm {
	case "fixed_window":
		return NewFixedWindow(cfg.Limit, cfg.Window, rdb), nil
	case "token_bucket":
		return NewTokenBucket(cfg.Limit, cfg.Window, cfg.Burst, rdb), nil
	case "sliding_window":
		return NewSlidingWindow(cfg.Limit, cfg.Window, rdb), nil
	case "leaky_bucket":
		return NewLeakyBucket(cfg.Limit, cfg.Window, rdb), nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q", cfg.Algorithm)
	}
}

func New(cfg config.Config, rdb *redis.Client) (Limiter, error) {
	// If Redis is enabled, distributed atomic operations handle coordination across instances
	if cfg.RedisEnabled {
		return createBaseLimiter(cfg, rdb)
	}

	// If Striped is enabled for in-memory, shard across N independent instances
	if cfg.Striped {
		return shards.New(cfg.Shards, func() (shards.Shardable, error) {
			return createBaseLimiter(cfg, nil)
		})
	}

	// Default unstriped single in-memory instance
	return createBaseLimiter(cfg, nil)
}
