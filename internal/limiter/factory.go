package limiter

import (
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/praveshdev3/rate-limiter-lab/internal/config"
)

func New(cfg config.Config, rdb *redis.Client) (Limiter, error) {
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
