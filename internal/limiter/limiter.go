package limiter

import (
	"context"
	"time"
)

type Limiter interface {
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}
