package shards

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"
)

// Shardable represents any limiter that can act as an isolated shard.
type Shardable interface {
	Allow(ctx context.Context, key string) (bool, time.Duration, error)
}

// StripedLimiter partitions rate-limiting across N shards to eliminate
// global mutex contention across unrelated keys in memory.
type StripedLimiter struct {
	numShards uint32
	shards    []Shardable
}

// New creates a StripedLimiter with numShards instances.
func New(numShards int, factory func() (Shardable, error)) (*StripedLimiter, error) {
	if numShards <= 0 {
		return nil, fmt.Errorf("numShards must be greater than 0")
	}

	shardsList := make([]Shardable, numShards)
	for i := 0; i < numShards; i++ {
		instance, err := factory()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize shard %d: %w", i, err)
		}
		shardsList[i] = instance
	}

	return &StripedLimiter{
		numShards: uint32(numShards),
		shards:    shardsList,
	}, nil
}

// shardIndex computes a stable hash for a key using FNV-1a.
func (s *StripedLimiter) shardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % s.numShards
}

// Allow routes the key to its assigned shard.
func (s *StripedLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	idx := s.shardIndex(key)
	return s.shards[idx].Allow(ctx, key)
}
