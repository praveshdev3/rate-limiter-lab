package shards_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/praveshdev3/rate-limiter-lab/internal/limiter"
	"github.com/praveshdev3/rate-limiter-lab/internal/shards"
)

// TestStripedLimiter_Contention runs concurrent requests across distinct keys
func TestStripedLimiter_Contention(t *testing.T) {
	striped, err := shards.New(16, func() (shards.Shardable, error) {
		return limiter.NewFixedWindow(1000, time.Minute, nil), nil
	})
	if err != nil {
		t.Fatalf("failed to create striped limiter: %v", err)
	}

	var wg sync.WaitGroup
	ctx := context.Background()
	numWorkers := 50
	requestsPerWorker := 200

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < requestsPerWorker; j++ {
				key := fmt.Sprintf("client-%d-%d", workerID, j%10)
				_, _, _ = striped.Allow(ctx, key)
			}
		}(i)
	}
	wg.Wait()
}

// BenchmarkUnstriped measures throughput when a single global mutex is shared by all keys
func BenchmarkUnstriped(b *testing.B) {
	unstriped := limiter.NewFixedWindow(1000000, time.Minute, nil)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			key := fmt.Sprintf("user-%d", i%1000)
			_, _, _ = unstriped.Allow(ctx, key)
		}
	})
}

// BenchmarkStriped measures throughput when keys are distributed across 16 independent mutexes
func BenchmarkStriped(b *testing.B) {
	striped, _ := shards.New(16, func() (shards.Shardable, error) {
		return limiter.NewFixedWindow(1000000, time.Minute, nil), nil
	})
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			key := fmt.Sprintf("user-%d", i%1000)
			_, _, _ = striped.Allow(ctx, key)
		}
	})
}
