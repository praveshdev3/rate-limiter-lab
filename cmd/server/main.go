package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/praveshdev3/rate-limiter-lab/internal/config"
	"github.com/praveshdev3/rate-limiter-lab/internal/limiter"
	"github.com/praveshdev3/rate-limiter-lab/internal/middleware"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	var rdb *redis.Client
	if cfg.RedisEnabled {
		rdb = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Fatalf("failed to connect to redis: %v", err)
		}
		defer rdb.Close()
		log.Printf("redis connected at %s", cfg.RedisAddr)
	}

	lim, err := limiter.New(cfg, rdb)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/demo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Request allowed\n"))
	})

	mux.HandleFunc("/api/work", func(w http.ResponseWriter, r *http.Request) {
		// Simulates 150ms of CPU/DB work
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("Work completed\n"))
	})

	var handler http.Handler = mux
	handler = middleware.RateLimit(lim)(handler)
	handler = middleware.LoadShed(cfg.MaxConcurrent)(handler)

	port := cfg.Port
	if port == "" {
		port = "8080"
	}

	log.Printf("server starting on :%s | algo=%s redis=%t striped=%t max_concurrent=%d",
		port, cfg.Algorithm, cfg.RedisEnabled, cfg.Striped, cfg.MaxConcurrent)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), handler))
}
