package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port string

	Algorithm string
	Limit     int
	Window    time.Duration
	Burst     int

	Striped bool
	Shards  int

	RedisEnabled  bool
	RedisAddr     string
	RedisPassword string
	RedisDB       int
}

func Load() (Config, error) {
	cfg := Config{
		Port:          env("PORT", "8080"),
		Algorithm:     env("ALGORITHM", "fixed_window"),
		Limit:         1,
		Window:        time.Minute,
		Burst:         5,
		Striped:       envBool("STRIPED", false),
		Shards:        16,
		RedisEnabled:  envBool("REDIS_ENABLED", false),
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		RedisPassword: env("REDIS_PASSWORD", ""),
		RedisDB:       0,
	}

	var err error

	if cfg.Limit, err = envInt("RATE_LIMIT", cfg.Limit); err != nil {
		return cfg, err
	}

	if cfg.Burst, err = envInt("BURST", cfg.Burst); err != nil {
		return cfg, err
	}

	if cfg.Shards, err = envInt("SHARDS", cfg.Shards); err != nil {
		return cfg, err
	}

	if cfg.RedisDB, err = envInt("REDIS_DB", cfg.RedisDB); err != nil {
		return cfg, err
	}

	window := env("WINDOW", "1m")
	if cfg.Window, err = time.ParseDuration(window); err != nil {
		return cfg, fmt.Errorf("invalid WINDOW: %w", err)
	}

	if cfg.Limit <= 0 || cfg.Window <= 0 || cfg.Burst <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT, WINDOW and BURST must be positive")
	}

	if cfg.Shards <= 0 {
		return cfg, fmt.Errorf("SHARDS must be positive")
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
