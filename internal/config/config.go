package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Algorithm string
	Limit     int
	Window    time.Duration
	Burst     int
}

func Load() (Config, error) {
	cfg := Config{
		Algorithm: env("ALGORITHM", "fixed_window"),
		Limit:     1,
		Window:    time.Minute,
		Burst:     5,
	}

	var err error

	if cfg.Limit, err = envInt("RATE_LIMIT", cfg.Limit); err != nil {
		return cfg, err
	}

	if cfg.Burst, err = envInt("BURST", cfg.Burst); err != nil {
		return cfg, err
	}

	window := env("WINDOW", "1m")
	if cfg.Window, err = time.ParseDuration(window); err != nil {
		return cfg, fmt.Errorf("invalid WINDOW: %w", err)
	}

	if cfg.Limit <= 0 || cfg.Window <= 0 || cfg.Burst <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT, WINDOW and BURST must be positive")
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
