package main

import (
	"log"
	"net/http"

	"github.com/praveshdev3/rate-limiter-lab/internal/config"
	"github.com/praveshdev3/rate-limiter-lab/internal/limiter"
	"github.com/praveshdev3/rate-limiter-lab/internal/middleware"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	lim, err := limiter.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/demo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Request allowed\n"))
	})

	handler := middleware.RateLimit(lim)(mux)

	log.Printf("algorithm=%s limit=%d window=%s burst=%d",
		cfg.Algorithm, cfg.Limit, cfg.Window, cfg.Burst)

	log.Fatal(http.ListenAndServe(":8080", handler))
}
