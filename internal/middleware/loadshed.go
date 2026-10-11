package middleware

import (
	"net/http"
)

// LoadShed returns an HTTP middleware that limits concurrent in-flight requests.
// When the semaphore is full, excess requests fail fast with 503 Service Unavailable.
func LoadShed(maxConcurrent int) func(http.Handler) http.Handler {
	sem := make(chan struct{}, maxConcurrent)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				// Acquired a concurrency slot
				defer func() { <-sem }()
				next.ServeHTTP(w, r)
			default:
				// Saturated: shed load immediately
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Service Unavailable: Server saturated\n", http.StatusServiceUnavailable)
			}
		})
	}
}
