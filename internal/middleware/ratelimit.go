package middleware

import (
	"net"
	"net/http"

	"github.com/praveshdev3/rate-limiter-lab/internal/limiter"
)

func RateLimit(l limiter.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				http.Error(w, "Invalid remote address", http.StatusBadRequest)
				return
			}

			allowed, _, err := l.Allow(r.Context(), host)

			if err != nil {
				http.Error(
					w,
					"rate limiter unavailable",
					http.StatusServiceUnavailable,
				)
				return
			}

			if !allowed {
				http.Error(
					w,
					"rate limit exceeded",
					http.StatusTooManyRequests,
				)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
