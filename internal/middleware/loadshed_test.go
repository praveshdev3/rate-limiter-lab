package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/praveshdev3/rate-limiter-lab/internal/middleware"
)

func TestLoadShed_Saturation(t *testing.T) {
	maxConcurrent := 2
	shedder := middleware.LoadShed(maxConcurrent)

	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(shedder(slowHandler))
	defer ts.Close()

	var wg sync.WaitGroup
	statusCodes := make([]int, 5)

	// Send 5 concurrent requests simultaneously against a capacity of 2
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := http.Get(ts.URL)
			if err != nil {
				t.Errorf("request failed: %v", err)
				return
			}
			statusCodes[idx] = resp.StatusCode
			_ = resp.Body.Close()
		}(i)
	}

	wg.Wait()

	var count503, count200 int
	for _, code := range statusCodes {
		if code == http.StatusServiceUnavailable {
			count503++
		} else if code == http.StatusOK {
			count200++
		}
	}

	if count503 == 0 {
		t.Fatalf("expected at least one 503 response, got none (all status codes: %v)", statusCodes)
	}
	if count200 > maxConcurrent {
		t.Fatalf("expected at most %d 200 OK responses, got %d", maxConcurrent, count200)
	}
}
