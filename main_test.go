package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerPicksDistinctKeys(t *testing.T) {
	s := newScheduler([]string{"nvapi-one", "nvapi-two", "nvapi-three", "nvapi-four"})
	picked := s.pick(3, map[int]struct{}{})
	if len(picked) != 3 {
		t.Fatalf("got %d keys, want 3", len(picked))
	}
	seen := map[int]bool{}
	for _, key := range picked {
		if seen[key.index] {
			t.Fatalf("duplicate key index %d", key.index)
		}
		seen[key.index] = true
		s.release(key.index)
	}
	snapshot := s.snapshot()
	if snapshot.Available != 4 || snapshot.Cooldown != 0 || snapshot.Disabled != 0 || snapshot.InFlight != 0 {
		t.Fatalf("unexpected scheduler state after normal release: %+v", snapshot)
	}
}

func TestFastestValidSSEWinsAndLosersStayHealthy(t *testing.T) {
	keys := []string{"nvapi-slow-a", "nvapi-fast", "nvapi-slow-b"}
	var started atomic.Int32
	ready := make(chan struct{})
	var closeReady sync.Once
	var canceled atomic.Int32
	seenKeys := sync.Map{}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		seenKeys.Store(key, true)
		if started.Add(1) == 3 {
			closeReady.Do(func() { close(ready) })
		}
		select {
		case <-ready:
		case <-r.Context().Done():
			canceled.Add(1)
			return
		}

		delay := 150 * time.Millisecond
		marker := "slow"
		if key == "nvapi-fast" {
			delay = 10 * time.Millisecond
			marker = "winner"
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			canceled.Add(1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", marker)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if key == "nvapi-fast" {
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		}
	}))
	defer upstream.Close()

	baseURL, err := url.Parse(upstream.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &proxyServer{
		cfg: config{
			BaseURL:      baseURL,
			Fanout:       3,
			MaxWaves:     1,
			MaxBodyBytes: defaultMaxBodyBytes,
			MaxRespBytes: defaultMaxReplyBytes,
		},
		client:    upstream.Client(),
		scheduler: newScheduler(keys),
	}

	request := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", strings.NewReader(`{"model":"test","stream":true}`))
	recorder := httptest.NewRecorder()
	proxy.handleProxy(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.StatusCode, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "winner") || strings.Contains(recorder.Body.String(), "slow") {
		t.Fatalf("unexpected winner response: %s", recorder.Body.String())
	}
	count := 0
	seenKeys.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 3 {
		t.Fatalf("upstream saw %d distinct keys, want 3", count)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := proxy.scheduler.snapshot()
		if snapshot.InFlight == 0 && canceled.Load() == 2 {
			if snapshot.Available != 3 || snapshot.Cooldown != 0 || snapshot.Disabled != 0 {
				t.Fatalf("losers must remain healthy after cancellation: %+v", snapshot)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("contenders did not settle: state=%+v canceled_losers=%d", proxy.scheduler.snapshot(), canceled.Load())
}

func TestWinnerCancellationDoesNotTruncateStream(t *testing.T) {
	keys := []string{"nvapi-a", "nvapi-b", "nvapi-c"}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		flusher.Flush()
		select {
		case <-time.After(20 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"last\"}}]}\n\ndata: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()
	baseURL, _ := url.Parse(upstream.URL + "/v1")
	proxy := &proxyServer{
		cfg:       config{BaseURL: baseURL, Fanout: 3, MaxWaves: 1, MaxBodyBytes: defaultMaxBodyBytes, MaxRespBytes: defaultMaxReplyBytes},
		client:    upstream.Client(),
		scheduler: newScheduler(keys),
	}
	request := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	recorder := httptest.NewRecorder()
	proxy.handleProxy(recorder, request)
	if body := recorder.Body.String(); !strings.Contains(body, "first") || !strings.Contains(body, "last") || !strings.Contains(body, "[DONE]") {
		t.Fatalf("winner stream was truncated: %s", body)
	}
}

func TestJoinUpstreamURLDeduplicatesV1(t *testing.T) {
	base, _ := url.Parse("https://integrate.api.nvidia.com/v1")
	incoming, _ := url.Parse("https://proxy/v1/chat/completions?x=1")
	got := joinUpstreamURL(base, incoming)
	want := "https://integrate.api.nvidia.com/v1/chat/completions?x=1"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCanceledAttemptDoesNotCooldownKey(t *testing.T) {
	s := newScheduler([]string{"nvapi-one", "nvapi-two", "nvapi-three"})
	picked := s.pick(3, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ctx.Err() == nil {
		t.Fatal("expected canceled context")
	}
	for _, key := range picked {
		s.release(key.index)
	}
	snapshot := s.snapshot()
	if snapshot.Available != 3 || snapshot.Cooldown != 0 {
		t.Fatalf("normal cancellation changed key health: %+v", snapshot)
	}
}

func TestHTTPClientCancellationPrimitive(t *testing.T) {
	started := make(chan struct{})
	serverCanceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
			close(serverCanceled)
		case <-time.After(time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer upstream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		response, _ := upstream.Client().Do(request)
		if response != nil {
			response.Body.Close()
		}
		close(done)
	}()
	<-started
	cancel()

	select {
	case <-serverCanceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("standard HTTP request context did not cancel the upstream handler")
	}
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("standard HTTP client did not return after cancellation")
	}
}
