package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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

func TestSchedulerPickSkipsKeysAlreadyInFlight(t *testing.T) {
	s := newScheduler([]string{"nvapi-one", "nvapi-two", "nvapi-three"})
	first := s.pick(1, nil)
	if len(first) != 1 || first[0].index != 0 {
		t.Fatalf("first pick=%v, want slot 0", first)
	}

	// Force the next scan to begin at the occupied slot. An in-flight key must
	// not be handed to another request even when it is otherwise healthy.
	s.mu.Lock()
	s.cursor = first[0].index
	s.mu.Unlock()
	second := s.pick(3, nil)
	defer func() {
		s.release(first[0].index)
		for _, key := range second {
			s.release(key.index)
		}
	}()

	if len(second) != 2 {
		t.Fatalf("second pick returned %d keys, want the 2 idle keys", len(second))
	}
	for _, key := range second {
		if key.index == first[0].index {
			t.Fatalf("second pick reused in-flight slot %d", key.index)
		}
	}
}

func TestThreeWayRaceFastestValidSSEWinsAndLosersStayHealthy(t *testing.T) {
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

func TestSSEErrorStatusVariantsReturn429AndCooldownForOneMinute(t *testing.T) {
	tests := []struct {
		name         string
		errorPayload string
	}{
		{name: "status number", errorPayload: `{"status":429,"message":"raw-status-marker"}`},
		{name: "status_code string", errorPayload: `{"status_code":"429","message":"raw-status-code-marker"}`},
		{name: "code number", errorPayload: `{"code":429,"message":"raw-code-marker"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"error\":%s}\n\n", test.errorPayload)
			}))
			defer upstream.Close()

			proxy := newTestProxy(t, upstream, []string{"fake-sse-key"}, 1, 1)
			started := time.Now()
			recorder := exerciseProxy(proxy, `{"model":"test","stream":true}`)
			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status=%d body=%s, want 429", recorder.Code, recorder.Body.String())
			}
			assertCooldownNear(t, proxy.scheduler, 0, started.Add(time.Minute), time.Second)
		})
	}
}

func TestHTTPAndSSE429UseTheSameOneMinutePolicy(t *testing.T) {
	httpUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A shorter upstream hint must not weaken the proxy's one-minute policy.
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"raw-http-429-marker"}}`)
	}))
	defer httpUpstream.Close()
	httpProxy := newTestProxy(t, httpUpstream, []string{"fake-http-key"}, 1, 1)
	httpStarted := time.Now()
	httpRecorder := exerciseProxy(httpProxy, `{"model":"test","stream":false}`)

	longRetryUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer longRetryUpstream.Close()
	longRetryProxy := newTestProxy(t, longRetryUpstream, []string{"fake-long-retry-key"}, 1, 1)
	longRetryStarted := time.Now()
	longRetryRecorder := exerciseProxy(longRetryProxy, `{"model":"test","stream":false}`)

	sseUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"error\":{\"status\":429,\"message\":\"raw-sse-429-marker\"}}\n\n")
	}))
	defer sseUpstream.Close()
	sseProxy := newTestProxy(t, sseUpstream, []string{"fake-sse-key"}, 1, 1)
	sseStarted := time.Now()
	sseRecorder := exerciseProxy(sseProxy, `{"model":"test","stream":true}`)

	if httpRecorder.Code != http.StatusTooManyRequests || longRetryRecorder.Code != http.StatusTooManyRequests || sseRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("short HTTP status=%d, long HTTP status=%d, SSE status=%d; want all 429", httpRecorder.Code, longRetryRecorder.Code, sseRecorder.Code)
	}
	assertCooldownNear(t, httpProxy.scheduler, 0, httpStarted.Add(time.Minute), time.Second)
	assertCooldownNear(t, longRetryProxy.scheduler, 0, longRetryStarted.Add(2*time.Minute), time.Second)
	assertCooldownNear(t, sseProxy.scheduler, 0, sseStarted.Add(time.Minute), time.Second)
}

func TestFailureLogsDoNotLeakKeyOrRawUpstreamBody(t *testing.T) {
	const (
		fakeKey       = "nvapi-super-secret-test-key"
		fakeBodyToken = "raw-upstream-body-should-not-appear"
	)
	fakeKeys := []string{fakeKey, "fake-key-two", "fake-key-three"}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprintf(w, `{"error":{"message":%q}}`, fakeBodyToken)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	originalPrefix := log.Prefix()
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	defer func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
		log.SetPrefix(originalPrefix)
	}()

	proxy := newTestProxy(t, upstream, fakeKeys, 3, 1)
	recorder := exerciseProxy(proxy, `{"model":"test","stream":false}`)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d body=%s, want 429", recorder.Code, recorder.Body.String())
	}

	logged := logs.String()
	if !strings.Contains(logged, "status=429") || !strings.Contains(logged, "slot=") {
		t.Fatalf("failure log lacks safe status/slot metadata: %q", logged)
	}
	for _, secret := range append(fakeKeys, fakeBodyToken) {
		if strings.Contains(logged, secret) {
			t.Fatalf("failure log leaked sensitive marker %q: %q", secret, logged)
		}
	}
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

func TestFirstValidByteTimeoutReleasesKeysWithoutCooldown(t *testing.T) {
	keys := []string{"nvapi-timeout-a", "nvapi-timeout-b", "nvapi-timeout-c"}
	var canceled atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		<-r.Context().Done()
		canceled.Add(1)
	}))
	defer upstream.Close()
	baseURL, _ := url.Parse(upstream.URL + "/v1")
	proxy := &proxyServer{
		cfg: config{
			BaseURL:      baseURL,
			Fanout:       3,
			MaxWaves:     1,
			MaxBodyBytes: defaultMaxBodyBytes,
			MaxRespBytes: defaultMaxReplyBytes,
			FirstByteTTL: 40 * time.Millisecond,
		},
		client:    upstream.Client(),
		scheduler: newScheduler(keys),
	}

	request := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	recorder := httptest.NewRecorder()
	started := time.Now()
	proxy.handleProxy(recorder, request)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("first byte timeout took too long: %s", elapsed)
	}
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", recorder.Code, http.StatusBadGateway)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := proxy.scheduler.snapshot()
		if snapshot.InFlight == 0 && canceled.Load() == 3 {
			if snapshot.Available != 3 || snapshot.Cooldown != 0 || snapshot.Disabled != 0 {
				t.Fatalf("timeout must not penalize keys: %+v", snapshot)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out attempts did not settle: state=%+v canceled=%d", proxy.scheduler.snapshot(), canceled.Load())
}

func newTestProxy(t *testing.T, upstream *httptest.Server, keys []string, fanout, maxWaves int) *proxyServer {
	t.Helper()
	baseURL, err := url.Parse(upstream.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	return &proxyServer{
		cfg: config{
			BaseURL:      baseURL,
			Fanout:       fanout,
			MaxWaves:     maxWaves,
			MaxBodyBytes: defaultMaxBodyBytes,
			MaxRespBytes: defaultMaxReplyBytes,
		},
		client:    upstream.Client(),
		scheduler: newScheduler(keys),
	}
}

func exerciseProxy(proxy *proxyServer, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	proxy.handleProxy(recorder, request)
	return recorder
}

func assertCooldownNear(t *testing.T, scheduler *scheduler, index int, want time.Time, tolerance time.Duration) {
	t.Helper()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if index < 0 || index >= len(scheduler.keys) {
		t.Fatalf("slot index %d is out of range", index)
	}
	got := scheduler.keys[index].cooldownUntil
	delta := got.Sub(want)
	if delta < -tolerance || delta > tolerance {
		t.Fatalf("slot %d cooldownUntil=%s, want %s ± %s (delta %s)", index, got, want, tolerance, delta)
	}
}
