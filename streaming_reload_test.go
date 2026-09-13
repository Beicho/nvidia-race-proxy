package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLiveStreamFlushesAndOwnsKeyThroughReload(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	defer finish()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"last\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	proxy := newTestProxy(t, upstream, []string{"nvapi-busy"}, 1, 1)
	server := httptest.NewServer(http.HandlerFunc(proxy.handleProxy))
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"stream-test","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(line, "first") {
		t.Fatalf("first event unavailable before completion: %q %v", line, err)
	}
	if snapshot := proxy.scheduler.snapshot(); snapshot.InFlight != 1 || snapshot.Available != 0 {
		t.Fatalf("live winner lost reservation: %+v", snapshot)
	}
	if got := proxy.scheduler.pick(1, nil); len(got) != 0 {
		t.Fatal("live winner was reused")
	}
	proxy.scheduler.reload([]string{"nvapi-new-a", "nvapi-new-b", "nvapi-new-c"})
	if snapshot := proxy.scheduler.snapshot(); snapshot.Total != 3 || snapshot.InFlight != 1 || snapshot.Draining != 1 {
		t.Fatalf("removed winner did not drain: %+v", snapshot)
	}
	finish()
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "last") || !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("reload truncated stream: %s %v", rest, err)
	}
	waitCondition(t, func() bool { return proxy.scheduler.snapshot().InFlight == 0 })
	waitCondition(t, func() bool {
		proxy.metrics.mu.Lock()
		defer proxy.metrics.mu.Unlock()
		return proxy.metrics.model("stream-test").Active == 0
	})
	proxy.metrics.mu.Lock()
	entry := *proxy.metrics.model("stream-test")
	proxy.metrics.mu.Unlock()
	if entry.FirstOutput.Count != 1 || entry.Completed != 1 || entry.Active != 0 {
		t.Fatalf("bad stream metrics: %+v", entry)
	}
}

func TestClientDisconnectReleasesStreamingWinner(t *testing.T) {
	canceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer upstream.Close()
	proxy := newTestProxy(t, upstream, []string{"nvapi-busy"}, 1, 1)
	server := httptest.NewServer(http.HandlerFunc(proxy.handleProxy))
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"cancel-test","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not canceled")
	}
	waitCondition(t, func() bool { return proxy.scheduler.snapshot().InFlight == 0 })
	if state := proxy.scheduler.snapshot(); state.Cooldown != 0 || state.Available != 1 {
		t.Fatalf("disconnect damaged key state: %+v", state)
	}
}

func TestRoleOnlyContenderCannotWin(t *testing.T) {
	roleSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.HasSuffix(r.Header.Get("Authorization"), "role") {
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n")
			w.(http.Flusher).Flush()
			close(roleSent)
			<-r.Context().Done()
			return
		}
		select {
		case <-roleSent:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"real-winner\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	proxy := newTestProxy(t, upstream, []string{"nvapi-role", "nvapi-content"}, 2, 1)
	proxy.cfg.FirstByteTTL = time.Second
	recorder := exerciseProxy(proxy, `{"stream":true}`)
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), "real-winner") {
		t.Fatalf("role-only contender won: %d %s", recorder.Code, recorder.Body.String())
	}
	waitCondition(t, func() bool { return proxy.scheduler.snapshot().InFlight == 0 })
}

func TestMeaningfulSSEEvents(t *testing.T) {
	for name, event := range map[string]string{
		"content":           `{"choices":[{"delta":{"content":"hello"}}]}`,
		"reasoning":         `{"choices":[{"delta":{"reasoning_content":"think"}}]}`,
		"reasoning variant": `{"choices":[{"delta":{"reasoning":"think"}}]}`,
		"tool":              `{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}]}`,
		"function":          `{"choices":[{"delta":{"function_call":{"name":"lookup"}}}]}`,
		"refusal":           `{"choices":[{"delta":{"refusal":"no"}}]}`,
		"finish only":       `{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			input := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: " + event + "\n\ndata: [DONE]\n\n"
			prefix, reader, err := readValidSSEPrefix(strings.NewReader(input), true)
			if err != nil {
				t.Fatal(err)
			}
			rest, _ := io.ReadAll(reader)
			if string(prefix)+string(rest) != input || !strings.Contains(string(prefix), event) {
				t.Fatal("prefix bytes or meaningful selection changed")
			}
		})
	}
	for name, input := range map[string]string{
		"empty":           "data: {}\n\ndata: [DONE]\n\n",
		"null":            "data: null\n\n",
		"malformed":       "data: invalid\n\n",
		"role only":       "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n",
		"tool index only": "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0}]}}]}\n\ndata: [DONE]\n\n",
		"oversized line":  "data: " + strings.Repeat("x", maxSSEPrefixBytes+1),
		"unframed":        "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := readValidSSEPrefix(strings.NewReader(input), true); err == nil {
				t.Fatal("invalid/non-meaningful stream accepted")
			}
		})
	}
}

func TestSSEMultilineAndNonChatCompatibility(t *testing.T) {
	input := ": heartbeat\r\nevent: message\r\ndata: {\"choices\":\r\ndata: [{\"delta\":{\"content\":\"hello\"}}]}\r\n\r\n"
	prefix, _, err := readValidSSEPrefix(strings.NewReader(input), true)
	if err != nil || string(prefix) != input {
		t.Fatalf("multiline SSE failed: %v", err)
	}
	if _, _, err := readValidSSEPrefix(strings.NewReader("data: {\"result\":1}\n\n"), false); err != nil {
		t.Fatal(err)
	}
	_, _, err = readValidSSEPrefix(strings.NewReader("event: error\ndata: unavailable\n\n"), true)
	var payloadErr *upstreamPayloadError
	if !errors.As(err, &payloadErr) {
		t.Fatalf("error event lost: %v", err)
	}
}

func TestReloadPreservesHealthAndRejectsInvalidFile(t *testing.T) {
	s := newScheduler([]string{"nvapi-a", "nvapi-b", "nvapi-c"})
	busy := s.pick(1, nil)[0]
	s.disable(1)
	s.cooldown(2, time.Hour)
	s.reload([]string{"nvapi-c", "nvapi-b", "nvapi-a", "nvapi-d"})
	if state := s.snapshot(); state.Total != 4 || state.Disabled != 1 || state.Cooldown != 1 || state.InFlight != 1 || state.Available != 1 {
		t.Fatalf("reload reset health: %+v", state)
	}
	s.release(busy.index)
	path := filepath.Join(t.TempDir(), "pool.keys")
	proxy := &proxyServer{scheduler: s, cfg: config{KeysFile: path, Fanout: 3}}
	if err := os.WriteFile(path, []byte("invalid-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := s.snapshot()
	if err := proxy.reloadKeys(); err == nil {
		t.Fatal("bad file accepted")
	}
	if got := s.snapshot(); got != before {
		t.Fatalf("bad reload changed state: %+v", got)
	}
	if err := os.WriteFile(path, []byte("nvapi-a\nnvapi-b\nnvapi-c\nnvapi-d\nnvapi-e\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := proxy.reloadKeys(); err != nil {
		t.Fatal(err)
	}
	if got := s.snapshot(); got.Total != 5 || got.Disabled != 1 || got.Cooldown != 1 {
		t.Fatalf("reload failed: %+v", got)
	}
	if proxy.metrics.reloadSuccess != 1 || proxy.metrics.reloadFailure != 1 {
		t.Fatal("reload counters incorrect")
	}
}

func TestConcurrentReloadAndPick(t *testing.T) {
	s := newScheduler([]string{"nvapi-a", "nvapi-b", "nvapi-c"})
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for i := 0; i < 100; i++ {
				if worker == 0 {
					s.reload([]string{"nvapi-c", "nvapi-b", "nvapi-a", "nvapi-new"})
				} else {
					for _, picked := range s.pick(2, nil) {
						s.release(picked.index)
					}
					s.snapshot()
				}
			}
		}(worker)
	}
	group.Wait()
	if got := s.snapshot(); got.Total != 4 || got.InFlight != 0 || got.Available != 4 {
		t.Fatalf("concurrent reload damaged pool: %+v", got)
	}
}

type countingBody struct {
	io.Reader
	closes atomic.Int32
}

func (b *countingBody) Close() error { b.closes.Add(1); return nil }

type failingWriter struct{ header http.Header }

func (w *failingWriter) Header() http.Header       { return w.header }
func (w *failingWriter) WriteHeader(int)           {}
func (w *failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestTransferFailureClosesAndReleasesExactlyOnce(t *testing.T) {
	body := &countingBody{Reader: strings.NewReader("data: event\n\n")}
	var released atomic.Int32
	selected := &candidate{status: 200, header: http.Header{}, reader: body, body: body, onClose: func() { released.Add(1) }}
	proxy := &proxyServer{}
	err := proxy.writeCandidate(&failingWriter{header: http.Header{}}, selected)
	selected.close()
	if !errors.Is(err, io.ErrClosedPipe) || body.closes.Load() != 1 || released.Load() != 1 {
		t.Fatalf("failed transfer cleanup: err=%v closed=%d released=%d", err, body.closes.Load(), released.Load())
	}
}

func TestMetricsAreBoundedAndSeparateStreamingLatency(t *testing.T) {
	proxy := &proxyServer{scheduler: newScheduler([]string{"nvapi-secret-marker"})}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("model-%d", i)
		proxy.metrics.start(name, true)
		proxy.metrics.firstOutput(name, 200*time.Millisecond)
		proxy.metrics.upstreamFailure(name, 429)
		proxy.metrics.loserCanceled(name, time.Millisecond)
		proxy.metrics.finish(name, true, "completed", time.Second)
	}
	recorder := httptest.NewRecorder()
	proxy.handleMetrics(recorder, httptest.NewRequest("GET", "/metrics", nil))
	var result struct {
		Models map[string]modelMetrics `json:"models"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Models) != maxMetricModels+1 || strings.Contains(recorder.Body.String(), "nvapi-secret-marker") {
		t.Fatal("unbounded or sensitive metrics")
	}
	var requests uint64
	for _, entry := range result.Models {
		requests += entry.Requests
		if entry.Active != 0 || entry.ActiveStreaming != 0 || entry.FirstOutput.Count != entry.Requests || entry.RateLimited != entry.Requests || entry.Duration.Sum != float64(entry.Requests) {
			t.Fatalf("incorrect metrics: %+v", entry)
		}
	}
	if requests != 100 {
		t.Fatalf("lost requests: %d", requests)
	}
	if got := metricModel([]byte(`{"model":"nvapi-secret-marker"}`)); got != "_other" {
		t.Fatal("sensitive model label accepted")
	}
}

func TestCancelDuringCandidateHandoffDoesNotLeak(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	proxy := newTestProxy(t, upstream, []string{"nvapi-a", "nvapi-b", "nvapi-c"}, 3, 1)
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"stream":true}`)).WithContext(ctx)
		done := make(chan struct{})
		go func() { proxy.handleProxy(httptest.NewRecorder(), request); close(done) }()
		time.Sleep(time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("cancel hung")
		}
		waitCondition(t, func() bool { return proxy.scheduler.snapshot().InFlight == 0 })
	}
}
