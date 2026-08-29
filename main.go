package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	xproxy "golang.org/x/net/proxy"
)

const (
	defaultBaseURL       = "https://integrate.api.nvidia.com/v1"
	defaultListenAddr    = ":8080"
	defaultFanout        = 3
	defaultMaxWaves      = 1
	defaultMaxBodyBytes  = 16 << 20
	defaultMaxReplyBytes = 64 << 20
	maxErrorBodyBytes    = 1 << 20
	maxSSEPrefixBytes    = 256 << 10
)

type config struct {
	ListenAddr   string
	BaseURL      *url.URL
	KeysFile     string
	SOCKS5URL    string
	Fanout       int
	MaxWaves     int
	MaxBodyBytes int64
	MaxRespBytes int64
	HTTPTimeout  time.Duration
}

type keyState struct {
	secret        string
	disabled      bool
	cooldownUntil time.Time
	inFlight      int
}

type scheduler struct {
	mu     sync.Mutex
	keys   []keyState
	cursor int
}

type pickedKey struct {
	index  int
	secret string
}

type healthSnapshot struct {
	Total     int `json:"total_keys"`
	Available int `json:"available_keys"`
	Cooldown  int `json:"cooldown_keys"`
	Disabled  int `json:"disabled_keys"`
	InFlight  int `json:"in_flight"`
}

func newScheduler(keys []string) *scheduler {
	states := make([]keyState, len(keys))
	for i, key := range keys {
		states[i].secret = key
	}
	return &scheduler{keys: states}
}

func (s *scheduler) pick(n int, excluded map[int]struct{}) []pickedKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n <= 0 || len(s.keys) == 0 {
		return nil
	}
	now := time.Now()
	picked := make([]pickedKey, 0, n)
	seen := make(map[int]struct{}, n)
	start := s.cursor
	for offset := 0; offset < len(s.keys) && len(picked) < n; offset++ {
		idx := (start + offset) % len(s.keys)
		if _, ok := excluded[idx]; ok {
			continue
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		state := &s.keys[idx]
		if state.disabled || now.Before(state.cooldownUntil) {
			continue
		}
		state.inFlight++
		picked = append(picked, pickedKey{index: idx, secret: state.secret})
		seen[idx] = struct{}{}
	}
	if len(picked) > 0 {
		s.cursor = (picked[len(picked)-1].index + 1) % len(s.keys)
	}
	return picked
}

func (s *scheduler) release(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= 0 && index < len(s.keys) && s.keys[index].inFlight > 0 {
		s.keys[index].inFlight--
	}
}

func (s *scheduler) disable(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= 0 && index < len(s.keys) {
		s.keys[index].disabled = true
	}
}

func (s *scheduler) cooldown(index int, duration time.Duration) {
	if duration <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.keys) || s.keys[index].disabled {
		return
	}
	until := time.Now().Add(duration)
	if until.After(s.keys[index].cooldownUntil) {
		s.keys[index].cooldownUntil = until
	}
}

func (s *scheduler) snapshot() healthSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	snapshot := healthSnapshot{Total: len(s.keys)}
	for i := range s.keys {
		state := &s.keys[i]
		snapshot.InFlight += state.inFlight
		switch {
		case state.disabled:
			snapshot.Disabled++
		case now.Before(state.cooldownUntil):
			snapshot.Cooldown++
		default:
			snapshot.Available++
		}
	}
	return snapshot
}

type proxyServer struct {
	cfg       config
	client    *http.Client
	scheduler *scheduler
	requestID atomic.Uint64
}

type candidate struct {
	keyIndex int
	status   int
	header   http.Header
	reader   io.Reader
	body     io.Closer
	buffered []byte
}

func (c *candidate) close() {
	if c != nil && c.body != nil {
		_ = c.body.Close()
	}
}

type upstreamFailure struct {
	status   int
	header   http.Header
	body     []byte
	terminal bool
	err      error
}

type attemptResult struct {
	candidate *candidate
	failure   *upstreamFailure
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	keys, err := loadKeys(cfg.KeysFile)
	if err != nil {
		log.Fatalf("key file error: %v", err)
	}
	transport, err := buildTransport(cfg.SOCKS5URL)
	if err != nil {
		log.Fatalf("transport error: %v", err)
	}
	server := &proxyServer{
		cfg: cfg,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.HTTPTimeout,
		},
		scheduler: newScheduler(keys),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", server.handleHealth)
	mux.HandleFunc("/", server.handleProxy)
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownCtx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	log.Printf("nvidia race proxy listening on %s with %d keys, fanout=%d, waves=%d, socks5=%t", cfg.ListenAddr, len(keys), cfg.Fanout, cfg.MaxWaves, cfg.SOCKS5URL != "")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func loadConfig() (config, error) {
	baseRaw := envOr("NVIDIA_BASE_URL", defaultBaseURL)
	baseURL, err := url.Parse(baseRaw)
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" {
		return config{}, fmt.Errorf("NVIDIA_BASE_URL must be an absolute https URL")
	}
	if baseURL.User != nil {
		return config{}, fmt.Errorf("NVIDIA_BASE_URL must not contain credentials")
	}
	keysFile := strings.TrimSpace(os.Getenv("NVIDIA_KEYS_FILE"))
	if keysFile == "" {
		return config{}, fmt.Errorf("NVIDIA_KEYS_FILE is required")
	}
	fanout, err := envInt("NVIDIA_FANOUT", defaultFanout, 2, 10)
	if err != nil {
		return config{}, err
	}
	maxWaves, err := envInt("NVIDIA_MAX_WAVES", defaultMaxWaves, 1, 5)
	if err != nil {
		return config{}, err
	}
	maxBody, err := envInt64("MAX_REQUEST_BODY_BYTES", defaultMaxBodyBytes, 1024, 256<<20)
	if err != nil {
		return config{}, err
	}
	maxResp, err := envInt64("MAX_RESPONSE_BODY_BYTES", defaultMaxReplyBytes, 1024, 512<<20)
	if err != nil {
		return config{}, err
	}
	timeoutSeconds, err := envInt("UPSTREAM_TIMEOUT_SECONDS", 600, 10, 3600)
	if err != nil {
		return config{}, err
	}
	return config{
		ListenAddr:   envOr("LISTEN_ADDR", defaultListenAddr),
		BaseURL:      baseURL,
		KeysFile:     keysFile,
		SOCKS5URL:    strings.TrimSpace(os.Getenv("UPSTREAM_SOCKS5")),
		Fanout:       fanout,
		MaxWaves:     maxWaves,
		MaxBodyBytes: maxBody,
		MaxRespBytes: maxResp,
		HTTPTimeout:  time.Duration(timeoutSeconds) * time.Second,
	}, nil
}

func loadKeys(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	seen := make(map[string]struct{})
	keys := make([]string, 0, 128)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		key := strings.TrimSpace(scanner.Text())
		if key == "" || strings.HasPrefix(key, "#") {
			continue
		}
		if !strings.HasPrefix(key, "nvapi-") || strings.ContainsAny(key, " \t\r\n") {
			return nil, fmt.Errorf("invalid NVIDIA key shape on line %d", len(keys)+1)
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(keys) < defaultFanout {
		return nil, fmt.Errorf("need at least %d distinct NVIDIA keys, got %d", defaultFanout, len(keys))
	}
	return keys, nil
}

func buildTransport(socksRaw string) (*http.Transport, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 120 * time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if socksRaw == "" {
		return transport, nil
	}
	proxyURL, err := url.Parse(socksRaw)
	if err != nil || (proxyURL.Scheme != "socks5" && proxyURL.Scheme != "socks5h") || proxyURL.Host == "" {
		return nil, fmt.Errorf("UPSTREAM_SOCKS5 must be a socks5:// or socks5h:// URL")
	}
	var auth *xproxy.Auth
	if proxyURL.User != nil {
		password, _ := proxyURL.User.Password()
		auth = &xproxy.Auth{User: proxyURL.User.Username(), Password: password}
	}
	socksDialer, err := xproxy.SOCKS5("tcp", proxyURL.Host, auth, dialer)
	if err != nil {
		return nil, err
	}
	if contextDialer, ok := socksDialer.(xproxy.ContextDialer); ok {
		transport.DialContext = contextDialer.DialContext
	} else {
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			type dialResult struct {
				conn net.Conn
				err  error
			}
			result := make(chan dialResult, 1)
			go func() {
				conn, dialErr := socksDialer.Dial(network, address)
				result <- dialResult{conn: conn, err: dialErr}
			}()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case item := <-result:
				return item.conn, item.err
			}
		}
	}
	return transport, nil
}

func (s *proxyServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.scheduler.snapshot()
	w.Header().Set("Content-Type", "application/json")
	if snapshot.Available < s.cfg.Fanout {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"keys":   snapshot,
		"fanout": s.cfg.Fanout,
		"warp":   s.cfg.SOCKS5URL != "",
	})
}

func (s *proxyServer) handleProxy(w http.ResponseWriter, r *http.Request) {
	requestID := s.requestID.Add(1)
	started := time.Now()
	if !strings.HasPrefix(r.URL.Path, "/v1/") && r.URL.Path != "/v1" {
		writeJSONError(w, http.StatusNotFound, "unsupported path")
		return
	}
	body, err := readRequestBody(w, r, s.cfg.MaxBodyBytes)
	if err != nil {
		return
	}
	streaming := requestWantsStream(body)
	excluded := make(map[int]struct{})
	var lastFailure *upstreamFailure

	for wave := 0; wave < s.cfg.MaxWaves; wave++ {
		picked := s.scheduler.pick(s.cfg.Fanout, excluded)
		if len(picked) != s.cfg.Fanout {
			for _, key := range picked {
				s.scheduler.release(key.index)
			}
			writeJSONError(w, http.StatusServiceUnavailable, "fewer than three NVIDIA keys are currently available")
			return
		}
		for _, key := range picked {
			excluded[key.index] = struct{}{}
		}

		waveCtx, cancelWave := context.WithCancelCause(r.Context())
		winner := make(chan *candidate, 1)
		failures := make(chan *upstreamFailure, len(picked))
		var claimed atomic.Bool
		attemptCancels := make(map[int]context.CancelCauseFunc, len(picked))

		for _, key := range picked {
			key := key
			attemptCtx, cancelAttempt := context.WithCancelCause(waveCtx)
			attemptCancels[key.index] = cancelAttempt
			go func() {
				defer s.scheduler.release(key.index)
				result := s.runAttempt(attemptCtx, r, body, streaming, key)
				if result.candidate != nil {
					if claimed.CompareAndSwap(false, true) {
						winner <- result.candidate
					} else {
						result.candidate.close()
					}
					return
				}
				select {
				case failures <- result.failure:
				case <-waveCtx.Done():
				}
			}()
		}

		failed := 0
		for failed < len(picked) {
			select {
			case <-r.Context().Done():
				cancelWave(r.Context().Err())
				return
			case selected := <-winner:
				for index, cancelAttempt := range attemptCancels {
					if index != selected.keyIndex {
						cancelAttempt(errors.New("another contender won"))
					}
				}
				s.writeCandidate(w, selected)
				attemptCancels[selected.keyIndex](errors.New("winner response completed"))
				cancelWave(errors.New("wave completed"))
				log.Printf("request=%d method=%s path=%s status=%d winner_slot=%d duration_ms=%d", requestID, r.Method, r.URL.Path, selected.status, selected.keyIndex, time.Since(started).Milliseconds())
				return
			case failure := <-failures:
				failed++
				if failure != nil {
					lastFailure = failure
					if failure.terminal {
						cancelWave(errors.New("terminal upstream response"))
						s.writeFailure(w, failure)
						log.Printf("request=%d method=%s path=%s status=%d terminal=true duration_ms=%d", requestID, r.Method, r.URL.Path, failure.status, time.Since(started).Milliseconds())
						return
					}
				}
			}
		}
		cancelWave(errors.New("all contenders failed"))
	}

	if lastFailure != nil && lastFailure.status != 0 {
		s.writeFailure(w, lastFailure)
		return
	}
	writeJSONError(w, http.StatusBadGateway, "all NVIDIA contenders failed")
}

func (s *proxyServer) runAttempt(ctx context.Context, inbound *http.Request, body []byte, streaming bool, key pickedKey) attemptResult {
	target := joinUpstreamURL(s.cfg.BaseURL, inbound.URL)
	request, err := http.NewRequestWithContext(ctx, inbound.Method, target, bytes.NewReader(body))
	if err != nil {
		return attemptResult{failure: &upstreamFailure{err: err}}
	}
	copyRequestHeaders(request.Header, inbound.Header)
	request.Header.Set("Authorization", "Bearer "+key.secret)
	request.Header.Set("Accept-Encoding", "identity")

	response, err := s.client.Do(request)
	if err != nil {
		if ctx.Err() == nil {
			s.scheduler.cooldown(key.index, 5*time.Second)
		}
		return attemptResult{failure: &upstreamFailure{err: err}}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		failureBody, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
		failure := &upstreamFailure{
			status: response.StatusCode,
			header: response.Header.Clone(),
			body:   failureBody,
		}
		switch response.StatusCode {
		case http.StatusUnauthorized:
			s.scheduler.disable(key.index)
		case http.StatusForbidden:
			s.scheduler.cooldown(key.index, 10*time.Minute)
		case http.StatusTooManyRequests:
			s.scheduler.cooldown(key.index, retryAfter(response.Header, time.Minute))
		case http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity:
			failure.terminal = true
		default:
			if response.StatusCode >= 500 {
				s.scheduler.cooldown(key.index, 10*time.Second)
			}
		}
		return attemptResult{failure: failure}
	}

	if streaming {
		prefix, reader, err := readValidSSEPrefix(response.Body)
		if err != nil {
			_ = response.Body.Close()
			if ctx.Err() == nil {
				s.scheduler.cooldown(key.index, 5*time.Second)
			}
			return attemptResult{failure: &upstreamFailure{err: err}}
		}
		return attemptResult{candidate: &candidate{
			keyIndex: key.index,
			status:   response.StatusCode,
			header:   response.Header.Clone(),
			reader:   io.MultiReader(bytes.NewReader(prefix), reader),
			body:     response.Body,
		}}
	}

	reply, err := io.ReadAll(io.LimitReader(response.Body, s.cfg.MaxRespBytes+1))
	_ = response.Body.Close()
	if err != nil {
		return attemptResult{failure: &upstreamFailure{err: err}}
	}
	if int64(len(reply)) > s.cfg.MaxRespBytes {
		return attemptResult{failure: &upstreamFailure{err: errors.New("upstream response exceeds configured limit")}}
	}
	if err := validateJSONReply(reply); err != nil {
		return attemptResult{failure: &upstreamFailure{err: err}}
	}
	return attemptResult{candidate: &candidate{
		keyIndex: key.index,
		status:   response.StatusCode,
		header:   response.Header.Clone(),
		buffered: reply,
	}}
}

func readValidSSEPrefix(body io.Reader) ([]byte, *bufio.Reader, error) {
	reader := bufio.NewReader(body)
	prefix := make([]byte, 0, 4096)
	for len(prefix) < maxSSEPrefixBytes {
		line, err := reader.ReadBytes('\n')
		prefix = append(prefix, line...)
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 && !bytes.HasPrefix(trimmed, []byte(":")) {
			payload := trimmed
			if bytes.HasPrefix(payload, []byte("data:")) {
				payload = bytes.TrimSpace(bytes.TrimPrefix(payload, []byte("data:")))
			}
			if len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) {
				var decoded map[string]json.RawMessage
				if json.Unmarshal(payload, &decoded) == nil {
					if raw, exists := decoded["error"]; exists && len(bytes.TrimSpace(raw)) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
						return nil, nil, errors.New("upstream SSE returned an error payload")
					}
					return prefix, reader, nil
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, errors.New("upstream stream ended before a valid data event")
			}
			return nil, nil, err
		}
	}
	return nil, nil, errors.New("upstream stream prefix exceeded validation limit")
}

func validateJSONReply(reply []byte) error {
	if len(bytes.TrimSpace(reply)) == 0 {
		return errors.New("empty upstream response")
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(reply, &decoded); err != nil {
		return fmt.Errorf("invalid upstream JSON: %w", err)
	}
	if raw, exists := decoded["error"]; exists && len(bytes.TrimSpace(raw)) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("upstream returned an error payload")
	}
	return nil
}

func (s *proxyServer) writeCandidate(w http.ResponseWriter, selected *candidate) {
	defer selected.close()
	copyResponseHeaders(w.Header(), selected.header)
	w.WriteHeader(selected.status)
	if len(selected.buffered) > 0 {
		_, _ = w.Write(selected.buffered)
		return
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	_, _ = io.CopyBuffer(w, selected.reader, make([]byte, 32<<10))
}

func (s *proxyServer) writeFailure(w http.ResponseWriter, failure *upstreamFailure) {
	if failure == nil || failure.status == 0 {
		writeJSONError(w, http.StatusBadGateway, "NVIDIA upstream request failed")
		return
	}
	copyResponseHeaders(w.Header(), failure.header)
	w.WriteHeader(failure.status)
	_, _ = w.Write(failure.body)
}

func readRequestBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	reader := http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(reader)
	if err != nil {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request body exceeds configured limit")
		return nil, err
	}
	return body, nil
}

func requestWantsStream(body []byte) bool {
	var request struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &request) == nil && request.Stream
}

func joinUpstreamURL(base *url.URL, inbound *url.URL) string {
	target := *base
	basePath := strings.TrimSuffix(target.Path, "/")
	incomingPath := inbound.EscapedPath()
	if incomingPath == "" {
		incomingPath = "/"
	}
	if basePath == "/v1" && (incomingPath == "/v1" || strings.HasPrefix(incomingPath, "/v1/")) {
		target.RawPath = ""
		target.Path = incomingPath
	} else {
		target.RawPath = ""
		target.Path = basePath + "/" + strings.TrimPrefix(incomingPath, "/")
	}
	target.RawQuery = inbound.RawQuery
	return target.String()
}

func retryAfter(header http.Header, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(header.Get("Retry-After"))
	if raw == "" {
		return fallback
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		duration := time.Until(when)
		if duration > 0 {
			return duration
		}
	}
	return fallback
}

func copyRequestHeaders(destination, source http.Header) {
	for key, values := range source {
		if isHopHeader(key) || strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "Proxy-Authorization") || strings.EqualFold(key, "Host") {
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func copyResponseHeaders(destination, source http.Header) {
	for key, values := range source {
		if isHopHeader(key) || strings.EqualFold(key, "Content-Length") {
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func isHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "nvidia_race_proxy_error",
		},
	})
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback, minValue, maxValue int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minValue || value > maxValue {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, minValue, maxValue)
	}
	return value, nil
}

func envInt64(name string, fallback, minValue, maxValue int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < minValue || value > maxValue {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, minValue, maxValue)
	}
	return value, nil
}
