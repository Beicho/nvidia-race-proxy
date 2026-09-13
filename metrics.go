package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxMetricModels = 64

var latencyBounds = [...]float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}

type latencyHistogram struct {
	Count   uint64                     `json:"count"`
	Sum     float64                    `json:"sum_seconds"`
	Buckets [len(latencyBounds)]uint64 `json:"cumulative_buckets"`
}

func (h *latencyHistogram) observe(duration time.Duration) {
	seconds := max(0, duration.Seconds())
	h.Count++
	h.Sum += seconds
	for i, bound := range latencyBounds {
		if seconds <= bound {
			h.Buckets[i]++
		}
	}
}

type modelMetrics struct {
	Requests          uint64           `json:"requests"`
	Active            int64            `json:"active_requests"`
	ActiveStreaming   int64            `json:"active_streaming_requests"`
	Completed         uint64           `json:"completed"`
	Failed            uint64           `json:"failed"`
	Canceled          uint64           `json:"canceled"`
	TransferErrors    uint64           `json:"transfer_errors"`
	UpstreamFailures  uint64           `json:"upstream_failures"`
	RateLimited       uint64           `json:"upstream_429"`
	FirstOutput       latencyHistogram `json:"stream_first_output"`
	Duration          latencyHistogram `json:"request_duration"`
	LoserCancellation latencyHistogram `json:"loser_cancellation"`
}

type metricRegistry struct {
	mu            sync.Mutex
	models        map[string]*modelMetrics
	reloadSuccess uint64
	reloadFailure uint64
}

func metricModel(body []byte) string {
	var request struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &request) != nil || request.Model == "" || len(request.Model) > 128 || strings.Contains(request.Model, "nvapi-") || strings.HasPrefix(request.Model, "sk-") {
		return "_other"
	}
	for _, char := range request.Model {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("./_-", char)) {
			return "_other"
		}
	}
	return request.Model
}

// Called with the registry mutex held; overflow has one stable label.
func (m *metricRegistry) model(name string) *modelMetrics {
	if m.models == nil {
		m.models = map[string]*modelMetrics{"_other": {}}
	}
	if entry := m.models[name]; entry != nil {
		return entry
	}
	if len(m.models) > maxMetricModels {
		return m.models["_other"]
	}
	entry := &modelMetrics{}
	m.models[name] = entry
	return entry
}

func (m *metricRegistry) start(name string, stream bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.model(name)
	entry.Requests++
	entry.Active++
	if stream {
		entry.ActiveStreaming++
	}
}

func (m *metricRegistry) finish(name string, stream bool, outcome string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.model(name)
	entry.Active--
	if stream {
		entry.ActiveStreaming--
	}
	entry.Duration.observe(duration)
	switch outcome {
	case "completed":
		entry.Completed++
	case "canceled":
		entry.Canceled++
	case "transfer_error":
		entry.TransferErrors++
	default:
		entry.Failed++
	}
}

func (m *metricRegistry) firstOutput(name string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.model(name).FirstOutput.observe(duration)
}

func (m *metricRegistry) loserCanceled(name string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.model(name).LoserCancellation.observe(duration)
}

func (m *metricRegistry) upstreamFailure(name string, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.model(name)
	entry.UpstreamFailures++
	if status == http.StatusTooManyRequests {
		entry.RateLimited++
	}
}

func (m *metricRegistry) reload(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if success {
		m.reloadSuccess++
	} else {
		m.reloadFailure++
	}
}

func (s *proxyServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.metrics.mu.Lock()
	models := make(map[string]modelMetrics, len(s.metrics.models))
	for name, entry := range s.metrics.models {
		models[name] = *entry
	}
	success, failure := s.metrics.reloadSuccess, s.metrics.reloadFailure
	s.metrics.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"latency_bucket_upper_bounds_seconds": latencyBounds,
		"models":                              models,
		"keys":                                s.scheduler.snapshot(),
		"reload_success":                      success,
		"reload_failure":                      failure,
	})
}
