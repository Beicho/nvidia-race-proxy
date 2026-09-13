# NVIDIA Race Proxy

[English](#english) | [简体中文](#chinese)

<a id="english"></a>

## English

A small OpenAI-compatible reverse proxy for the NVIDIA API. For each incoming request it selects three distinct API keys, sends the requests concurrently, forwards the first valid response, and cancels the two losing requests.

## Features

- Three-key racing with configurable fan-out and waves.
- The first meaningful chat SSE event or valid JSON response wins.
- Streaming winners keep their key reserved until the response closes; each forwarded chunk is flushed.
- Chat races accept content, reasoning, tool/function output or a finish reason, ignoring role-only events.
- SIGHUP reloads the key file without interrupting streams and preserves existing key health.
- Internal `/metrics` JSON endpoint with bounded per-model latency histograms and counters.
- Losing requests are cancelled without cooling down or penalizing their keys.
- Real upstream failures update key health: 401 disables, 403/429/5xx and network failures apply bounded cooldowns.
- Optional SOCKS5/SOCKS5H upstream proxy.
- OpenAI-compatible streaming and non-streaming pass-through.
- Health endpoint with aggregate counts only; keys are never returned.
- Static Linux amd64 binary available from GitHub Releases.

> [!IMPORTANT]
> Racing sends multiple upstream requests for one client request. NVIDIA may count work performed before losing requests are cancelled. Use this proxy only when reduced tail latency is worth the additional upstream usage.

## Quick start

Download the Linux amd64 binary and checksum from the [latest release](../../releases/latest), then create a key file containing one NVIDIA key per line:

```text
nvapi-...
nvapi-...
nvapi-...
```

Run it locally:

```bash
chmod 600 nvidia.keys
chmod +x nvidia-race-proxy-linux-amd64
NVIDIA_KEYS_FILE="$PWD/nvidia.keys" \
LISTEN_ADDR="127.0.0.1:8080" \
./nvidia-race-proxy-linux-amd64
```

The default upstream is `https://integrate.api.nvidia.com/v1`.

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"nvidia/nemotron-3-nano-30b-a3b","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

The client `Authorization` header is ignored and replaced with the selected NVIDIA key.

## Configuration

| Environment variable | Default | Description |
|---|---:|---|
| `NVIDIA_KEYS_FILE` | required | Key file path, one distinct `nvapi-...` key per line |
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `NVIDIA_BASE_URL` | `https://integrate.api.nvidia.com/v1` | Absolute HTTPS upstream base URL |
| `NVIDIA_FANOUT` | `3` | Concurrent keys per wave, from 2 to 10 |
| `NVIDIA_MAX_WAVES` | `1` | Maximum waves, from 1 to 5 |
| `FIRST_VALID_BYTE_TIMEOUT_SECONDS` | `120` | Per-contender wait for its first valid event, from 1 to 300 seconds |
| `UPSTREAM_TIMEOUT_SECONDS` | `600` | Overall upstream HTTP timeout |
| `UPSTREAM_SOCKS5` | empty | Optional `socks5://` or `socks5h://` URL |
| `MAX_REQUEST_BODY_BYTES` | 16 MiB | Maximum incoming request body |
| `MAX_RESPONSE_BODY_BYTES` | 64 MiB | Maximum buffered non-stream response |

## Key Reload and Metrics

Replace the key file atomically and send `SIGHUP` to the proxy process (`docker kill --signal=HUP nvidia-race-proxy` for Docker). Invalid files leave the current pool intact. Retained keys preserve their cooldown, disabled state and active reservations; removed keys finish current requests but are excluded from new picks. Reload does not restore a disabled key. Health reports only active pool membership in `total_keys`, with removed busy keys counted separately in `draining_keys` and included in `in_flight`.

For Docker, mount a dedicated directory, such as `/opt/nvidia-race-proxy/secrets:/run/secrets:ro`, and set `NVIDIA_KEYS_FILE=/run/secrets/nvidia.keys`. A single-file bind mount can keep pointing at the old inode after an atomic replacement. Keep the directory restricted to the service owner/group and the key file mode `0600`. Never put credentials inside the source checkout.

`GET /metrics` returns aggregate JSON for internal monitoring. It includes cumulative latency buckets (seconds), counts and sums for streaming first meaningful output, total request duration and loser cancellation delay, plus per-model request outcomes, active requests, upstream failures and 429 counts. First-output latency is measured when the proxy selects the winner, not when the end user receives it; non-streaming requests contribute only total latency. `active_streaming_requests` includes contenders waiting for output. At most 64 model labels plus `_other` are retained. Counters and histograms reset on restart, while key reload leaves them intact. No keys or request/response bodies are exposed. Keep health and metrics on the private service network.

Shutdown drains active HTTP requests for up to `UPSTREAM_TIMEOUT_SECONDS`; set the container stop timeout accordingly. Reload is preferred for key-only updates.

## API compatibility

Paths under `/v1/` are passed to the configured upstream. NVIDIA supports `/v1/chat/completions`, but currently returns 404 for `/v1/responses`; this proxy does not translate between the Responses and Chat Completions schemas.

## Build and test

```bash
go test ./...
go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o nvidia-race-proxy-linux-amd64 .
```

## License

MIT

---

<a id="chinese"></a>

## 简体中文

这是一个面向 NVIDIA API 的轻量级 OpenAI 兼容反向代理。每次收到用户请求时，它会选择三个不同的 API Key 并发请求 NVIDIA，将最先返回的有效响应转发给用户，同时取消另外两路请求。

### 功能特性

- 默认使用三个不同的 Key 进行竞速，可配置并发数量与竞速轮次。
- 首个有效 SSE 事件或 JSON 响应胜出。
- 竞速败者会被取消，不会进入冷却，也不会被计为失败。
- 只有真实上游错误才影响 Key 健康状态：401 禁用；403、429、5xx 和网络错误进入有限时间冷却。
- 支持可选的 SOCKS5/SOCKS5H 上游代理。
- 透明转发 OpenAI 兼容的流式和非流式请求。
- 健康检查只返回汇总数量，绝不返回 Key。
- 流式胜者在响应关闭前持续占用 Key，每个数据块写出后刷新。
- 聊天竞速忽略仅包含角色或空内容的事件，等待正文、思考内容、工具调用或结束原因。
- 支持 SIGHUP 热加载密钥，保留已有 Key 的冷却、禁用和占用状态，不中断当前请求。
- 内网 `/metrics` 返回按模型统计的延迟直方图、请求结果、429 与取消耗时。
- GitHub Releases 提供静态 Linux amd64 二进制。

> [!IMPORTANT]
> 竞速会为一个用户请求同时发起多个上游请求。即使败者随后被取消，NVIDIA 仍可能计算取消前已经产生的用量。请仅在降低长尾延迟值得额外上游用量时使用本代理。

### 快速开始

从[最新版本](../../releases/latest)下载 Linux amd64 二进制和校验文件，然后创建 Key 文件，每行填写一个 NVIDIA Key：

```text
nvapi-...
nvapi-...
nvapi-...
```

仅监听本机运行：

```bash
chmod 600 nvidia.keys
chmod +x nvidia-race-proxy-linux-amd64
NVIDIA_KEYS_FILE="$PWD/nvidia.keys" \
LISTEN_ADDR="127.0.0.1:8080" \
./nvidia-race-proxy-linux-amd64
```

默认上游地址为 `https://integrate.api.nvidia.com/v1`。

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"nvidia/nemotron-3-nano-30b-a3b","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

客户端传入的 `Authorization` 请求头会被忽略，并替换为本次竞速选中的 NVIDIA Key。

### 配置

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `NVIDIA_KEYS_FILE` | 必填 | Key 文件路径，每行一个不同的 `nvapi-...` Key |
| `LISTEN_ADDR` | `:8080` | HTTP 监听地址 |
| `NVIDIA_BASE_URL` | `https://integrate.api.nvidia.com/v1` | HTTPS 上游基础地址 |
| `NVIDIA_FANOUT` | `3` | 每轮并发 Key 数量，可设为 2 至 10 |
| `NVIDIA_MAX_WAVES` | `1` | 最大竞速轮次，可设为 1 至 5 |
| `FIRST_VALID_BYTE_TIMEOUT_SECONDS` | `120` | 每个候选等待首个有效事件的时间，可设为 1 至 300 秒 |
| `UPSTREAM_TIMEOUT_SECONDS` | `600` | 整个上游 HTTP 请求的超时时间 |
| `UPSTREAM_SOCKS5` | 空 | 可选的 `socks5://` 或 `socks5h://` 代理地址 |
| `MAX_REQUEST_BODY_BYTES` | 16 MiB | 用户请求体大小上限 |
| `MAX_RESPONSE_BODY_BYTES` | 64 MiB | 非流式响应的最大缓冲大小 |

### 密钥热加载与统计

原子替换密钥文件后执行 `docker kill --signal=HUP nvidia-race-proxy`。无效文件不会改变当前池；移除的 Key 会完成当前请求后退出调度。Docker 应只读挂载专用密钥目录，避免单文件挂载在原子替换后仍指向旧文件。真实密钥不得放入源码仓库。

`/metrics` 仅供内网使用，最多保留 64 个模型及一个汇总标签。首输出延迟只统计流式胜者产生有效输出的时间，不等于客户端收字时间；总耗时统计所有请求。指标在重启后清零，热加载不会清零。收到停止信号后服务会等待请求结束，最长为 `UPSTREAM_TIMEOUT_SECONDS`，Docker 停止超时应与之匹配。

### API 兼容性

代理会将 `/v1/` 下的路径转发给配置的上游。NVIDIA 支持 `/v1/chat/completions`，但目前会对 `/v1/responses` 返回 404；本代理不会在 Responses API 与 Chat Completions API 之间转换数据格式。

### 构建与测试

```bash
go test ./...
go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o nvidia-race-proxy-linux-amd64 .
```

### 许可证

MIT
