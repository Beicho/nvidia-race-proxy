# NVIDIA Race Proxy

A small OpenAI-compatible reverse proxy for the NVIDIA API. For each incoming request it selects three distinct API keys, sends the requests concurrently, forwards the first valid response, and cancels the two losing requests.

## Features

- Three-key racing with configurable fan-out and waves.
- The first valid SSE event or JSON response wins.
- Losing requests are cancelled without cooling down or penalizing their keys.
- Real upstream failures update key health: 401 disables, 403/429/5xx and network failures apply bounded cooldowns.
- Optional SOCKS5/SOCKS5H upstream proxy.
- OpenAI-compatible streaming and non-streaming pass-through.
- Health endpoint with aggregate counts only; keys are never returned.
- Static Linux amd64 binary available from GitHub Releases.

> [!IMPORTANT]
> Racing sends multiple upstream requests for one client request. NVIDIA may count work performed before losing requests are cancelled. Use this proxy only when reduced tail latency is worth the additional upstream usage.

## Quick start

Download the Linux amd64 binary and checksum from the latest release, then create a key file containing one NVIDIA key per line:

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
