# Streaming Reliability, Key Reload and Metrics

The approved change preserves three concurrent contenders, one wave and the existing upstream timeout and cooldown policies. It fixes streamed key ownership and event delivery, tightens winner selection, and adds live key reload and aggregate observability.

## Implementation

- A candidate owns its key reservation until its body is closed. Exactly-once cleanup covers winners, losers, client cancellation and failed handoff. Only successful handoff transfers ownership to the response writer.
- Forward SSE body chunks with an explicit flush after each write. Record transfer errors separately from the initial upstream HTTP status.
- For chat/completions streams, ignore role-only/empty chunks until content, reasoning, tool/function call data or a legitimate finish reason arrives. Parse complete SSE events, including multiline data, with a bounded prefix; preserve the original bytes. Other endpoints retain generic non-error JSON event validation.
- SIGHUP reads and validates the complete key file before atomically changing scheduler membership. Stable slots preserve cooldown, disabled and in-flight state for retained keys; removed keys drain and cannot be selected. Bad files leave the old pool intact. The container must mount the containing secrets directory to observe atomic file replacements.
- An internal JSON `/metrics` endpoint exposes bounded per-model request counts, active streams, first-output and total-duration histogram buckets, upstream 429 counts and loser cancellation latency. No keys, prompts, responses or raw errors appear in metrics. Metrics reset at process restart.
- Shutdown waits for the HTTP server to drain instead of exiting while its shutdown goroutine is still running.

## Validation and Deployment

GitHub currently has no Actions workflows. Following the user's requested fallback, use an isolated Go container on OVH for formatting, vet, ordinary tests, race tests and a static amd64 build. Include real HTTP tests proving that the first SSE event reaches a client while the stream remains open and that its key cannot be reused. Cover reload membership, rejected reload, active removals, concurrent scheduling, meaningful events, cancellation and metric bounds.

Push reviewed source to the existing GitHub repository. Deploy the exact verified binary, retain the current binary/container specification and 820-key file for rollback, and verify JSON/SSE inference, SIGHUP without restart and safe metrics in production. Do not compile locally.
