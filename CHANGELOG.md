# Changelog

## v1.2.0 - 2026-09-13

- Keep winning stream keys reserved until response cleanup, including client cancellation and live key removal.
- Flush SSE chunks incrementally and distinguish transfer failures from completed responses.
- Race on meaningful chat output instead of role-only events; handle bounded, complete multiline SSE events.
- Reload keys on SIGHUP with atomic validation and preserved health/reservations; drain HTTP requests during shutdown.
- Add an internal JSON metrics endpoint with bounded per-model latency histograms, request outcomes, 429 counts and loser cancellation latency.
- Add real HTTP streaming, cancellation, reload concurrency, SSE validation and metrics regression coverage.

## v1.1.0 - 2026-08-30

- Keep the existing three-key race while preventing reuse of keys that are still in flight.
- Detect structured NVIDIA errors returned inside SSE and JSON responses.
- Apply the same per-key failure policy to HTTP, SSE, and JSON errors; rate-limited keys now cool down for at least 60 seconds and honor longer `Retry-After` values.
- Add failure and exhaustion logs using slot indexes and controlled metadata only, without logging keys, headers, prompts, response bodies, or raw upstream errors.
- Add regression coverage for three-way racing, in-flight exclusion, structured 429 variants, cooldown behavior, and log redaction.

## v1.0.0 - 2026-08-29

- Initial public release of the NVIDIA three-key race proxy.
