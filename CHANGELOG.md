# Changelog

## v1.1.0 - 2026-08-30

- Keep the existing three-key race while preventing reuse of keys that are still in flight.
- Detect structured NVIDIA errors returned inside SSE and JSON responses.
- Apply the same per-key failure policy to HTTP, SSE, and JSON errors; rate-limited keys now cool down for at least 60 seconds and honor longer `Retry-After` values.
- Add failure and exhaustion logs using slot indexes and controlled metadata only, without logging keys, headers, prompts, response bodies, or raw upstream errors.
- Add regression coverage for three-way racing, in-flight exclusion, structured 429 variants, cooldown behavior, and log redaction.

## v1.0.0 - 2026-08-29

- Initial public release of the NVIDIA three-key race proxy.
