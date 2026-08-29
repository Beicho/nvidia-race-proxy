# Public release design

## Scope

Publish the existing `Beicho/nvidia-race-proxy` repository under the MIT license and create a `v1.0.0` GitHub Release. The release contains the existing Codespaces-built static Linux amd64 binary and a SHA-256 checksum file. No local rebuild is performed.

## Safety

Before changing repository visibility, scan every tracked revision for NVIDIA keys, authenticated proxy URLs, Bearer tokens, OVH addresses, and key-file content. The repository must contain only source, tests, documentation, and module metadata. Runtime key files and binaries remain ignored by Git.

## Documentation

The README documents racing semantics, key health behavior, configuration, deployment, extra upstream usage, and the NVIDIA `/v1/responses` limitation. Examples use placeholders only and bind to loopback by default.

## Release verification

Verify the artifact hash against the previously recorded Codespaces build, publish the binary plus a checksum file, then confirm repository visibility, release assets, tag, and public download URLs through the GitHub API.
