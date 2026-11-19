# Project Instructions — KMTProto-go

Follow the shared global AI engineering rules. This file adds repository-specific context only.

## Purpose
Go implementation of KMTProto: a lightweight, transport-independent protocol for reliable messaging, session recovery, and state synchronization.

## Boundaries
- Preserve transport independence; do not couple the protocol core to a specific network transport or application.
- Preserve protocol compatibility and state/session semantics. Propose wire-format or compatibility changes before implementation.
- Do not add speculative protocol features or abstractions.

## Verification
- Read the implementation and tests under `v1/` before changes.
- Run `go test ./...`; run `go test -race ./...` for concurrency changes, and report any unavailable checks.
