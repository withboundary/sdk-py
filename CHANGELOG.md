# Changelog

All notable changes to `withboundary-sdk` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-11

Initial release.

### Added

- `create_boundary_logger` and `create_async_boundary_logger`: high-level factories that compose the transport, batcher, and logger. Both return objects implementing the `ContractLogger` Protocol from `withboundary-contract` so the SDK plugs into `define_contract(logger=...)` with no glue code. Returns `None` when neither an API key nor a custom write sink is configured, so dev environments stay zero-impact.
- `SyncBoundaryLogger` and `AsyncBoundaryLogger`: parallel implementations of the contract Protocol's ten hook methods. Sync uses a daemon thread + `httpx.Client`; async uses an event-loop task + `httpx.AsyncClient`. Per-run state is keyed by the contract runner's `run_handle` for safe concurrent contract runs.
- `BoundaryEvent` Pydantic discriminated union (`AcceptedEvent | FailedEvent`) with camelCase aliases matching the hosted ingest validator. Mid-run failures emit `FailedEvent(final=False)`; terminals emit either `AcceptedEvent` or `FailedEvent(final=True)`. Every event carries a `run_id` of the form `bnd_run_<22 url-safe chars>` and a `client_event_id` so retransmissions are idempotent at ingest.
- `BatchOptions`: size + time triggers (`size`, `interval`) plus a `max_queue_size` cap. Queue uses drop-oldest semantics on overflow with a dropped-event counter routed to `on_error`.
- `CapturePolicy`: gates for `inputs` / `outputs` / `repairs`. Default-off for prompts and outputs, default-on for repair messages. Every event carries a `ResolvedCapture` snapshot so the dashboard can distinguish "field not captured by policy" from "model returned nothing".
- `RedactionOptions` + `make_redaction`: three composable layers — field-name matches, regex patterns, and a custom callable. Cycle-safe walk via `id(obj)` tracking. Returning the `REDACT` sentinel from the custom layer drops the leaf entirely. Scrubbed paths land in `capture.redacted_fields` on every event.
- `RetryOptions`: exponential backoff with jitter on 5xx and network errors. `parse_retry_after` honors the 429 `Retry-After` header in both seconds-int and HTTP-date forms.
- `BreakerOptions`: circuit breaker with CLOSED → OPEN → HALF_OPEN states. Opens after a threshold of consecutive failures so a degraded endpoint doesn't burn the queue; auth errors bypass the breaker.
- `SyncIngestTransport` / `AsyncIngestTransport`: HTTP transports for the hosted ingest endpoint. 413 split-and-retry when a batch exceeds the per-request cap; 401 / 403 disables the logger and surfaces the error once via `on_error`.
- `before_send` hook and `write` sink: per-event transform / drop and per-batch custom destination. Both run alongside the HTTP transport when an API key is configured; the write sink becomes the single destination when only `write` is supplied.
- `lifecycle.register_atexit`: graceful shutdown for sync loggers, registered automatically when `flush_on_exit=True` (the default). Bounded by a configurable timeout so a degraded endpoint never holds the process hostage.
- Five runnable examples in `examples/` covering sync and async quickstarts, layered redaction, custom sinks with `before_send`, and the serverless flush pattern.
- PEP 561 `py.typed` marker so downstream type checkers honor the SDK's type hints.
- CI on Python 3.10, 3.11, 3.12, and 3.13 (ruff lint + format check, mypy strict, pytest).

[0.1.0]: https://github.com/withboundary/sdk-py/releases/tag/v0.1.0
