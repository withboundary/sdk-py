"""Synchronous HTTP transport for the ingest endpoint.

Owns an ``httpx.Client`` (either user-supplied for connection-pooling
or test injection, or one the transport constructs itself). Serializes
every event in a batch to its wire form and posts a JSON array to
``{endpoint}/v1/ingest`` under bearer auth.

The send pipeline wraps three concerns in this order:

1. **Circuit breaker** — short-circuits the network call when the
   endpoint has been failing repeatedly.
2. **Retry loop** — exponential backoff on 5xx and network errors; the
   server's ``Retry-After`` honored on 429.
3. **Batch splitting** — a 413 response causes the transport to split
   the batch in half and retry each half, recursing once. The hosted
   ingest validator caps batches at 500 events; the SDK's batcher
   stays well under that, but a misconfigured downstream proxy could
   still cap lower.

Auth failures (401 / 403) raise :class:`AuthError` immediately without
retry. Other 4xx codes raise :class:`NonRetryableStatusError`. Network
errors and 5xx exhaust the retry budget then surface as
:class:`TransportError`.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from pydantic import TypeAdapter

from .._meta import user_agent
from ..config import BreakerOptions, RetryOptions, resolve_breaker, resolve_retry
from ..events import BoundaryEvent
from .breaker import CircuitBreaker
from .errors import (
    AuthError,
    NonRetryableStatusError,
    RateLimitError,
    TransportError,
)
from .retry import compute_backoff_ms, parse_retry_after

INGEST_PATH = "/v1/ingest"
"""The path appended to the configured endpoint base. Pinned by the
hosted validator and shared with the async transport so both speak
the same URL convention."""

_EVENT_DUMP: TypeAdapter[Any] = TypeAdapter(BoundaryEvent)


class SyncIngestTransport:
    """POSTs batches of events to the hosted ingest endpoint.

    Construct with the endpoint base URL, the API key, and optional
    retry / breaker configuration. Injecting an ``httpx.Client`` is
    supported for test doubles and connection-pool sharing; when
    omitted the transport builds one with sensible timeouts.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        retry: RetryOptions | None = None,
        breaker: BreakerOptions | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._retry = resolve_retry(retry)
        self._owned_client = client is None
        self._client = client or httpx.Client(timeout=self._retry.timeout)
        breaker_opts = resolve_breaker(breaker)
        self._breaker = CircuitBreaker(
            threshold=breaker_opts.threshold,
            cooldown=breaker_opts.cooldown,
        )

    # ── Public API ───────────────────────────────────────────────────────

    def send(self, events: list[BoundaryEvent]) -> None:
        """Post a batch to ``/v1/ingest``.

        Empty batches return immediately. Single-event batches that
        hit a 413 raise :class:`NonRetryableStatusError` (can't split
        a one-element batch). Batches with more than one event split
        recursively on 413.
        """
        if not events:
            return
        self._send_with_retry(events)

    def close(self) -> None:
        """Close the underlying ``httpx.Client`` if the transport owns
        it. No-op when the caller supplied their own client."""
        if self._owned_client:
            self._client.close()

    @property
    def breaker(self) -> CircuitBreaker:
        """Exposed for tests and the batcher's status reporting."""
        return self._breaker

    # ── Internals ────────────────────────────────────────────────────────

    def _send_with_retry(self, events: list[BoundaryEvent]) -> None:
        """Try the request up to ``max_attempts`` times with
        exponential backoff between attempts. Raises the final error
        once the budget is exhausted."""
        last_error: Exception | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            delay_ms = compute_backoff_ms(attempt, base_ms=self._retry.base_ms)
            if delay_ms:
                time.sleep(delay_ms / 1000.0)
            try:
                self._send_once(events)
                return
            except RateLimitError as exc:
                # Wait the server-indicated duration, then loop. The
                # rate-limit wait is in addition to the backoff
                # because the server is telling us when to come back.
                last_error = exc
                if attempt < self._retry.max_attempts:
                    time.sleep(exc.retry_after_seconds)
                    continue
                raise
            except (AuthError, NonRetryableStatusError):
                # Terminal — re-raise without consuming retries.
                raise
            except TransportError as exc:
                last_error = exc
                continue
        # Exhausted retries; raise the last network/server error.
        if last_error is None:
            return
        raise last_error

    def _send_once(self, events: list[BoundaryEvent]) -> None:
        """One round trip, with breaker + status-code dispatch.

        Auth errors bypass the breaker (a bad API key isn't a
        connectivity problem). Everything else interacts with the
        breaker normally: success closes / keeps it closed, failures
        accumulate toward the trip threshold.
        """
        self._breaker.before_call()
        try:
            response = self._client.post(
                self._endpoint + INGEST_PATH,
                json=_serialize_events(events),
                headers=self._headers(),
                timeout=self._retry.timeout,
            )
        except httpx.HTTPError as exc:
            self._breaker.record_failure()
            raise TransportError(
                f"network error talking to {self._endpoint}{INGEST_PATH}: {exc}",
                cause=exc,
            ) from exc

        status = response.status_code

        if 200 <= status < 300:
            self._breaker.record_success()
            return

        if status in (401, 403):
            # Auth errors bypass the breaker — the network is fine.
            raise AuthError(f"ingest endpoint rejected the API key ({status})", status=status)

        if status == 413 and len(events) > 1:
            # Don't count a split-and-retry as a failure for the
            # breaker — the server is asking us to chunk smaller.
            mid = len(events) // 2
            self._send_with_retry(events[:mid])
            self._send_with_retry(events[mid:])
            return

        if status == 429:
            self._breaker.record_failure()
            seconds = parse_retry_after(response.headers.get("retry-after")) or 1.0
            raise RateLimitError(
                f"ingest endpoint rate-limited; retry after {seconds:.1f}s",
                retry_after_seconds=seconds,
            )

        if 400 <= status < 500:
            self._breaker.record_failure()
            raise NonRetryableStatusError(
                f"ingest endpoint returned {status}",
                status=status,
                body=_safe_body(response),
            )

        # 5xx — transient, retryable.
        self._breaker.record_failure()
        raise TransportError(
            f"ingest endpoint returned {status}",
            status=status,
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "User-Agent": user_agent(),
        }


# ── Helpers ──────────────────────────────────────────────────────────────


def _serialize_events(events: list[BoundaryEvent]) -> list[dict[str, Any]]:
    """Render each event to its wire JSON shape with camelCase aliases
    and ``None`` fields stripped."""
    return [_EVENT_DUMP.dump_python(event, by_alias=True, exclude_none=True) for event in events]


def _safe_body(response: httpx.Response) -> str | None:
    """Read the response body for the exception message without
    blowing up on encoding issues or missing content. Trimmed to
    keep error logs small."""
    try:
        text = response.text
    except (UnicodeDecodeError, httpx.DecodingError):
        return None
    if not text:
        return None
    return text if len(text) <= 1024 else text[:1024] + "… (truncated)"


__all__ = [
    "INGEST_PATH",
    "SyncIngestTransport",
]
