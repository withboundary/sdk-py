"""Asynchronous HTTP transport for the ingest endpoint.

Mirror of :mod:`sync` for ``asyncio``-native callers. Uses
``httpx.AsyncClient`` and ``asyncio.sleep`` so the event loop stays
responsive while the transport waits for retries or rate-limit
windows.

Same three layers — circuit breaker, retry, batch splitting — and the
same error taxonomy.
"""

from __future__ import annotations

import asyncio
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
from .sync import INGEST_PATH

_EVENT_DUMP: TypeAdapter[Any] = TypeAdapter(BoundaryEvent)


class AsyncIngestTransport:
    """``asyncio``-native variant of :class:`SyncIngestTransport`.

    Same construction shape, same wire contract. Returns coroutines
    for ``send`` and ``close`` so callers can ``await`` them inside
    the event loop.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        retry: RetryOptions | None = None,
        breaker: BreakerOptions | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._retry = resolve_retry(retry)
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(timeout=self._retry.timeout)
        breaker_opts = resolve_breaker(breaker)
        self._breaker = CircuitBreaker(
            threshold=breaker_opts.threshold,
            cooldown=breaker_opts.cooldown,
        )

    # ── Public API ───────────────────────────────────────────────────────

    async def send(self, events: list[BoundaryEvent]) -> None:
        if not events:
            return
        await self._send_with_retry(events)

    async def close(self) -> None:
        if self._owned_client:
            await self._client.aclose()

    @property
    def breaker(self) -> CircuitBreaker:
        return self._breaker

    # ── Internals ────────────────────────────────────────────────────────

    async def _send_with_retry(self, events: list[BoundaryEvent]) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            delay_ms = compute_backoff_ms(attempt, base_ms=self._retry.base_ms)
            if delay_ms:
                await asyncio.sleep(delay_ms / 1000.0)
            try:
                await self._send_once(events)
                return
            except RateLimitError as exc:
                last_error = exc
                if attempt < self._retry.max_attempts:
                    await asyncio.sleep(exc.retry_after_seconds)
                    continue
                raise
            except (AuthError, NonRetryableStatusError):
                raise
            except TransportError as exc:
                last_error = exc
                continue
        if last_error is None:
            return
        raise last_error

    async def _send_once(self, events: list[BoundaryEvent]) -> None:
        self._breaker.before_call()
        try:
            response = await self._client.post(
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
            raise AuthError(f"ingest endpoint rejected the API key ({status})", status=status)

        if status == 413 and len(events) > 1:
            mid = len(events) // 2
            await self._send_with_retry(events[:mid])
            await self._send_with_retry(events[mid:])
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
    return [_EVENT_DUMP.dump_python(event, by_alias=True, exclude_none=True) for event in events]


def _safe_body(response: httpx.Response) -> str | None:
    try:
        text = response.text
    except (UnicodeDecodeError, httpx.DecodingError):
        return None
    if not text:
        return None
    return text if len(text) <= 1024 else text[:1024] + "… (truncated)"


__all__ = [
    "AsyncIngestTransport",
]
