"""Typed exceptions the transport surfaces to upstream callers.

Every failure mode the SDK can recognise has a dedicated class so the
batcher (and downstream ``on_error`` callbacks) can pattern-match on
the type rather than parsing exception strings. The exception messages
themselves are written for human eyes — the structured fields are the
machine-readable surface.

Hierarchy::

    IngestError                       (root — anything from this transport)
    ├── TransportError                (network / unexpected response)
    ├── AuthError                     (401 / 403 — terminal, no retry)
    ├── NonRetryableStatusError       (4xx other than 401 / 403 / 413 / 429)
    ├── RateLimitError                (429 — retry after Retry-After)
    └── BreakerOpenError              (breaker stopped the call before it left)

The batcher catches :class:`IngestError` to keep its retry / circuit-
breaker loop tight, and re-raises anything else (programmer errors,
broken hooks) untouched so failures stay visible.
"""

from __future__ import annotations


class IngestError(Exception):
    """Root of every exception the transport raises to callers."""


class TransportError(IngestError):
    """A network-level problem or an unexpected response shape.

    Carries the underlying ``status`` if the response was received but
    couldn't be interpreted; ``None`` for pure I/O failures (timeout,
    connection refused, DNS failure)."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.cause = cause


class AuthError(IngestError):
    """The endpoint rejected the API key (401 or 403).

    Terminal — the batcher disables the logger when it sees this and
    routes the error to ``on_error`` exactly once. Retrying the same
    request would not succeed and would burn the rate-limit budget.
    """

    def __init__(self, message: str, *, status: int) -> None:
        super().__init__(message)
        self.status = status


class NonRetryableStatusError(IngestError):
    """A 4xx response other than 401 / 403 / 413 / 429.

    Examples: 400 (malformed request body), 402 (payment required), 404
    (organization not found). Indicates a client-side problem that
    retry will not fix; the batch is dropped and the error surfaces to
    ``on_error``.
    """

    def __init__(self, message: str, *, status: int, body: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class RateLimitError(IngestError):
    """The endpoint returned 429.

    ``retry_after_seconds`` is parsed from the ``Retry-After`` header
    (HTTP-date or integer-seconds form) and capped at 60s. The batcher
    waits for the indicated delay before retrying within the
    surrounding retry policy.
    """

    def __init__(self, message: str, *, retry_after_seconds: float) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class BreakerOpenError(IngestError):
    """The circuit breaker is open — the call never left the SDK.

    Surfaces immediately without consuming a retry attempt. The
    batcher routes this to ``on_error`` and keeps the events queued
    for the next attempt cycle once the breaker cools down.
    """


__all__ = [
    "AuthError",
    "BreakerOpenError",
    "IngestError",
    "NonRetryableStatusError",
    "RateLimitError",
    "TransportError",
]
