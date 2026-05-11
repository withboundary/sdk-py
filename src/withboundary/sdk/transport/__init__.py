"""HTTP transport for the hosted ingest endpoint.

The transport layer is the network-facing piece of the SDK. It accepts
batches of wire-shaped events, serialises them, posts them to
``/v1/ingest``, and surfaces structured errors when the request fails.

Three concerns split across submodules:

* :mod:`errors` — typed exceptions for the failure modes the SDK
  surfaces upward (auth failure, rate limit, non-retryable status,
  breaker open).
* :mod:`retry` — backoff math and ``Retry-After`` parsing. Pure
  functions; no I/O.
* :mod:`breaker` — circuit-breaker state machine that sits in front of
  the retry layer to stop hammering an unreachable endpoint.

The two transport implementations (:mod:`sync`, :mod:`async_`) live
alongside this module and share the helpers above. They expose the
same ``send(events) -> None`` surface so the batcher can treat them
interchangeably.
"""

from __future__ import annotations

from .breaker import BreakerState, CircuitBreaker
from .errors import (
    AuthError,
    BreakerOpenError,
    IngestError,
    NonRetryableStatusError,
    RateLimitError,
    TransportError,
)
from .retry import compute_backoff_ms, parse_retry_after

__all__ = [
    "AuthError",
    "BreakerOpenError",
    "BreakerState",
    "CircuitBreaker",
    "IngestError",
    "NonRetryableStatusError",
    "RateLimitError",
    "TransportError",
    "compute_backoff_ms",
    "parse_retry_after",
]
