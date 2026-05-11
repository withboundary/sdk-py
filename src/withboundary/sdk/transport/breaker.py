"""Circuit breaker for the ingest transport.

Sits in front of the retry layer. Once a configured threshold of
consecutive failures trips it, the breaker blocks outbound calls
immediately without touching the network — sparing the local CPU and
the remote endpoint while the underlying issue resolves.

States:

* ``CLOSED`` — normal operation. Successes reset the failure counter;
  failures increment it. When the counter reaches the threshold the
  breaker transitions to ``OPEN``.
* ``OPEN`` — fail fast. :meth:`before_call` raises :class:`BreakerOpenError`
  every time until ``cooldown`` seconds have elapsed since the trip.
* ``HALF_OPEN`` — admit exactly one probe. The next call attempt is
  allowed through; success closes the breaker, failure re-opens it for
  another cooldown period.

Thread-safe via a single ``threading.Lock`` — the same instance can
serve the sync and async transports because the operations are short
and don't block on I/O.
"""

from __future__ import annotations

import threading
import time
from enum import Enum

from .errors import BreakerOpenError


class BreakerState(str, Enum):
    """Public state enum — handy in logs and dashboards."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Consecutive-failure breaker with a cooldown window.

    Construct with ``threshold`` (failures before tripping) and
    ``cooldown`` (seconds before the breaker admits a probe). Both
    default to the production-safe values used by
    :class:`BreakerOptions`.
    """

    def __init__(self, *, threshold: int = 5, cooldown: float = 30.0) -> None:
        if threshold <= 0:
            raise ValueError(f"threshold must be positive, got {threshold}")
        if cooldown < 0:
            raise ValueError(f"cooldown must be non-negative, got {cooldown}")
        self._threshold = threshold
        self._cooldown = cooldown
        self._state: BreakerState = BreakerState.CLOSED
        self._failure_count: int = 0
        self._opened_at: float = 0.0
        self._lock = threading.Lock()

    # ── Probes ────────────────────────────────────────────────────────────

    def before_call(self) -> None:
        """Check the breaker before issuing a call.

        - In ``CLOSED`` state: returns immediately.
        - In ``OPEN`` state: raises :class:`BreakerOpenError` until
          ``cooldown`` has elapsed since the trip; then transitions to
          ``HALF_OPEN`` and returns so the caller can probe.
        - In ``HALF_OPEN`` state: returns; the next
          :meth:`record_success` closes the breaker,
          :meth:`record_failure` re-opens it.
        """
        with self._lock:
            if self._state is BreakerState.OPEN:
                if self._cooldown_elapsed():
                    self._state = BreakerState.HALF_OPEN
                    return
                raise BreakerOpenError(
                    f"circuit breaker is open; cooldown {self._remaining():.1f}s remaining"
                )

    def record_success(self) -> None:
        """Reset on a successful call. Transitions ``HALF_OPEN`` ->
        ``CLOSED`` and clears the failure counter."""
        with self._lock:
            self._state = BreakerState.CLOSED
            self._failure_count = 0

    def record_failure(self) -> None:
        """Count a failure. Trips the breaker into ``OPEN`` when the
        consecutive-failure counter reaches the threshold. A failure
        in ``HALF_OPEN`` state re-opens the breaker immediately
        regardless of the counter."""
        with self._lock:
            now = time.monotonic()
            if self._state is BreakerState.HALF_OPEN:
                self._open(now)
                return
            self._failure_count += 1
            if self._failure_count >= self._threshold:
                self._open(now)

    # ── Inspection ────────────────────────────────────────────────────────

    @property
    def state(self) -> BreakerState:
        """Current state. Reading without acquiring the lock is fine
        for observation — slightly stale views are acceptable."""
        return self._state

    @property
    def failure_count(self) -> int:
        """How many consecutive failures since the last success.
        Resets to ``0`` on every success."""
        return self._failure_count

    # ── Internals ────────────────────────────────────────────────────────

    def _open(self, now: float) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = now
        self._failure_count = self._threshold

    def _cooldown_elapsed(self) -> bool:
        return (time.monotonic() - self._opened_at) >= self._cooldown

    def _remaining(self) -> float:
        return max(0.0, self._cooldown - (time.monotonic() - self._opened_at))


__all__ = [
    "BreakerState",
    "CircuitBreaker",
]
