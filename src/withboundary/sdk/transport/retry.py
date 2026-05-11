"""Backoff math and ``Retry-After`` parsing.

Two pure functions — no I/O, no state — so the transport implementations
can share the same retry semantics and the tests can verify the math in
isolation.

:func:`compute_backoff_ms` returns the delay (in milliseconds) the
batcher should sleep before attempt ``n``. Attempts are 1-indexed:
attempt 1 is the original request, attempt 2 is the first retry, and so
on. Attempt 1 always returns ``0`` — there's nothing to back off from.

:func:`parse_retry_after` converts the ``Retry-After`` header value
(seconds or HTTP-date) into a seconds count, capped at 60. The cap
keeps the SDK from blocking the drain thread for unreasonable durations
even when a misbehaving proxy returns a far-future date.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

DEFAULT_BASE_MS = 100
"""Default base delay. Backoff schedule for attempt ``n`` is
``base_ms * 4^(n-2)`` plus up to 50% jitter: 100ms, 400ms, 1600ms, …"""

DEFAULT_JITTER = 0.5
"""Max fraction of the computed delay added as jitter. ``0.5`` means
the actual delay falls in ``[base, 1.5 * base]``."""

MAX_RETRY_AFTER_SECONDS = 60.0
"""Hard cap on ``Retry-After`` honor. A misbehaving server can return
HTTP-dates far in the future; we never block longer than this."""


def compute_backoff_ms(
    attempt: int,
    *,
    base_ms: int = DEFAULT_BASE_MS,
    jitter: float = DEFAULT_JITTER,
) -> int:
    """Return the milliseconds to sleep before ``attempt`` runs.

    Returns ``0`` for ``attempt <= 1`` (no prior failure to back off
    from). For ``attempt >= 2``, returns
    ``base_ms * 4^(attempt - 2) + random(0, base * jitter * 4^(attempt - 2))``.

    Default schedule:

    * attempt 2 — 100 ms (+ up to 50ms jitter)
    * attempt 3 — 400 ms (+ up to 200ms jitter)
    * attempt 4 — 1600 ms (+ up to 800ms jitter)

    Jitter prevents thundering-herd retries from a fleet of identical
    SDK instances all reconnecting at the same moment after a brief
    outage.
    """
    if attempt <= 1:
        return 0
    if base_ms < 0:
        return 0
    if jitter < 0:
        jitter = 0.0
    multiplier = 4 ** (attempt - 2)
    base = base_ms * multiplier
    extra = random.random() * jitter * base
    return int(base + extra)


def parse_retry_after(value: str | None) -> float | None:
    """Convert a ``Retry-After`` header value to a seconds count.

    Accepts both forms RFC 9110 §10.2.3 defines:

    * **Delta seconds** — a non-negative integer (``"30"`` → 30.0).
    * **HTTP-date** — an RFC-822 / RFC-850 / asctime timestamp
      (``"Sat, 10 May 2026 12:00:00 GMT"``); converted to seconds from
      now, clamped at zero on past dates.

    Returns ``None`` when the header is absent or unparseable, so the
    caller can fall back to the surrounding exponential backoff.

    Always capped at :data:`MAX_RETRY_AFTER_SECONDS` regardless of the
    parsed value — even a server-supplied "wait an hour" stays at 60s
    so the queue keeps moving.
    """
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None

    # Integer-seconds form: cheap to test first.
    if stripped.isdigit():
        return min(float(stripped), MAX_RETRY_AFTER_SECONDS)

    # HTTP-date form. ``parsedate_to_datetime`` returns ``None`` for
    # unparseable strings (Python 3.10+); on older Pythons it raises
    # ``TypeError`` — guard both.
    try:
        parsed = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None

    # The parsed datetime may be naive; treat naive as UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    seconds = (parsed - now).total_seconds()
    if seconds <= 0:
        return 0.0
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


__all__ = [
    "DEFAULT_BASE_MS",
    "DEFAULT_JITTER",
    "MAX_RETRY_AFTER_SECONDS",
    "compute_backoff_ms",
    "parse_retry_after",
]
