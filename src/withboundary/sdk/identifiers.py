"""Stable, opaque identifiers minted by the SDK.

Two flavours, both URL-safe and free of the security-sensitive charge of a
secret token:

* ``mint_run_id`` — one per ``on_run_start`` hook, stamped on every wire
  event the SDK emits for that run. The hosted backend coalesces events
  by this id, so two attempts of the same contract run land on the same
  dashboard row. Format pinned to ``bnd_run_<22 url-safe chars>`` to
  match the validator's regex (``^bnd_run_[A-Za-z0-9_-]{1,40}$``).

* ``mint_event_id`` — one per emitted event, used as the optional
  ``clientEventId`` idempotency key. If the SDK retransmits an event
  (e.g. after a 5xx that retried successfully but the client already
  re-queued the batch), the backend deduplicates on this id within a
  batch.

We intentionally do NOT use a separate ``nanoid`` dependency — the
stdlib ``secrets`` module produces high-quality URL-safe tokens with the
right alphabet. ``token_urlsafe(16)`` returns ~22 characters from
``[A-Za-z0-9_-]``, which fits the validator's bounds with room to spare.
"""

from __future__ import annotations

import secrets

RUN_ID_PREFIX = "bnd_run_"
RUN_ID_BODY_LENGTH = 22
"""How many characters to keep from the URL-safe token. 22 is what
``token_urlsafe(16)`` naturally produces (16 random bytes, base64
encoded with ``=`` padding stripped); the validator allows up to 40 so
this leaves headroom."""

EVENT_ID_BYTES = 12
"""Byte count for the per-event idempotency key. ``token_urlsafe(12)``
yields a 16-character string — short enough to stay under the 64-char
cap on the wire field, long enough to make accidental collision within
a single process effectively impossible."""


def mint_run_id() -> str:
    """Generate a new run identifier in the wire-shape the ingest
    validator expects.

    Returns a string like ``"bnd_run_xY3kz9...zP"`` (prefix + 22
    URL-safe characters). One per ``on_run_start``; stamped on every
    subsequent event for the same run.
    """
    body = secrets.token_urlsafe(16)[:RUN_ID_BODY_LENGTH]
    return f"{RUN_ID_PREFIX}{body}"


def mint_event_id() -> str:
    """Generate a per-event idempotency key.

    Returned shape is a URL-safe token. The backend uses this within a
    batch to deduplicate retransmitted events, so it's not a security
    credential — uniqueness within a process and across plausible
    retries is sufficient.
    """
    return secrets.token_urlsafe(EVENT_ID_BYTES)


__all__ = [
    "EVENT_ID_BYTES",
    "RUN_ID_BODY_LENGTH",
    "RUN_ID_PREFIX",
    "mint_event_id",
    "mint_run_id",
]
