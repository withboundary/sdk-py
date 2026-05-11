"""Three-layer redaction: field names, regex patterns, and a custom callable.

Every wire event passes through the redactor before it lands in a
batch. The three layers run in order so each can see the output of
the previous one:

1. **Field redaction** — keys that match by name (case sensitive)
   are replaced with ``[REDACTED]``.
2. **Pattern redaction** — every string leaf is scanned for matches
   against the configured regex set; matches are replaced.
3. **Custom callable** — receives ``(value, path_tuple)`` for every
   leaf and may return a replacement, or the ``REDACT`` sentinel
   to drop the value entirely.

The ``capture.redacted_fields`` list on every emitted event records
the dotted paths the redactor touched, so the dashboard can show
which inputs and outputs were scrubbed without leaking the original
content.

Run::

    python examples/redaction.py
"""

from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel, Field
from withboundary.contract import ContractAttempt, define_contract

from withboundary.sdk import (
    REDACT,
    BoundaryEvent,
    CapturePolicy,
    create_boundary_logger,
    make_redaction,
)


class CustomerProfile(BaseModel):
    full_name: str = Field(min_length=1)
    email: str
    notes: str
    ssn_hint: str


captured_events: list[BoundaryEvent] = []


def write(events: list[BoundaryEvent]) -> None:
    captured_events.extend(events)


# ── Layered redaction policy ────────────────────────────────────────────


def scrub_short_strings(value: Any, path: tuple[str, ...]) -> Any:
    """Custom layer — drop any string under 3 characters in
    ``output.notes`` outright by returning the ``REDACT`` sentinel."""
    if path[:2] == ("output", "notes") and isinstance(value, str) and len(value) < 3:
        return REDACT
    return value


redaction = make_redaction(
    fields=["full_name", "ssn_hint"],
    patterns=[re.compile(r"[\w.+-]+@[\w.-]+\.[a-z]{2,}")],
    custom=scrub_short_strings,
)

logger = create_boundary_logger(
    write=write,
    redact=redaction,
    capture=CapturePolicy(inputs=False, outputs=True, repairs=False),
    flush_on_exit=False,
)
assert logger is not None


# ── Drive a contract ─────────────────────────────────────────────────────


contract = define_contract(name="customer-snapshot", schema=CustomerProfile, logger=logger)


def run(_ctx: ContractAttempt) -> str:
    return (
        '{"full_name": "Ada Lovelace", '
        '"email": "ada@example.com", '
        '"notes": "VIP",'
        '"ssn_hint": "***-**-4242"}'
    )


contract.accept(run)
logger.shutdown(timeout=2.0)


# ── Inspect the scrubbed wire payload ──────────────────────────────────

for event in captured_events:
    payload = event.model_dump(by_alias=True, exclude_none=True)
    print(json.dumps(payload, indent=2))
