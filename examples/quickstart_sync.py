"""Synchronous quickstart: wire a logger, run a contract, inspect the events.

The ``write`` sink replaces the HTTP transport so the example runs
offline. Drop it for a real ``api_key=...`` to ship events to the
hosted dashboard at ``https://api.withboundary.com``.

Run::

    python examples/quickstart_sync.py
"""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field
from withboundary.contract import ContractAttempt, define_contract

from withboundary.sdk import (
    BoundaryEvent,
    CapturePolicy,
    create_boundary_logger,
)


class LeadScore(BaseModel):
    score: int = Field(ge=0, le=100, description="0-100 quality score")
    tier: Literal["hot", "warm", "cold"]


# ── Inspect what the SDK ships ───────────────────────────────────────────

captured_events: list[BoundaryEvent] = []


def write(events: list[BoundaryEvent]) -> None:
    """Stand-in for the HTTP transport. The dashboard accepts the
    same wire shape this list collects."""
    captured_events.extend(events)


logger = create_boundary_logger(
    write=write,
    environment="local-dev",
    capture=CapturePolicy(inputs=True, outputs=True, repairs=True),
    flush_on_exit=False,
)
assert logger is not None, "factory always returns a logger when a write sink is supplied"


# ── Run a contract ───────────────────────────────────────────────────────


contract = define_contract(name="lead-scoring", schema=LeadScore, logger=logger)


def run(_ctx: ContractAttempt) -> str:
    """Stand-in for an LLM call. Returns a valid JSON payload so the
    happy path runs without retries."""
    return '{"score": 88, "tier": "hot"}'


result = contract.accept(run)
logger.shutdown(timeout=2.0)

print(f"contract result: {result!r}")
print(f"events captured: {len(captured_events)}")
for event in captured_events:
    print(json.dumps(event.model_dump(by_alias=True, exclude_none=True), indent=2))
