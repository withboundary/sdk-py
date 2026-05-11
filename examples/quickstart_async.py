"""Asyncio quickstart: wire an async logger, run a contract, inspect the events.

Mirrors ``quickstart_sync.py`` but uses the asyncio-native factory and
the contract's ``aaccept`` entry point. Drop the ``write`` sink for an
``api_key=...`` to ship events to the hosted dashboard.

Run::

    python examples/quickstart_async.py
"""

from __future__ import annotations

import asyncio
import json
from typing import Literal

from pydantic import BaseModel, Field
from withboundary.contract import ContractAttempt, define_contract

from withboundary.sdk import (
    BoundaryEvent,
    CapturePolicy,
    create_async_boundary_logger,
)


class LeadScore(BaseModel):
    score: int = Field(ge=0, le=100)
    tier: Literal["hot", "warm", "cold"]


captured_events: list[BoundaryEvent] = []


async def write(events: list[BoundaryEvent]) -> None:
    """The async ``write`` sink may be a coroutine or a plain
    function; the batcher awaits whichever form it gets."""
    captured_events.extend(events)


async def main() -> None:
    logger = create_async_boundary_logger(
        write=write,
        environment="local-dev",
        capture=CapturePolicy(inputs=True, outputs=True, repairs=True),
    )
    assert logger is not None

    contract = define_contract(name="lead-scoring", schema=LeadScore, logger=logger)

    async def run(_ctx: ContractAttempt) -> str:
        return '{"score": 88, "tier": "hot"}'

    try:
        result = await contract.aaccept(run)
    finally:
        await logger.shutdown(timeout=2.0)

    print(f"contract result: {result!r}")
    print(f"events captured: {len(captured_events)}")
    for event in captured_events:
        print(json.dumps(event.model_dump(by_alias=True, exclude_none=True), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
