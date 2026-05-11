"""Route events to a custom sink alongside (or instead of) the hosted ingest.

The ``write`` kwarg accepts any callable that takes a list of
``BoundaryEvent``. It runs in addition to the HTTP transport when an
API key is configured; when only ``write`` is supplied (no
``api_key``) the SDK skips the network entirely and your sink
becomes the single destination. Useful for piping events into an
existing logging pipeline (Datadog, Honeycomb, an internal Kafka
topic) without losing the dashboard-shaped wire payload.

``before_send`` is the per-event hook that runs just before each
batch dispatches. Returning ``None`` drops the event; returning a
substituted event replaces the original. Use it to tag every event
with a deployment identifier, to scrub additional fields the static
redactor can't reach, or to filter out internal sandbox runs that
should not surface in production dashboards.

Run::

    python examples/custom_sink.py
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel
from withboundary.contract import ContractAttempt, define_contract

from withboundary.sdk import (
    BoundaryEvent,
    create_boundary_logger,
)


class Reply(BaseModel):
    answer: str
    confidence: float


# ── Custom destination ──────────────────────────────────────────────────


structured_log: list[dict[str, Any]] = []


def write(events: list[BoundaryEvent]) -> None:
    """Pretend pipeline that ships dashboard-shaped events to a
    structured-log sink. In a real deployment this would push to
    Datadog, Loki, or your own data lake."""
    for event in events:
        structured_log.append(event.model_dump(by_alias=True, exclude_none=True))


def tag_deployment(event: BoundaryEvent) -> BoundaryEvent:
    """Annotate every event with a deployment label using Pydantic's
    ``model_copy`` so we don't mutate the original object."""
    return event.model_copy(update={"environment": "deploy-abc123"})


logger = create_boundary_logger(
    write=write,
    before_send=tag_deployment,
    flush_on_exit=False,
)
assert logger is not None


# ── Run a contract ──────────────────────────────────────────────────────


contract = define_contract(name="qa", schema=Reply, logger=logger)


def run(_ctx: ContractAttempt) -> str:
    return '{"answer": "42", "confidence": 0.98}'


contract.accept(run)
logger.shutdown(timeout=2.0)


for payload in structured_log:
    print(json.dumps(payload, indent=2))
