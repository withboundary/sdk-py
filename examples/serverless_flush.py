"""Force a drain on every invocation so serverless / lambda runs never lose events.

In a long-running process the SDK's background batcher takes care
of shipping events on its own schedule. In a serverless container
the process is frozen between invocations — the drain thread never
runs, so events queued during a request can sit for minutes before
the next invocation thaws the runtime. ``flush(timeout)`` blocks
until the queue empties (or the timeout fires), which gives you a
clean handoff point at the end of each request.

The same pattern applies to AWS Lambda, Modal, Google Cloud Run,
Vercel Functions, and any other "request → response → freeze"
runtime.

Run::

    python examples/serverless_flush.py
"""

from __future__ import annotations

import json

from pydantic import BaseModel
from withboundary.contract import ContractAttempt, define_contract

from withboundary.sdk import (
    BoundaryEvent,
    create_boundary_logger,
)


class Greeting(BaseModel):
    message: str


# ── A long-lived logger across invocations ──────────────────────────────


shipped_batches: list[list[BoundaryEvent]] = []


def write(events: list[BoundaryEvent]) -> None:
    shipped_batches.append(list(events))


# ``flush_on_exit=False`` is the right default for serverless: the
# process exit happens long after the request has returned, and the
# atexit handler doesn't help if the runtime is frozen mid-flight.
# Use the explicit ``flush`` call below instead.
logger = create_boundary_logger(write=write, flush_on_exit=False)
assert logger is not None

contract = define_contract(name="greet", schema=Greeting, logger=logger)


def lambda_handler(name: str) -> dict[str, object]:
    """Stand-in for a serverless request handler. Calls the contract,
    then flushes the SDK so events ship before the platform freezes
    the container."""

    def run(_ctx: ContractAttempt) -> str:
        return json.dumps({"message": f"hello, {name}"})

    result = contract.accept(run)
    logger.flush(timeout=2.0)  # type: ignore[union-attr]
    return {"result": repr(result), "shipped_batches_so_far": len(shipped_batches)}


# ── Simulate three invocations ──────────────────────────────────────────


for name in ("ada", "alan", "grace"):
    print(lambda_handler(name))

logger.shutdown(timeout=2.0)

print(f"\ntotal batches shipped: {len(shipped_batches)}")
print(f"total events shipped: {sum(len(b) for b in shipped_batches)}")
