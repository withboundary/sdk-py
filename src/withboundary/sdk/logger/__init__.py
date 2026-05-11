"""User-facing logger objects returned by the factory functions.

Two implementations of the contract package's ``ContractLogger``
Protocol live here:

* :class:`SyncBoundaryLogger` — for sync apps (Flask, Django, scripts).
  Backed by :class:`SyncBatcher` and a daemon thread.
* :class:`AsyncBoundaryLogger` — for asyncio apps (FastAPI, Quart,
  Litestar). Backed by :class:`AsyncBatcher` and an event-loop task.

Both expose the same public surface beyond the ContractLogger hooks:

    flush(timeout) — drain the queue, bounded by the timeout
    shutdown(timeout) — flush, then disable; idempotent

The :func:`create_boundary_logger` and :func:`create_async_boundary_logger`
factories construct these objects with the right options merged in.
Returning ``None`` from the factory is the dev-mode safe path.
"""

from __future__ import annotations

from .async_ import AsyncBoundaryLogger
from .sync import SyncBoundaryLogger

__all__ = [
    "AsyncBoundaryLogger",
    "SyncBoundaryLogger",
]
