"""Background batching for outbound events.

The batcher sits between the contract-hook layer (which enqueues
events) and the transport layer (which ships them). It owns the
worker that drains the queue on three triggers — size, time, or
explicit ``flush()`` — and runs the user's ``before_send`` /
``write`` / ``on_error`` callbacks at the right moments.

Two implementations share the same option shape:

* :class:`SyncBatcher` — daemon thread driving the drain loop, used
  by the sync logger.
* :class:`AsyncBatcher` — ``asyncio.Task`` driving the drain loop,
  used by the async logger.

Each one exposes ``push``, ``flush``, and ``shutdown`` at the level
of detail the surrounding logger needs.
"""

from __future__ import annotations

from .async_ import AsyncBatcher
from .sync import SyncBatcher

__all__ = [
    "AsyncBatcher",
    "SyncBatcher",
]
