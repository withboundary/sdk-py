"""Process-lifecycle hooks the SDK registers automatically.

The sync logger can register an ``atexit`` handler so events queued
when the process is about to exit get one last chance to flush.
This is on by default (``flush_on_exit=True`` on the factory). The
handler waits at most ``timeout`` seconds for the flush so it never
holds the process hostage to a slow endpoint.

Atexit is not useful for async loggers — by the time the registered
handler runs the event loop is closed and there's nothing we can
await on. Async users call ``await logger.shutdown(...)`` from their
framework's shutdown event (FastAPI ``on_event("shutdown")``,
Litestar's lifespan handler, etc.).
"""

from __future__ import annotations

import atexit
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .logger.sync import SyncBoundaryLogger


# Tracks every logger we've registered so the public ``unregister``
# helper (used in tests) knows which atexit callbacks to skip.
_registered: set[int] = set()
_registered_lock = threading.Lock()


def register_atexit(
    logger: SyncBoundaryLogger,
    *,
    timeout: float = 5.0,
) -> None:
    """Attach an ``atexit`` callback that drains ``logger`` within
    ``timeout`` seconds.

    The default 5s leaves the process responsive on shutdown — long
    enough for a healthy ingest endpoint to absorb a final batch,
    short enough that a failing endpoint doesn't keep the process
    alive indefinitely.
    """
    with _registered_lock:
        _registered.add(id(logger))

    def _on_exit() -> None:
        with _registered_lock:
            if id(logger) not in _registered:
                return
            _registered.discard(id(logger))
        # ``shutdown`` is idempotent and never raises; safe to call
        # from atexit regardless of state.
        logger.shutdown(timeout)

    atexit.register(_on_exit)


def unregister(logger: SyncBoundaryLogger) -> None:
    """Cancel a previously registered atexit handler.

    The actual ``atexit`` callback still fires, but it short-circuits
    on the missing registry entry. Used by tests to prevent leakage
    between cases.
    """
    with _registered_lock:
        _registered.discard(id(logger))


def is_registered(logger: SyncBoundaryLogger) -> bool:
    """True if ``logger`` has a live atexit handler. Test helper."""
    with _registered_lock:
        return id(logger) in _registered


__all__ = [
    "is_registered",
    "register_atexit",
    "unregister",
]
