"""In-memory event queue with a hard cap.

Backed by ``collections.deque(maxlen=…)`` so the FIFO + bounded-capacity
semantics come straight from the stdlib. Drop-oldest is automatic when
the cap is exceeded; ``EventQueue`` tracks the count of dropped events
separately (deque silently discards without notifying) so the batcher
can surface the count to ``on_error``.

Two thin wrapper classes provide thread-safe and asyncio-safe access:

* :class:`SyncEventQueue` guards every operation with
  ``threading.Lock`` — used by the sync logger's daemon thread.
* :class:`AsyncEventQueue` uses ``asyncio.Lock`` — used by the async
  logger's event-loop task.

Both expose the same surface: ``push``, ``drain(n)``, ``__len__``, and
``take_dropped()`` (returns and resets the dropped-since-last-check
counter).

The underlying :class:`EventQueue` is intentionally lock-free — it's
only meant for single-threaded internal use; the wrapping classes
add the concurrency guards. This keeps the core logic testable in
isolation.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import Iterable

from .events import BoundaryEvent


class EventQueue:
    """Bounded FIFO with a drop-oldest overflow policy.

    Not thread-safe on its own — wrap with :class:`SyncEventQueue` or
    :class:`AsyncEventQueue` for concurrent use. Tests exercise this
    type directly when verifying overflow accounting without the
    surrounding lock noise.
    """

    def __init__(self, max_size: int) -> None:
        if max_size <= 0:
            raise ValueError(f"max_size must be positive, got {max_size}")
        self._dq: deque[BoundaryEvent] = deque(maxlen=max_size)
        self._dropped: int = 0
        self._max_size = max_size

    def push(self, event: BoundaryEvent) -> None:
        """Append ``event``. If the queue is already at capacity, the
        oldest event is dropped and the dropped counter is incremented."""
        if len(self._dq) >= self._max_size:
            self._dropped += 1
        self._dq.append(event)

    def drain(self, n: int) -> list[BoundaryEvent]:
        """Pop up to ``n`` events from the front and return them. Used
        by the batcher to assemble each outbound HTTP request body."""
        if n <= 0:
            return []
        out: list[BoundaryEvent] = []
        while self._dq and len(out) < n:
            out.append(self._dq.popleft())
        return out

    def take_dropped(self) -> int:
        """Return the number of events dropped since the last call,
        then reset the counter. The batcher routes a non-zero value to
        the user's ``on_error`` callback so silent data loss is
        observable."""
        out = self._dropped
        self._dropped = 0
        return out

    @property
    def capacity(self) -> int:
        """The configured ``max_size`` — convenient for tests and the
        info that lands in error messages."""
        return self._max_size

    def __len__(self) -> int:
        return len(self._dq)


# ── Thread-safe wrapper ───────────────────────────────────────────────────


class SyncEventQueue:
    """Threading-lock-guarded façade over :class:`EventQueue`.

    Used by the sync logger's background drain thread. Every method
    acquires the lock for the duration of the underlying call — the
    operations are short, so contention is minimal even at high
    throughput.
    """

    def __init__(self, max_size: int) -> None:
        self._inner = EventQueue(max_size)
        self._lock = threading.Lock()
        # Signaled when push() lands; the drain thread waits on it so
        # it can fire immediately on a size trigger instead of waiting
        # for the periodic tick.
        self._not_empty = threading.Event()

    def push(self, event: BoundaryEvent) -> int:
        """Append ``event`` and return the resulting queue length so
        the caller can decide whether to wake the drain thread."""
        with self._lock:
            self._inner.push(event)
            length = len(self._inner)
        self._not_empty.set()
        return length

    def push_many(self, events: Iterable[BoundaryEvent]) -> int:
        """Bulk push — convenience for resilience paths where the
        transport returned a batch worth of events back to the queue."""
        with self._lock:
            for event in events:
                self._inner.push(event)
            length = len(self._inner)
        self._not_empty.set()
        return length

    def drain(self, n: int) -> list[BoundaryEvent]:
        with self._lock:
            return self._inner.drain(n)

    def take_dropped(self) -> int:
        with self._lock:
            return self._inner.take_dropped()

    def __len__(self) -> int:
        with self._lock:
            return len(self._inner)

    def wait_for_push(self, timeout: float | None = None) -> bool:
        """Block until at least one event lands or ``timeout`` elapses.
        Returns True on a signaled push, False on timeout. The flag is
        cleared by the caller via :meth:`clear_push_signal` once it has
        observed it — otherwise spurious wakeups stack up."""
        return self._not_empty.wait(timeout)

    def clear_push_signal(self) -> None:
        """Reset the push signal. The drain loop calls this after it
        observes the wakeup so the next push re-arms cleanly."""
        self._not_empty.clear()


# ── Asyncio-safe wrapper ──────────────────────────────────────────────────


class AsyncEventQueue:
    """``asyncio.Lock``-guarded façade for the event-loop drain task.

    Same shape as :class:`SyncEventQueue` but with awaitable methods.
    Uses ``asyncio.Event`` for the push-signal so loop tasks can await
    it without spinning.
    """

    def __init__(self, max_size: int) -> None:
        self._inner = EventQueue(max_size)
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()

    async def push(self, event: BoundaryEvent) -> int:
        async with self._lock:
            self._inner.push(event)
            length = len(self._inner)
        self._not_empty.set()
        return length

    async def push_many(self, events: Iterable[BoundaryEvent]) -> int:
        async with self._lock:
            for event in events:
                self._inner.push(event)
            length = len(self._inner)
        self._not_empty.set()
        return length

    async def drain(self, n: int) -> list[BoundaryEvent]:
        async with self._lock:
            return self._inner.drain(n)

    async def take_dropped(self) -> int:
        async with self._lock:
            return self._inner.take_dropped()

    async def length(self) -> int:
        """Async length accessor. We can't override ``__len__`` to be
        awaitable, so async callers use this helper."""
        async with self._lock:
            return len(self._inner)

    async def wait_for_push(self, timeout: float | None = None) -> bool:
        """Await until at least one event lands or ``timeout`` elapses.

        Returns True if signaled, False on timeout. ``timeout=None``
        waits indefinitely."""
        if timeout is None:
            await self._not_empty.wait()
            return True
        try:
            await asyncio.wait_for(self._not_empty.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def clear_push_signal(self) -> None:
        """Reset the push signal — sibling to the sync variant."""
        self._not_empty.clear()


__all__ = [
    "AsyncEventQueue",
    "EventQueue",
    "SyncEventQueue",
]
