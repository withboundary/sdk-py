"""Synchronous background batcher.

A daemon thread owns the drain loop. It waits on a ``threading.Event``
signaled by three sources:

* ``push`` calls that bring the queue length to or past the size
  trigger.
* The periodic timer tick (``BatchOptions.interval`` seconds).
* Explicit ``flush(timeout)`` calls.

On every wake the worker drains in chunks of ``batch.size`` and ships
each chunk via the configured transport. ``flush(timeout)`` waits on
a ``drained`` event the worker sets when the queue empties, so the
caller can block precisely until in-flight events are gone.

``shutdown`` cancels the thread, drains within the timeout, and
flips the disabled flag so subsequent ``push`` calls become no-ops.

Pipeline order on every drain cycle:

    raw events
      ↓
    before_send (per event; None drops it)
      ↓
    write sink + transport.send (in parallel; failures route to
                                 on_error, batch is dropped)

A ``write`` sink failure does not block the transport from sending,
and vice versa — both run to completion (or surface their error to
on_error) before the worker moves to the next chunk.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable

from ..config import BatchOptions, resolve_batch
from ..events import BoundaryEvent
from ..queue import SyncEventQueue
from ..transport.errors import IngestError
from ..transport.sync import SyncIngestTransport


class SyncBatcher:
    """Drains events on size, time, or explicit triggers.

    Holds the queue and the transport; the surrounding logger talks
    to the batcher rather than either directly. Designed for one
    sender — the daemon thread — but ``push`` and ``flush`` are
    callable from any thread.
    """

    def __init__(
        self,
        *,
        transport: SyncIngestTransport | None,
        options: BatchOptions | None = None,
        write: Callable[[list[BoundaryEvent]], None] | None = None,
        before_send: Callable[[BoundaryEvent], BoundaryEvent | None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._options = resolve_batch(options)
        self._transport = transport
        self._write = write
        self._before_send = before_send
        self._on_error = on_error or _default_on_error

        self._queue = SyncEventQueue(max_size=self._options.max_queue_size)
        # Drain coordination — the worker waits on _wake; the worker
        # sets _drained when the queue empties so flush() callers can
        # return precisely once in-flight events are gone.
        self._wake = threading.Event()
        self._drained = threading.Event()
        self._drained.set()  # No events queued yet → already drained.

        self._disabled = False
        self._shutdown = False
        self._thread: threading.Thread | None = None
        self._thread_started = threading.Lock()

    # ── Public API ───────────────────────────────────────────────────────

    def push(self, event: BoundaryEvent) -> None:
        """Enqueue an event. Lazily starts the drain worker on the
        first push so logger construction stays cheap. Size-trigger
        wake fires when the queue crosses ``BatchOptions.size``.

        No-op once the batcher is disabled (shutdown completed or auth
        error tripped)."""
        if self._disabled:
            return
        self._ensure_thread_running()
        self._drained.clear()
        length = self._queue.push(event)
        if length >= self._options.size:
            self._wake.set()

    def flush(self, timeout: float | None = None) -> None:
        """Drain the queue, blocking until empty or ``timeout`` fires.

        Wakes the worker so any periodic-tick wait is cut short.
        ``timeout=None`` waits indefinitely (the on-exit handler uses
        a bounded timeout; serverless flush callers should always
        specify one).

        Returns when the queue is empty *or* the timeout elapses; the
        latter case can leave events in flight, since we don't
        cancel HTTP requests once they're posted.
        """
        if self._disabled or len(self._queue) == 0:
            return
        self._ensure_thread_running()
        self._wake.set()
        self._drained.wait(timeout)

    def shutdown(self, timeout: float | None = None) -> None:
        """Flush within ``timeout``, stop the worker thread, mark the
        batcher disabled. Idempotent — calling twice is a no-op on
        the second call."""
        if self._shutdown:
            return
        self.flush(timeout)
        self._shutdown = True
        self._disabled = True
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)

    # Read-only inspection used by tests + the logger
    def __len__(self) -> int:
        return len(self._queue)

    @property
    def disabled(self) -> bool:
        return self._disabled

    def mark_disabled(self) -> None:
        """Surface from the logger when an auth error tripped — stops
        the worker from making more outbound calls but keeps the queue
        intact so a manual ``flush`` is still safe."""
        self._disabled = True
        self._wake.set()

    # ── Worker ────────────────────────────────────────────────────────────

    def _ensure_thread_running(self) -> None:
        with self._thread_started:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._drain_loop,
                name="boundary-sdk-drain",
                daemon=True,
            )
            self._thread.start()

    def _drain_loop(self) -> None:
        """Main worker. Wakes on ``_wake`` (signaled by push, flush,
        shutdown, or the interval timer), drains everything currently
        queued, sets ``_drained``, then waits again. The timer rides
        on ``Event.wait(timeout)`` so we don't need a separate timer
        thread."""
        interval = self._options.interval if self._options.interval > 0 else None
        while not self._shutdown:
            # Wait for a wake signal OR the periodic interval. Wait
            # returns False on timeout, True on signal; we drain in
            # either case because both are valid triggers.
            self._wake.wait(interval)
            self._wake.clear()
            self._queue.clear_push_signal()
            try:
                self._drain_once()
            except Exception as exc:  # noqa: BLE001 — bug shield around worker body
                self._safe_on_error(exc)
            if len(self._queue) == 0:
                self._drained.set()

    def _drain_once(self) -> None:
        """Pull events in batch-sized chunks until the queue is empty.

        Dropped-event counter is read once per drain so a flood of
        oversize pushes doesn't spam ``on_error`` per event.
        """
        dropped = self._queue.take_dropped()
        if dropped:
            self._safe_on_error(_OverflowError(dropped, self._options.max_queue_size))

        while True:
            chunk = self._queue.drain(self._options.size)
            if not chunk:
                return
            self._dispatch(chunk)

    def _dispatch(self, events: list[BoundaryEvent]) -> None:
        """Run before_send, fan out to the write sink and transport.

        The write sink and transport share the same final events list
        (post-``before_send``). Failures in either go to ``on_error``;
        we never re-queue events on failure to avoid loops on a
        consistently misbehaving sink.
        """
        filtered = self._apply_before_send(events)
        if not filtered:
            return

        if self._write is not None:
            try:
                self._write(filtered)
            except Exception as exc:  # noqa: BLE001 — user sink, isolate
                self._safe_on_error(exc)

        if self._transport is not None and not self._disabled:
            try:
                self._transport.send(filtered)
            except Exception as exc:  # noqa: BLE001 — transport surfaces typed errors here
                self._safe_on_error(exc)
                # Auth errors trip the breaker / disable the SDK.
                if _is_auth_error(exc):
                    self.mark_disabled()

    def _apply_before_send(self, events: list[BoundaryEvent]) -> list[BoundaryEvent]:
        """Run the user's ``before_send`` callable on each event.

        Returning ``None`` drops the event silently. Any exception is
        routed to ``on_error``; the event passes through unchanged so
        a buggy hook doesn't lose data."""
        if self._before_send is None:
            return events
        out: list[BoundaryEvent] = []
        for event in events:
            try:
                result = self._before_send(event)
            except Exception as exc:  # noqa: BLE001 — user code
                self._safe_on_error(exc)
                out.append(event)
                continue
            if result is not None:
                out.append(result)
        return out

    def _safe_on_error(self, exc: Exception) -> None:
        """Invoke ``on_error`` without letting it crash the worker.

        A buggy ``on_error`` is the worst possible failure mode here
        — it can starve the drain thread. ``contextlib.suppress``
        swallows any exception the callback raises so the drain loop
        always reaches the next iteration.
        """
        with contextlib.suppress(Exception):
            self._on_error(exc)


# ── Helpers ──────────────────────────────────────────────────────────────


class _OverflowError(IngestError):
    """The queue dropped events to stay under its capacity cap."""

    def __init__(self, dropped: int, capacity: int) -> None:
        super().__init__(f"queue overflow: dropped {dropped} events (capacity={capacity})")
        self.dropped = dropped
        self.capacity = capacity


def _default_on_error(exc: Exception) -> None:
    """Best-effort default. Prints to stderr exactly once per process —
    keeps the SDK quiet in production but doesn't swallow the first
    sign of trouble during development."""
    import sys

    global _default_on_error_warned
    if _default_on_error_warned:
        return
    _default_on_error_warned = True
    print(f"[withboundary-sdk] outbound event error: {exc}", file=sys.stderr)


_default_on_error_warned = False


def _is_auth_error(exc: Exception) -> bool:
    """Detect auth errors without importing AuthError at module load.

    Avoids the small startup-time cost of importing the entire error
    module when callers only use the SDK in happy-path mode.
    """
    return type(exc).__name__ == "AuthError"


__all__ = [
    "SyncBatcher",
]


def reset_default_on_error_warned() -> None:
    """Test helper: reset the one-shot warning flag on the default
    on_error callback so multiple tests can exercise it in sequence."""
    global _default_on_error_warned
    _default_on_error_warned = False
