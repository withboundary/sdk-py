"""Asyncio-native batcher.

Mirror of :class:`SyncBatcher` with a single drain task scheduled on
the running event loop. The task is created lazily on the first
``push`` so a caller can construct an async logger outside a loop
context (e.g. at module import) without crashing.

Three wake sources match the sync sibling: size trigger, interval
timer, explicit ``flush``. The timer rides on
``asyncio.wait_for(event.wait(), timeout)`` so we don't need a
``call_later`` chain.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import cast

from ..config import BatchOptions, resolve_batch
from ..events import BoundaryEvent
from ..queue import AsyncEventQueue
from ..transport.async_ import AsyncIngestTransport
from .sync import _is_auth_error, _OverflowError

# Callable shapes the async batcher accepts. The ``write`` sink can
# return either an awaitable or ``None`` (sync side-effect); the
# batcher awaits the result when it's a coroutine.
AsyncWrite = Callable[[list[BoundaryEvent]], "Awaitable[None] | None"]
AsyncBeforeSend = Callable[
    [BoundaryEvent], "Awaitable[BoundaryEvent | None] | BoundaryEvent | None"
]


class AsyncBatcher:
    """asyncio-native batching worker.

    Same option shape as :class:`SyncBatcher`; runs the drain loop as
    an ``asyncio.Task``. ``flush`` and ``shutdown`` are coroutines.
    """

    def __init__(
        self,
        *,
        transport: AsyncIngestTransport | None,
        options: BatchOptions | None = None,
        write: AsyncWrite | None = None,
        before_send: AsyncBeforeSend | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._options = resolve_batch(options)
        self._transport = transport
        self._write = write
        self._before_send = before_send
        self._on_error = on_error or _async_default_on_error

        self._queue = AsyncEventQueue(max_size=self._options.max_queue_size)
        self._wake = asyncio.Event()
        self._drained = asyncio.Event()
        self._drained.set()

        self._disabled = False
        self._shutdown = False
        self._task: asyncio.Task[None] | None = None
        self._task_lock = asyncio.Lock()

    # ── Public API ───────────────────────────────────────────────────────

    async def push(self, event: BoundaryEvent) -> None:
        if self._disabled:
            return
        await self._ensure_task_running()
        self._drained.clear()
        length = await self._queue.push(event)
        if length >= self._options.size:
            self._wake.set()

    async def flush(self, timeout: float | None = None) -> None:
        if self._disabled or await self._queue.length() == 0:
            return
        await self._ensure_task_running()
        self._wake.set()
        if timeout is None:
            await self._drained.wait()
            return
        try:
            await asyncio.wait_for(self._drained.wait(), timeout)
        except asyncio.TimeoutError:
            return

    async def shutdown(self, timeout: float | None = None) -> None:
        if self._shutdown:
            return
        await self.flush(timeout)
        self._shutdown = True
        self._disabled = True
        self._wake.set()
        task = self._task
        if task is None or task.done():
            return
        try:
            if timeout is None:
                await task
            else:
                await asyncio.wait_for(task, timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return

    async def length(self) -> int:
        return await self._queue.length()

    @property
    def disabled(self) -> bool:
        return self._disabled

    def mark_disabled(self) -> None:
        self._disabled = True
        self._wake.set()

    # ── Worker ────────────────────────────────────────────────────────────

    async def _ensure_task_running(self) -> None:
        async with self._task_lock:
            if self._task is not None and not self._task.done():
                return
            self._task = asyncio.create_task(self._drain_loop(), name="boundary-sdk-drain")

    async def _drain_loop(self) -> None:
        interval = self._options.interval if self._options.interval > 0 else None
        while not self._shutdown:
            await self._wait_with_interval(interval)
            self._wake.clear()
            self._queue.clear_push_signal()
            try:
                await self._drain_once()
            except Exception as exc:  # noqa: BLE001 — bug shield
                self._safe_on_error(exc)
            if await self._queue.length() == 0:
                self._drained.set()

    async def _wait_with_interval(self, interval: float | None) -> None:
        """Wait for ``_wake`` to fire, or the interval to elapse if one
        is configured. Returns whichever happens first; either is a
        valid drain trigger."""
        if interval is None:
            await self._wake.wait()
            return
        try:
            await asyncio.wait_for(self._wake.wait(), interval)
        except asyncio.TimeoutError:
            return

    async def _drain_once(self) -> None:
        dropped = await self._queue.take_dropped()
        if dropped:
            self._safe_on_error(_OverflowError(dropped, self._options.max_queue_size))

        while True:
            chunk = await self._queue.drain(self._options.size)
            if not chunk:
                return
            await self._dispatch(chunk)

    async def _dispatch(self, events: list[BoundaryEvent]) -> None:
        filtered = await self._apply_before_send(events)
        if not filtered:
            return

        if self._write is not None:
            try:
                result = self._write(filtered)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001 — user sink
                self._safe_on_error(exc)

        if self._transport is not None and not self._disabled:
            try:
                await self._transport.send(filtered)
            except Exception as exc:  # noqa: BLE001 — typed transport errors
                self._safe_on_error(exc)
                if _is_auth_error(exc):
                    self.mark_disabled()

    async def _apply_before_send(self, events: list[BoundaryEvent]) -> list[BoundaryEvent]:
        if self._before_send is None:
            return events
        before_send = self._before_send
        out: list[BoundaryEvent] = []
        for event in events:
            try:
                returned = before_send(event)
                if asyncio.iscoroutine(returned):
                    resolved = await cast(Awaitable[BoundaryEvent | None], returned)
                else:
                    resolved = cast(BoundaryEvent | None, returned)
            except Exception as exc:  # noqa: BLE001 — user code
                self._safe_on_error(exc)
                out.append(event)
                continue
            if resolved is not None:
                out.append(resolved)
        return out

    def _safe_on_error(self, exc: Exception) -> None:
        # contextlib.suppress is the idiomatic try/except/pass replacement
        # ruff suggests; the surrounding worker keeps running regardless
        # of what a buggy on_error callback raises.
        with contextlib.suppress(Exception):
            self._on_error(exc)


def _async_default_on_error(exc: Exception) -> None:
    """Async-batcher default error sink. Same one-shot stderr nag the
    sync batcher uses; keeps the SDK quiet in production but surfaces
    the first sign of trouble while developing."""
    import sys

    global _async_default_on_error_warned
    if _async_default_on_error_warned:
        return
    _async_default_on_error_warned = True
    print(f"[withboundary-sdk] outbound event error: {exc}", file=sys.stderr)


_async_default_on_error_warned = False


def reset_async_default_on_error_warned() -> None:
    """Test helper. Mirrors the sync sibling so suites can reset the
    one-shot flag between scenarios."""
    global _async_default_on_error_warned
    _async_default_on_error_warned = False


__all__ = [
    "AsyncBatcher",
]
