"""EventQueue + the sync/async lock-guarded façades."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from withboundary.sdk import (
    AcceptedEvent,
    AsyncEventQueue,
    EventQueue,
    SyncEventQueue,
)


def _accepted(suffix: str = "AAAAAAAAAAAAAAAAAAAAA1") -> AcceptedEvent:
    return AcceptedEvent(
        contract_name="x",
        timestamp="2026-05-10T00:00:00+00:00",
        attempt=1,
        max_attempts=1,
        duration_ms=0,
        run_id=f"bnd_run_{suffix}",
    )


# ── EventQueue core ───────────────────────────────────────────────────────


class TestEventQueueCore:
    def test_push_and_drain(self) -> None:
        q = EventQueue(max_size=5)
        for i in range(3):
            q.push(_accepted(f"AAAAAAAAAAAAAAAAAAAA{i:02d}"))
        assert len(q) == 3
        out = q.drain(10)
        assert len(out) == 3
        assert len(q) == 0

    def test_drain_n_caps_returned_count(self) -> None:
        q = EventQueue(max_size=10)
        for i in range(5):
            q.push(_accepted(f"AAAAAAAAAAAAAAAAAAAA{i:02d}"))
        assert len(q.drain(2)) == 2
        assert len(q) == 3

    def test_drain_zero_returns_empty(self) -> None:
        q = EventQueue(max_size=10)
        q.push(_accepted())
        assert q.drain(0) == []
        assert len(q) == 1

    def test_overflow_drops_oldest(self) -> None:
        q = EventQueue(max_size=2)
        q.push(_accepted("AAAAAAAAAAAAAAAAAAAA01"))
        q.push(_accepted("AAAAAAAAAAAAAAAAAAAA02"))
        q.push(_accepted("AAAAAAAAAAAAAAAAAAAA03"))  # drops 01
        assert len(q) == 2
        first = q.drain(1)[0]
        # Oldest (01) was dropped; first remaining is 02.
        assert first.run_id.endswith("AAAAAAAAAAAAAAAAAAAA02")

    def test_dropped_counter_tracks_overflow(self) -> None:
        q = EventQueue(max_size=2)
        q.push(_accepted())
        q.push(_accepted())
        q.push(_accepted())
        q.push(_accepted())
        # 2 events fit; 2 dropped on the way in.
        assert q.take_dropped() == 2

    def test_take_dropped_resets_counter(self) -> None:
        q = EventQueue(max_size=1)
        q.push(_accepted())
        q.push(_accepted())  # 1 dropped
        assert q.take_dropped() == 1
        # Second read returns zero — counter was reset.
        assert q.take_dropped() == 0

    def test_invalid_max_size_rejected(self) -> None:
        with pytest.raises(ValueError):
            EventQueue(max_size=0)
        with pytest.raises(ValueError):
            EventQueue(max_size=-1)


# ── SyncEventQueue ───────────────────────────────────────────────────────


class TestSyncEventQueue:
    def test_push_returns_length(self) -> None:
        q = SyncEventQueue(max_size=5)
        assert q.push(_accepted()) == 1
        assert q.push(_accepted()) == 2

    def test_push_many(self) -> None:
        q = SyncEventQueue(max_size=10)
        q.push_many([_accepted(), _accepted(), _accepted()])
        assert len(q) == 3

    def test_wait_for_push_signals_on_push(self) -> None:
        q = SyncEventQueue(max_size=5)

        def push_after_delay() -> None:
            time.sleep(0.05)
            q.push(_accepted())

        threading.Thread(target=push_after_delay).start()
        assert q.wait_for_push(timeout=1.0) is True

    def test_wait_for_push_returns_false_on_timeout(self) -> None:
        q = SyncEventQueue(max_size=5)
        assert q.wait_for_push(timeout=0.01) is False

    def test_concurrent_push_thread_safe(self) -> None:
        """Sanity-check the lock under high contention. 50 threads
        each pushing 20 events; total should always be 1000."""
        q = SyncEventQueue(max_size=2000)

        def worker() -> None:
            for _ in range(20):
                q.push(_accepted())

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(q) == 1000

    def test_clear_push_signal_rearms_wait(self) -> None:
        q = SyncEventQueue(max_size=5)
        q.push(_accepted())
        assert q.wait_for_push(timeout=0.01) is True
        q.clear_push_signal()
        # After clearing, a fresh wait sees no signal until another push.
        assert q.wait_for_push(timeout=0.01) is False


# ── AsyncEventQueue ──────────────────────────────────────────────────────


class TestAsyncEventQueue:
    async def test_push_returns_length(self) -> None:
        q = AsyncEventQueue(max_size=5)
        assert await q.push(_accepted()) == 1
        assert await q.push(_accepted()) == 2

    async def test_drain_returns_events(self) -> None:
        q = AsyncEventQueue(max_size=5)
        await q.push(_accepted())
        await q.push(_accepted())
        events = await q.drain(10)
        assert len(events) == 2

    async def test_wait_for_push_signals(self) -> None:
        q = AsyncEventQueue(max_size=5)

        async def push_after_delay() -> None:
            await asyncio.sleep(0.05)
            await q.push(_accepted())

        task = asyncio.create_task(push_after_delay())
        result = await q.wait_for_push(timeout=1.0)
        await task
        assert result is True

    async def test_wait_for_push_times_out(self) -> None:
        q = AsyncEventQueue(max_size=5)
        assert await q.wait_for_push(timeout=0.01) is False

    async def test_length_helper(self) -> None:
        q = AsyncEventQueue(max_size=5)
        await q.push(_accepted())
        assert await q.length() == 1
