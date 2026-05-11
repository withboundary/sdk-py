"""Async batcher — parity coverage with the sync sibling."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from pytest_httpx import HTTPXMock

from withboundary.sdk import (
    AcceptedEvent,
    AsyncBatcher,
    AsyncIngestTransport,
    AuthError,
    BatchOptions,
)

ENDPOINT = "https://api.example.test"
INGEST_URL = f"{ENDPOINT}/v1/ingest"


def _event(suffix: str = "AAAAAAAAAAAAAAAAAAAAA1") -> AcceptedEvent:
    return AcceptedEvent(
        contract_name="x",
        timestamp="2026-05-10T00:00:00+00:00",
        attempt=1,
        max_attempts=1,
        duration_ms=0,
        run_id=f"bnd_run_{suffix}",
    )


class RecordingAsyncTransport:
    """Stand-in transport with the AsyncIngestTransport shape."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.batches: list[list[Any]] = []
        self.fail = fail
        self.send_count = 0

    async def send(self, events: list[Any]) -> None:
        self.send_count += 1
        self.batches.append(list(events))
        if self.fail is not None:
            raise self.fail

    async def close(self) -> None:
        return None


async def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


# ── Size trigger ────────────────────────────────────────────────────────


class TestSizeTrigger:
    async def test_drains_when_queue_hits_size(self) -> None:
        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=3, interval=60.0, max_queue_size=100),
        )
        try:
            for i in range(3):
                await batcher.push(_event(f"AAAAAAAAAAAAAAAAAAAA{i:02d}"))
            assert await _wait_until(lambda: transport.send_count >= 1)
            assert len(transport.batches[0]) == 3
        finally:
            await batcher.shutdown(timeout=1.0)


# ── Time trigger ────────────────────────────────────────────────────────


class TestTimeTrigger:
    async def test_drains_on_interval(self) -> None:
        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=100, interval=0.05, max_queue_size=100),
        )
        try:
            await batcher.push(_event())
            assert await _wait_until(lambda: transport.send_count >= 1, timeout=2.0)
        finally:
            await batcher.shutdown(timeout=1.0)


# ── Flush ───────────────────────────────────────────────────────────────


class TestFlush:
    async def test_flush_drains_queue(self) -> None:
        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=100, interval=60.0, max_queue_size=100),
        )
        try:
            await batcher.push(_event())
            await batcher.push(_event())
            await batcher.flush(timeout=1.0)
            assert transport.send_count >= 1
        finally:
            await batcher.shutdown(timeout=1.0)

    async def test_flush_with_empty_queue_returns_immediately(self) -> None:
        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(transport=transport)  # type: ignore[arg-type]
        try:
            start = asyncio.get_event_loop().time()
            await batcher.flush(timeout=5.0)
            elapsed = asyncio.get_event_loop().time() - start
            assert elapsed < 0.5
        finally:
            await batcher.shutdown(timeout=1.0)


# ── Custom write sink ────────────────────────────────────────────────────


class TestWriteSink:
    async def test_async_write_awaited(self) -> None:
        captured: list[list[Any]] = []

        async def write(events: list[Any]) -> None:
            await asyncio.sleep(0)
            captured.append(list(events))

        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            write=write,
        )
        try:
            await batcher.push(_event())
            assert await _wait_until(lambda: len(captured) >= 1)
        finally:
            await batcher.shutdown(timeout=1.0)

    async def test_sync_write_callable_supported(self) -> None:
        captured: list[list[Any]] = []

        def write(events: list[Any]) -> None:
            captured.append(list(events))

        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            write=write,
        )
        try:
            await batcher.push(_event())
            assert await _wait_until(lambda: len(captured) >= 1)
        finally:
            await batcher.shutdown(timeout=1.0)


# ── before_send ─────────────────────────────────────────────────────────


class TestBeforeSend:
    async def test_async_before_send_awaited(self) -> None:
        async def annotate(event: AcceptedEvent) -> AcceptedEvent:
            return event.model_copy(update={"environment": "tagged"})

        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            before_send=annotate,
        )
        try:
            await batcher.push(_event())
            assert await _wait_until(lambda: transport.send_count >= 1)
            assert transport.batches[0][0].environment == "tagged"
        finally:
            await batcher.shutdown(timeout=1.0)

    async def test_returning_none_drops_event(self) -> None:
        def drop(_event: Any) -> Any:
            return None

        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            before_send=drop,
        )
        try:
            await batcher.push(_event())
            await asyncio.sleep(0.1)
            assert transport.send_count == 0
        finally:
            await batcher.shutdown(timeout=1.0)


# ── Auth error disables ─────────────────────────────────────────────────


class TestAuthDisable:
    async def test_auth_error_disables_subsequent_pushes(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        transport = AsyncIngestTransport(endpoint=ENDPOINT, api_key="bad")
        captured: list[Exception] = []
        batcher = AsyncBatcher(
            transport=transport,
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            on_error=captured.append,
        )
        try:
            await batcher.push(_event())
            assert await _wait_until(
                lambda: any(isinstance(e, AuthError) for e in captured),
                timeout=2.0,
            )
            assert batcher.disabled is True
            await batcher.push(_event())
            assert await batcher.length() == 0
        finally:
            await batcher.shutdown(timeout=1.0)
            await transport.close()


# ── Shutdown is idempotent ─────────────────────────────────────────────


class TestShutdown:
    async def test_double_shutdown(self) -> None:
        transport = RecordingAsyncTransport()
        batcher = AsyncBatcher(transport=transport)  # type: ignore[arg-type]
        await batcher.push(_event())
        await batcher.shutdown(timeout=1.0)
        await batcher.shutdown(timeout=1.0)
        assert batcher.disabled is True
