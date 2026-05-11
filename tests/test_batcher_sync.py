"""Synchronous batcher — size + time triggers, flush timing, callbacks."""

from __future__ import annotations

import threading
import time
from typing import Any

from pytest_httpx import HTTPXMock

from withboundary.sdk import (
    AcceptedEvent,
    AuthError,
    BatchOptions,
    SyncBatcher,
    SyncIngestTransport,
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


class RecordingTransport:
    """Stand-in transport that captures every `send` call. Lets the
    batcher tests verify dispatch shape without spinning up httpx_mock
    for the cases that don't need it."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.batches: list[list[AcceptedEvent]] = []
        self.fail = fail
        self.lock = threading.Lock()
        self.send_count = 0

    def send(self, events: list[Any]) -> None:
        with self.lock:
            self.send_count += 1
            self.batches.append(list(events))
        if self.fail is not None:
            raise self.fail

    def close(self) -> None:
        return None


def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    """Spin until predicate returns truthy or timeout. Used because
    the batcher's drain happens on a background thread; tests need a
    point at which they can safely assert."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


# ── Size trigger ────────────────────────────────────────────────────────


class TestSizeTrigger:
    def test_drains_when_queue_hits_size(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=3, interval=60.0, max_queue_size=100),
        )
        try:
            for i in range(3):
                batcher.push(_event(f"AAAAAAAAAAAAAAAAAAAA{i:02d}"))
            assert _wait_until(lambda: transport.send_count >= 1)
            assert len(transport.batches[0]) == 3
        finally:
            batcher.shutdown(timeout=1.0)

    def test_smaller_pushes_do_not_drain_yet(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=5, interval=60.0, max_queue_size=100),
        )
        try:
            for i in range(2):
                batcher.push(_event(f"AAAAAAAAAAAAAAAAAAAA{i:02d}"))
            # Wait long enough that a size-trigger drain would have
            # fired; verify none has.
            time.sleep(0.1)
            assert transport.send_count == 0
            assert len(batcher) == 2
        finally:
            batcher.shutdown(timeout=1.0)


# ── Time trigger ────────────────────────────────────────────────────────


class TestTimeTrigger:
    def test_drains_on_interval(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=100, interval=0.05, max_queue_size=100),
        )
        try:
            batcher.push(_event())
            # Under the size trigger but the timer should fire within
            # ~50ms; allow generous wall-clock slack.
            assert _wait_until(lambda: transport.send_count >= 1, timeout=2.0)
        finally:
            batcher.shutdown(timeout=1.0)


# ── Explicit flush ──────────────────────────────────────────────────────


class TestFlush:
    def test_flush_drains_queue(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=100, interval=60.0, max_queue_size=100),
        )
        try:
            batcher.push(_event())
            batcher.push(_event())
            batcher.flush(timeout=1.0)
            assert transport.send_count >= 1
            assert len(batcher) == 0
        finally:
            batcher.shutdown(timeout=1.0)

    def test_flush_with_empty_queue_returns_immediately(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(transport=transport)  # type: ignore[arg-type]
        try:
            start = time.monotonic()
            batcher.flush(timeout=5.0)
            elapsed = time.monotonic() - start
            assert elapsed < 0.5
        finally:
            batcher.shutdown(timeout=1.0)

    def test_flush_respects_timeout(self) -> None:
        """If the transport hangs longer than the timeout, flush
        returns once the timeout elapses without blocking forever."""

        class SlowTransport:
            def send(self, events: list[Any]) -> None:
                time.sleep(1.0)

            def close(self) -> None:
                return None

        batcher = SyncBatcher(
            transport=SlowTransport(),  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
        )
        try:
            batcher.push(_event())
            start = time.monotonic()
            batcher.flush(timeout=0.1)
            elapsed = time.monotonic() - start
            # Flush returned within the timeout window (allow generous slack).
            assert elapsed < 0.5
        finally:
            batcher.shutdown(timeout=0.1)


# ── Custom write sink ────────────────────────────────────────────────────


class TestWriteSink:
    def test_write_called_with_batch(self) -> None:
        captured: list[list[Any]] = []

        def write(events: list[Any]) -> None:
            captured.append(list(events))

        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=2, interval=60.0, max_queue_size=10),
            write=write,
        )
        try:
            batcher.push(_event())
            batcher.push(_event())
            assert _wait_until(lambda: len(captured) >= 1)
            assert len(captured[0]) == 2
            # Transport also received the batch — both fire.
            assert _wait_until(lambda: transport.send_count >= 1)
        finally:
            batcher.shutdown(timeout=1.0)

    def test_write_failure_routes_to_on_error(self) -> None:
        errors: list[Exception] = []

        def bad_write(_events: list[Any]) -> None:
            raise RuntimeError("write boom")

        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            write=bad_write,
            on_error=errors.append,
        )
        try:
            batcher.push(_event())
            assert _wait_until(lambda: len(errors) >= 1)
            # Transport still received the batch even though write
            # raised — both run independently.
            assert _wait_until(lambda: transport.send_count >= 1)
        finally:
            batcher.shutdown(timeout=1.0)


# ── before_send ──────────────────────────────────────────────────────────


class TestBeforeSend:
    def test_returning_none_drops_event(self) -> None:
        def drop_all(_event: Any) -> Any:
            return None

        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            before_send=drop_all,
        )
        try:
            batcher.push(_event())
            time.sleep(0.1)
            # Transport never called — every event filtered out.
            assert transport.send_count == 0
        finally:
            batcher.shutdown(timeout=1.0)

    def test_returning_modified_event_replaces(self) -> None:
        def annotate(event: AcceptedEvent) -> AcceptedEvent:
            return event.model_copy(update={"environment": "tagged"})

        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            before_send=annotate,
        )
        try:
            batcher.push(_event())
            assert _wait_until(lambda: transport.send_count >= 1)
            assert transport.batches[0][0].environment == "tagged"
        finally:
            batcher.shutdown(timeout=1.0)

    def test_exception_keeps_event_and_reports(self) -> None:
        errors: list[Exception] = []

        def broken(_event: Any) -> Any:
            raise RuntimeError("hook boom")

        transport = RecordingTransport()
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            before_send=broken,
            on_error=errors.append,
        )
        try:
            batcher.push(_event())
            assert _wait_until(lambda: transport.send_count >= 1)
            assert any("hook boom" in str(e) for e in errors)
        finally:
            batcher.shutdown(timeout=1.0)


# ── Queue overflow surfaces to on_error ─────────────────────────────────


class TestOverflowReporting:
    def test_dropped_events_surface_to_on_error(self) -> None:
        errors: list[Exception] = []

        class GatedTransport:
            """Blocks the first send until the test releases it, so the
            queue can grow past its cap and trigger drops."""

            def __init__(self) -> None:
                self.gate = threading.Event()
                self.calls = 0

            def send(self, events: list[Any]) -> None:
                self.calls += 1
                self.gate.wait()

            def close(self) -> None:
                return None

        gated = GatedTransport()
        batcher = SyncBatcher(
            transport=gated,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=2),
            on_error=errors.append,
        )
        try:
            # First push triggers the worker; it pulls a batch of 1
            # and gets stuck inside transport.send. The next pushes
            # exceed max_queue_size=2 and drop oldest events.
            for _ in range(5):
                batcher.push(_event())
            gated.gate.set()
            assert _wait_until(
                lambda: any("overflow" in str(e).lower() for e in errors),
                timeout=2.0,
            )
        finally:
            gated.gate.set()
            batcher.shutdown(timeout=1.0)


# ── Auth error disables batcher ─────────────────────────────────────────


class TestAuthDisable:
    def test_auth_error_disables_subsequent_pushes(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        transport = SyncIngestTransport(endpoint=ENDPOINT, api_key="bad")
        captured: list[Exception] = []
        batcher = SyncBatcher(
            transport=transport,
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            on_error=captured.append,
        )
        try:
            batcher.push(_event())
            assert _wait_until(
                lambda: any(isinstance(e, AuthError) for e in captured),
                timeout=2.0,
            )
            assert batcher.disabled is True
            # Further pushes are no-ops on a disabled batcher.
            batcher.push(_event())
            assert len(batcher) == 0
        finally:
            batcher.shutdown(timeout=1.0)


# ── Shutdown semantics ─────────────────────────────────────────────────


class TestShutdown:
    def test_shutdown_is_idempotent(self) -> None:
        transport = RecordingTransport()
        batcher = SyncBatcher(transport=transport)  # type: ignore[arg-type]
        batcher.push(_event())
        batcher.shutdown(timeout=1.0)
        batcher.shutdown(timeout=1.0)  # second call is a no-op
        assert batcher.disabled is True


# ── on_error never crashes worker ──────────────────────────────────────


class TestOnErrorSafety:
    def test_buggy_on_error_does_not_kill_worker(self) -> None:
        calls = []

        def explode(exc: Exception) -> None:
            calls.append(exc)
            raise RuntimeError("on_error broken")

        # Use a transport that fails so the on_error path actually fires.
        transport = RecordingTransport(fail=RuntimeError("send boom"))
        batcher = SyncBatcher(
            transport=transport,  # type: ignore[arg-type]
            options=BatchOptions(size=1, interval=60.0, max_queue_size=10),
            on_error=explode,
        )
        try:
            batcher.push(_event())
            assert _wait_until(lambda: len(calls) >= 1)
            # Push another event; the worker should still be alive.
            batcher.push(_event())
            assert _wait_until(lambda: len(calls) >= 2)
        finally:
            batcher.shutdown(timeout=1.0)
