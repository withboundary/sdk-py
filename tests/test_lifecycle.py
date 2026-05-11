"""atexit registration / unregistration helpers."""

from __future__ import annotations

from typing import Any

import pytest

from withboundary.sdk.lifecycle import is_registered, register_atexit, unregister


class _StubLogger:
    """Fake SyncBoundaryLogger — only needs ``shutdown`` for the
    lifecycle handler to call. Lets us verify the handler interactions
    without spinning up the full SDK."""

    def __init__(self) -> None:
        self.shutdown_calls: list[float | None] = []

    def shutdown(self, timeout: float | None = None) -> None:
        self.shutdown_calls.append(timeout)


class TestRegistration:
    def test_register_then_is_registered(self) -> None:
        logger = _StubLogger()
        register_atexit(logger, timeout=1.0)  # type: ignore[arg-type]
        try:
            assert is_registered(logger)  # type: ignore[arg-type]
        finally:
            unregister(logger)  # type: ignore[arg-type]

    def test_unregister_removes(self) -> None:
        logger = _StubLogger()
        register_atexit(logger)  # type: ignore[arg-type]
        unregister(logger)  # type: ignore[arg-type]
        assert not is_registered(logger)  # type: ignore[arg-type]

    def test_is_registered_default_false(self) -> None:
        logger = _StubLogger()
        assert not is_registered(logger)  # type: ignore[arg-type]


class TestAtexitCallback:
    def test_callback_calls_shutdown_with_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Capture the atexit callback ``register_atexit`` would
        attach and invoke it directly so we don't have to wait for
        process exit."""
        captured: list[Any] = []

        def fake_register(callback: Any) -> Any:
            captured.append(callback)
            return callback

        monkeypatch.setattr("withboundary.sdk.lifecycle.atexit.register", fake_register)

        logger = _StubLogger()
        register_atexit(logger, timeout=2.5)  # type: ignore[arg-type]
        assert captured, "no atexit callback registered"
        callback = captured[0]
        callback()
        assert logger.shutdown_calls == [2.5]
        # After the callback fires the registry slot is freed so a
        # second invocation (from a duplicate atexit run, for example)
        # is a no-op.
        callback()
        assert logger.shutdown_calls == [2.5]

    def test_callback_no_op_after_unregister(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: list[Any] = []

        def fake_register(callback: Any) -> Any:
            captured.append(callback)
            return callback

        monkeypatch.setattr("withboundary.sdk.lifecycle.atexit.register", fake_register)

        logger = _StubLogger()
        register_atexit(logger)  # type: ignore[arg-type]
        unregister(logger)  # type: ignore[arg-type]
        callback = captured[0]
        callback()
        assert logger.shutdown_calls == []
