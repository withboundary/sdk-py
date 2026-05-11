"""create_async_boundary_logger — dev-mode safety + option threading."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from withboundary.sdk import AsyncBoundaryLogger, BoundaryEvent, create_async_boundary_logger
from withboundary.sdk.factory import API_KEY_ENV_VAR


@pytest.fixture(autouse=True)
def _scrub_api_key_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    yield


# ── Dev-mode safety ───────────────────────────────────────────────────


class TestDevModeSafe:
    async def test_returns_none_without_api_key_or_write(self) -> None:
        assert create_async_boundary_logger() is None

    async def test_returns_logger_when_only_write_supplied(self) -> None:
        async def write(events: list[BoundaryEvent]) -> None:
            return None

        logger = create_async_boundary_logger(write=write)
        assert isinstance(logger, AsyncBoundaryLogger)
        await logger.shutdown(timeout=0.5)


# ── API key resolution ────────────────────────────────────────────────


class TestApiKeyResolution:
    async def test_explicit_api_key_used(self) -> None:
        logger = create_async_boundary_logger(api_key="bnd_live_sk_test")
        assert isinstance(logger, AsyncBoundaryLogger)
        await logger.shutdown(timeout=0.5)

    async def test_env_var_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_KEY_ENV_VAR, "bnd_live_sk_env")
        logger = create_async_boundary_logger()
        assert isinstance(logger, AsyncBoundaryLogger)
        await logger.shutdown(timeout=0.5)


# ── Endpoint plumbing ──────────────────────────────────────────────────


class TestEndpoint:
    async def test_custom_endpoint_honored(self) -> None:
        logger = create_async_boundary_logger(
            api_key="k",
            endpoint="https://staging.example.test",
        )
        assert logger is not None
        from withboundary.sdk.batcher.async_ import AsyncBatcher

        batcher = logger._batcher  # noqa: SLF001 — test inspection
        assert isinstance(batcher, AsyncBatcher)
        transport = batcher._transport  # noqa: SLF001 — test inspection
        assert transport is not None
        assert transport._endpoint == "https://staging.example.test"  # noqa: SLF001
        await logger.shutdown(timeout=0.5)


# ── flush_on_exit is accepted but inert ──────────────────────────────


class TestFlushOnExit:
    async def test_flush_on_exit_kwarg_is_accepted(self) -> None:
        """Kwarg parity with the sync factory; runtime no-op since
        atexit can't await a coroutine on a closed loop. Verifies the
        call doesn't raise."""
        logger = create_async_boundary_logger(api_key="k", flush_on_exit=True)
        assert isinstance(logger, AsyncBoundaryLogger)
        await logger.shutdown(timeout=0.5)


# ── Injected http_client ─────────────────────────────────────────────


class TestInjectedClient:
    async def test_user_client_used(self) -> None:
        custom = httpx.AsyncClient(timeout=5.0)
        try:
            logger = create_async_boundary_logger(api_key="k", http_client=custom)
            assert logger is not None
            await logger.shutdown(timeout=0.5)
        finally:
            await custom.aclose()
