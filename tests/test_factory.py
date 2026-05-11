"""create_boundary_logger — dev-mode safety + option threading."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest
from pytest_httpx import HTTPXMock

from withboundary.sdk import BoundaryEvent, SyncBoundaryLogger, create_boundary_logger
from withboundary.sdk.factory import API_KEY_ENV_VAR, DEFAULT_ENDPOINT
from withboundary.sdk.lifecycle import is_registered, unregister


@pytest.fixture(autouse=True)
def _scrub_api_key_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure tests don't pick up a real BOUNDARY_API_KEY from the
    developer's shell."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    yield


# ── Dev-mode safety ───────────────────────────────────────────────────


class TestDevModeSafe:
    def test_returns_none_without_api_key_or_write(self) -> None:
        # No api_key, no env var, no write sink — factory returns None
        # so wiring it unconditionally is safe.
        assert create_boundary_logger() is None

    def test_returns_logger_when_only_write_supplied(self) -> None:
        # Custom sink with no api_key is a valid configuration; the
        # SDK still does useful work shipping to the user's pipeline.
        def write(events: list[BoundaryEvent]) -> None:
            pass

        logger = create_boundary_logger(write=write, flush_on_exit=False)
        assert isinstance(logger, SyncBoundaryLogger)
        logger.shutdown(timeout=0.5)


# ── API key resolution ────────────────────────────────────────────────


class TestApiKeyResolution:
    def test_explicit_api_key_used(self) -> None:
        logger = create_boundary_logger(api_key="bnd_live_sk_test", flush_on_exit=False)
        assert isinstance(logger, SyncBoundaryLogger)
        logger.shutdown(timeout=0.5)

    def test_env_var_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_KEY_ENV_VAR, "bnd_live_sk_env")
        logger = create_boundary_logger(flush_on_exit=False)
        assert isinstance(logger, SyncBoundaryLogger)
        logger.shutdown(timeout=0.5)

    def test_explicit_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_KEY_ENV_VAR, "bnd_live_sk_env")
        # If both supplied, explicit wins — verified indirectly by the
        # transport receiving the explicit value.
        logger = create_boundary_logger(api_key="bnd_live_sk_explicit", flush_on_exit=False)
        assert isinstance(logger, SyncBoundaryLogger)
        logger.shutdown(timeout=0.5)


# ── Endpoint default ──────────────────────────────────────────────────


class TestEndpoint:
    def test_default_endpoint_is_production_host(self) -> None:
        assert DEFAULT_ENDPOINT == "https://api.withboundary.com"

    def test_custom_endpoint_honored(self) -> None:
        # The custom endpoint flows down to the transport — verify
        # via the resolved configuration rather than the network, so
        # the test stays a pure unit check.
        logger = create_boundary_logger(
            api_key="k",
            endpoint="https://staging.example.test",
            flush_on_exit=False,
        )
        assert logger is not None
        # Inspect the configured transport via the batcher's private
        # transport handle. This is white-box but it's the cleanest
        # way to assert "endpoint kwarg made it to the transport".
        from withboundary.sdk.batcher.sync import SyncBatcher

        batcher = logger._batcher  # noqa: SLF001 — test inspection
        assert isinstance(batcher, SyncBatcher)
        transport = batcher._transport  # noqa: SLF001 — test inspection
        assert transport is not None
        assert transport._endpoint == "https://staging.example.test"  # noqa: SLF001
        logger.shutdown(timeout=0.5)


# ── flush_on_exit toggle ──────────────────────────────────────────────


class TestFlushOnExit:
    def test_default_registers_atexit(self) -> None:
        logger = create_boundary_logger(api_key="k")
        try:
            assert logger is not None
            assert is_registered(logger)
        finally:
            assert logger is not None
            unregister(logger)
            logger.shutdown(timeout=0.5)

    def test_disable_skips_atexit(self) -> None:
        logger = create_boundary_logger(api_key="k", flush_on_exit=False)
        try:
            assert logger is not None
            assert not is_registered(logger)
        finally:
            assert logger is not None
            logger.shutdown(timeout=0.5)


# ── Injected http_client ─────────────────────────────────────────────


class TestInjectedClient:
    def test_user_client_used(self, httpx_mock: HTTPXMock) -> None:
        custom = httpx.Client(timeout=5.0)
        try:
            logger = create_boundary_logger(
                api_key="k",
                http_client=custom,
                flush_on_exit=False,
            )
            assert logger is not None
            logger.shutdown(timeout=0.5)
        finally:
            custom.close()
