"""Async ingest transport — parity coverage with the sync sibling."""

from __future__ import annotations

import json

import httpx
import pytest
from pytest_httpx import HTTPXMock

from withboundary.sdk import (
    AcceptedEvent,
    AsyncIngestTransport,
    AuthError,
    BreakerOpenError,
    BreakerOptions,
    BreakerState,
    NonRetryableStatusError,
    RateLimitError,
    RetryOptions,
    TransportError,
)
from withboundary.sdk._meta import SDK_NAME

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


def _transport(
    *,
    api_key: str = "bnd_live_sk_test",
    retry: RetryOptions | None = None,
    breaker: BreakerOptions | None = None,
) -> AsyncIngestTransport:
    return AsyncIngestTransport(
        endpoint=ENDPOINT,
        api_key=api_key,
        retry=retry or RetryOptions(max_attempts=3, base_ms=1, timeout=1.0),
        breaker=breaker,
    )


class TestHappyPath:
    async def test_basic_post(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        await _transport().send([_event()])
        request = httpx_mock.get_request()
        assert request is not None
        body = json.loads(request.content)
        assert isinstance(body, list)
        assert body[0]["contractName"] == "x"

    async def test_headers(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        await _transport(api_key="bnd_live_sk_77").send([_event()])
        request = httpx_mock.get_request()
        assert request is not None
        assert request.headers["authorization"] == "Bearer bnd_live_sk_77"
        assert SDK_NAME in request.headers["user-agent"]

    async def test_empty_batch_no_request(self, httpx_mock: HTTPXMock) -> None:
        await _transport().send([])


class TestAuthErrors:
    async def test_401_no_retry(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        transport = _transport()
        with pytest.raises(AuthError):
            await transport.send([_event()])
        assert len(httpx_mock.get_requests()) == 1


class TestRateLimit:
    async def test_429_retried_then_succeeds(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=429, headers={"Retry-After": "0"})
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        await _transport().send([_event()])
        assert len(httpx_mock.get_requests()) == 2

    async def test_429_exhausted(self, httpx_mock: HTTPXMock) -> None:
        for _ in range(3):
            httpx_mock.add_response(url=INGEST_URL, status_code=429, headers={"Retry-After": "0"})
        with pytest.raises(RateLimitError):
            await _transport().send([_event()])


class TestServerErrors:
    async def test_500_retried_then_success(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=500)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        await _transport().send([_event()])
        assert len(httpx_mock.get_requests()) == 2

    async def test_5xx_exhausted_raises_transport_error(self, httpx_mock: HTTPXMock) -> None:
        for _ in range(3):
            httpx_mock.add_response(url=INGEST_URL, status_code=502)
        with pytest.raises(TransportError) as exc_info:
            await _transport().send([_event()])
        assert exc_info.value.status == 502


class TestBatchSplit:
    async def test_413_splits(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=413)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        await _transport().send([_event(f"AAAAAAAAAAAAAAAAAAAA{i:02d}") for i in range(4)])
        requests = httpx_mock.get_requests()
        assert len(requests) == 3

    async def test_413_single_event_non_retryable(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=413)
        with pytest.raises(NonRetryableStatusError):
            await _transport().send([_event()])


class TestBreaker:
    async def test_breaker_trips_after_failures(self, httpx_mock: HTTPXMock) -> None:
        transport = _transport(
            retry=RetryOptions(max_attempts=1, base_ms=1, timeout=1.0),
            breaker=BreakerOptions(threshold=2, cooldown=60.0),
        )
        for _ in range(2):
            httpx_mock.add_response(url=INGEST_URL, status_code=500)
        with pytest.raises(TransportError):
            await transport.send([_event()])
        with pytest.raises(TransportError):
            await transport.send([_event()])
        assert transport.breaker.state is BreakerState.OPEN
        with pytest.raises(BreakerOpenError):
            await transport.send([_event()])


class TestCustomClient:
    async def test_user_supplied_client(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        client = httpx.AsyncClient(timeout=5.0)
        try:
            transport = AsyncIngestTransport(endpoint=ENDPOINT, api_key="k", client=client)
            await transport.send([_event()])
            assert len(httpx_mock.get_requests()) == 1
        finally:
            await client.aclose()


# pytest-httpx asserts every registered response is consumed; each
# test registers exactly what it expects to send.
