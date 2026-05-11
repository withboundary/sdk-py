"""Sync ingest transport — HTTP, retries, breaker, batch split."""

from __future__ import annotations

import json

import httpx
import pytest
from pytest_httpx import HTTPXMock

from withboundary.sdk import (
    AcceptedEvent,
    AuthError,
    BreakerOpenError,
    NonRetryableStatusError,
    RateLimitError,
    RetryOptions,
    SyncIngestTransport,
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
    httpx_mock: HTTPXMock,
    *,
    api_key: str = "bnd_live_sk_test",
    retry: RetryOptions | None = None,
) -> SyncIngestTransport:
    """Build a SyncIngestTransport whose underlying client is mocked.
    pytest-httpx auto-patches httpx.Client when the fixture is active."""
    return SyncIngestTransport(
        endpoint=ENDPOINT,
        api_key=api_key,
        retry=retry or RetryOptions(max_attempts=3, base_ms=1, timeout=1.0),
    )


# ── Happy path ────────────────────────────────────────────────────────────


class TestHappyPath:
    def test_single_event_posted(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, method="POST", status_code=202)
        _transport(httpx_mock).send([_event()])
        request = httpx_mock.get_request()
        assert request is not None
        body = json.loads(request.content)
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["contractName"] == "x"

    def test_headers_include_auth_and_user_agent(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        _transport(httpx_mock, api_key="bnd_live_sk_42").send([_event()])
        request = httpx_mock.get_request()
        assert request is not None
        assert request.headers["authorization"] == "Bearer bnd_live_sk_42"
        assert request.headers["content-type"] == "application/json"
        assert SDK_NAME in request.headers["user-agent"]

    def test_empty_batch_no_request(self, httpx_mock: HTTPXMock) -> None:
        # No mock added — if the transport tried to call out, pytest-httpx
        # would surface an "unexpected request" failure.
        _transport(httpx_mock).send([])

    def test_camel_case_wire_shape(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        _transport(httpx_mock).send([_event()])
        request = httpx_mock.get_request()
        assert request is not None
        body = json.loads(request.content)
        # Required wire fields surface with their camelCase aliases.
        for key in ("contractName", "maxAttempts", "durationMs", "runId"):
            assert key in body[0], key
        # No snake_case keys leak through.
        for snake in ("contract_name", "max_attempts", "duration_ms", "run_id"):
            assert snake not in body[0], snake


# ── Auth errors ───────────────────────────────────────────────────────────


class TestAuthErrors:
    def test_401_raises_auth_error_no_retry(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        transport = _transport(httpx_mock)
        with pytest.raises(AuthError):
            transport.send([_event()])
        # Only one attempt — auth errors don't burn retries.
        assert len(httpx_mock.get_requests()) == 1

    def test_403_raises_auth_error(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=403)
        transport = _transport(httpx_mock)
        with pytest.raises(AuthError):
            transport.send([_event()])

    def test_auth_error_bypasses_breaker(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        httpx_mock.add_response(url=INGEST_URL, status_code=401)
        transport = _transport(httpx_mock)
        # Two auth failures in a row — breaker should NOT trip
        # (auth isn't a connectivity issue).
        with pytest.raises(AuthError):
            transport.send([_event()])
        with pytest.raises(AuthError):
            transport.send([_event()])
        from withboundary.sdk import BreakerState

        assert transport.breaker.state is BreakerState.CLOSED


# ── Non-retryable status ──────────────────────────────────────────────────


class TestNonRetryable:
    def test_400_raises_non_retryable(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url=INGEST_URL,
            status_code=400,
            json={"error": "Invalid event shape"},
        )
        transport = _transport(httpx_mock)
        with pytest.raises(NonRetryableStatusError) as exc_info:
            transport.send([_event()])
        assert exc_info.value.status == 400
        assert exc_info.value.body is not None
        assert "Invalid event shape" in exc_info.value.body

    def test_402_payment_required_non_retryable(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=402)
        transport = _transport(httpx_mock)
        with pytest.raises(NonRetryableStatusError):
            transport.send([_event()])
        # Only one request — no retry.
        assert len(httpx_mock.get_requests()) == 1


# ── 429 + Retry-After ─────────────────────────────────────────────────────


class TestRateLimit:
    def test_429_retried_after_delay(self, httpx_mock: HTTPXMock) -> None:
        # First call is 429 with a brief retry-after; second succeeds.
        httpx_mock.add_response(
            url=INGEST_URL,
            status_code=429,
            headers={"Retry-After": "0"},
        )
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        transport = _transport(httpx_mock)
        transport.send([_event()])
        assert len(httpx_mock.get_requests()) == 2

    def test_429_exhausted_raises_rate_limit_error(self, httpx_mock: HTTPXMock) -> None:
        # All attempts return 429.
        for _ in range(3):
            httpx_mock.add_response(
                url=INGEST_URL,
                status_code=429,
                headers={"Retry-After": "0"},
            )
        transport = _transport(
            httpx_mock,
            retry=RetryOptions(max_attempts=3, base_ms=1, timeout=1.0),
        )
        with pytest.raises(RateLimitError):
            transport.send([_event()])


# ── 5xx retries ───────────────────────────────────────────────────────────


class TestServerErrors:
    def test_500_retried_then_success(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=500)
        httpx_mock.add_response(url=INGEST_URL, status_code=502)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        transport = _transport(httpx_mock)
        transport.send([_event()])
        assert len(httpx_mock.get_requests()) == 3

    def test_500_exhausted_raises_transport_error(self, httpx_mock: HTTPXMock) -> None:
        for _ in range(3):
            httpx_mock.add_response(url=INGEST_URL, status_code=503)
        transport = _transport(
            httpx_mock,
            retry=RetryOptions(max_attempts=3, base_ms=1, timeout=1.0),
        )
        with pytest.raises(TransportError) as exc_info:
            transport.send([_event()])
        assert exc_info.value.status == 503


# ── 413 batch split ───────────────────────────────────────────────────────


class TestBatchSplit:
    def test_413_splits_batch(self, httpx_mock: HTTPXMock) -> None:
        # First call (4 events) → 413; the transport splits into two
        # halves of 2 events each, each succeeds.
        httpx_mock.add_response(url=INGEST_URL, status_code=413)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        transport = _transport(httpx_mock)
        transport.send([_event(f"AAAAAAAAAAAAAAAAAAAA{i:02d}") for i in range(4)])
        requests = httpx_mock.get_requests()
        assert len(requests) == 3
        # First was 4 events; the two follow-ups should be 2 each.
        bodies = [json.loads(r.content) for r in requests]
        assert len(bodies[0]) == 4
        assert len(bodies[1]) == 2
        assert len(bodies[2]) == 2

    def test_413_on_single_event_raises_non_retryable(self, httpx_mock: HTTPXMock) -> None:
        # Can't split a 1-event batch any further.
        httpx_mock.add_response(url=INGEST_URL, status_code=413)
        transport = _transport(httpx_mock)
        with pytest.raises(NonRetryableStatusError):
            transport.send([_event()])


# ── Network errors ────────────────────────────────────────────────────────


class TestNetworkErrors:
    def test_connection_error_retried_then_success(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_exception(httpx.ConnectError("dns"))
        httpx_mock.add_response(url=INGEST_URL, status_code=202)
        transport = _transport(httpx_mock)
        transport.send([_event()])
        assert len(httpx_mock.get_requests()) == 2

    def test_persistent_network_error_raises(self, httpx_mock: HTTPXMock) -> None:
        for _ in range(3):
            httpx_mock.add_exception(httpx.ConnectError("nope"))
        transport = _transport(
            httpx_mock,
            retry=RetryOptions(max_attempts=3, base_ms=1, timeout=1.0),
        )
        with pytest.raises(TransportError):
            transport.send([_event()])


# ── Breaker integration ───────────────────────────────────────────────────


class TestBreakerIntegration:
    def test_repeated_failures_trip_breaker(self, httpx_mock: HTTPXMock) -> None:
        # Construct a transport with a low-threshold breaker; saturate
        # it; verify the next call short-circuits.
        from withboundary.sdk import BreakerOptions, BreakerState

        transport = SyncIngestTransport(
            endpoint=ENDPOINT,
            api_key="k",
            retry=RetryOptions(max_attempts=1, base_ms=1, timeout=1.0),
            breaker=BreakerOptions(threshold=2, cooldown=60.0),
        )
        for _ in range(2):
            httpx_mock.add_response(url=INGEST_URL, status_code=500)
        # First call exhausts retries on a 500 (1 attempt only) — one
        # breaker failure recorded.
        with pytest.raises(TransportError):
            transport.send([_event()])
        # Second call exhausts retries on the second 500 — breaker trips.
        with pytest.raises(TransportError):
            transport.send([_event()])
        assert transport.breaker.state is BreakerState.OPEN

        # Third call should now be short-circuited by the breaker
        # without any HTTP traffic.
        with pytest.raises(BreakerOpenError):
            transport.send([_event()])


# ── Custom client injection ───────────────────────────────────────────────


class TestCustomClient:
    def test_user_supplied_client_used(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        custom_client = httpx.Client(timeout=5.0)
        try:
            transport = SyncIngestTransport(endpoint=ENDPOINT, api_key="k", client=custom_client)
            transport.send([_event()])
            assert len(httpx_mock.get_requests()) == 1
        finally:
            custom_client.close()

    def test_close_no_op_when_client_supplied(self) -> None:
        # No httpx_mock fixture — we never send; we're only verifying
        # that transport.close() leaves a user-supplied client alone.
        custom_client = httpx.Client()
        transport = SyncIngestTransport(endpoint=ENDPOINT, api_key="k", client=custom_client)
        transport.close()
        # User-owned client is still usable after transport.close().
        assert not custom_client.is_closed
        custom_client.close()


# ── Endpoint trailing slash normalisation ─────────────────────────────────


class TestEndpointNormalization:
    def test_trailing_slash_stripped(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(url=INGEST_URL, status_code=200)
        transport = SyncIngestTransport(endpoint=ENDPOINT + "/", api_key="k")
        transport.send([_event()])
        request = httpx_mock.get_request()
        # Resolved URL should not have a double slash.
        assert request is not None
        assert str(request.url) == INGEST_URL


# pytest-httpx asserts that every registered response was consumed at the
# end of the test. The retry-budget tests register exactly the number
# of responses they expect to consume; no autouse cleanup needed.
