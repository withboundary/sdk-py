"""Backoff math + Retry-After parsing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from withboundary.sdk.transport.retry import (
    DEFAULT_BASE_MS,
    DEFAULT_JITTER,
    MAX_RETRY_AFTER_SECONDS,
    compute_backoff_ms,
    parse_retry_after,
)

# ── compute_backoff_ms ───────────────────────────────────────────────────


class TestComputeBackoff:
    def test_attempt_one_is_zero(self) -> None:
        # No prior failure to back off from.
        assert compute_backoff_ms(1) == 0

    def test_attempt_zero_is_zero(self) -> None:
        assert compute_backoff_ms(0) == 0

    def test_negative_attempt_is_zero(self) -> None:
        assert compute_backoff_ms(-5) == 0

    def test_default_schedule_lower_bounds(self) -> None:
        """The base delay (jitter floor) for each retry attempt should
        match the documented schedule. Jitter only adds time, never
        subtracts."""
        for attempt, expected_base in [(2, 100), (3, 400), (4, 1600), (5, 6400)]:
            base = compute_backoff_ms(attempt, jitter=0.0)
            assert base == expected_base

    def test_jitter_adds_to_base(self) -> None:
        # Run many trials so we hit close to the jitter upper bound.
        observations = [compute_backoff_ms(2, jitter=0.5) for _ in range(200)]
        # Lower bound: at least one trial at the floor (jitter sample
        # = 0). Upper bound: never above base * 1.5.
        assert min(observations) >= 100
        assert max(observations) <= 150

    def test_custom_base_ms(self) -> None:
        # base 500, attempt 2 → 500ms (+ jitter), attempt 3 → 2000ms
        assert compute_backoff_ms(2, base_ms=500, jitter=0.0) == 500
        assert compute_backoff_ms(3, base_ms=500, jitter=0.0) == 2000

    def test_negative_base_returns_zero(self) -> None:
        # Defensive — negative base_ms shouldn't crash; treat as a
        # disabled backoff.
        assert compute_backoff_ms(2, base_ms=-100) == 0

    def test_negative_jitter_clamped_to_zero(self) -> None:
        # Negative jitter would otherwise produce a delay below the
        # floor; clamp to non-negative.
        assert compute_backoff_ms(2, base_ms=100, jitter=-0.5) == 100

    def test_default_constants_documented(self) -> None:
        assert DEFAULT_BASE_MS == 100
        assert DEFAULT_JITTER == 0.5


# ── parse_retry_after ────────────────────────────────────────────────────


class TestParseRetryAfter:
    def test_none_returns_none(self) -> None:
        assert parse_retry_after(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_retry_after("") is None
        assert parse_retry_after("   ") is None

    def test_integer_seconds(self) -> None:
        assert parse_retry_after("30") == 30.0

    def test_integer_seconds_capped(self) -> None:
        # Server says 600 seconds; we cap at 60.
        assert parse_retry_after("600") == MAX_RETRY_AFTER_SECONDS

    def test_http_date_in_future(self) -> None:
        future = datetime.now(timezone.utc) + timedelta(seconds=10)
        header = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = parse_retry_after(header)
        assert result is not None
        # Allow ±2s wall-clock slop.
        assert 7 < result <= 12

    def test_http_date_in_past_returns_zero(self) -> None:
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        header = past.strftime("%a, %d %b %Y %H:%M:%S GMT")
        assert parse_retry_after(header) == 0.0

    def test_http_date_capped(self) -> None:
        # Far in the future; cap at 60s.
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        header = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
        result = parse_retry_after(header)
        assert result == MAX_RETRY_AFTER_SECONDS

    def test_unparseable_returns_none(self) -> None:
        assert parse_retry_after("not-a-date") is None
        assert parse_retry_after("Sat, 99 May 2026") is None

    @pytest.mark.parametrize("value", ["0", "1", "60", "120"])
    def test_integer_alpha_handling(self, value: str) -> None:
        result = parse_retry_after(value)
        assert result is not None
        assert result <= MAX_RETRY_AFTER_SECONDS
