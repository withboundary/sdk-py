"""Circuit-breaker state machine."""

from __future__ import annotations

import time

import pytest

from withboundary.sdk import BreakerOpenError, BreakerState, CircuitBreaker


class TestConstruction:
    def test_invalid_threshold(self) -> None:
        with pytest.raises(ValueError):
            CircuitBreaker(threshold=0)
        with pytest.raises(ValueError):
            CircuitBreaker(threshold=-1)

    def test_invalid_cooldown(self) -> None:
        with pytest.raises(ValueError):
            CircuitBreaker(cooldown=-1)

    def test_initial_state_closed(self) -> None:
        breaker = CircuitBreaker()
        assert breaker.state is BreakerState.CLOSED
        assert breaker.failure_count == 0


class TestClosedState:
    def test_calls_pass_through(self) -> None:
        breaker = CircuitBreaker(threshold=3)
        breaker.before_call()
        breaker.before_call()  # Should not raise.

    def test_success_resets_failure_count(self) -> None:
        breaker = CircuitBreaker(threshold=5)
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.failure_count == 2
        breaker.record_success()
        assert breaker.failure_count == 0


class TestTrip:
    def test_consecutive_failures_open_breaker(self) -> None:
        breaker = CircuitBreaker(threshold=3, cooldown=10.0)
        breaker.record_failure()
        breaker.record_failure()
        # Still closed — only 2 of 3.
        assert breaker.state is BreakerState.CLOSED
        breaker.record_failure()
        assert breaker.state is BreakerState.OPEN

    def test_open_breaker_raises_on_before_call(self) -> None:
        breaker = CircuitBreaker(threshold=1, cooldown=10.0)
        breaker.record_failure()
        with pytest.raises(BreakerOpenError):
            breaker.before_call()


class TestCooldown:
    def test_cooldown_elapsed_moves_to_half_open(self) -> None:
        breaker = CircuitBreaker(threshold=1, cooldown=0.05)
        breaker.record_failure()
        assert breaker.state is BreakerState.OPEN
        time.sleep(0.06)
        # before_call admits the probe and transitions HALF_OPEN.
        breaker.before_call()
        assert breaker.state is BreakerState.HALF_OPEN

    def test_remaining_cooldown_still_raises(self) -> None:
        breaker = CircuitBreaker(threshold=1, cooldown=10.0)
        breaker.record_failure()
        with pytest.raises(BreakerOpenError) as exc_info:
            breaker.before_call()
        # Useful diagnostic content in the message.
        assert "cooldown" in str(exc_info.value)


class TestHalfOpen:
    def test_success_closes_breaker(self) -> None:
        breaker = CircuitBreaker(threshold=1, cooldown=0.0)
        breaker.record_failure()
        breaker.before_call()  # transition to half-open
        breaker.record_success()
        assert breaker.state is BreakerState.CLOSED
        assert breaker.failure_count == 0

    def test_failure_reopens_breaker(self) -> None:
        breaker = CircuitBreaker(threshold=3, cooldown=0.0)
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_failure()
        breaker.before_call()  # transition to half-open
        breaker.record_failure()
        assert breaker.state is BreakerState.OPEN
        # Counter pinned at threshold so the breaker stays tripped
        # without needing repeated failures to keep it there.
        assert breaker.failure_count == 3
