"""Configuration dataclasses + resolver helpers."""

from __future__ import annotations

import re
from dataclasses import FrozenInstanceError

import pytest

from withboundary.sdk.config import (
    REDACT,
    BatchOptions,
    BreakerOptions,
    CapturePolicy,
    RedactionOptions,
    RetryOptions,
    make_redaction,
    merged_capture,
    resolve_batch,
    resolve_breaker,
    resolve_capture,
    resolve_redact,
    resolve_retry,
)

# ── Defaults ───────────────────────────────────────────────────────────────


class TestBatchDefaults:
    def test_size(self) -> None:
        assert BatchOptions().size == 20

    def test_interval(self) -> None:
        assert BatchOptions().interval == 5.0

    def test_max_queue_size(self) -> None:
        assert BatchOptions().max_queue_size == 1000


class TestCaptureDefaults:
    def test_inputs_default_off(self) -> None:
        # Privacy default — prompts don't ride along unless opted in.
        assert CapturePolicy().inputs is False

    def test_outputs_default_off(self) -> None:
        assert CapturePolicy().outputs is False

    def test_repairs_default_on(self) -> None:
        assert CapturePolicy().repairs is True


class TestRetryDefaults:
    def test_max_attempts(self) -> None:
        assert RetryOptions().max_attempts == 3

    def test_base_ms(self) -> None:
        assert RetryOptions().base_ms == 100

    def test_timeout(self) -> None:
        assert RetryOptions().timeout == 10.0


class TestBreakerDefaults:
    def test_threshold(self) -> None:
        assert BreakerOptions().threshold == 5

    def test_cooldown(self) -> None:
        assert BreakerOptions().cooldown == 30.0


class TestRedactionDefaults:
    def test_empty(self) -> None:
        opts = RedactionOptions()
        assert opts.fields == ()
        assert opts.patterns == ()
        assert opts.custom is None


# ── Frozen-ness ────────────────────────────────────────────────────────────


class TestFrozen:
    def test_batch_options_frozen(self) -> None:
        opts = BatchOptions()
        with pytest.raises(FrozenInstanceError):
            opts.size = 99  # type: ignore[misc]

    def test_capture_policy_frozen(self) -> None:
        opts = CapturePolicy()
        with pytest.raises(FrozenInstanceError):
            opts.inputs = True  # type: ignore[misc]

    def test_redaction_options_frozen(self) -> None:
        opts = RedactionOptions()
        with pytest.raises(FrozenInstanceError):
            opts.fields = ("x",)  # type: ignore[misc]


# ── Resolvers ──────────────────────────────────────────────────────────────


class TestResolvers:
    def test_batch_none_returns_default(self) -> None:
        assert resolve_batch(None) == BatchOptions()

    def test_batch_passthrough(self) -> None:
        custom = BatchOptions(size=50)
        assert resolve_batch(custom) is custom

    def test_capture_none_returns_default(self) -> None:
        assert resolve_capture(None) == CapturePolicy()

    def test_capture_passthrough(self) -> None:
        custom = CapturePolicy(inputs=True)
        assert resolve_capture(custom) is custom

    def test_redact_none_returns_default(self) -> None:
        assert resolve_redact(None) == RedactionOptions()

    def test_retry_none_returns_default(self) -> None:
        assert resolve_retry(None) == RetryOptions()

    def test_breaker_none_returns_default(self) -> None:
        assert resolve_breaker(None) == BreakerOptions()


# ── make_redaction ─────────────────────────────────────────────────────────


class TestMakeRedaction:
    def test_empty_inputs(self) -> None:
        opts = make_redaction()
        assert opts.fields == ()
        assert opts.patterns == ()
        assert opts.custom is None

    def test_fields_are_tupled(self) -> None:
        opts = make_redaction(fields=["ssn", "email"])
        assert opts.fields == ("ssn", "email")

    def test_string_patterns_are_compiled(self) -> None:
        opts = make_redaction(patterns=[r"\d{3}-\d{2}-\d{4}"])
        assert len(opts.patterns) == 1
        assert isinstance(opts.patterns[0], re.Pattern)
        assert opts.patterns[0].search("foo 123-45-6789 bar") is not None

    def test_compiled_patterns_passed_through(self) -> None:
        compiled = re.compile(r"hello")
        opts = make_redaction(patterns=[compiled])
        assert opts.patterns[0] is compiled

    def test_mixed_pattern_types(self) -> None:
        compiled = re.compile(r"hello")
        opts = make_redaction(patterns=[compiled, r"\d+"])
        assert opts.patterns[0] is compiled
        assert isinstance(opts.patterns[1], re.Pattern)

    def test_custom_callable_preserved(self) -> None:
        def my_redactor(value: object, _path: tuple[str, ...]) -> object:
            return value

        opts = make_redaction(custom=my_redactor)
        assert opts.custom is my_redactor


# ── merged_capture ─────────────────────────────────────────────────────────


class TestMergedCapture:
    def test_none_overrides_returns_base(self) -> None:
        base = CapturePolicy(inputs=True, outputs=False, repairs=True)
        assert merged_capture(base, None) is base

    def test_override_replaces_base(self) -> None:
        base = CapturePolicy(inputs=False, outputs=False, repairs=True)
        override = CapturePolicy(inputs=True, outputs=True, repairs=False)
        result = merged_capture(base, override)
        assert result.inputs is True
        assert result.outputs is True
        assert result.repairs is False


# ── REDACT sentinel ────────────────────────────────────────────────────────


class TestRedactSentinel:
    def test_repr(self) -> None:
        # The sentinel ends up in tracebacks if a redactor accidentally
        # returns it where a real value is expected — make sure it's
        # immediately recognisable.
        assert repr(REDACT) == "<REDACT>"

    def test_singleton_identity(self) -> None:
        # Identity comparison is the contract — the redactor checks
        # `value is REDACT` rather than `==`.
        from withboundary.sdk.config import REDACT as also_redact

        assert also_redact is REDACT
