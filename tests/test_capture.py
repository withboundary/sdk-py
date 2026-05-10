"""Capture-policy enforcement on outbound events."""

from __future__ import annotations

from typing import Any

from withboundary.sdk import (
    AcceptedEvent,
    CapturePolicy,
    FailedEvent,
)
from withboundary.sdk.capture import apply_capture, gates_match


def _accepted(**overrides: Any) -> AcceptedEvent:
    base: dict[str, Any] = {
        "contract_name": "x",
        "timestamp": "2026-05-10T00:00:00+00:00",
        "attempt": 1,
        "max_attempts": 1,
        "duration_ms": 0,
        "run_id": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        "input": {"prompt": "hi"},
        "output": {"score": 9},
    }
    base.update(overrides)
    return AcceptedEvent(**base)


def _failed(**overrides: Any) -> FailedEvent:
    base: dict[str, Any] = {
        "contract_name": "x",
        "timestamp": "2026-05-10T00:00:00+00:00",
        "attempt": 1,
        "max_attempts": 1,
        "duration_ms": 0,
        "run_id": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        "final": False,
        "category": "VALIDATION_ERROR",
        "issues": ["bad"],
        "input": {"prompt": "hi"},
        "output": {"score": 9},
        "repairs": [{"role": "user", "content": "fix"}],
    }
    base.update(overrides)
    return FailedEvent(**base)


# ── Gates ────────────────────────────────────────────────────────────────


class TestInputsGate:
    def test_inputs_off_drops_input(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=False, outputs=True, repairs=True))
        assert result.input is None

    def test_inputs_on_preserves_input(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=True))
        assert result.input == {"prompt": "hi"}


class TestOutputsGate:
    def test_outputs_off_drops_output(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=False, repairs=True))
        assert result.output is None

    def test_outputs_on_preserves_output(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=True))
        assert result.output == {"score": 9}


class TestRepairsGate:
    def test_repairs_off_drops_repairs_on_failed_event(self) -> None:
        event = _failed()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=False))
        assert isinstance(result, FailedEvent)
        assert result.repairs is None

    def test_repairs_on_preserves_repairs(self) -> None:
        event = _failed()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=True))
        assert isinstance(result, FailedEvent)
        assert result.repairs == [{"role": "user", "content": "fix"}]

    def test_repairs_gate_no_op_on_accepted_event(self) -> None:
        # AcceptedEvent has no repairs slot — the gate must not crash
        # when applied to one.
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=False))
        assert isinstance(result, AcceptedEvent)


# ── Snapshot stamping ─────────────────────────────────────────────────────


class TestResolvedCaptureStamp:
    def test_snapshot_reflects_policy(self) -> None:
        event = _accepted()
        policy = CapturePolicy(inputs=True, outputs=False, repairs=True)
        result = apply_capture(event, policy)
        assert result.capture is not None
        assert result.capture.inputs is True
        assert result.capture.outputs is False
        assert result.capture.repairs is True
        assert gates_match(result.capture, policy)

    def test_redacted_fields_threaded_through(self) -> None:
        event = _accepted()
        result = apply_capture(
            event,
            CapturePolicy(inputs=True, outputs=True, repairs=True),
            redacted_fields=["input.user.ssn", "output.email"],
        )
        assert result.capture is not None
        assert result.capture.redacted_fields == ["input.user.ssn", "output.email"]

    def test_no_redacted_fields_yields_none(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=True, outputs=True, repairs=True))
        assert result.capture is not None
        assert result.capture.redacted_fields is None


# ── Immutability ─────────────────────────────────────────────────────────


class TestImmutability:
    def test_input_event_unchanged(self) -> None:
        event = _accepted()
        before_input = event.input
        apply_capture(event, CapturePolicy(inputs=False, outputs=False, repairs=True))
        assert event.input is before_input

    def test_returns_new_event_instance(self) -> None:
        event = _accepted()
        result = apply_capture(event, CapturePolicy(inputs=False, outputs=True, repairs=True))
        assert result is not event


# ── End-to-end with default policy ────────────────────────────────────────


class TestDefaultPolicy:
    def test_defaults_drop_input_and_output_keep_repairs(self) -> None:
        event = _failed()
        result = apply_capture(event, CapturePolicy())
        assert result.input is None
        assert result.output is None
        assert isinstance(result, FailedEvent)
        assert result.repairs is not None
