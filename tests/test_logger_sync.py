"""End-to-end SyncBoundaryLogger driven by contract-py's runner."""

from __future__ import annotations

import threading
import time
from typing import Any

from pydantic import BaseModel, Field
from withboundary.contract import (
    ContractAttempt,
    Failure,
    RetryOptions,
    Success,
    define_contract,
)

from withboundary.sdk import (
    AcceptedEvent,
    BatchOptions,
    BoundaryEvent,
    CapturePolicy,
    FailedEvent,
    RedactionOptions,
    SyncBatcher,
    SyncBoundaryLogger,
    make_redaction,
)


class Lead(BaseModel):
    score: int = Field(ge=0, le=100)
    tier: str


class RecordingTransport:
    """Captures every transport call so assertions can inspect the
    final wire shapes the SDK produced."""

    def __init__(self) -> None:
        self.batches: list[list[BoundaryEvent]] = []
        self.lock = threading.Lock()

    @property
    def events(self) -> list[BoundaryEvent]:
        flat: list[BoundaryEvent] = []
        for batch in self.batches:
            flat.extend(batch)
        return flat

    def send(self, events: list[BoundaryEvent]) -> None:
        with self.lock:
            self.batches.append(list(events))

    def close(self) -> None:
        return None


def _make_logger(
    transport: RecordingTransport,
    *,
    capture: CapturePolicy | None = None,
    redact: RedactionOptions | None = None,
    environment: str | None = None,
) -> SyncBoundaryLogger:
    batcher = SyncBatcher(
        transport=transport,  # type: ignore[arg-type]
        options=BatchOptions(size=1, interval=60.0, max_queue_size=100),
    )
    return SyncBoundaryLogger(
        batcher=batcher,
        capture=capture,
        redact=redact,
        environment=environment,
    )


def _wait_for(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


# ── Happy path: single attempt succeeds ──────────────────────────────────


class TestSuccessPath:
    def test_emits_terminal_accepted_event(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="lead-scoring", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 88, "tier": "hot"}'

        try:
            result = contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        assert isinstance(result, Success)
        assert _wait_for(lambda: len(transport.events) >= 1)
        events = transport.events
        # Single accepted event for a one-attempt success.
        accepted = [e for e in events if isinstance(e, AcceptedEvent)]
        assert len(accepted) == 1
        assert accepted[0].contract_name == "lead-scoring"
        assert accepted[0].final is True
        assert accepted[0].attempt == 1

    def test_attaches_run_id_to_every_event(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        run_ids = {e.run_id for e in transport.events}
        assert len(run_ids) == 1
        assert next(iter(run_ids)).startswith("bnd_run_")


# ── Retry path: failure → success ────────────────────────────────────────


class TestRetrySuccess:
    def test_emits_mid_run_failure_then_accepted(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(ctx: ContractAttempt) -> str:
            if ctx.attempt == 1:
                return "not json"
            return '{"score": 50, "tier": "warm"}'

        try:
            result = contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        assert isinstance(result, Success)
        # Two events: a mid-run failure (final=False) then a terminal
        # success.
        assert _wait_for(lambda: len(transport.events) >= 2)
        events = sorted(transport.events, key=lambda e: e.attempt)
        # First is a failed mid-run.
        first = events[0]
        assert isinstance(first, FailedEvent)
        assert first.final is False
        assert first.attempt == 1
        # Second is the accepted terminal.
        second = events[1]
        assert isinstance(second, AcceptedEvent)
        assert second.attempt == 2

    def test_repairs_carry_into_next_attempt_event(self) -> None:
        transport = RecordingTransport()
        # Capture repairs so they're inspectable on the wire payload.
        logger = _make_logger(transport, capture=CapturePolicy(repairs=True))
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(ctx: ContractAttempt) -> str:
            if ctx.attempt == 1:
                return "garbage"
            return '{"score": 10, "tier": "cold"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        # The attempt-1 failure event has no repairs (none generated
        # yet); the accepted event for attempt 2 is shaped as an
        # AcceptedEvent which intentionally has no ``repairs`` field at
        # all (only failed events carry repair bodies).
        failed = [e for e in transport.events if isinstance(e, FailedEvent)]
        assert failed
        assert failed[0].repairs is None
        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert accepted
        assert not hasattr(accepted[0], "repairs") or getattr(accepted[0], "repairs", None) is None


# ── Terminal failure: max attempts exhausted ────────────────────────────


class TestTerminalFailure:
    def test_emits_terminal_failed_event(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(transport)
        contract = define_contract(
            name="t",
            schema=Lead,
            logger=logger,
            retry=RetryOptions(max_attempts=2),
        )

        def run(_ctx: ContractAttempt) -> str:
            return "garbage"

        try:
            result = contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        assert isinstance(result, Failure)
        # max_attempts=2 with every attempt failing: two mid-run
        # FailedEvents (one per attempt, final=False) plus one terminal
        # FailedEvent for the run summary (final=True).
        assert _wait_for(lambda: len(transport.events) >= 3)
        failed = [e for e in transport.events if isinstance(e, FailedEvent)]
        terminals = [e for e in failed if e.final]
        mid_runs = [e for e in failed if not e.final]
        assert len(terminals) == 1
        assert len(mid_runs) == 2


# ── Capture policy ──────────────────────────────────────────────────────


class TestCaptureGates:
    def test_inputs_off_strips_prompt_from_event(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=True),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        for event in transport.events:
            assert event.input is None

    def test_outputs_on_includes_validated_data(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=True),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 99, "tier": "hot"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert accepted
        assert accepted[0].output == {"score": 99, "tier": "hot"}


# ── Redaction wired through to the wire payload ─────────────────────────


class TestRedaction:
    def test_field_redaction_applied(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=False),
            redact=make_redaction(fields=["tier"]),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 50, "tier": "hot"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert accepted
        assert accepted[0].output == {"score": 50, "tier": "[REDACTED]"}
        # Resolved-capture snapshot records the scrubbed path.
        assert accepted[0].capture is not None
        assert accepted[0].capture.redacted_fields is not None
        assert "output.tier" in accepted[0].capture.redacted_fields


# ── Environment label ──────────────────────────────────────────────────


class TestEnvironmentLabel:
    def test_environment_stamped_on_every_event(self) -> None:
        transport = RecordingTransport()
        logger = _make_logger(transport, environment="production")
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        for event in transport.events:
            assert event.environment == "production"


# ── flush ──────────────────────────────────────────────────────────────


class TestFlush:
    def test_flush_drains_queued_events(self) -> None:
        transport = RecordingTransport()
        # interval=60s so the size trigger (size=1) is the only thing
        # that drains; calling flush should still empty the queue.
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            contract.accept(run)
            logger.flush(timeout=1.0)
        finally:
            logger.shutdown(timeout=1.0)

        # Flush returned before shutdown — the events must have landed.
        assert len(transport.events) >= 1


# ── Schema + rules emission semantics ──────────────────────────────────


class TestSchemaEmission:
    def test_schema_attached_to_first_event_only(self) -> None:
        # Clear the contract's once-per-process WeakSet so the test
        # gets a clean slate; without this the schema flag depends on
        # other tests' ordering.
        from withboundary.contract.runner import _emitted_describe

        _emitted_describe.clear()

        transport = RecordingTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="schema-emit-test", schema=Lead, logger=logger)

        def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            contract.accept(run)
            contract.accept(run)
            contract.accept(run)
        finally:
            logger.shutdown(timeout=1.0)

        events = sorted(transport.events, key=lambda e: e.timestamp)
        with_schema = [e for e in events if e.schema_ is not None]
        # Schema rides on exactly the first event of the first run.
        assert len(with_schema) == 1
        assert with_schema[0].schema_ is not None
        assert len(with_schema[0].schema_) >= 1


# ── Hook exception swallowing (defence-in-depth) ───────────────────────


class TestHookSafety:
    def test_unknown_run_handle_does_not_crash(self) -> None:
        # If a hook ever fires with an unknown run handle (e.g. due to
        # races between on_run_start and shutdown), the logger should
        # short-circuit silently rather than blowing up.
        transport = RecordingTransport()
        logger = _make_logger(transport)
        from withboundary.contract.logger import (
            AttemptStartCtx,
            VerifyFailureCtx,
        )

        # Fire hooks for a handle that was never registered.
        logger.on_attempt_start(
            AttemptStartCtx(contract_name="ghost", run_handle="rh_ghost", attempt=1, max_attempts=1)
        )
        logger.on_verify_failure(
            VerifyFailureCtx(
                contract_name="ghost",
                run_handle="rh_ghost",
                attempt=1,
                category="RUN_ERROR",
                issues=["x"],
                duration_ms=0,
            )
        )
        # No events emitted; no exception raised.
        assert transport.events == []
        logger.shutdown(timeout=1.0)
