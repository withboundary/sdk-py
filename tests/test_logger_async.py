"""End-to-end AsyncBoundaryLogger driven by contract-py's async runner."""

from __future__ import annotations

import asyncio
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
    AsyncBatcher,
    AsyncBoundaryLogger,
    BatchOptions,
    BoundaryEvent,
    CapturePolicy,
    FailedEvent,
    RedactionOptions,
    make_redaction,
)


class Lead(BaseModel):
    score: int = Field(ge=0, le=100)
    tier: str


class RecordingAsyncTransport:
    """Captures every send so assertions can inspect the wire payloads."""

    def __init__(self) -> None:
        self.batches: list[list[BoundaryEvent]] = []
        self.lock = asyncio.Lock()

    @property
    def events(self) -> list[BoundaryEvent]:
        flat: list[BoundaryEvent] = []
        for batch in self.batches:
            flat.extend(batch)
        return flat

    async def send(self, events: list[BoundaryEvent]) -> None:
        async with self.lock:
            self.batches.append(list(events))

    async def close(self) -> None:
        return None


def _make_logger(
    transport: RecordingAsyncTransport,
    *,
    capture: CapturePolicy | None = None,
    redact: RedactionOptions | None = None,
    environment: str | None = None,
) -> AsyncBoundaryLogger:
    batcher = AsyncBatcher(
        transport=transport,  # type: ignore[arg-type]
        options=BatchOptions(size=1, interval=60.0, max_queue_size=100),
    )
    return AsyncBoundaryLogger(
        batcher=batcher,
        capture=capture,
        redact=redact,
        environment=environment,
    )


async def _wait_for(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


# ── Happy path: single attempt succeeds ──────────────────────────────────


class TestSuccessPath:
    async def test_emits_terminal_accepted_event(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="lead-scoring", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 88, "tier": "hot"}'

        try:
            result = await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        assert isinstance(result, Success)
        assert await _wait_for(lambda: len(transport.events) >= 1)
        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert len(accepted) == 1
        assert accepted[0].contract_name == "lead-scoring"
        assert accepted[0].final is True
        assert accepted[0].attempt == 1

    async def test_attaches_run_id_to_every_event(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        run_ids = {e.run_id for e in transport.events}
        assert len(run_ids) == 1
        assert next(iter(run_ids)).startswith("bnd_run_")


# ── Retry path: failure → success ────────────────────────────────────────


class TestRetrySuccess:
    async def test_emits_mid_run_failure_then_accepted(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(ctx: ContractAttempt) -> str:
            if ctx.attempt == 1:
                return "not json"
            return '{"score": 50, "tier": "warm"}'

        try:
            result = await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        assert isinstance(result, Success)
        assert await _wait_for(lambda: len(transport.events) >= 2)
        events = sorted(transport.events, key=lambda e: e.attempt)
        first = events[0]
        assert isinstance(first, FailedEvent)
        assert first.final is False
        assert first.attempt == 1
        second = events[1]
        assert isinstance(second, AcceptedEvent)
        assert second.attempt == 2


# ── Terminal failure: max attempts exhausted ────────────────────────────


class TestTerminalFailure:
    async def test_emits_terminal_failed_event(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(
            name="t",
            schema=Lead,
            logger=logger,
            retry=RetryOptions(max_attempts=2),
        )

        async def run(_ctx: ContractAttempt) -> str:
            return "garbage"

        try:
            result = await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        assert isinstance(result, Failure)
        assert await _wait_for(lambda: len(transport.events) >= 3)
        failed = [e for e in transport.events if isinstance(e, FailedEvent)]
        terminals = [e for e in failed if e.final]
        mid_runs = [e for e in failed if not e.final]
        assert len(terminals) == 1
        assert len(mid_runs) == 2


# ── Capture policy ──────────────────────────────────────────────────────


class TestCaptureGates:
    async def test_inputs_off_strips_prompt_from_event(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=True),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        for event in transport.events:
            assert event.input is None

    async def test_outputs_on_includes_validated_data(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=True),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 99, "tier": "hot"}'

        try:
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert accepted
        assert accepted[0].output == {"score": 99, "tier": "hot"}


# ── Redaction wired through ─────────────────────────────────────────────


class TestRedaction:
    async def test_field_redaction_applied(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(
            transport,
            capture=CapturePolicy(inputs=False, outputs=True, repairs=False),
            redact=make_redaction(fields=["tier"]),
        )
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 50, "tier": "hot"}'

        try:
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        accepted = [e for e in transport.events if isinstance(e, AcceptedEvent)]
        assert accepted
        assert accepted[0].output == {"score": 50, "tier": "[REDACTED]"}
        assert accepted[0].capture is not None
        assert accepted[0].capture.redacted_fields is not None
        assert "output.tier" in accepted[0].capture.redacted_fields


# ── Environment label ──────────────────────────────────────────────────


class TestEnvironmentLabel:
    async def test_environment_stamped_on_every_event(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport, environment="production")
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        for event in transport.events:
            assert event.environment == "production"


# ── flush ──────────────────────────────────────────────────────────────


class TestFlush:
    async def test_flush_drains_queued_events(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="t", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            await contract.aaccept(run)
            await logger.flush(timeout=1.0)
        finally:
            await logger.shutdown(timeout=1.0)

        assert len(transport.events) >= 1


# ── Schema emission semantics ──────────────────────────────────────────


class TestSchemaEmission:
    async def test_schema_attached_to_first_event_only(self) -> None:
        from withboundary.contract.runner import _emitted_describe

        _emitted_describe.clear()

        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        contract = define_contract(name="async-schema-emit-test", schema=Lead, logger=logger)

        async def run(_ctx: ContractAttempt) -> str:
            return '{"score": 1, "tier": "cold"}'

        try:
            await contract.aaccept(run)
            await contract.aaccept(run)
            await contract.aaccept(run)
        finally:
            await logger.shutdown(timeout=1.0)

        events = sorted(transport.events, key=lambda e: e.timestamp)
        with_schema = [e for e in events if e.schema_ is not None]
        assert len(with_schema) == 1
        assert with_schema[0].schema_ is not None
        assert len(with_schema[0].schema_) >= 1


# ── Hook safety ────────────────────────────────────────────────────────


class TestHookSafety:
    async def test_unknown_run_handle_does_not_crash(self) -> None:
        transport = RecordingAsyncTransport()
        logger = _make_logger(transport)
        from withboundary.contract.logger import (
            AttemptStartCtx,
            VerifyFailureCtx,
        )

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
        await logger.shutdown(timeout=1.0)
        assert transport.events == []
