"""Synchronous ``ContractLogger`` implementation.

Implements every hook in :class:`withboundary.contract.logger.ContractLogger`
and threads events through the SDK's data-shaping pipeline:

    hook context  →  EventBuilder  →  capture  →  redact  →  SyncBatcher

The logger owns a :class:`PerRunRegistry` that tracks the live runs by
their ``run_handle``. Each run lands in ``on_run_start``, gets per-
attempt state stashed in ``on_attempt_start`` (instructions for the
``input`` field, attempt-start timestamp for duration math), and is
freed in ``on_run_success`` / ``on_run_failure``.

Hook ordering produced by the contract runner is:

    on_run_start
      on_attempt_start
      on_cleaned_output
      on_verify_failure → on_repair_generated → on_retry_scheduled  (per failed attempt)
      OR
      on_verify_success
    on_run_success | on_run_failure

The SDK doesn't emit a wire event on every hook — only on the ones
that produce a complete picture: ``on_verify_failure`` (mid-run
``FailedEvent`` with ``final=False``), ``on_run_success`` (terminal
``AcceptedEvent``), and ``on_run_failure`` (terminal ``FailedEvent``
with ``final=True``). The intermediate hooks update per-run state so
those events have the right input/output/repairs slot populated.
"""

from __future__ import annotations

import time
from typing import Any

from withboundary.contract.logger import (
    AttemptStartCtx,
    CleanedOutputCtx,
    RawOutputCtx,
    RepairGeneratedCtx,
    RetryScheduledCtx,
    RunFailureCtx,
    RunStartCtx,
    RunSuccessCtx,
    VerifyFailureCtx,
    VerifySuccessCtx,
)

from .._meta import __version__
from ..batcher.sync import SyncBatcher
from ..capture import apply_capture
from ..config import CapturePolicy, RedactionOptions, resolve_capture, resolve_redact
from ..events import BoundaryEvent, EventBuilder, rule_failure_names
from ..identifiers import mint_run_id
from ..redact import apply_redaction
from ..runs import PerRunRegistry, PerRunState


class SyncBoundaryLogger:
    """The ``ContractLogger`` returned by :func:`create_boundary_logger`.

    Owns the event builder, the per-run state registry, the batcher,
    and the user-configured capture + redaction policies. Hooks
    mutate per-run state in place; only terminal hooks (or mid-run
    failures) materialise wire events and push them to the batcher.
    """

    def __init__(
        self,
        *,
        batcher: SyncBatcher,
        builder: EventBuilder | None = None,
        capture: CapturePolicy | None = None,
        redact: RedactionOptions | None = None,
        environment: str | None = None,
        default_model: str | None = None,
    ) -> None:
        self._batcher = batcher
        self._builder = builder or EventBuilder(
            sdk_version=__version__,
            environment=environment,
            default_model=default_model,
        )
        self._capture = resolve_capture(capture)
        self._redact = resolve_redact(redact)
        self._runs = PerRunRegistry()

    # ── ContractLogger hooks ────────────────────────────────────────────

    def on_run_start(self, ctx: RunStartCtx) -> None:
        """Allocate per-run state. Runs come in with schema and rules
        attached the first time we see them per process per contract;
        we stash both for the first event we emit and drop them
        thereafter."""
        state = PerRunState.from_run_start(
            contract_name=ctx.contract_name,
            run_handle=ctx.run_handle,
            run_id=mint_run_id(),
            started_at=time.time(),
            max_attempts=ctx.max_attempts,
            model=ctx.model,
            schema=ctx.schema,
            rules=ctx.rules,
        )
        self._runs.register(state)

    def on_attempt_start(self, ctx: AttemptStartCtx) -> None:
        """Stash the per-attempt instructions and start time. The
        instructions become the ``input`` field on the next event the
        SDK emits for this run; the start time drives ``durationMs``
        on the mid-run failure event."""
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        state.instructions = ctx.instructions
        state.last_attempt_started_at = time.time()
        # Per-attempt repairs are accumulated on ``on_repair_generated``
        # *before* the next attempt; reset the slot at attempt start so
        # the wire event reflects the actual prompt sent this round.
        state.repairs = list(ctx.repairs)

    def on_raw_output(self, ctx: RawOutputCtx) -> None:
        """No-op for the wire — the cleaned output is what the
        dashboard surfaces. Kept on the interface so a subclass could
        plug in raw-bytes capture if needed."""

    def on_cleaned_output(self, ctx: CleanedOutputCtx) -> None:
        """Cache the cleaned value temporarily on the per-run state so
        a subsequent verify-failure can attach it as the rejected
        payload. ``on_verify_success`` doesn't need it — the validated
        ``data`` on the success ctx is the canonical output."""
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        # Stash on a dynamic attribute; PerRunState is a regular
        # dataclass so we can attach this without growing the schema.
        state._cleaned_output = ctx.cleaned  # type: ignore[attr-defined]

    def on_verify_success(self, ctx: VerifySuccessCtx[Any]) -> None:
        """No wire event yet — the run-success hook will emit the
        terminal AcceptedEvent. The verify-success ctx is informational
        only at this layer."""

    def on_verify_failure(self, ctx: VerifyFailureCtx) -> None:
        """Build and ship a mid-run ``FailedEvent`` (``final=False``).

        Uses the cleaned output stashed on ``on_cleaned_output`` if
        present so the dashboard can see what the model returned even
        though it failed validation.
        """
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        cleaned = getattr(state, "_cleaned_output", None)
        event = self._builder.attempt_failure(
            run=state,
            attempt=ctx.attempt,
            category=ctx.category,
            issues=list(ctx.issues),
            duration_ms=ctx.duration_ms,
            rule_failures=rule_failure_names(ctx.rule_issues),
            cleaned_output=cleaned,
        )
        self._push(event)

    def on_repair_generated(self, ctx: RepairGeneratedCtx) -> None:
        """Accumulate the repair body the engine generated for the
        next attempt. Threaded onto ``PerRunState.repairs`` so the
        next event captures the prompt the model will see."""
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        state.repairs.append({"role": "user", "content": ctx.repair_message})

    def on_retry_scheduled(self, ctx: RetryScheduledCtx) -> None:
        """Informational — no wire event. The mid-run failure event
        already encodes that a retry is coming via ``final=False``."""

    def on_run_success(self, ctx: RunSuccessCtx[Any]) -> None:
        state = self._runs.pop(ctx.run_handle)
        if state is None:
            return
        event = self._builder.run_success(
            run=state,
            attempts=ctx.attempts,
            total_duration_ms=ctx.total_duration_ms,
            data=ctx.data,
        )
        self._push(event)

    def on_run_failure(self, ctx: RunFailureCtx) -> None:
        state = self._runs.pop(ctx.run_handle)
        if state is None:
            return
        event = self._builder.run_failure(
            run=state,
            attempts=ctx.attempts,
            total_duration_ms=ctx.total_duration_ms,
            category=ctx.category or "RUN_ERROR",
            message=ctx.message,
        )
        self._push(event)

    # ── Public surface beyond ContractLogger ─────────────────────────────

    def flush(self, timeout: float | None = None) -> None:
        """Drain queued events. ``timeout`` bounds the wait — callers
        running in serverless / lambda contexts should always pass
        one."""
        self._batcher.flush(timeout)

    def shutdown(self, timeout: float | None = None) -> None:
        """Flush within ``timeout``, stop the background drain, mark
        the logger disabled. Idempotent."""
        self._batcher.shutdown(timeout)

    # Read-only inspection (used by tests + the lifecycle handler).
    @property
    def disabled(self) -> bool:
        return self._batcher.disabled

    # ── Internal pipeline ────────────────────────────────────────────────

    def _push(self, event: BoundaryEvent) -> None:
        """Run capture + redact, then push to the batcher.

        Order matters: redact first so the scrubbed-fields list is
        accurate; capture second so the resolved policy snapshot
        always reflects the final state of the wire payload."""
        redacted, scrubbed = apply_redaction(event, self._redact)
        gated = apply_capture(redacted, self._capture, redacted_fields=scrubbed)
        self._batcher.push(gated)


__all__ = [
    "SyncBoundaryLogger",
]
