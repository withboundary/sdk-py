"""Asyncio-native ``ContractLogger`` implementation.

Structural mirror of :class:`SyncBoundaryLogger`. The two loggers
expose the same hook surface and emit the same wire payloads — the
only difference is that the async sibling is backed by an
:class:`AsyncBatcher` (event-loop task, ``asyncio.Queue``,
``httpx.AsyncClient``).

Hook callbacks remain synchronous because the contract runner invokes
hooks with a plain ``callback(ctx)`` call, not ``await``. To bridge
the sync hook into the async batcher, each emitted event is shipped
via ``loop.create_task(self._batcher.push(event))``. The scheduled
tasks are tracked in a set so ``flush()`` and ``shutdown()`` can wait
on them before draining the batcher itself — otherwise a flush
called immediately after the final hook fires could observe an empty
queue and return before the push tasks have run.
"""

from __future__ import annotations

import asyncio
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
from ..batcher.async_ import AsyncBatcher
from ..capture import apply_capture
from ..config import CapturePolicy, RedactionOptions, resolve_capture, resolve_redact
from ..events import BoundaryEvent, EventBuilder, rule_failure_names
from ..identifiers import mint_run_id
from ..redact import apply_redaction
from ..runs import PerRunRegistry, PerRunState


class AsyncBoundaryLogger:
    """The ``ContractLogger`` returned by :func:`create_async_boundary_logger`.

    Same shape as :class:`SyncBoundaryLogger`; uses an :class:`AsyncBatcher`
    under the hood. The hook surface is sync (the contract runner calls
    hooks with plain function-call syntax), but the public ``flush`` and
    ``shutdown`` are coroutines that await the batcher drain.
    """

    def __init__(
        self,
        *,
        batcher: AsyncBatcher,
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
        # Tracks every ``create_task`` scheduled from a sync hook so
        # ``flush``/``shutdown`` can join them before draining the
        # batcher's own queue.
        self._pending_pushes: set[asyncio.Task[None]] = set()

    # ── ContractLogger hooks ────────────────────────────────────────────

    def on_run_start(self, ctx: RunStartCtx) -> None:
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
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        state.instructions = ctx.instructions
        state.last_attempt_started_at = time.time()
        state.repairs = list(ctx.repairs)

    def on_raw_output(self, ctx: RawOutputCtx) -> None:
        """No-op: cleaned output is the wire-facing payload."""

    def on_cleaned_output(self, ctx: CleanedOutputCtx) -> None:
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        state._cleaned_output = ctx.cleaned  # type: ignore[attr-defined]

    def on_verify_success(self, ctx: VerifySuccessCtx[Any]) -> None:
        """No wire event — the terminal AcceptedEvent rides ``on_run_success``."""

    def on_verify_failure(self, ctx: VerifyFailureCtx) -> None:
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
        state = self._runs.get(ctx.run_handle)
        if state is None:
            return
        state.repairs.append({"role": "user", "content": ctx.repair_message})

    def on_retry_scheduled(self, ctx: RetryScheduledCtx) -> None:
        """Informational — the mid-run failure event already encoded the retry."""

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

    async def flush(self, timeout: float | None = None) -> None:
        """Drain everything queued so far. Waits for any in-flight
        push tasks scheduled from sync hooks before delegating to the
        batcher's own flush so callers see consistent end-of-flush
        state.
        """
        await self._await_pending_pushes(timeout)
        await self._batcher.flush(timeout)

    async def shutdown(self, timeout: float | None = None) -> None:
        """Flush, then stop the batcher. Idempotent."""
        await self._await_pending_pushes(timeout)
        await self._batcher.shutdown(timeout)

    @property
    def disabled(self) -> bool:
        return self._batcher.disabled

    # ── Internal pipeline ────────────────────────────────────────────────

    def _push(self, event: BoundaryEvent) -> None:
        """Redact + capture + schedule onto the batcher.

        Same order as the sync sibling: redact first so the scrubbed-
        fields list is accurate, capture second so the ResolvedCapture
        snapshot reflects the final wire payload. Push is fire-and-
        forget via ``loop.create_task``; the resulting task is tracked
        so ``flush``/``shutdown`` can join it.
        """
        redacted, scrubbed = apply_redaction(event, self._redact)
        gated = apply_capture(redacted, self._capture, redacted_fields=scrubbed)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Hook fired outside an event loop — nothing we can do.
            # In practice this only happens in test paths exercising
            # the sync hook surface directly.
            return
        task = loop.create_task(self._batcher.push(gated))
        self._pending_pushes.add(task)
        task.add_done_callback(self._pending_pushes.discard)

    async def _await_pending_pushes(self, timeout: float | None) -> None:
        """Join every task created by a sync hook so the caller sees a
        coherent post-flush state."""
        if not self._pending_pushes:
            return
        pending = list(self._pending_pushes)
        if timeout is None:
            await asyncio.gather(*pending, return_exceptions=True)
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout,
            )
        except asyncio.TimeoutError:
            return


__all__ = [
    "AsyncBoundaryLogger",
]
