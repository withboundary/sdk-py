"""Per-run state the SDK keeps between contract hooks.

Contract-py emits ten lifecycle hooks per run; the SDK needs to thread
state across them — which run id corresponds to which run handle, when
did the attempt start, what instructions did the user prompt with, what
repair messages did the engine generate. :class:`PerRunState` collects
all of that.

A :class:`PerRunRegistry` indexes the live runs by the contract's
``run_handle`` (the per-call unique id contract-py mints inside its
runner). Both sync and async loggers share the same registry shape; the
sync variant guards it with ``threading.Lock``, the async variant with
``asyncio.Lock``.

Schema and rules are forwarded once per run — the contract engine only
populates ``RunStartCtx.schema`` / ``RunStartCtx.rules`` on the first
run per process per contract. The SDK stashes them on the per-run state
and consumes them on the first event sent for that run; subsequent
events leave the fields ``None`` so the wire payload stays small.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from withboundary.contract.types import Message

if TYPE_CHECKING:
    from withboundary.contract.types import RuleDefinition, SchemaField


@dataclass
class PerRunState:
    """Mutable state for a single ``accept()`` call.

    Allocated in ``on_run_start`` and freed when the run terminates
    (``on_run_success`` or ``on_run_failure``). Mutated under the
    registry's lock by the lifecycle hooks; never accessed outside the
    registry's serialised access paths.
    """

    contract_name: str
    run_handle: str
    run_id: str
    started_at: float
    max_attempts: int
    model: str | None
    _schema: list[SchemaField] | None = None
    _rules: list[RuleDefinition] | None = None
    instructions: str = ""
    repairs: list[Message] = field(default_factory=list)
    last_attempt_started_at: float = 0.0

    def consume_schema(self) -> list[SchemaField] | None:
        """Return the stashed schema once, then clear it. The wire
        treats schema as upsert with first-non-null-wins, so re-sending
        is harmless but wasteful — stop after the first event.
        """
        out = self._schema
        self._schema = None
        return out

    def consume_rules(self) -> list[RuleDefinition] | None:
        """Same single-shot semantics as :meth:`consume_schema` — the
        backend upserts rules per contract, so we only need to send the
        definitions on the first event of the first run per process."""
        out = self._rules
        self._rules = None
        return out

    @classmethod
    def from_run_start(
        cls,
        *,
        contract_name: str,
        run_handle: str,
        run_id: str,
        started_at: float,
        max_attempts: int,
        model: str | None,
        schema: list[SchemaField] | None,
        rules: list[RuleDefinition] | None,
    ) -> PerRunState:
        """Allocate from a contract ``RunStartCtx``. The schema and
        rules arguments are stored on the private ``_schema`` /
        ``_rules`` slots so they get consumed exactly once via
        :meth:`consume_schema` / :meth:`consume_rules`."""
        return cls(
            contract_name=contract_name,
            run_handle=run_handle,
            run_id=run_id,
            started_at=started_at,
            max_attempts=max_attempts,
            model=model,
            _schema=schema,
            _rules=rules,
        )


# ── Registry ──────────────────────────────────────────────────────────────


class PerRunRegistry:
    """Thread-safe map from ``run_handle`` → :class:`PerRunState`.

    The contract engine threads ``run_handle`` through every hook
    context, so the SDK looks up state by that key on every hook
    invocation. Concurrent ``accept()`` calls on the same contract get
    isolated state under their own handles.

    Used by the sync logger (with ``threading.Lock``); the async logger
    uses :class:`AsyncPerRunRegistry` below for ``asyncio.Lock``-guarded
    access from the event loop.
    """

    def __init__(self) -> None:
        self._runs: dict[str, PerRunState] = {}
        self._lock = threading.Lock()

    def register(self, state: PerRunState) -> None:
        with self._lock:
            self._runs[state.run_handle] = state

    def get(self, run_handle: str) -> PerRunState | None:
        with self._lock:
            return self._runs.get(run_handle)

    def pop(self, run_handle: str) -> PerRunState | None:
        """Remove and return the run's state. Called when the run
        terminates so memory doesn't leak as runs accumulate."""
        with self._lock:
            return self._runs.pop(run_handle, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._runs)

    def __contains__(self, run_handle: object) -> bool:
        with self._lock:
            return run_handle in self._runs


__all__ = [
    "PerRunRegistry",
    "PerRunState",
]
