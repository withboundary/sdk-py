"""Wire-shape events the SDK ships to the hosted ingest endpoint.

The hosted backend at ``/v1/ingest`` accepts a discriminated union on
``ok``: ``AcceptedEvent`` (terminal success) and ``FailedEvent`` (mid-run
or terminal failure). Both share a common base of run/contract identity,
timing, and optional opt-in capture fields. This module defines those
Pydantic models and the :class:`EventBuilder` helper that translates
contract-py hook contexts into them.

Field aliases on every model use the camelCase names the validator
expects. Serialize with ``.model_dump(by_alias=True, exclude_none=True)``
when you need the wire JSON.

The ``BoundaryEvent`` union itself is discriminated by ``ok`` — Pydantic
v2 routes a parsed dict to the right branch automatically.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field
from withboundary.contract.types import (
    Message,
    RuleDefinition,
    SchemaField,
)

from ._meta import SDK_NAME, runtime
from .identifiers import mint_event_id

# ── SDK metadata stamped on every event ────────────────────────────────────


class SdkMeta(BaseModel):
    """Lightweight identification block the dashboard groups by.

    Sent on every event so server-side debugging can split traffic by
    SDK release without correlating against headers.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    runtime: str | None = None


# ── Capture/redaction trace stamped on every event ─────────────────────────


class ResolvedCapture(BaseModel):
    """The resolved capture-policy snapshot the SDK stamps on every event.

    Lets the dashboard distinguish "field absent because policy disabled
    capture" from "field absent because the model returned nothing" —
    the policy is right there to read alongside the payload.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    inputs: bool
    outputs: bool
    repairs: bool
    redacted_fields: list[str] | None = Field(default=None, max_length=64, alias="redactedFields")


# ── Common event base ──────────────────────────────────────────────────────


class _EventBase(BaseModel):
    """Shared identity + timing + optional fields across every event."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    contract_name: str = Field(min_length=1, max_length=128, alias="contractName")
    environment: str | None = Field(default=None, max_length=64)
    timestamp: str
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1, alias="maxAttempts")
    duration_ms: int = Field(ge=0, alias="durationMs")
    input: Any = None
    output: Any = None
    model: str | None = Field(default=None, max_length=128)
    capture: ResolvedCapture | None = None
    schema_: list[SchemaField] | None = Field(default=None, alias="schema", max_length=256)
    rules: list[RuleDefinition] | None = Field(default=None, max_length=256)
    client_event_id: str | None = Field(default=None, max_length=64, alias="clientEventId")
    run_id: str = Field(pattern=r"^bnd_run_[A-Za-z0-9_-]{1,40}$", max_length=48, alias="runId")
    sdk: SdkMeta | None = None


# ── Discriminated union members ────────────────────────────────────────────


class AcceptedEvent(_EventBase):
    """A terminal success. ``ok=True`` and ``final=True`` are pinned by
    the hosted validator — an accepted event is always the end of its run."""

    ok: Literal[True] = True
    final: Literal[True] = True


class FailedEvent(_EventBase):
    """A failed attempt — either mid-run (``final=False``, the engine
    will retry) or terminal (``final=True``, the run exhausted its
    budget or halted via an explicit repair override).

    ``category`` mirrors the contract's failure taxonomy. ``issues`` is
    the human-readable explanation; ``rule_failures`` lists rule names
    that rejected the payload (subset of ``rules[].name``); ``repairs``
    carries the engine-generated repair body unless capture policy
    suppressed it.
    """

    ok: Literal[False] = False
    final: bool
    category: str = Field(max_length=64)
    issues: list[str]
    rule_failures: list[str] | None = Field(default=None, max_length=256, alias="ruleFailures")
    repairs: list[Message] | None = None


BoundaryEvent = Annotated[AcceptedEvent | FailedEvent, Discriminator("ok")]
"""The wire-event discriminated union. Use ``.model_dump(by_alias=True,
exclude_none=True)`` to produce the JSON the ingest endpoint accepts."""


# ── Helpers ────────────────────────────────────────────────────────────────


def now_iso() -> str:
    """Current wall-clock time as an ISO 8601 string with the ``Z``
    UTC designator. The hosted ingest validator's datetime check only
    accepts the ``Z`` literal — a numeric ``+00:00`` offset is rejected
    at the wire-validation step, even though both denote UTC."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sdk_meta(version: str) -> SdkMeta:
    """Build the SDK metadata block stamped on every outbound event.
    Centralised so the per-process metadata stays consistent regardless
    of which builder method materialises an event."""
    return SdkMeta(name=SDK_NAME, version=version, runtime=runtime())


# ── Event builder ──────────────────────────────────────────────────────────


class EventBuilder:
    """Translates contract hook contexts into wire-shaped events.

    Holds the per-process SDK metadata, the optional environment
    label, and the default model name. Per-run / per-attempt state is
    threaded in via the :class:`PerRunState` argument on every method
    so the builder itself stays stateless.
    """

    def __init__(
        self,
        *,
        sdk_version: str,
        environment: str | None = None,
        default_model: str | None = None,
    ) -> None:
        self._sdk = sdk_meta(sdk_version)
        self._environment = environment
        self._default_model = default_model

    # ── Per-attempt failure (mid-run, engine will retry) ──────────────────

    def attempt_failure(
        self,
        *,
        run: PerRunState,
        attempt: int,
        category: str,
        issues: list[str],
        duration_ms: int,
        rule_failures: list[str] | None = None,
        cleaned_output: Any | None = None,
    ) -> FailedEvent:
        """Build the mid-run failure event the SDK ships when an attempt
        is rejected and the engine has at least one retry budget left.

        ``cleaned_output`` is the raw value the verifier rejected (when
        the cleaner extracted something parsable). It surfaces as
        ``output`` on the wire when capture policy permits.
        """
        return FailedEvent(
            contract_name=run.contract_name,
            environment=self._environment,
            timestamp=now_iso(),
            attempt=attempt,
            max_attempts=run.max_attempts,
            duration_ms=duration_ms,
            input=run.instructions or None,
            output=cleaned_output,
            model=run.model or self._default_model,
            schema_=run.consume_schema(),
            rules=run.consume_rules(),
            client_event_id=mint_event_id(),
            run_id=run.run_id,
            sdk=self._sdk,
            final=False,
            category=category,
            issues=list(issues),
            rule_failures=list(rule_failures) if rule_failures else None,
            repairs=list(run.repairs) if run.repairs else None,
        )

    # ── Terminal success ──────────────────────────────────────────────────

    def run_success(
        self,
        *,
        run: PerRunState,
        attempts: int,
        total_duration_ms: int,
        data: Any,
    ) -> AcceptedEvent:
        """Build the terminal success event for an accepted run.

        ``data`` is the validated/typed payload the contract returned —
        surfaces as ``output`` on the wire when capture policy permits.
        """
        return AcceptedEvent(
            contract_name=run.contract_name,
            environment=self._environment,
            timestamp=now_iso(),
            attempt=attempts,
            max_attempts=run.max_attempts,
            duration_ms=total_duration_ms,
            input=run.instructions or None,
            output=_dump(data),
            model=run.model or self._default_model,
            schema_=run.consume_schema(),
            rules=run.consume_rules(),
            client_event_id=mint_event_id(),
            run_id=run.run_id,
            sdk=self._sdk,
        )

    # ── Terminal failure (run exhausted retries) ──────────────────────────

    def run_failure(
        self,
        *,
        run: PerRunState,
        attempts: int,
        total_duration_ms: int,
        category: str,
        message: str,
        rule_failures: list[str] | None = None,
    ) -> FailedEvent:
        """Build the terminal failure event for a run that exhausted
        its retry budget (or halted via a repair override). ``message``
        is the contract-py ``ContractError.message`` and surfaces as the
        sole entry of ``issues``."""
        return FailedEvent(
            contract_name=run.contract_name,
            environment=self._environment,
            timestamp=now_iso(),
            attempt=attempts,
            max_attempts=run.max_attempts,
            duration_ms=total_duration_ms,
            input=run.instructions or None,
            model=run.model or self._default_model,
            schema_=run.consume_schema(),
            rules=run.consume_rules(),
            client_event_id=mint_event_id(),
            run_id=run.run_id,
            sdk=self._sdk,
            final=True,
            category=category,
            issues=[message],
            rule_failures=list(rule_failures) if rule_failures else None,
            repairs=None,
        )


# ── Output coercion ────────────────────────────────────────────────────────


def _dump(value: Any) -> Any:
    """Best-effort conversion of arbitrary outputs to a JSON-friendly
    shape. Pydantic models become dicts; dataclasses become dicts;
    everything else passes through and the JSON encoder at send-time
    will fall back to ``default=str`` for the holdouts."""
    if isinstance(value, BaseModel):
        return value.model_dump()
    if hasattr(value, "__dataclass_fields__"):
        try:
            from dataclasses import asdict

            return asdict(value)
        except TypeError:
            return value
    return value


# ── Late import to avoid a cycle ───────────────────────────────────────────

# PerRunState lives in `runs.py` and depends on the SchemaField /
# RuleDefinition types this module re-exports. Import at module bottom
# so the type annotation above is satisfied without forming a cycle at
# import time.
from .runs import PerRunState  # noqa: E402

# ── Convenience: stamping rule-failure names from RuleIssue iterables ──────


def rule_failure_names(issues: Iterable[Any] | None) -> list[str] | None:
    """Project a sequence of ``RuleIssue`` instances down to the rule
    names that failed. Returns ``None`` when the input is empty so the
    builder can omit the field entirely (rather than send ``[]``,
    which would conflate "no rules failed" with "rules opt-out")."""
    if not issues:
        return None
    names: list[str] = []
    for issue in issues:
        rule = getattr(issue, "rule", None)
        if rule is None:
            continue
        name = getattr(rule, "name", None)
        if isinstance(name, str):
            names.append(name)
    return names or None


__all__ = [
    "AcceptedEvent",
    "BoundaryEvent",
    "EventBuilder",
    "FailedEvent",
    "ResolvedCapture",
    "SdkMeta",
    "now_iso",
    "rule_failure_names",
    "sdk_meta",
]
