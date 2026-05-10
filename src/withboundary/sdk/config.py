"""User-facing configuration dataclasses.

The SDK accepts five orthogonal option groups that sit alongside the
required ``api_key``:

* :class:`BatchOptions` — when the queue drains (size trigger, time
  trigger, hard cap before drop-oldest).
* :class:`CapturePolicy` — which sensitive fields ride along on the
  wire (LLM prompts, completions, repair messages).
* :class:`RedactionOptions` — how to scrub sensitive content out of any
  payloads that *do* ride along, layered: by field name, by regex, then
  by a user-supplied callable.
* :class:`RetryOptions` — backoff math for the ingest transport when
  the endpoint is unreachable.
* :class:`BreakerOptions` — circuit-breaker thresholds that pause
  outbound traffic when the endpoint stays unreachable.

Every field is a frozen dataclass with sensible defaults — partial
overrides at the factory call site cleanly merge with the rest of the
defaults via :func:`resolve_*` helpers.

The ``REDACT`` sentinel exposed here is the value a custom redactor
returns to drop a leaf entirely (rather than masking it).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

# ── Public sentinel for the custom redactor ────────────────────────────────


class _RedactSentinel:
    """Singleton sentinel a custom redactor can return to indicate that
    the visited leaf should be removed entirely from the event.

    Has a friendly ``repr`` so tracebacks and dashboard logs name it
    clearly when something accidentally serializes one. Compared by
    identity throughout the redactor; never construct a second instance.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<REDACT>"


REDACT: _RedactSentinel = _RedactSentinel()
"""Return ``REDACT`` from a custom redactor callable to remove the
visited leaf from the wire payload entirely. Returning ``None`` instead
keeps the field with a literal ``None`` value, which carries different
semantics on the dashboard."""


# ── Batch options ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BatchOptions:
    """When the in-process event queue drains.

    Defaults are tuned for low-overhead background delivery: 20 events
    per request keeps payloads small, a 5-second tick provides a hard
    upper bound on visibility delay, and the 1000-event queue cap
    protects against memory growth if the endpoint is unavailable for
    an extended period — older events drop first to keep the freshest
    state.
    """

    size: int = 20
    """Drain immediately once the queue holds this many events.
    Stay well under the ingest endpoint's per-request cap (500) so a
    single batched request never needs to be re-split."""

    interval: float = 5.0
    """Seconds between periodic drains, regardless of queue size.
    Set to ``0`` to disable the timer; explicit ``flush()`` and the
    size trigger remain active."""

    max_queue_size: int = 1000
    """Hard cap on in-memory queue depth. Once exceeded, the oldest
    queued event is silently dropped to make room for the new one and
    the count of dropped events is surfaced to ``on_error`` on the
    next flush attempt."""


# ── Capture policy ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CapturePolicy:
    """Which sensitive fields the SDK includes on outbound events.

    Defaults err on the side of privacy — neither the LLM input prompt
    nor the model's raw output ride along by default. Repair messages
    do, since they're already a transformed view of the failure (and
    extremely useful on the dashboard). Users opt in to richer capture
    by passing ``CapturePolicy(inputs=True, outputs=True)``.
    """

    inputs: bool = False
    """When ``True``, include the schema-driven prompt block (and any
    repair messages prepended to it) as ``input`` on every event."""

    outputs: bool = False
    """When ``True``, include the cleaned/typed model output as
    ``output`` on success events and the rejected payload on failure
    events."""

    repairs: bool = True
    """When ``True``, include the repair messages the engine generated
    for the next attempt on failure events. Defaults on because the
    repair body is already a curated, redactable view of the failure
    rather than raw model output."""


# ── Redaction options ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class RedactionOptions:
    """Three composable layers run in order on every captured payload.

    The redactor walks every dict / list / scalar reachable from an
    event. Each leaf is offered to (1) the field-name layer, then (2)
    the pattern layer if it's still a string, then (3) the custom
    callable. The first layer to mask wins for that leaf; later layers
    still run and may further transform the masked value.

    Field names are matched case-sensitively against any key encountered
    in the walk. Patterns are applied to string leaves; matching spans
    are replaced with ``"[REDACTED]"``. The custom callable receives
    each leaf with its full path tuple so users can implement
    location-aware policies (e.g. only redact ``ssn`` under
    ``customer.*``).
    """

    fields: tuple[str, ...] = ()
    """Exact key names to scrub recursively. Case-sensitive. Stored as
    a tuple so the dataclass stays hashable."""

    patterns: tuple[re.Pattern[str], ...] = ()
    """Compiled regex patterns. Each is run against every string leaf;
    matching spans are replaced with ``[REDACTED]``."""

    custom: Callable[[Any, tuple[str, ...]], Any] | None = None
    """Last-layer callable. Receives ``(value, path)`` for every leaf;
    its return value replaces the leaf. Return :data:`REDACT` to drop
    the leaf entirely."""


# ── Retry options ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RetryOptions:
    """Exponential-backoff retry policy for the ingest transport.

    Applies only to network errors and 5xx responses; auth failures
    (401/403) and explicit non-retryable 4xx codes raise immediately
    without consulting this policy.
    """

    max_attempts: int = 3
    """Total attempts — including the first call. ``1`` disables retry
    entirely."""

    base_ms: int = 100
    """Backoff base. Delay before attempt ``n`` is ``base_ms * 4^(n-1)``
    plus up to 50% jitter. Default schedule: 100ms, 400ms, 1600ms."""

    timeout: float = 10.0
    """Per-attempt request timeout in seconds. Enforced via the HTTP
    client; an exceeded timeout counts as a network error and triggers
    the next retry."""


# ── Circuit-breaker options ──────────────────────────────────────────────


@dataclass(frozen=True)
class BreakerOptions:
    """Circuit-breaker thresholds for the ingest transport.

    The breaker sits in front of the retry layer. Once it trips into
    ``OPEN``, send attempts fail immediately without touching the
    network until the cooldown elapses, at which point one probe
    request is allowed; success closes the breaker, failure re-opens
    it for another cooldown period.
    """

    threshold: int = 5
    """Consecutive failures that trip the breaker into ``OPEN``."""

    cooldown: float = 30.0
    """Seconds the breaker stays ``OPEN`` before admitting a probe."""


# ── Resolution helpers ────────────────────────────────────────────────────


def resolve_batch(options: BatchOptions | None) -> BatchOptions:
    """Return ``options`` if non-None, otherwise the default :class:`BatchOptions`.

    Centralised so the factory functions can stay terse — every option
    group goes through its matching ``resolve_*`` helper.
    """
    return options if options is not None else BatchOptions()


def resolve_capture(options: CapturePolicy | None) -> CapturePolicy:
    return options if options is not None else CapturePolicy()


def resolve_redact(options: RedactionOptions | None) -> RedactionOptions:
    return options if options is not None else RedactionOptions()


def resolve_retry(options: RetryOptions | None) -> RetryOptions:
    return options if options is not None else RetryOptions()


def resolve_breaker(options: BreakerOptions | None) -> BreakerOptions:
    return options if options is not None else BreakerOptions()


# ── Convenience builders ──────────────────────────────────────────────────


def make_redaction(
    *,
    fields: list[str] | None = None,
    patterns: list[re.Pattern[str] | str] | None = None,
    custom: Callable[[Any, tuple[str, ...]], Any] | None = None,
) -> RedactionOptions:
    """Build a :class:`RedactionOptions` from list inputs.

    Accepts patterns as either compiled ``re.Pattern`` objects or raw
    strings (compiled here for convenience). Returns a frozen options
    instance suitable for passing to the factory.
    """
    compiled: tuple[re.Pattern[str], ...] = tuple(
        p if isinstance(p, re.Pattern) else re.compile(p) for p in (patterns or [])
    )
    return RedactionOptions(
        fields=tuple(fields or ()),
        patterns=compiled,
        custom=custom,
    )


def merged_capture(base: CapturePolicy, overrides: CapturePolicy | None) -> CapturePolicy:
    """Layer ``overrides`` over ``base`` with non-None field semantics.

    Currently a thin pass-through (every CapturePolicy field is
    required), but the indirection keeps the call sites future-proof
    against partial-override shapes we may introduce later.
    """
    if overrides is None:
        return base
    return replace(
        base, inputs=overrides.inputs, outputs=overrides.outputs, repairs=overrides.repairs
    )


# Re-export the `field` helper for downstream subclassers that want to add
# default factories without re-importing dataclasses themselves.
__all__ = [
    "REDACT",
    "BatchOptions",
    "BreakerOptions",
    "CapturePolicy",
    "RedactionOptions",
    "RetryOptions",
    "field",
    "make_redaction",
    "merged_capture",
    "resolve_batch",
    "resolve_breaker",
    "resolve_capture",
    "resolve_redact",
    "resolve_retry",
]
