"""Apply the capture policy to an outbound event.

Three sensitive fields are gated by ``CapturePolicy``:

* ``input`` — the schema-driven prompt block (and any repair messages
  the engine prepended to it). Off by default.
* ``output`` — the cleaned/typed model output. Off by default.
* ``repairs`` — the engine-generated repair messages on failure events.
  On by default; the repair body is already a curated, redactable view
  of the failure rather than raw model output.

:func:`apply_capture` walks an event, drops the fields the policy
disallows, and stamps the resolved policy (plus any redacted-field
paths) onto the event's ``capture`` slot. Stamping the resolved
snapshot makes the dashboard's behaviour auditable — a missing field
is "policy disabled" if the snapshot says so, "model returned nothing"
if the snapshot says capture was on.

Pure function — never raises, never mutates the input event. Returns
a new event so calling code can pipeline through redaction without
worrying about ordering side effects.
"""

from __future__ import annotations

from .config import CapturePolicy
from .events import AcceptedEvent, BoundaryEvent, FailedEvent, ResolvedCapture


def apply_capture(
    event: BoundaryEvent,
    policy: CapturePolicy,
    *,
    redacted_fields: list[str] | None = None,
) -> BoundaryEvent:
    """Return a copy of ``event`` with the capture policy enforced.

    Drops ``input`` when ``policy.inputs`` is False, ``output`` when
    ``policy.outputs`` is False, and ``repairs`` (failure events only)
    when ``policy.repairs`` is False. Always stamps a
    :class:`ResolvedCapture` snapshot on ``event.capture`` so the
    dashboard can introspect the policy that produced the payload.

    ``redacted_fields`` is an optional list of dotted field paths the
    redactor scrubbed; included verbatim in the resolved snapshot so
    consumers can correlate "this field is masked" with "this field
    name was on the redaction list".
    """
    snapshot = ResolvedCapture(
        inputs=policy.inputs,
        outputs=policy.outputs,
        repairs=policy.repairs,
        redacted_fields=list(redacted_fields) if redacted_fields else None,
    )

    updates: dict[str, object] = {"capture": snapshot}
    if not policy.inputs:
        updates["input"] = None
    if not policy.outputs:
        updates["output"] = None

    if isinstance(event, FailedEvent):
        if not policy.repairs:
            updates["repairs"] = None
        return event.model_copy(update=updates)

    # AcceptedEvent has no ``repairs`` slot to clear.
    return event.model_copy(update=updates)


def gates_match(snapshot: ResolvedCapture, policy: CapturePolicy) -> bool:
    """Return True iff the snapshot's gates reflect the same policy.
    Useful for tests — verifies that the snapshot stamped on an event
    accurately describes the policy that produced it.
    """
    return (
        snapshot.inputs == policy.inputs
        and snapshot.outputs == policy.outputs
        and snapshot.repairs == policy.repairs
    )


__all__ = [
    "apply_capture",
    "gates_match",
]


# ── Convenience accessors ─────────────────────────────────────────────────


# Re-export the event union types so callers writing custom capture
# pipelines can import them from this module without reaching into
# ``events`` directly.
_ = AcceptedEvent  # keep the import alive for re-export from __init__
