"""Three-layer redactor for outbound event payloads.

Walks every dict / list / scalar reachable from an event's
capture-gated fields (``input``, ``output``, ``repairs``) and applies
three composable layers in order:

1. **Field-name** — when the walk encounters a key matching one of
   ``RedactionOptions.fields`` (case-sensitive), the value is replaced
   with ``"[REDACTED]"`` and the path is recorded.
2. **Pattern** — for every string leaf, each compiled regex is run;
   matching spans are replaced with ``"[REDACTED]"``. The path is
   recorded if any pattern matched.
3. **Custom callable** — runs last for every leaf. Receives
   ``(value, path_tuple)``; the return value replaces the leaf. Returning
   the :data:`REDACT` sentinel drops the leaf entirely; returning
   ``None`` keeps the leaf as a literal ``None``.

The redactor returns a tuple of ``(redacted_event, scrubbed_paths)`` so
the calling code can stamp the scrubbed paths on
:class:`ResolvedCapture.redacted_fields`.

Cycle detection via an ``id(obj)`` set: if the walk revisits an object
it's already inside, the visit short-circuits without recursing. This
prevents infinite recursion on self-referential dicts (which can occur
when users pass a structured logging context that includes itself).

Pure function — never raises, never mutates the input event.
"""

from __future__ import annotations

import re
from typing import Any

from .config import REDACT, RedactionOptions
from .events import BoundaryEvent, FailedEvent

REDACTED_VALUE = "[REDACTED]"
"""The sentinel string the field-name and pattern layers substitute
in place of a redacted leaf. Distinct from the :data:`REDACT`
sentinel that drops a leaf entirely."""

# Fields on the event base that may carry user-supplied payload data.
# Limited list — we don't walk identity / timing fields because they
# never carry user-redactable content.
_PAYLOAD_FIELDS = ("input", "output", "repairs")


def apply_redaction(
    event: BoundaryEvent,
    options: RedactionOptions,
) -> tuple[BoundaryEvent, list[str]]:
    """Return ``(redacted_event, scrubbed_paths)``.

    ``scrubbed_paths`` is the de-duplicated list of dotted leaf paths
    the redactor masked or dropped — suitable for the
    ``ResolvedCapture.redacted_fields`` slot. Empty if no layer fired.
    """
    if not options.fields and not options.patterns and options.custom is None:
        # No-op fast path — avoid model_copy for the common case where
        # no redaction is configured.
        return event, []

    scrubbed: list[str] = []
    seen: set[int] = set()
    updates: dict[str, Any] = {}

    for field_name in _PAYLOAD_FIELDS:
        if field_name == "repairs" and not isinstance(event, FailedEvent):
            continue
        original = getattr(event, field_name, None)
        if original is None:
            continue
        new_value = _walk(
            value=original,
            path=(field_name,),
            options=options,
            scrubbed=scrubbed,
            seen=seen,
        )
        if new_value is _DROP:
            updates[field_name] = None
        else:
            updates[field_name] = new_value

    if not updates:
        return event, _dedupe(scrubbed)

    return event.model_copy(update=updates), _dedupe(scrubbed)


# ── Sentinel for the walk's "drop this leaf entirely" signal ───────────────


class _DropSentinel:
    """Internal marker the walker returns to say "remove this leaf
    from its parent"."""

    __slots__ = ()


_DROP = _DropSentinel()


# ── Recursive walk ─────────────────────────────────────────────────────────


def _walk(
    *,
    value: Any,
    path: tuple[str, ...],
    options: RedactionOptions,
    scrubbed: list[str],
    seen: set[int],
) -> Any:
    """Walk ``value`` recursively, applying the three redaction layers
    at every leaf. Mutates ``scrubbed`` in place. Returns the new
    value; returns :data:`_DROP` when the custom layer says to drop
    the leaf."""

    if isinstance(value, dict):
        return _walk_dict(value=value, path=path, options=options, scrubbed=scrubbed, seen=seen)
    if isinstance(value, list):
        return _walk_list(value=value, path=path, options=options, scrubbed=scrubbed, seen=seen)
    if isinstance(value, tuple):
        # Tuples in payloads are unusual but possible (e.g. JSON-decoded
        # output that the user later re-shaped); treat them as lists for
        # the walk.
        return tuple(
            _walk_list(
                value=list(value),
                path=path,
                options=options,
                scrubbed=scrubbed,
                seen=seen,
            )
        )
    return _apply_leaf_layers(value=value, path=path, options=options, scrubbed=scrubbed)


def _walk_dict(
    *,
    value: dict[Any, Any],
    path: tuple[str, ...],
    options: RedactionOptions,
    scrubbed: list[str],
    seen: set[int],
) -> dict[Any, Any]:
    if id(value) in seen:
        return value
    seen.add(id(value))
    out: dict[Any, Any] = {}
    for key, sub in value.items():
        sub_path = path + (str(key),)

        # Layer 1: field-name redaction wins outright before any walk
        # into the value. Replaces the value with the sentinel string;
        # later layers still run on the sentinel (a custom callable can
        # promote the masked value to something else).
        if isinstance(key, str) and key in options.fields:
            scrubbed.append(_dotted(sub_path))
            sub = REDACTED_VALUE

        new_sub = _walk(value=sub, path=sub_path, options=options, scrubbed=scrubbed, seen=seen)
        if new_sub is _DROP:
            scrubbed.append(_dotted(sub_path))
            continue
        out[key] = new_sub
    return out


def _walk_list(
    *,
    value: list[Any],
    path: tuple[str, ...],
    options: RedactionOptions,
    scrubbed: list[str],
    seen: set[int],
) -> list[Any]:
    if id(value) in seen:
        return value
    seen.add(id(value))
    out: list[Any] = []
    for idx, sub in enumerate(value):
        sub_path = path + (str(idx),)
        new_sub = _walk(value=sub, path=sub_path, options=options, scrubbed=scrubbed, seen=seen)
        if new_sub is _DROP:
            scrubbed.append(_dotted(sub_path))
            continue
        out.append(new_sub)
    return out


def _apply_leaf_layers(
    *,
    value: Any,
    path: tuple[str, ...],
    options: RedactionOptions,
    scrubbed: list[str],
) -> Any:
    """Run the pattern + custom layers on a single non-collection leaf."""

    # Layer 2: pattern redaction. Only meaningful on string leaves.
    if isinstance(value, str) and options.patterns:
        new_value, hit = _apply_patterns(value, options.patterns)
        if hit:
            scrubbed.append(_dotted(path))
        value = new_value

    # Layer 3: custom callable. Runs last so users can override or
    # extend the earlier layers' decisions.
    if options.custom is not None:
        try:
            replacement = options.custom(value, path)
        except Exception:  # noqa: BLE001 — custom code, isolate from the walker
            return value
        if replacement is REDACT:
            scrubbed.append(_dotted(path))
            return _DROP
        if replacement is not value:
            scrubbed.append(_dotted(path))
        return replacement

    return value


def _apply_patterns(value: str, patterns: tuple[re.Pattern[str], ...]) -> tuple[str, bool]:
    """Run every regex against ``value`` and return ``(masked, hit)``.
    ``hit`` is True when at least one pattern matched."""
    hit = False
    for pattern in patterns:
        new_value, count = pattern.subn(REDACTED_VALUE, value)
        if count:
            hit = True
            value = new_value
    return value, hit


# ── Path helpers ──────────────────────────────────────────────────────────


def _dotted(path: tuple[str, ...]) -> str:
    """Render a path tuple as a dotted string. Used for the
    ``redacted_fields`` slot on ``ResolvedCapture`` so consumers see
    ``input.user.email`` rather than a tuple."""
    return ".".join(path)


def _dedupe(paths: list[str]) -> list[str]:
    """Drop duplicates while preserving first-seen order. The walker
    can record the same path twice when multiple layers fire on the
    same leaf; the snapshot only needs to surface it once."""
    seen: set[str] = set()
    out: list[str] = []
    for p in paths:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


__all__ = [
    "REDACTED_VALUE",
    "apply_redaction",
]
