"""Three-layer redaction with cycle detection."""

from __future__ import annotations

import re
from typing import Any

from withboundary.sdk import (
    REDACT,
    AcceptedEvent,
    FailedEvent,
    RedactionOptions,
    make_redaction,
)
from withboundary.sdk.redact import REDACTED_VALUE, apply_redaction


def _accepted(**overrides: Any) -> AcceptedEvent:
    base: dict[str, Any] = {
        "contract_name": "x",
        "timestamp": "2026-05-10T00:00:00+00:00",
        "attempt": 1,
        "max_attempts": 1,
        "duration_ms": 0,
        "run_id": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
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
    }
    base.update(overrides)
    return FailedEvent(**base)


# ── No-op fast path ───────────────────────────────────────────────────────


class TestNoOp:
    def test_empty_options_pass_through_unchanged(self) -> None:
        event = _accepted(input={"x": 1}, output="hello")
        result, scrubbed = apply_redaction(event, RedactionOptions())
        assert result is event
        assert scrubbed == []


# ── Layer 1: field-name redaction ─────────────────────────────────────────


class TestFieldRedaction:
    def test_top_level_field_masked(self) -> None:
        event = _accepted(input={"ssn": "123", "name": "alice"})
        result, scrubbed = apply_redaction(event, make_redaction(fields=["ssn"]))
        assert result.input == {"ssn": REDACTED_VALUE, "name": "alice"}
        assert "input.ssn" in scrubbed

    def test_nested_field_masked(self) -> None:
        event = _accepted(input={"user": {"ssn": "123", "email": "a@b.c"}})
        result, scrubbed = apply_redaction(event, make_redaction(fields=["ssn"]))
        assert result.input == {"user": {"ssn": REDACTED_VALUE, "email": "a@b.c"}}
        assert "input.user.ssn" in scrubbed

    def test_field_inside_list_element_masked(self) -> None:
        event = _accepted(
            input={"users": [{"ssn": "111", "name": "a"}, {"ssn": "222", "name": "b"}]}
        )
        result, scrubbed = apply_redaction(event, make_redaction(fields=["ssn"]))
        assert result.input == {
            "users": [
                {"ssn": REDACTED_VALUE, "name": "a"},
                {"ssn": REDACTED_VALUE, "name": "b"},
            ]
        }
        assert "input.users.0.ssn" in scrubbed
        assert "input.users.1.ssn" in scrubbed

    def test_case_sensitive_field_match(self) -> None:
        event = _accepted(input={"SSN": "123", "ssn": "456"})
        result, _ = apply_redaction(event, make_redaction(fields=["ssn"]))
        # Only the exact-case match is masked
        assert result.input == {"SSN": "123", "ssn": REDACTED_VALUE}


# ── Layer 2: pattern redaction ────────────────────────────────────────────


class TestPatternRedaction:
    def test_string_leaf_pattern_match(self) -> None:
        event = _accepted(input={"note": "ssn 123-45-6789 attached"})
        result, scrubbed = apply_redaction(event, make_redaction(patterns=[r"\d{3}-\d{2}-\d{4}"]))
        assert result.input is not None
        assert REDACTED_VALUE in result.input["note"]
        assert "123-45-6789" not in result.input["note"]
        assert "input.note" in scrubbed

    def test_pattern_skipped_on_non_string_leaves(self) -> None:
        event = _accepted(input={"count": 12345})
        result, scrubbed = apply_redaction(event, make_redaction(patterns=[r"\d+"]))
        # Integer leaf is unchanged because patterns only run on strings.
        assert result.input == {"count": 12345}
        assert scrubbed == []

    def test_compiled_patterns_work(self) -> None:
        compiled = re.compile(r"secret-\w+")
        event = _accepted(input={"msg": "shared secret-abc123"})
        result, _ = apply_redaction(event, RedactionOptions(patterns=(compiled,)))
        assert result.input is not None
        assert "secret-abc123" not in result.input["msg"]


# ── Layer 3: custom callable ──────────────────────────────────────────────


class TestCustomLayer:
    def test_custom_replaces_value(self) -> None:
        def upper(value: Any, _path: tuple[str, ...]) -> Any:
            if isinstance(value, str):
                return value.upper()
            return value

        event = _accepted(input={"name": "alice"})
        result, scrubbed = apply_redaction(event, make_redaction(custom=upper))
        assert result.input == {"name": "ALICE"}
        assert "input.name" in scrubbed

    def test_custom_drops_leaf_with_REDACT_sentinel(self) -> None:
        def drop_emails(value: Any, path: tuple[str, ...]) -> Any:
            if path and path[-1] == "email":
                return REDACT
            return value

        event = _accepted(input={"name": "alice", "email": "a@b.c"})
        result, scrubbed = apply_redaction(event, make_redaction(custom=drop_emails))
        assert result.input == {"name": "alice"}
        assert "input.email" in scrubbed

    def test_custom_path_aware(self) -> None:
        seen_paths: list[tuple[str, ...]] = []

        def watcher(value: Any, path: tuple[str, ...]) -> Any:
            seen_paths.append(path)
            return value

        event = _accepted(input={"a": 1, "b": {"c": 2}})
        apply_redaction(event, make_redaction(custom=watcher))
        # Both leaves get visited with their full paths.
        assert ("input", "a") in seen_paths
        assert ("input", "b", "c") in seen_paths

    def test_custom_exception_does_not_break_walker(self) -> None:
        def explode(_value: Any, _path: tuple[str, ...]) -> Any:
            raise RuntimeError("boom")

        event = _accepted(input={"name": "alice"})
        result, _ = apply_redaction(event, make_redaction(custom=explode))
        # On exception the walker preserves the leaf rather than
        # crashing.
        assert result.input == {"name": "alice"}


# ── Layer composition ─────────────────────────────────────────────────────


class TestLayerComposition:
    def test_field_then_pattern_both_visible(self) -> None:
        # Field-name layer sets the leaf to REDACTED_VALUE; pattern
        # layer then runs on that string. Both layers can fire.
        event = _accepted(input={"ssn": "100"})
        result, scrubbed = apply_redaction(
            event,
            make_redaction(fields=["ssn"], patterns=[r"\d+"]),
        )
        # Field redaction wins first; pattern redaction has nothing to
        # match on REDACTED_VALUE.
        assert result.input == {"ssn": REDACTED_VALUE}
        assert "input.ssn" in scrubbed

    def test_all_three_layers_run(self) -> None:
        custom_calls: list[tuple[str, ...]] = []

        def custom(value: Any, path: tuple[str, ...]) -> Any:
            custom_calls.append(path)
            return value

        event = _accepted(input={"ssn": "111-22-3333", "name": "alice"})
        apply_redaction(
            event,
            make_redaction(
                fields=["name"],
                patterns=[r"\d{3}-\d{2}-\d{4}"],
                custom=custom,
            ),
        )
        # Custom sees every leaf — including the post-redaction values.
        assert ("input", "ssn") in custom_calls
        assert ("input", "name") in custom_calls


# ── Cycle detection ──────────────────────────────────────────────────────


class TestCycleDetection:
    def test_self_referential_dict_does_not_recurse_forever(self) -> None:
        cycle: dict[str, Any] = {"x": 1}
        cycle["self"] = cycle
        event = _accepted(input=cycle)
        result, _ = apply_redaction(event, make_redaction(fields=["x"]))
        # Did not recurse infinitely. The first encounter masks x; on
        # the second encounter the cycle short-circuits.
        assert result.input is not None
        assert result.input["x"] == REDACTED_VALUE

    def test_cycle_through_list(self) -> None:
        cycle_list: list[Any] = [1, 2]
        cycle_list.append(cycle_list)
        event = _accepted(input={"items": cycle_list})
        # Should complete without RecursionError.
        result, _ = apply_redaction(event, make_redaction(fields=["nope"]))
        assert result.input is not None


# ── Failed event repairs ──────────────────────────────────────────────────


class TestFailedEventRepairs:
    def test_repairs_walked_for_redaction(self) -> None:
        event = _failed(repairs=[{"role": "user", "content": "ssn 111-22-3333"}])
        result, scrubbed = apply_redaction(
            event,
            make_redaction(patterns=[r"\d{3}-\d{2}-\d{4}"]),
        )
        assert isinstance(result, FailedEvent)
        assert result.repairs is not None
        assert REDACTED_VALUE in result.repairs[0]["content"]
        assert any(p.startswith("repairs.") for p in scrubbed)

    def test_accepted_event_does_not_walk_repairs_field(self) -> None:
        # AcceptedEvent has no repairs slot; redactor must not crash.
        event = _accepted(input={"x": 1})
        result, _ = apply_redaction(event, make_redaction(fields=["x"]))
        assert isinstance(result, AcceptedEvent)


# ── Scrubbed-paths dedup ─────────────────────────────────────────────────


class TestScrubbedPathsDedup:
    def test_same_path_recorded_once(self) -> None:
        # Field + pattern both fire on the same leaf — record once.
        event = _accepted(input={"ssn": "111"})
        _, scrubbed = apply_redaction(
            event,
            make_redaction(fields=["ssn"], patterns=[r"\d+"]),
        )
        assert scrubbed.count("input.ssn") == 1

    def test_preserves_first_seen_order(self) -> None:
        event = _accepted(input={"a": "x", "b": "y", "c": "z"})
        _, scrubbed = apply_redaction(event, make_redaction(fields=["a", "b", "c"]))
        assert scrubbed == ["input.a", "input.b", "input.c"]
