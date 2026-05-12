"""Wire-event Pydantic models, the discriminated union, and the EventBuilder."""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError
from withboundary.contract.types import RuleDefinition, SchemaField

from withboundary.sdk import (
    AcceptedEvent,
    BoundaryEvent,
    EventBuilder,
    FailedEvent,
    PerRunState,
    ResolvedCapture,
)
from withboundary.sdk._meta import SDK_NAME
from withboundary.sdk.events import now_iso, rule_failure_names, sdk_meta

# Fresh TypeAdapter so each test sees the current schema rather than a
# stale cached version from a prior failed import.
EVENT_ADAPTER: TypeAdapter[Any] = TypeAdapter(BoundaryEvent)


# Pinned wire-format regex from the hosted ingest validator.
RUN_ID_PATTERN = re.compile(r"^bnd_run_[A-Za-z0-9_-]{1,40}$")


def _state(**overrides: Any) -> PerRunState:
    """Make a PerRunState with sensible defaults plus overrides."""
    base: dict[str, Any] = {
        "contract_name": "lead-scoring",
        "run_handle": "rh_abc",
        "run_id": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        "started_at": 0.0,
        "max_attempts": 3,
        "model": None,
    }
    base.update(overrides)
    return PerRunState(**base)


# ── Discriminated union routing ────────────────────────────────────────────


class TestDiscriminator:
    def test_ok_true_routes_to_accepted_event(self) -> None:
        payload = {
            "contractName": "x",
            "timestamp": "2026-05-10T00:00:00+00:00",
            "attempt": 1,
            "maxAttempts": 1,
            "durationMs": 0,
            "runId": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            "ok": True,
            "final": True,
        }
        event = EVENT_ADAPTER.validate_python(payload)
        assert isinstance(event, AcceptedEvent)

    def test_ok_false_routes_to_failed_event(self) -> None:
        payload = {
            "contractName": "x",
            "timestamp": "2026-05-10T00:00:00+00:00",
            "attempt": 1,
            "maxAttempts": 1,
            "durationMs": 0,
            "runId": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            "ok": False,
            "final": True,
            "category": "VALIDATION_ERROR",
            "issues": ["bad"],
        }
        event = EVENT_ADAPTER.validate_python(payload)
        assert isinstance(event, FailedEvent)

    def test_accepted_event_must_be_final(self) -> None:
        # The hosted validator pins `ok=True ⇒ final=True`. Pydantic's
        # `Literal[True]` enforces that here too.
        with pytest.raises(ValidationError):
            AcceptedEvent(
                contract_name="x",
                timestamp="2026-05-10T00:00:00+00:00",
                attempt=1,
                max_attempts=1,
                duration_ms=0,
                run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
                final=False,  # type: ignore[arg-type]
            )

    def test_failed_event_requires_category_and_issues(self) -> None:
        with pytest.raises(ValidationError):
            FailedEvent(
                contract_name="x",
                timestamp="2026-05-10T00:00:00+00:00",
                attempt=1,
                max_attempts=1,
                duration_ms=0,
                run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
                final=True,
                # category + issues missing
            )  # type: ignore[call-arg]


# ── Wire serialisation ─────────────────────────────────────────────────────


class TestSerialization:
    def test_uses_camel_case_aliases_on_dump(self) -> None:
        event = AcceptedEvent(
            contract_name="x",
            timestamp="2026-05-10T00:00:00+00:00",
            attempt=1,
            max_attempts=3,
            duration_ms=42,
            run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        )
        dumped = event.model_dump(by_alias=True, exclude_none=True)
        assert "contractName" in dumped
        assert "maxAttempts" in dumped
        assert "durationMs" in dumped
        assert "runId" in dumped
        # snake_case never appears on the wire
        assert "contract_name" not in dumped
        assert "max_attempts" not in dumped
        assert "run_id" not in dumped

    def test_schema_alias_avoids_pydantic_collision(self) -> None:
        """The wire field is ``schema``; Pydantic v2 reserves that name
        on BaseModel, so the SDK uses ``schema_`` internally with an
        alias. Confirm the dump uses ``schema``."""
        event = AcceptedEvent(
            contract_name="x",
            timestamp="2026-05-10T00:00:00+00:00",
            attempt=1,
            max_attempts=1,
            duration_ms=0,
            run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            schema_=[SchemaField(name="score", type="number")],
        )
        dumped = event.model_dump(by_alias=True, exclude_none=True)
        assert "schema" in dumped
        assert "schema_" not in dumped

    def test_exclude_none_drops_optional_fields(self) -> None:
        event = AcceptedEvent(
            contract_name="x",
            timestamp="2026-05-10T00:00:00+00:00",
            attempt=1,
            max_attempts=1,
            duration_ms=0,
            run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        )
        dumped = event.model_dump(by_alias=True, exclude_none=True)
        # Optional fields with None values must not appear on the wire.
        for field_name in (
            "environment",
            "input",
            "output",
            "model",
            "capture",
            "schema",
            "rules",
            "clientEventId",
            "sdk",
        ):
            assert field_name not in dumped, field_name

    def test_round_trip_via_json(self) -> None:
        original = FailedEvent(
            contract_name="t",
            timestamp="2026-05-10T00:00:00+00:00",
            attempt=2,
            max_attempts=3,
            duration_ms=10,
            run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            final=False,
            category="RULE_ERROR",
            issues=["score must exceed 70"],
            rule_failures=["hot_requires_high_score"],
        )
        payload = json.loads(json.dumps(original.model_dump(by_alias=True, exclude_none=True)))
        restored = EVENT_ADAPTER.validate_python(payload)
        assert isinstance(restored, FailedEvent)
        assert restored.category == "RULE_ERROR"
        assert restored.rule_failures == ["hot_requires_high_score"]


# ── Constraint enforcement ─────────────────────────────────────────────────


class TestConstraints:
    def test_run_id_pattern_enforced(self) -> None:
        with pytest.raises(ValidationError):
            AcceptedEvent(
                contract_name="x",
                timestamp="t",
                attempt=1,
                max_attempts=1,
                duration_ms=0,
                run_id="not-a-valid-run-id",
            )

    def test_run_id_max_length(self) -> None:
        with pytest.raises(ValidationError):
            AcceptedEvent(
                contract_name="x",
                timestamp="t",
                attempt=1,
                max_attempts=1,
                duration_ms=0,
                run_id="bnd_run_" + "A" * 41,  # body is 41, total 49 > 48 cap
            )

    def test_attempt_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            AcceptedEvent(
                contract_name="x",
                timestamp="t",
                attempt=0,
                max_attempts=1,
                duration_ms=0,
                run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            )

    def test_extra_fields_rejected(self) -> None:
        # extra="forbid" on the base — unknown wire fields fail loud.
        with pytest.raises(ValidationError):
            EVENT_ADAPTER.validate_python(
                {
                    "contractName": "x",
                    "timestamp": "t",
                    "attempt": 1,
                    "maxAttempts": 1,
                    "durationMs": 0,
                    "runId": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
                    "ok": True,
                    "final": True,
                    "garbage": "value",
                }
            )


# ── ResolvedCapture / SdkMeta ─────────────────────────────────────────────


class TestResolvedCapture:
    def test_alias_for_redacted_fields(self) -> None:
        rc = ResolvedCapture(inputs=True, outputs=False, repairs=True, redacted_fields=["ssn"])
        dumped = rc.model_dump(by_alias=True, exclude_none=True)
        assert dumped["redactedFields"] == ["ssn"]
        assert "redacted_fields" not in dumped

    def test_redacted_fields_optional(self) -> None:
        rc = ResolvedCapture(inputs=False, outputs=False, repairs=True)
        dumped = rc.model_dump(by_alias=True, exclude_none=True)
        assert "redactedFields" not in dumped


class TestSdkMeta:
    def test_helper_populates_name_and_runtime(self) -> None:
        meta = sdk_meta("0.0.0")
        assert meta.name == SDK_NAME
        assert meta.version == "0.0.0"
        assert meta.runtime is not None
        assert meta.runtime.startswith("python/")


# ── now_iso ────────────────────────────────────────────────────────────────


class TestNowIso:
    def test_uses_z_utc_designator(self) -> None:
        # Hosted ingest validates timestamps with z.iso.datetime(), whose
        # default form only accepts the `Z` UTC literal — a numeric
        # `+00:00` offset is rejected even though both mean UTC.
        ts = now_iso()
        assert ts.endswith("Z")
        assert "+00:00" not in ts


# ── EventBuilder ───────────────────────────────────────────────────────────


class TestEventBuilder:
    def test_attempt_failure_basic_shape(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0")
        run = _state()
        run.instructions = "Respond with JSON."
        run.repairs = [{"role": "user", "content": "fix it"}]

        event = builder.attempt_failure(
            run=run,
            attempt=1,
            category="VALIDATION_ERROR",
            issues=["bad shape"],
            duration_ms=5,
        )
        assert isinstance(event, FailedEvent)
        assert event.final is False
        assert event.category == "VALIDATION_ERROR"
        assert event.issues == ["bad shape"]
        assert event.input == "Respond with JSON."
        assert event.repairs == [{"role": "user", "content": "fix it"}]
        assert event.run_id == run.run_id
        assert event.contract_name == "lead-scoring"
        assert event.client_event_id is not None and len(event.client_event_id) > 0
        assert event.sdk is not None and event.sdk.name == SDK_NAME

    def test_run_success_basic_shape(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0", environment="production")
        run = _state(model="gpt-4o")
        run.instructions = "Respond with JSON."

        event = builder.run_success(run=run, attempts=1, total_duration_ms=100, data={"score": 42})
        assert isinstance(event, AcceptedEvent)
        assert event.environment == "production"
        assert event.attempt == 1
        assert event.duration_ms == 100
        assert event.output == {"score": 42}
        assert event.model == "gpt-4o"

    def test_run_failure_basic_shape(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0")
        run = _state()

        event = builder.run_failure(
            run=run,
            attempts=3,
            total_duration_ms=1500,
            category="NO_JSON",
            message="Contract failed after 3 attempt(s) [NO_JSON]: ...",
        )
        assert isinstance(event, FailedEvent)
        assert event.final is True
        assert event.category == "NO_JSON"
        assert event.issues == ["Contract failed after 3 attempt(s) [NO_JSON]: ..."]
        assert event.attempt == 3
        # Terminal failures don't carry repairs even if state had them queued.
        assert event.repairs is None

    def test_default_model_used_when_run_has_none(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0", default_model="claude-haiku")
        run = _state(model=None)
        event = builder.run_success(run=run, attempts=1, total_duration_ms=0, data={})
        assert event.model == "claude-haiku"

    def test_run_model_overrides_default(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0", default_model="claude-haiku")
        run = _state(model="gpt-4o")
        event = builder.run_success(run=run, attempts=1, total_duration_ms=0, data={})
        assert event.model == "gpt-4o"

    def test_schema_consumed_only_once(self) -> None:
        """Per the wire upsert semantics, schema/rules ride on only the
        first event of a run. Subsequent events from the same run state
        must omit them."""
        builder = EventBuilder(sdk_version="0.0.0")
        run = _state()
        run._schema = [SchemaField(name="score", type="number")]
        run._rules = [RuleDefinition(name="must-pass")]

        first = builder.run_success(run=run, attempts=1, total_duration_ms=0, data={})
        second = builder.run_success(run=run, attempts=1, total_duration_ms=0, data={})

        assert first.schema_ == [SchemaField(name="score", type="number")]
        assert first.rules == [RuleDefinition(name="must-pass")]
        assert second.schema_ is None
        assert second.rules is None

    def test_pydantic_data_dumps_to_dict(self) -> None:
        from pydantic import BaseModel

        class Out(BaseModel):
            score: int

        builder = EventBuilder(sdk_version="0.0.0")
        run = _state()
        event = builder.run_success(run=run, attempts=1, total_duration_ms=0, data=Out(score=7))
        assert event.output == {"score": 7}

    def test_event_serialises_with_camel_case(self) -> None:
        builder = EventBuilder(sdk_version="0.0.0")
        run = _state()
        event = builder.run_success(run=run, attempts=1, total_duration_ms=10, data={"a": 1})
        dumped = event.model_dump(by_alias=True, exclude_none=True)
        # All required wire fields present in their camelCase form
        for required in (
            "contractName",
            "timestamp",
            "attempt",
            "maxAttempts",
            "durationMs",
            "runId",
            "ok",
            "final",
        ):
            assert required in dumped, required
        assert dumped["ok"] is True
        assert dumped["final"] is True
        assert RUN_ID_PATTERN.match(dumped["runId"]) is not None


# ── rule_failure_names projection ──────────────────────────────────────────


class TestRuleFailureNames:
    def test_returns_none_for_empty_input(self) -> None:
        assert rule_failure_names(None) is None
        assert rule_failure_names([]) is None

    def test_extracts_names(self) -> None:
        from withboundary.contract.types import RuleIssue

        issues = [
            RuleIssue(rule={"name": "must-be-adult"}, message="too young"),  # type: ignore[arg-type]
            RuleIssue(rule={"name": "valid-email"}, message="bad format"),  # type: ignore[arg-type]
        ]
        assert rule_failure_names(issues) == ["must-be-adult", "valid-email"]

    def test_skips_issues_without_rule(self) -> None:
        class Bare:
            rule = None

        assert rule_failure_names([Bare()]) is None  # type: ignore[list-item]
