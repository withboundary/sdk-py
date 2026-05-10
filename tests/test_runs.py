"""Per-run state and the registry that holds it."""

from __future__ import annotations

import threading
from typing import Any

from withboundary.contract.types import RuleDefinition, SchemaField

from withboundary.sdk import PerRunRegistry, PerRunState


def _state(**overrides: Any) -> PerRunState:
    base: dict[str, Any] = {
        "contract_name": "lead-scoring",
        "run_handle": "rh_one",
        "run_id": "bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
        "started_at": 0.0,
        "max_attempts": 3,
        "model": None,
    }
    base.update(overrides)
    return PerRunState(**base)


# ── PerRunState ────────────────────────────────────────────────────────────


class TestPerRunState:
    def test_from_run_start_populates_schema_and_rules(self) -> None:
        schema = [SchemaField(name="score", type="number")]
        rules = [RuleDefinition(name="must-pass")]
        state = PerRunState.from_run_start(
            contract_name="t",
            run_handle="rh_x",
            run_id="bnd_run_AAAAAAAAAAAAAAAAAAAAA1",
            started_at=1.0,
            max_attempts=3,
            model="gpt-4o",
            schema=schema,
            rules=rules,
        )
        assert state.consume_schema() == schema
        assert state.consume_rules() == rules

    def test_consume_schema_is_one_shot(self) -> None:
        state = _state()
        state._schema = [SchemaField(name="x", type="number")]
        first = state.consume_schema()
        second = state.consume_schema()
        assert first is not None
        assert second is None

    def test_consume_rules_is_one_shot(self) -> None:
        state = _state()
        state._rules = [RuleDefinition(name="r")]
        assert state.consume_rules() == [RuleDefinition(name="r")]
        assert state.consume_rules() is None

    def test_no_schema_returns_none(self) -> None:
        state = _state()
        assert state.consume_schema() is None
        assert state.consume_rules() is None

    def test_repairs_default_empty_list(self) -> None:
        state = _state()
        assert state.repairs == []

    def test_instructions_default_empty(self) -> None:
        state = _state()
        assert state.instructions == ""


# ── PerRunRegistry ─────────────────────────────────────────────────────────


class TestPerRunRegistry:
    def test_register_and_get(self) -> None:
        registry = PerRunRegistry()
        state = _state(run_handle="rh_a")
        registry.register(state)
        assert registry.get("rh_a") is state

    def test_get_missing_returns_none(self) -> None:
        assert PerRunRegistry().get("nope") is None

    def test_pop_removes_state(self) -> None:
        registry = PerRunRegistry()
        state = _state(run_handle="rh_a")
        registry.register(state)
        assert registry.pop("rh_a") is state
        assert registry.get("rh_a") is None
        assert "rh_a" not in registry

    def test_pop_missing_returns_none(self) -> None:
        assert PerRunRegistry().pop("nope") is None

    def test_len_tracks_live_runs(self) -> None:
        registry = PerRunRegistry()
        assert len(registry) == 0
        registry.register(_state(run_handle="a"))
        registry.register(_state(run_handle="b"))
        assert len(registry) == 2
        registry.pop("a")
        assert len(registry) == 1

    def test_contains(self) -> None:
        registry = PerRunRegistry()
        registry.register(_state(run_handle="a"))
        assert "a" in registry
        assert "missing" not in registry

    def test_concurrent_register_thread_safe(self) -> None:
        """Sanity-check the lock — 100 threads each registering 10
        unique handles should all show up."""
        registry = PerRunRegistry()

        def worker(thread_id: int) -> None:
            for j in range(10):
                handle = f"t{thread_id}_{j}"
                registry.register(
                    _state(run_handle=handle, run_id=f"bnd_run_{handle:>22.22}".ljust(30, "A"))
                )

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(registry) == 1000
