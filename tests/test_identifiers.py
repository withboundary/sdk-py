"""Run id and event id minting."""

from __future__ import annotations

import re

from withboundary.sdk.identifiers import (
    EVENT_ID_BYTES,
    RUN_ID_BODY_LENGTH,
    RUN_ID_PREFIX,
    mint_event_id,
    mint_run_id,
)

# Mirror of the validator regex on the hosted ingest endpoint. Run ids that
# don't match this regex are rejected with HTTP 400.
RUN_ID_PATTERN = re.compile(r"^bnd_run_[A-Za-z0-9_-]{1,40}$")


class TestRunId:
    def test_matches_wire_validator_regex(self) -> None:
        run_id = mint_run_id()
        assert RUN_ID_PATTERN.match(run_id) is not None, run_id

    def test_has_expected_prefix_and_length(self) -> None:
        run_id = mint_run_id()
        assert run_id.startswith(RUN_ID_PREFIX)
        body = run_id.removeprefix(RUN_ID_PREFIX)
        assert len(body) == RUN_ID_BODY_LENGTH

    def test_unique_across_calls(self) -> None:
        # 1000 mints should never collide; if they do something is wrong
        # with the entropy source.
        ids = {mint_run_id() for _ in range(1000)}
        assert len(ids) == 1000

    def test_uses_url_safe_alphabet_only(self) -> None:
        body = mint_run_id().removeprefix(RUN_ID_PREFIX)
        assert re.fullmatch(r"[A-Za-z0-9_-]+", body), body

    def test_fits_under_validator_max_length(self) -> None:
        # Validator caps run_id at 48 chars; we should leave headroom.
        assert len(mint_run_id()) <= 40


class TestEventId:
    def test_url_safe_alphabet_only(self) -> None:
        event_id = mint_event_id()
        assert re.fullmatch(r"[A-Za-z0-9_-]+", event_id), event_id

    def test_unique_across_calls(self) -> None:
        ids = {mint_event_id() for _ in range(1000)}
        assert len(ids) == 1000

    def test_fits_under_wire_max_length(self) -> None:
        # clientEventId on the wire is capped at 64 chars.
        assert len(mint_event_id()) <= 64

    def test_has_meaningful_entropy(self) -> None:
        # token_urlsafe(12) yields 16 chars; the constant is documented.
        assert EVENT_ID_BYTES == 12
        # Verify the actual length matches what token_urlsafe produces.
        assert len(mint_event_id()) >= 16
