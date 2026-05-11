"""Public factory for the synchronous Boundary logger.

The factory takes a wide kwarg surface so users only specify the
options they care about; everything else falls back to sensible
defaults. Returns ``None`` when neither an API key nor a custom
``write`` sink is configured — the dev-mode safe path. Callers can
wire the factory unconditionally and still get a no-op in
unconfigured environments.

The async sibling factory (``create_async_boundary_logger``) lands
in a follow-up phase alongside the async logger.
"""

from __future__ import annotations

import os
from collections.abc import Callable

import httpx

from ._meta import __version__
from .batcher.sync import SyncBatcher
from .config import (
    BatchOptions,
    BreakerOptions,
    CapturePolicy,
    RedactionOptions,
    RetryOptions,
)
from .events import BoundaryEvent, EventBuilder
from .lifecycle import register_atexit
from .logger.sync import SyncBoundaryLogger
from .transport.sync import SyncIngestTransport

DEFAULT_ENDPOINT = "https://api.withboundary.com"
"""The hosted ingest base URL. Override via the ``endpoint`` kwarg
for self-hosted deployments or staging environments."""

API_KEY_ENV_VAR = "BOUNDARY_API_KEY"
"""Environment variable consulted when no ``api_key`` kwarg is
supplied. The dashboard's onboarding flow exports this so a single
``pip install`` + env-var pair gets users to first event."""


def create_boundary_logger(
    *,
    api_key: str | None = None,
    environment: str | None = None,
    model: str | None = None,
    endpoint: str = DEFAULT_ENDPOINT,
    batch: BatchOptions | None = None,
    capture: CapturePolicy | None = None,
    redact: RedactionOptions | None = None,
    retry: RetryOptions | None = None,
    breaker: BreakerOptions | None = None,
    before_send: Callable[[BoundaryEvent], BoundaryEvent | None] | None = None,
    write: Callable[[list[BoundaryEvent]], None] | None = None,
    flush_on_exit: bool = True,
    on_error: Callable[[Exception], None] | None = None,
    http_client: httpx.Client | None = None,
) -> SyncBoundaryLogger | None:
    """Build a synchronous ``ContractLogger`` ready to plug into
    ``define_contract(logger=...)``.

    Returns ``None`` when neither ``api_key`` (or the
    ``BOUNDARY_API_KEY`` env var) nor a custom ``write`` sink is
    configured. That makes it safe to wire unconditionally in code
    that may run in unconfigured environments (CI, local dev without
    a key).

    Parameters
    ----------
    api_key
        Boundary API key. Falls back to ``BOUNDARY_API_KEY`` env var.
    environment
        Optional environment label (e.g. ``"production"``).
    model
        Default LLM model name; per-contract or per-event overrides
        win where set.
    endpoint
        Hosted ingest base URL. Defaults to the production endpoint.
    batch / capture / redact / retry / breaker
        Configuration overrides. Each falls back to the defaults
        defined alongside its dataclass.
    before_send
        Last-chance hook to transform or drop events. Return ``None``
        to drop; return a modified event to substitute.
    write
        Optional sink that receives every batch alongside the HTTP
        transport. Useful for piping into existing logging
        infrastructure.
    flush_on_exit
        Register an ``atexit`` handler that drains the queue on
        process exit (5s timeout). Defaults to ``True``.
    on_error
        Receives every error the transport / batcher surfaces.
        Defaults to a one-shot stderr warning.
    http_client
        Inject a pre-built ``httpx.Client`` for connection pooling
        or test doubles.
    """
    resolved_key = api_key or os.environ.get(API_KEY_ENV_VAR)

    # Dev-mode safe path: with no key and no custom sink there's
    # nothing for the SDK to do. Return None so users can wire the
    # factory unconditionally.
    if not resolved_key and write is None:
        return None

    transport: SyncIngestTransport | None = None
    if resolved_key:
        transport = SyncIngestTransport(
            endpoint=endpoint,
            api_key=resolved_key,
            retry=retry,
            breaker=breaker,
            client=http_client,
        )

    batcher = SyncBatcher(
        transport=transport,
        options=batch,
        write=write,
        before_send=before_send,
        on_error=on_error,
    )

    builder = EventBuilder(
        sdk_version=__version__,
        environment=environment,
        default_model=model,
    )

    logger = SyncBoundaryLogger(
        batcher=batcher,
        builder=builder,
        capture=capture,
        redact=redact,
    )

    if flush_on_exit:
        register_atexit(logger)

    return logger


__all__ = [
    "API_KEY_ENV_VAR",
    "DEFAULT_ENDPOINT",
    "create_boundary_logger",
]
