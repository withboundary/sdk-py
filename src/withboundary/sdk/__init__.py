"""Observability SDK for Boundary.

Streams contract runs from `withboundary-contract` to the hosted dashboard at
``https://api.withboundary.com``. The high-level entry points
``create_boundary_logger`` and ``create_async_boundary_logger`` land in
subsequent releases. This module currently exposes the configuration
dataclasses and identifier helpers so other modules (and downstream
applications building custom transports) can import a stable surface
without reaching into private modules.
"""

__version__ = "0.0.0"

from .config import (
    REDACT,
    BatchOptions,
    BreakerOptions,
    CapturePolicy,
    RedactionOptions,
    RetryOptions,
    make_redaction,
)
from .events import (
    AcceptedEvent,
    BoundaryEvent,
    EventBuilder,
    FailedEvent,
    ResolvedCapture,
    SdkMeta,
)
from .identifiers import mint_event_id, mint_run_id
from .runs import PerRunRegistry, PerRunState

__all__ = [
    "REDACT",
    "AcceptedEvent",
    "BatchOptions",
    "BoundaryEvent",
    "BreakerOptions",
    "CapturePolicy",
    "EventBuilder",
    "FailedEvent",
    "PerRunRegistry",
    "PerRunState",
    "RedactionOptions",
    "ResolvedCapture",
    "RetryOptions",
    "SdkMeta",
    "__version__",
    "make_redaction",
    "mint_event_id",
    "mint_run_id",
]
