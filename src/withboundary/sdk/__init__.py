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

from .batcher import AsyncBatcher, SyncBatcher
from .capture import apply_capture
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
from .queue import AsyncEventQueue, EventQueue, SyncEventQueue
from .redact import apply_redaction
from .runs import PerRunRegistry, PerRunState
from .transport import (
    AuthError,
    BreakerOpenError,
    BreakerState,
    CircuitBreaker,
    IngestError,
    NonRetryableStatusError,
    RateLimitError,
    TransportError,
)
from .transport.async_ import AsyncIngestTransport
from .transport.sync import SyncIngestTransport

__all__ = [
    "REDACT",
    "AcceptedEvent",
    "AsyncBatcher",
    "AsyncEventQueue",
    "AsyncIngestTransport",
    "AuthError",
    "BatchOptions",
    "BoundaryEvent",
    "BreakerOpenError",
    "BreakerOptions",
    "BreakerState",
    "CapturePolicy",
    "CircuitBreaker",
    "EventBuilder",
    "EventQueue",
    "FailedEvent",
    "IngestError",
    "NonRetryableStatusError",
    "PerRunRegistry",
    "PerRunState",
    "RateLimitError",
    "RedactionOptions",
    "ResolvedCapture",
    "RetryOptions",
    "SdkMeta",
    "SyncBatcher",
    "SyncEventQueue",
    "SyncIngestTransport",
    "TransportError",
    "__version__",
    "apply_capture",
    "apply_redaction",
    "make_redaction",
    "mint_event_id",
    "mint_run_id",
]
