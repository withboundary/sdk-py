"""Observability SDK for Boundary.

Streams contract runs from `withboundary-contract` to the hosted dashboard at
``https://api.withboundary.com``. The high-level entry points are
:func:`create_boundary_logger` and :func:`create_async_boundary_logger`;
both return objects implementing the ``ContractLogger`` Protocol so the
SDK plugs into ``define_contract(logger=...)`` with no glue code.

The module re-exports the configuration dataclasses, event types, and
identifier helpers from a stable namespace so applications building
custom transports or sinks can import without reaching into private
modules.
"""

__version__ = "0.1.0"

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
from .factory import create_async_boundary_logger, create_boundary_logger
from .identifiers import mint_event_id, mint_run_id
from .logger import AsyncBoundaryLogger, SyncBoundaryLogger
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
    "AsyncBoundaryLogger",
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
    "SyncBoundaryLogger",
    "SyncEventQueue",
    "SyncIngestTransport",
    "TransportError",
    "__version__",
    "apply_capture",
    "apply_redaction",
    "create_async_boundary_logger",
    "create_boundary_logger",
    "make_redaction",
    "mint_event_id",
    "mint_run_id",
]
