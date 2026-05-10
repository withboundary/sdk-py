"""Package metadata stamped on every wire event.

The ingest endpoint accepts an optional ``sdk`` block on every event so the
hosted dashboard can attribute traffic by client. We populate the block on
every emitted event from this single source of truth — the SDK name, its
version, and a runtime descriptor that includes the Python interpreter
version. Keeping this in one tiny module means the version source-of-truth
in ``__init__.py::__version__`` is the only thing that needs to move on
release.
"""

from __future__ import annotations

import platform

from . import __version__

SDK_NAME = "withboundary-sdk-python"
"""Stable identifier for this distribution. Sent verbatim in the ``sdk.name``
field on every event so dashboard rollups can split traffic by client SDK."""


def runtime() -> str:
    """Return a short descriptor of the Python interpreter executing this
    process.

    Format: ``"python/<version>"`` (e.g. ``"python/3.12.3"``). Sent in the
    ``sdk.runtime`` field on every wire event so support engineers can
    correlate dashboard issues with interpreter quirks without needing the
    user to dig the version out of their environment.
    """
    return f"python/{platform.python_version()}"


def user_agent() -> str:
    """Build the HTTP ``User-Agent`` string for outbound ingest requests.

    Format: ``"<sdk>/<version> <runtime>"`` — matches the conventions
    used by other observability clients so reverse-proxy logs and CDN
    rate-limit dashboards can identify the SDK at a glance.
    """
    return f"{SDK_NAME}/{__version__} {runtime()}"


__all__ = [
    "SDK_NAME",
    "runtime",
    "user_agent",
]
