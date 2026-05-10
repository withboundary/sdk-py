"""Package metadata helpers."""

from __future__ import annotations

import platform

from withboundary.sdk import __version__
from withboundary.sdk._meta import SDK_NAME, runtime, user_agent


def test_sdk_name_is_stable_string() -> None:
    """SDK_NAME is the wire identifier the dashboard groups by — it
    must not change across releases without a deliberate decision."""
    assert SDK_NAME == "withboundary-sdk-python"


def test_runtime_includes_python_version() -> None:
    rt = runtime()
    assert rt.startswith("python/")
    assert platform.python_version() in rt


def test_user_agent_format() -> None:
    """The User-Agent header is what reverse proxies see; make sure it
    includes the SDK name, the package version, and the runtime
    descriptor in that order so log greps stay stable."""
    ua = user_agent()
    assert ua.startswith(f"{SDK_NAME}/{__version__}")
    assert "python/" in ua
