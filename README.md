# withboundary-sdk

[![PyPI version](https://img.shields.io/pypi/v/withboundary-sdk.svg)](https://pypi.org/project/withboundary-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/withboundary-sdk.svg)](https://pypi.org/project/withboundary-sdk/)
[![License](https://img.shields.io/pypi/l/withboundary-sdk.svg)](https://github.com/withboundary/sdk-py/blob/main/LICENSE)

The observability SDK for [Boundary](https://withboundary.com).

`withboundary-sdk` plugs into the `withboundary-contract` engine and streams every LLM contract run — successes, failures, repairs, retries — to your Boundary dashboard. Batched, redactable, resilient. Works the same in sync and async codebases.

## Install

```bash
pip install withboundary-sdk
```

Requires Python 3.10+ and a Boundary API key.

## Quick example

```python
from withboundary.contract import define_contract
from withboundary.sdk import create_boundary_logger
from pydantic import BaseModel

logger = create_boundary_logger(
    api_key="bnd_live_sk_...",       # or set BOUNDARY_API_KEY
    environment="production",
)

class Lead(BaseModel):
    tier: str
    score: int

contract = define_contract(
    name="lead-scoring",
    schema=Lead,
    logger=logger,
)

result = contract.accept(call_llm)
```

That's it. Every attempt — successful or otherwise — flows to the dashboard in the background.

## Sync and async

Pick the factory that matches your app's concurrency model:

```python
from withboundary.sdk import create_boundary_logger        # sync
from withboundary.sdk import create_async_boundary_logger  # async
```

Both return objects implementing the same `ContractLogger` Protocol — pass either to `define_contract(logger=...)`.

## What the SDK does for you

- **Background batching** — events queue in memory and flush by size or time, whichever comes first.
- **Redaction** — three composable layers: by field name, by regex, or via a custom callable.
- **Capture policy** — opt in to shipping prompts and outputs; opt out to keep only metadata + repair messages.
- **Resilient delivery** — exponential backoff, 429 `Retry-After` honor, circuit breaker for the ingest endpoint.
- **Graceful shutdown** — `flush_on_exit=True` registers an `atexit` handler so events drain on process exit. For serverless, call `await logger.flush(timeout=...)` at the end of each request.
- **No-op when unconfigured** — `create_boundary_logger()` returns `None` if no API key + no custom sink, so dev environments stay zero-impact.

## Status

Pre-release scaffolding. The first PyPI release will land as `0.1.0` once the public API surface stabilises.

## License

MIT
