# withboundary-sdk

[![PyPI version](https://img.shields.io/pypi/v/withboundary-sdk.svg)](https://pypi.org/project/withboundary-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/withboundary-sdk.svg)](https://pypi.org/project/withboundary-sdk/)
[![License](https://img.shields.io/pypi/l/withboundary-sdk.svg)](https://github.com/withboundary/sdk-py/blob/main/LICENSE)

The observability SDK for [Boundary](https://withboundary.com).

`withboundary-sdk` plugs into the [`withboundary-contract`](https://pypi.org/project/withboundary-contract/) engine and streams every LLM contract run — successes, failures, repairs, retries — to your Boundary dashboard. Batched, redactable, resilient. Same options shape in sync and async codebases.

## Install

```bash
pip install withboundary-sdk
```

Requires Python 3.10+. The factory returns `None` when no API key is configured, so installing without `BOUNDARY_API_KEY` is a safe no-op in development.

## Quick example

```python
from pydantic import BaseModel, Field
from withboundary.contract import ContractAttempt, define_contract
from withboundary.sdk import create_boundary_logger


class Lead(BaseModel):
    score: int = Field(ge=0, le=100)
    tier: str


logger = create_boundary_logger(
    api_key="bnd_live_sk_...",   # or set BOUNDARY_API_KEY
    environment="production",
)

contract = define_contract(name="lead-scoring", schema=Lead, logger=logger)


def call_llm(ctx: ContractAttempt) -> str:
    # Hand ctx.instructions to your LLM provider; return raw text.
    ...


result = contract.accept(call_llm)
```

Every attempt flows to the dashboard in the background. Successful runs land as `AcceptedEvent`; each failed attempt lands as a mid-run `FailedEvent(final=False)`, and the terminal outcome lands as either `AcceptedEvent` or `FailedEvent(final=True)`.

## Sync and async

Pick the factory that matches your app's concurrency model:

```python
from withboundary.sdk import create_boundary_logger         # sync
from withboundary.sdk import create_async_boundary_logger   # async
```

Both return objects implementing the same `ContractLogger` Protocol — pass either to `define_contract(logger=...)`. The async logger uses `httpx.AsyncClient` under the hood and integrates with `aaccept`:

```python
logger = create_async_boundary_logger(api_key="bnd_live_sk_...")
contract = define_contract(name="qa", schema=Reply, logger=logger)
result = await contract.aaccept(call_llm)
await logger.shutdown(timeout=2.0)
```

## Configuration

```python
create_boundary_logger(
    api_key=...,           # str | None         BOUNDARY_API_KEY fallback
    environment=...,       # str | None         labeled on every event
    model=...,             # str | None         default model when contracts don't specify one
    endpoint=...,          # str                hosted ingest URL
    batch=BatchOptions(...),
    capture=CapturePolicy(...),
    redact=RedactionOptions(...),
    retry=RetryOptions(...),
    breaker=BreakerOptions(...),
    before_send=...,       # event hook         transform/drop events pre-dispatch
    write=...,             # list[event] hook   route batches to a custom sink
    flush_on_exit=True,    # bool               atexit handler for graceful shutdown (sync only)
    on_error=...,          # Exception hook     error reporting
    http_client=...,       # httpx.Client       inject your own client for pooling/proxies
)
```

| Group | Options | Notes |
| --- | --- | --- |
| `BatchOptions` | `size`, `interval`, `max_queue_size` | Size or interval triggers a flush, whichever comes first |
| `CapturePolicy` | `inputs`, `outputs`, `repairs` | Gates the three sensitive fields. Defaults: `repairs=True`, others `False` |
| `RedactionOptions` | `fields`, `patterns`, `custom` | Three layers, composable |
| `RetryOptions` | `max_attempts`, `base_ms`, `timeout` | Honors `Retry-After` on 429 |
| `BreakerOptions` | `threshold`, `cooldown` | Circuit breaker for the ingest endpoint |

## Capture policy

By default the SDK sends metadata and repair messages but **not** the prompt or model output. Opt in field-by-field:

```python
from withboundary.sdk import CapturePolicy, create_boundary_logger

logger = create_boundary_logger(
    api_key="bnd_live_sk_...",
    capture=CapturePolicy(inputs=True, outputs=True, repairs=True),
)
```

Every event carries a `capture` snapshot describing the policy that produced it, so the dashboard can distinguish "field not captured by policy" from "model returned nothing".

## Redaction

Three composable layers run on every outbound event before it leaves the process:

```python
import re
from withboundary.sdk import REDACT, make_redaction, create_boundary_logger

def scrub_short_strings(value, path):
    if isinstance(value, str) and len(value) < 3:
        return REDACT          # drops the leaf entirely
    return value

logger = create_boundary_logger(
    api_key="bnd_live_sk_...",
    redact=make_redaction(
        fields=["email", "ssn"],                                    # by key name
        patterns=[re.compile(r"[\w.+-]+@[\w.-]+\.[a-z]{2,}")],      # by regex
        custom=scrub_short_strings,                                  # by callable
    ),
)
```

Layer order: field names → regex patterns → custom callable. Each event records the scrubbed paths in `capture.redacted_fields`.

## Custom destinations

The `write` sink runs in parallel with the HTTP transport (or replaces it when no API key is configured):

```python
def write(events):
    for event in events:
        my_pipeline.send(event.model_dump(by_alias=True, exclude_none=True))

logger = create_boundary_logger(
    api_key="bnd_live_sk_...",
    write=write,
)
```

`before_send` lets you annotate or filter events one at a time:

```python
def tag_deployment(event):
    return event.model_copy(update={"environment": "deploy-abc123"})

logger = create_boundary_logger(api_key="bnd_live_sk_...", before_send=tag_deployment)
```

Return `None` from `before_send` to drop the event.

## Serverless / Lambda

The background drain doesn't help in a frozen container. Call `flush(timeout)` at the end of each request:

```python
logger = create_boundary_logger(api_key="bnd_live_sk_...", flush_on_exit=False)

def lambda_handler(event, context):
    result = contract.accept(call_llm)
    logger.flush(timeout=2.0)
    return {"statusCode": 200, "body": result.model_dump_json()}
```

Async runtimes mirror the pattern with `await logger.flush(timeout=2.0)`.

## Resilient delivery

The transport ships events to the hosted ingest endpoint with:

- **Exponential backoff** on 5xx and network errors
- **429 `Retry-After`** honored verbatim (seconds or HTTP-date, capped)
- **413 split-and-retry** when a batch exceeds the per-request cap
- **401 / 403** disables the logger immediately and surfaces the error once via `on_error`
- **Circuit breaker** opens after consecutive failures so a degraded endpoint doesn't burn the queue

Every event carries a `client_event_id` so retransmissions are idempotent at ingest.

## No-op when unconfigured

```python
logger = create_boundary_logger()   # no api_key, no write sink → returns None
```

Useful in CI or local dev: contract code can pass `logger=create_boundary_logger()` unconditionally and contract-py treats `None` as "no observability wired", with zero overhead.

## Examples

Five runnable scripts live under [`examples/`](./examples). Each runs offline by swapping the HTTP transport for a recording `write` sink:

- [`quickstart_sync.py`](./examples/quickstart_sync.py)
- [`quickstart_async.py`](./examples/quickstart_async.py)
- [`redaction.py`](./examples/redaction.py)
- [`custom_sink.py`](./examples/custom_sink.py)
- [`serverless_flush.py`](./examples/serverless_flush.py)

## License

MIT — see [LICENSE](./LICENSE).
