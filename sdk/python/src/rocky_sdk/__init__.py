"""rocky-sdk: a typed Python client for the Rocky SQL transformation engine.

The SDK wraps the ``rocky`` CLI binary (subprocess + ``--output json``) behind
a typed :class:`~rocky_sdk.client.RockyClient`. Each method builds the right
argv, runs the binary, parses the JSON output, and returns a Pydantic model
from :mod:`rocky_sdk.types`. Errors surface as
:class:`~rocky_sdk.exceptions.RockyError` subclasses.

For human Python callers — notebooks, scripts, and orchestrators. The
``dagster-rocky`` integration is a thin Dagster adapter over this client.
For AI agents use ``rocky mcp``; for a language-agnostic HTTP surface use
``rocky serve``.

    from rocky_sdk import RockyClient

    client = RockyClient(config_path="rocky.toml")
    result = client.compile()
    run = client.run(filter="tenant=acme", log_callback=print)
"""

from __future__ import annotations

from rocky_sdk.client import RockyClient
from rocky_sdk.exceptions import (
    RockyBinaryNotFoundError,
    RockyCommandError,
    RockyError,
    RockyOutputParseError,
    RockyPartialFailure,
    RockyTimeoutError,
    RockyVersionError,
)
from rocky_sdk.types import *  # noqa: F401,F403  (re-export the full typed surface)
from rocky_sdk.types import __all__ as _types_all
from rocky_sdk.types import parse_rocky_output

__all__ = [
    "RockyClient",
    "RockyError",
    "RockyBinaryNotFoundError",
    "RockyCommandError",
    "RockyPartialFailure",
    "RockyTimeoutError",
    "RockyVersionError",
    "RockyOutputParseError",
    "parse_rocky_output",
    *_types_all,
]
