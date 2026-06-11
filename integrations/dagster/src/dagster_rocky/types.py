"""Backward-compat shim — Rocky result types now live in the ``rocky-sdk`` package.

The Pydantic models for every Rocky CLI command's ``--output json`` payload moved
to :mod:`rocky_sdk.types` (the standalone SDK that ``dagster-rocky`` depends on).
This module re-exports the full surface so existing imports such as
``from dagster_rocky.types import RunResult`` / ``parse_rocky_output`` keep
working unchanged. New code can import from ``rocky_sdk`` directly.
"""

from __future__ import annotations

from rocky_sdk import types as _sdk_types
from rocky_sdk.types import *  # noqa: F401, F403  (re-export the full typed surface)

# Mirror the SDK's public surface so ``from dagster_rocky.types import *`` and
# tooling that reads ``__all__`` see exactly the same names.
__all__ = list(_sdk_types.__all__)
