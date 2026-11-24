# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Strict-JSON sanitization shared by engine result extraction.

Outcome details cross the CheckOutcome boundary, which accepts strict-JSON
native types only and caps payloads structurally. Engine result payloads
(GX validation results, DQX failure rows) can carry tuples, numpy scalars,
bytes, dates, non-finite floats, and raw row values -- every one of which
must be normalized before the boundary sees them.
"""

import datetime
import math
from collections.abc import Mapping
from typing import Any

# Outcome diagnostics are capped at depth 16 by dq.outcomes, and engines
# wrap details two levels deep ({check, success, details} -> details) before
# that gate counts, so the sanitizer must stop two levels earlier. Strings
# and lists are also truncated well inside the 1 MiB per-outcome byte budget.
MAX_SANITIZE_DEPTH = 14
MAX_SANITIZE_STRING = 65_536
MAX_SANITIZE_LIST = 2_000


def to_strict_json(value: Any, depth: int = 0) -> Any:
    """Normalize an engine result payload to strict-JSON native types.

    Args:
        value: Arbitrary payload fragment.
        depth: Current nesting depth (defensive recursion bound).

    Returns:
        The payload rebuilt from JSON-native types only. Unknown objects
        fall back to their string form.
    """
    if depth > MAX_SANITIZE_DEPTH:
        return str(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        if len(value) <= MAX_SANITIZE_STRING:
            return value
        return value[:MAX_SANITIZE_STRING] + "...[truncated]"
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, bytes):
        return value[:MAX_SANITIZE_STRING].decode("utf-8", "replace")
    if isinstance(value, Mapping):
        return {
            str(key): to_strict_json(item, depth + 1) for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        if len(value) <= MAX_SANITIZE_LIST:
            return [to_strict_json(item, depth + 1) for item in value]
        truncated = [
            to_strict_json(item, depth + 1) for item in list(value)[:MAX_SANITIZE_LIST]
        ]
        return truncated + ["...[truncated]"]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return to_strict_json(item(), depth + 1)
        except Exception:  # noqa: BLE001 - numpy-like scalar unwrap
            pass
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)
