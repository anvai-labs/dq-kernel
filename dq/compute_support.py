# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""What every compute adapter shares (ADR-010, ADR-014).

The kernel owns the semantics and the decisions; an adapter computes metric
values. Two steps of that are the same whichever engine computes: resolving a
column by its exact name, and turning a bound into an exact fraction. They
live here so that the Spark, Arrow and SQL adapters cannot drift apart in
what they accept, or in what they say when they refuse -- a corpus case that
fails closed must fail closed identically on every adapter.

This module imports nothing optional.
"""

from __future__ import annotations

import decimal
import math
from fractions import Fraction
from typing import Any, Mapping, Sequence

from dq.exceptions import ConfigurationError

#: How many column names a refusal lists before it stops.
MAX_LISTED_COLUMNS = 8


def require_exact_column(
    dataset: str, column: str, names: Sequence[str], fields: Mapping[str, Any]
) -> Any:
    """Resolve one column by exact name, rejecting ambiguity, and return its type.

    Args:
        dataset: The dataset the column is looked up in, for the message.
        column: The column's name, exactly as the plan spells it.
        names: Every column name of the dataset, duplicates included.
        fields: The dataset's columns by name, mapped to whatever represents
            the column's type -- the engine's own.

    Returns:
        The column's type, as ``fields`` holds it.

    Raises:
        ConfigurationError: If there is no column of that exact name, or the
            name is ambiguous: a duplicate, or another column whose name
            differs only in case.
    """
    if column not in fields:
        available = ", ".join(sorted(fields)[:MAX_LISTED_COLUMNS])
        raise ConfigurationError(
            f"column {column!r} in dataset {dataset!r} requires an exact "
            f"case-sensitive match; available columns: {available}"
        )
    duplicates = sum(1 for name in names if name == column) > 1
    if duplicates or any(
        other.lower() == column.lower() for other in fields if other != column
    ):
        raise ConfigurationError(
            f"column {column!r} in dataset {dataset!r} is ambiguous: duplicate or "
            "case-insensitive colliding fields cannot be resolved"
        )
    return fields[column]


def as_exact_fraction(value: Any, dataset: str, column: str):
    """Convert a bound to an exact Fraction; refuse infinite bounds.

    The bound arrives as the Python value the engine gives for its column
    type: an ``int``, a ``Decimal``, a ``float`` or ``None``. A float is
    taken through its shortest decimal, the text that round-trips to it, so
    the fraction is the number the engine showed and not the binary value
    behind it.

    Args:
        value: The bound as the engine returned it.
        dataset: The dataset, for the message.
        column: The column, for the message.

    Returns:
        The exact :class:`~fractions.Fraction`, or ``None`` for no bound.

    Raises:
        ConfigurationError: For an infinite bound, or a value of a type that
            has no exact fraction: a date, text, bytes.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        # A boolean is what an engine returns for a boolean column. It is an
        # int to Python, and is read as 0 or 1 -- as Spark reads it.
        return Fraction(int(value))
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, decimal.Decimal):
        return Fraction(value)
    if isinstance(value, float):
        if math.isinf(value):
            raise ConfigurationError(
                f"column {column!r} in dataset {dataset!r} has an infinite "
                "bound; ranges/v1 bounds must be finite"
            )
        return Fraction(decimal.Decimal(str(value)))
    raise ConfigurationError(
        f"column {column!r} in dataset {dataset!r} produced a bound of an "
        f"unsupported type: {type(value).__name__}"
    )
