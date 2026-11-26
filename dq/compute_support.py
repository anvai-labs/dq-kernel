# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""What every compute adapter shares (ADR-010, ADR-014).

The kernel owns the semantics and the decisions; an adapter computes metric
values. Everything around that computation is the same whichever engine
computes: refusing bindings that do not name exactly the plan's datasets,
resolving a column by its exact name, turning a bound into an exact fraction,
and the order of a plan's execution -- validate against the adapter's
capabilities, check the bindings, compute the values, hand them to the
kernel's own evaluation. All of it lives here, so the Spark, Arrow and SQL
adapters cannot drift apart in what they accept or in what they say when
they refuse: a corpus case that fails closed must fail closed identically on
every adapter.

An adapter is three callables (:class:`ComputeAdapter`). What it is beyond
them -- which engine it drives -- is its own module's business.

This module imports nothing optional.
"""

from __future__ import annotations

import decimal
import math
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Callable, Iterable, Mapping, Sequence

from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import CapabilitySet, ExecutionPlan

#: How many column names a refusal lists before it stops.
MAX_LISTED_COLUMNS = 8


@dataclass(frozen=True)
class ComputeAdapter:
    """What a compute adapter offers a plan.

    Attributes:
        capabilities_for: The adapter's capabilities for a plan's semantics.
        validate_bindings: ``(plan, datasets) -> bindings``: the plan's
            datasets by name as the engine's own frame, or a refusal.
        compute_values: ``(plan, bindings) -> {MetricKey: value}``: the
            plan's metric values, as the kernel requires them.
    """

    capabilities_for: Callable[[ExecutionPlan], CapabilitySet]
    validate_bindings: Callable[[ExecutionPlan, Mapping[str, Any]], dict]
    compute_values: Callable[[ExecutionPlan, dict], dict]


def execute_plan_with(
    plan: ExecutionPlan, datasets: Mapping[str, Any], adapter: ComputeAdapter
) -> tuple[CheckOutcome, ...]:
    """Compute a plan's metrics with an adapter and evaluate exact outcomes."""
    capabilities = adapter.capabilities_for(plan)
    plan.validate_for(capabilities)
    bindings = adapter.validate_bindings(plan, datasets)
    values = adapter.compute_values(plan, bindings)
    return plan.evaluate(values, capabilities)


def execute_envelope_with(
    envelope, datasets: Mapping[str, Any], adapter: ComputeAdapter
) -> tuple[CheckOutcome, ...]:
    """Execute every subplan of a :class:`~dq.envelope.RulesetEnvelope`.

    Mixed-semantics rulesets get one entry point: each subplan's metrics are
    computed with its own semantics and capabilities, and the envelope
    dispatches evaluation into one aggregate verdict (ADR-004/005).
    """
    from dq.envelope import RulesetEnvelope

    if not isinstance(envelope, RulesetEnvelope):
        raise ConfigurationError(
            "execute_envelope requires a RulesetEnvelope, not a bare plan; "
            "use execute_plan for single-semantics execution"
        )
    capabilities_by_version = {}
    values = {}
    for plan in envelope.plans:
        capabilities = adapter.capabilities_for(plan)
        plan.validate_for(capabilities)
        capabilities_by_version.setdefault(plan.semantic_version, capabilities)
        bindings = adapter.validate_bindings(plan, datasets)
        values.update(adapter.compute_values(plan, bindings))
    return tuple(envelope.evaluate(values, capabilities_by_version))


def require_exact_bindings(expected: Iterable[str], supplied: Iterable[str]) -> None:
    """Refuse bindings that do not name exactly a plan's datasets.

    Raises:
        ConfigurationError: When a dataset the plan requires is not bound, or
            a binding names a dataset the plan does not require.
    """
    expected, supplied = set(expected), set(supplied)
    missing = sorted(expected - supplied)
    unknown = sorted(supplied - expected)
    if missing or unknown:
        detail = []
        if missing:
            detail.append(f"missing {missing}")
        if unknown:
            detail.append(f"unknown {unknown}")
        raise ConfigurationError(
            "dataset bindings must exactly match plan datasets: " + "; ".join(detail)
        )


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
