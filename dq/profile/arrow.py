# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Arrow profiler (SPEC-019).

Sums up a pyarrow table as a :class:`dq.profile.profile.Profile`, in
memory, importing no pyarrow until it is used.

Each column is brought to the form the Arrow adapter computes on first
(``dq.arrow_adapter.canonical``), so that a dictionary-encoded column is
decoded, an odd width widened and signed zeros made one value. A nested
column, and a column of a type the profile does not know, is typed and
counted for nulls, and nothing more, by both profilers: Arrow cannot count
the distinct values of a nested column, and a type the profile has no
family for has no measure to ask for. So no column fails a profile.
"""

from __future__ import annotations

from typing import Any, Optional

from dq.exceptions import ConfigurationError
from dq.profile.profile import (
    BINARY,
    BOUNDED,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    LENGTHENED,
    NESTED,
    TEXT,
    TIMESTAMP,
    UNKNOWN,
    Column,
    Profile,
    moment,
    named_twice,
    spell,
)

_EXTRA = ("pyarrow>=16,<27", "profiler")


def _arrow():
    """Return ``(pyarrow, pyarrow.compute)``, or name the extra that provides them."""
    from dq.utils.imports import require_extra

    pin, kind = _EXTRA
    pyarrow = require_extra("pyarrow", "pyarrow", "arrow", pin=pin, kind=kind)
    compute = require_extra("pyarrow.compute", "pyarrow", "arrow", pin=pin, kind=kind)
    return pyarrow, compute


def family_of(kind) -> tuple:
    """Return an Arrow type's ``(family, precision, scale)`` in the profile's words.

    Args:
        kind: A ``pyarrow.DataType``.

    Returns:
        The type family -- one of ``profile.FAMILIES`` -- and, for a
        decimal, its precision and scale; ``None`` for both otherwise.
    """
    pyarrow, _ = _arrow()
    types = pyarrow.types
    if types.is_dictionary(kind):
        return family_of(kind.value_type)
    if types.is_integer(kind) or types.is_unsigned_integer(kind):
        return INTEGER, None, None
    if types.is_floating(kind):
        return FLOAT, None, None
    if types.is_decimal(kind):
        return DECIMAL, kind.precision, kind.scale
    if _any(types, kind, ("is_string", "is_large_string", "is_string_view")):
        return TEXT, None, None
    if _any(
        types,
        kind,
        ("is_binary", "is_large_binary", "is_fixed_size_binary", "is_binary_view"),
    ):
        return BINARY, None, None
    if types.is_boolean(kind):
        return BOOLEAN, None, None
    if types.is_date(kind):
        return DATE, None, None
    if types.is_timestamp(kind):
        return TIMESTAMP, None, None
    if _any(
        types,
        kind,
        ("is_list", "is_large_list", "is_fixed_size_list", "is_struct", "is_map"),
    ):
        return NESTED, None, None
    return UNKNOWN, None, None


def _any(types, kind, predicates: tuple) -> bool:
    """Return whether any predicate Arrow has at this version says yes of ``kind``."""
    return any(
        getattr(types, predicate)(kind)
        for predicate in predicates
        if hasattr(types, predicate)
    )


def profile_table(
    table, name: str, *, snapshot_sha256: Optional[str] = None
) -> Profile:
    """Return the profile of a pyarrow table.

    Args:
        table: A ``pyarrow.Table``.
        name: The dataset's name.
        snapshot_sha256: The digest of the bytes the table was read from,
            where the caller read them; ``None`` when there is none.

    Returns:
        The :class:`Profile`, its ``source`` being ``arrow``.

    Raises:
        ImportError: When pyarrow is not installed, naming the extra.
        ConfigurationError: When the table is not one, when it names a
            column twice, or when a name is blank or a digest malformed.
    """
    pyarrow, compute = _arrow()
    if not isinstance(table, pyarrow.Table):
        raise ConfigurationError(
            f"the Arrow profiler takes a pyarrow Table, got {type(table).__name__}"
        )
    from dq.arrow_adapter import canonical

    twice = named_twice([field.name for field in table.schema])
    if twice:
        raise ConfigurationError(f"the table names a column twice: {twice}")
    columns = []
    # By position, never by name: asked for a name, Arrow raises on a
    # second column that spells it too.
    for position, field in enumerate(table.schema):
        family, precision, scale = family_of(field.type)
        column = table.column(position)
        columns.append(
            _summarise(
                pyarrow,
                compute,
                column=column,
                measured=canonical(column),
                name=field.name,
                family=family,
                precision=precision,
                scale=scale,
            )
        )
    return Profile(
        dataset=name,
        snapshot_sha256=snapshot_sha256,
        rows=table.num_rows,
        columns=tuple(columns),
        source="arrow",
    )


def _summarise(
    pyarrow, compute, *, column, measured, name: str, family: str, precision, scale
) -> Column:
    """Return one column's summary, everything the type has no measure of left null."""
    said = dict(
        min=None,
        max=None,
        min_length=None,
        max_length=None,
        precision=precision,
        scale=scale,
        not_a_number=None,
    )
    distinct = None
    if family not in (NESTED, UNKNOWN):
        distinct = _measured(
            pyarrow,
            name,
            family,
            "counted",
            lambda: compute.count_distinct(measured).as_py(),
        )
    if family == FLOAT:
        said["not_a_number"] = _trues(compute, compute.is_nan(measured))
    if family in BOUNDED:
        ordered = measured
        if family == FLOAT:
            # A not-a-number is a value, never a bound.
            ordered = compute.if_else(
                compute.is_nan(measured),
                None,
                measured,
            )
        elif family == TIMESTAMP and measured.type.tz is not None:
            # The moment itself, in microseconds since the epoch. Handed
            # back to Python, a zoned timestamp is spelled in a zone pyarrow
            # chooses, under a process-wide switch another library sets on
            # import (PYARROW_IGNORE_TIMEZONE); as integers it is the instant.
            ordered = compute.cast(
                compute.cast(measured, pyarrow.timestamp("us", tz=measured.type.tz)),
                pyarrow.int64(),
            )
        bounds = _measured(
            pyarrow, name, family, "bounded", lambda: compute.min_max(ordered).as_py()
        )
        least, most = bounds["min"], bounds["max"]
        if family == TIMESTAMP and measured.type.tz is not None:
            least, most = moment(least), moment(most)
        said["min"], said["max"] = spell(least, family), spell(most, family)
    if family in LENGTHENED:
        lengths = (
            compute.utf8_length(measured)
            if family == TEXT
            else compute.binary_length(measured)
        )
        bounds = _measured(
            pyarrow, name, family, "bounded", lambda: compute.min_max(lengths).as_py()
        )
        said["min_length"], said["max_length"] = bounds["min"], bounds["max"]
    return Column(
        name,
        family,
        column.null_count,
        distinct,
        True,
        **said,
    )


def _measured(pyarrow, name: str, family: str, verb: str, measure) -> Any:
    """Return a measure, refusing in the profiler's words what Arrow cannot take."""
    try:
        return measure()
    except pyarrow.ArrowException as error:
        raise ConfigurationError(
            f"column {name!r}, of type {family}, cannot be {verb} in Arrow: {error}"
        ) from error


def _trues(compute, mask) -> int:
    """Return how many of a boolean column are true, its nulls left out."""
    return int(compute.sum(compute.if_else(mask, 1, 0), min_count=0).as_py() or 0)
