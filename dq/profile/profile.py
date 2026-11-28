# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The profile: a bounded summary of one dataset (SPEC-019).

A profile is strict JSON with one entry per column, and its size depends
on the column count alone -- never on the row count, and never on how long
a value is: text and binary columns say how long their values are, and
never which values are least or greatest.

What every number means is fixed here, so that the Spark and Arrow
profilers cannot disagree:

* a null is a SQL null; a float that is not a number is a value, counted
  on its own and never spelled as a bound;
* a signed zero is one value;
* a timestamp with a zone is spelled in UTC, one without as it stands;
* a decimal is spelled exactly, as text.

This module imports nothing optional, and neither do the suggesters: a
profile read from a file is all they need.

A reader refuses what it does not know -- a key, a version -- rather than
misread it, so anything added to a profile is a new version, and
:data:`SCHEMA_VERSION` is where it is named. The shape of a profile is
published beside the rule pack's, as ``dq/schemas/profile.schema.json``;
what the shape cannot say -- which measures a type has, and that a least
value is not above its greatest -- is said here.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from dq.exceptions import ConfigurationError

SCHEMA_VERSION = "dq-profile/v1"

#: The keys of a profile, and of its ``dataset``.
KEYS = ("schema_version", "dataset", "source", "rows", "columns")
DATASET_KEYS = ("name", "snapshot_sha256")

#: What a column's type is called, whatever engine spelled it.
INTEGER = "integer"
FLOAT = "float"
DECIMAL = "decimal"
TEXT = "text"
BINARY = "binary"
BOOLEAN = "boolean"
DATE = "date"
TIMESTAMP = "timestamp"
NESTED = "nested"
UNKNOWN = "unknown"

FAMILIES = (
    INTEGER,
    FLOAT,
    DECIMAL,
    TEXT,
    BINARY,
    BOOLEAN,
    DATE,
    TIMESTAMP,
    NESTED,
    UNKNOWN,
)

#: The families whose values have an order, and so a least and greatest.
BOUNDED = (INTEGER, FLOAT, DECIMAL, DATE, TIMESTAMP)

#: The families whose values have a length.
LENGTHENED = (TEXT, BINARY)

#: How an infinite float bound is spelled: strict JSON has no other way.
INFINITIES = {"Infinity": float("inf"), "-Infinity": float("-inf")}

#: Who measured: one of the two profilers.
SOURCES = ("arrow", "spark")

EPOCH = datetime(1970, 1, 1)


def moment(micros: Optional[int]) -> Optional[datetime]:
    """Return a moment given in microseconds since the epoch, as wall time in UTC.

    Both profilers ask a zoned timestamp for the moment itself -- Arrow
    keeps one as such microseconds, and Spark has ``unix_micros`` -- so
    that no zone's spelling of it comes between the data and the profile.
    """
    if micros is None:
        return None
    return EPOCH + timedelta(microseconds=micros)


def named_twice(names) -> list:
    """Return the names that a dataset holds more than once, in order.

    A profile has one entry per column, read by name, so a dataset that
    spells a column's name twice cannot be profiled: a profiler refuses it
    before it measures anything.
    """
    return sorted(name for name, times in Counter(names).items() if times > 1)


def spell(value: Any, family: str) -> Any:
    """Return a measured value as the profile spells it.

    Args:
        value: What the engine handed back; ``None`` where there is no
            value to spell.
        family: The column's type family.

    Returns:
        A JSON-native value: a number, text, or ``None``.

    Raises:
        ConfigurationError: When a float bound is a not-a-number: the
            profilers keep it out of the bounds, so one here is a defect.
    """
    if value is None:
        return None
    if family == DECIMAL:
        # In plain notation, to the value's own scale: Python writes a
        # decimal of a negative scale as ``-5E+2`` for minus five hundred.
        return format(value, "f")
    if family in (DATE, TIMESTAMP):
        return value.isoformat()
    if family == FLOAT:
        if isinstance(value, Decimal):
            value = float(value)
        if value != value:
            raise ConfigurationError(
                f"a float bound cannot be a not-a-number: {value!r}"
            )
        for spelled, infinite in INFINITIES.items():
            if value == infinite:
                return spelled
        return float(value)
    return int(value)


def unspell(value: Any, family: str) -> Any:
    """Return a spelled value as a number again, for a suggester to compare.

    Args:
        value: A bound as the profile spells it.
        family: The column's type family.

    Returns:
        An ``int``, a ``float`` or a ``Decimal`` -- ``None`` for anything
        else, a date and a timestamp included. Strict JSON spells a float
        that is not finite as text, so one that arrives as a number is not
        a spelling.
    """
    if value is None or isinstance(value, bool):
        return None
    if family == INTEGER and isinstance(value, int):
        return value
    if family == FLOAT:
        if isinstance(value, (int, float)):
            return value if math.isfinite(value) else None
        if isinstance(value, str) and value in INFINITIES:
            return INFINITIES[value]
    if family == DECIMAL and isinstance(value, str):
        try:
            return Decimal(value)
        except InvalidOperation:
            return None
    return None


def _spelled(value: Any, family: str) -> bool:
    """Return whether ``value`` is a spelling a ``family`` column's bound has."""
    if unspell(value, family) is not None:
        return True
    try:
        if family == DATE and isinstance(value, str):
            date.fromisoformat(value)
            return len(value) == 10
        if family == TIMESTAMP and isinstance(value, str):
            return datetime.fromisoformat(value).tzinfo is None
    except ValueError:
        return False
    return False


def _number(value: Any, low: int) -> bool:
    return type(value) is int and value >= low


@dataclass(frozen=True)
class Column:
    """One column's summary.

    Attributes:
        name: The column's name.
        type: Its type family, one of :data:`FAMILIES`.
        nulls: How many of its values are SQL nulls.
        distinct: How many values are distinct, nulls excluded; ``None``
            where the profiler does not count them (a nested column).
        distinct_exact: Whether that count is exact.
        min: The least value, spelled; ``None`` where the type has no
            order or every value is null.
        max: The greatest value, likewise.
        min_length: The least length of a text or binary value.
        max_length: The greatest length of one.
        precision: A decimal's precision.
        scale: A decimal's scale.
        not_a_number: How many of a float column's values are not a number.
    """

    name: str
    type: str
    nulls: int
    distinct: Optional[int]
    distinct_exact: bool
    min: Any = None
    max: Any = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    not_a_number: Optional[int] = None

    def to_document(self) -> Dict[str, Any]:
        """Return the summary as the profile document holds it."""
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "Column":
        """Return the summary a document holds, or refuse it."""
        if not isinstance(document, dict):
            raise ConfigurationError("a column summary must be a mapping")
        allowed = {item.name for item in cls.__dataclass_fields__.values()}
        _unknown(document, allowed, "a column summary")
        # Every measure is said, null where the type has none: a reader
        # never has to tell absence from none.
        for key in sorted(allowed - set(document)):
            raise ConfigurationError(f"a column summary has no {key!r}")
        return cls(
            name=_name(document["name"]),
            type=_family(document["type"]),
            nulls=_count(document["nulls"], "nulls"),
            distinct=(
                None
                if document["distinct"] is None
                else _count(document["distinct"], "distinct")
            ),
            distinct_exact=_flag(document["distinct_exact"]),
            **_measures(document),
        )


@dataclass(frozen=True)
class Profile:
    """A dataset's profile: who measured, of what, and what was found.

    Two profiles are equal when they say the same things about the data.
    Which profiler measured it (:attr:`source`) does not count, and neither
    does which snapshot the caller said it certifies: that is the caller's
    word about the bytes, not the profiler's about the data.
    """

    dataset: str
    snapshot_sha256: Optional[str] = field(compare=False)
    rows: int
    columns: Tuple[Column, ...]
    source: str = field(compare=False, default=None)

    def to_document(self) -> Dict[str, Any]:
        """Return the profile as the JSON document written to disk."""
        return {
            "schema_version": SCHEMA_VERSION,
            "dataset": {
                "name": self.dataset,
                "snapshot_sha256": self.snapshot_sha256,
            },
            "source": self.source,
            "rows": self.rows,
            "columns": [column.to_document() for column in self.columns],
        }

    def to_json(self) -> str:
        """Return the profile as JSON text, ending in a newline."""
        return json.dumps(self.to_document(), indent=2) + "\n"

    @classmethod
    def from_document(cls, document: Any) -> "Profile":
        """Return the profile a document holds, or refuse it.

        Raises:
            ConfigurationError: For anything that is not a profile of this
                version: a missing, malformed or unknown key, a column of an
                unknown type, a bound its type cannot have, a least value
                above its greatest, more nulls than rows.
        """
        if not isinstance(document, dict):
            raise ConfigurationError("a profile must be a JSON object")
        _unknown(document, KEYS, "a profile")
        if document.get("schema_version", None) != SCHEMA_VERSION:
            raise ConfigurationError(
                f"a profile's schema_version must be {SCHEMA_VERSION!r}, got "
                f"{document.get('schema_version', None)!r}"
            )
        dataset = document.get("dataset", None)
        if not isinstance(dataset, dict):
            raise ConfigurationError("a profile needs its dataset: a name and a digest")
        _unknown(dataset, DATASET_KEYS, "a profile's dataset")
        if "snapshot_sha256" not in dataset:
            raise ConfigurationError(
                "a profile's dataset has no 'snapshot_sha256': null where the "
                "profiler read no file"
            )
        rows = _count(document.get("rows", None), "rows")
        columns = document.get("columns", None)
        if not isinstance(columns, list):
            raise ConfigurationError("a profile's columns must be a list")
        summaries = tuple(Column.from_document(item) for item in columns)
        twice = named_twice([summary.name for summary in summaries])
        if twice:
            raise ConfigurationError(f"a profile names a column twice: {twice}")
        for summary in summaries:
            if summary.nulls > rows:
                raise ConfigurationError(
                    f"column {summary.name!r} has more nulls than the profile has "
                    f"rows: {summary.nulls} > {rows}"
                )
        return cls(
            dataset=_name(dataset.get("name", None)),
            snapshot_sha256=_digest(dataset.get("snapshot_sha256", None)),
            rows=rows,
            columns=summaries,
            source=_source(document.get("source", None)),
        )

    @classmethod
    def from_json(cls, text: str) -> "Profile":
        """Return the profile in ``text``, or refuse it.

        The text must be strict JSON: Python's reader would take ``NaN`` and
        ``Infinity`` for numbers, and a profile spells a float that is not
        finite as text.
        """
        try:
            document = json.loads(text, parse_constant=_not_a_number)
        except ValueError as error:
            raise ConfigurationError(f"a profile is not JSON: {error}") from error
        return cls.from_document(document)


# ------------------------------------------------------------ the refusals


def _not_a_number(spelled: str) -> None:
    """Refuse a JSON reader's spelling of a float that is not finite."""
    raise ConfigurationError(
        f"a profile is strict JSON and holds no {spelled}: an infinite bound "
        'is spelled "Infinity" or "-Infinity", and a not-a-number is never '
        "a bound"
    )


def _unknown(document: Dict[str, Any], keys: Any, what: str) -> None:
    """Refuse a mapping that holds a key outside ``keys``."""
    extra = sorted(set(document) - set(keys))
    if extra:
        raise ConfigurationError(f"{what} has unknown keys: {extra}")


def _name(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"a name must be a non-empty string, got {value!r}")
    return value


def _family(value: Any) -> str:
    if value not in FAMILIES:
        raise ConfigurationError(
            f"a column's type must be one of {list(FAMILIES)}, got {value!r}"
        )
    return value


def _count(value: Any, what: str) -> int:
    if not _number(value, 0):
        raise ConfigurationError(f"{what} must be a count, got {value!r}")
    return value


def _flag(value: Any) -> bool:
    if type(value) is not bool:
        raise ConfigurationError(f"distinct_exact must be true or false, got {value!r}")
    return value


def _digest(value: Any) -> Optional[str]:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ConfigurationError(
            f"a snapshot digest is 64 hexadecimal digits, got {value!r}"
        )
    return value


def _source(value: Any) -> str:
    if value not in SOURCES:
        raise ConfigurationError(f"a profile's source must be one of {list(SOURCES)}")
    return value


def _ordered(family: str, low: Any, high: Any) -> bool:
    """Return whether ``low`` is not above ``high``, as values of ``family``.

    Both have been checked to be spellings the family takes.
    """
    least, most = unspell(low, family), unspell(high, family)
    if least is None:
        parse = date.fromisoformat if family == DATE else datetime.fromisoformat
        least, most = parse(low), parse(high)
    return least <= most


def _measures(document: Dict[str, Any]) -> Dict[str, Any]:
    """Return a column's measures once each fits its type, or refuse one."""
    family = _family(document["type"])
    measures = {}
    for key in (
        "min",
        "max",
        "min_length",
        "max_length",
        "precision",
        "scale",
        "not_a_number",
    ):
        measures[key] = document.get(key, None)
    for key in ("min_length", "max_length"):
        if measures[key] is not None and family not in LENGTHENED:
            raise ConfigurationError(f"only text and binary columns have a {key}")
        if measures[key] is not None:
            _count(measures[key], key)
    if (measures["min_length"] is None) != (measures["max_length"] is None):
        raise ConfigurationError("a length bound comes with its other bound")
    if (
        measures["min_length"] is not None
        and measures["min_length"] > measures["max_length"]
    ):
        raise ConfigurationError("a least length is above the greatest")
    for key in ("min", "max"):
        if measures[key] is not None and family not in BOUNDED:
            raise ConfigurationError(f"only {list(BOUNDED)} columns have a {key}")
    if (measures["min"] is None) != (measures["max"] is None):
        raise ConfigurationError("a bound comes with its other bound")
    if measures["min"] is not None:
        for key in ("min", "max"):
            if not _spelled(measures[key], family):
                raise ConfigurationError(
                    f"{family!r} column: a bound is not spelled as a value of "
                    f"that type: {measures[key]!r}"
                )
        if not _ordered(family, measures["min"], measures["max"]):
            raise ConfigurationError(
                f"the least value is above the greatest: {measures['min']!r}, "
                f"{measures['max']!r}"
            )
    if family == DECIMAL:
        # A scale may be negative: a decimal(10, -2) counts hundreds.
        if not _number(measures["precision"], 1) or not _number(measures["scale"], -76):
            raise ConfigurationError("a decimal column has a precision and a scale")
        if measures["scale"] > measures["precision"]:
            raise ConfigurationError("a decimal's scale is above its precision")
    elif measures["precision"] is not None or measures["scale"] is not None:
        raise ConfigurationError("only a decimal column has a precision and a scale")
    if family == FLOAT:
        if measures["not_a_number"] is not None:
            _count(measures["not_a_number"], "not_a_number")
    elif measures["not_a_number"] is not None:
        raise ConfigurationError(
            "only a float column counts values that are not a number"
        )
    return measures
