# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The pandera engine's payload: a schema in pandera's own serialized form.

The document is what pandera's ``DataFrameSchema.to_json()`` writes. pandera
ignores a check or a key it does not recognise, and the schema then
validates anything. So the document is read and the schema written back
out, and every key of the input missing from pandera's own document is
refused. What pandera keeps but this engine cannot vouch for is refused by
name as well.

Validation reports one outcome per check and per column property the schema
enforces, passing and failing alike. A failure is matched to its outcome by
the column and the name it carries, never by its position.

This module imports no pandera. The backend is handed in, and everything
here works on what a schema object exposes.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from dq.engine import checkspec
from dq.exceptions import ConfigurationError
from dq.utils.sanitization import to_strict_json

#: What a document's ``schema_type`` names: the pandera module that reads it.
BACKENDS = {"dataframe": "pandera.pandas", "polars_dataframe": "pandera.polars"}

#: What a document may say at the top. pandera keeps more, and each of the
#: rest either validates something other than the frame as it is, or fails
#: in a way no column's outcome can carry.
DOCUMENT_KEYS = ("schema_type", "api", "name", "title", "description", "columns")

#: What a column may say besides a check. ``coerce`` is left out on purpose.
COLUMN_KEYS = ("dtype", "nullable", "unique", "required", "title", "description")

#: Why a key pandera keeps is still refused.
UNSUPPORTED = {
    "coerce": "pandera would validate a cast copy of the column, not the column as it is",
    "strict": "a column the schema does not declare fails with no column to carry it",
    "checks": "a check over the whole frame is not reported by this engine",
    "unique": "uniqueness over several columns is not reported by this engine",
}

#: A column property reported as an outcome. ``NOT_NULLABLE`` and ``UNIQUE``
#: are this engine's names; pandera calls those failures ``not_nullable`` and
#: ``field_uniqueness``, and a dtype's by its own spelling.
DTYPE = "dtype"
NOT_NULLABLE = "not_nullable"
UNIQUE = "unique"
_PROPERTY = {"not_nullable": NOT_NULLABLE, "field_uniqueness": UNIQUE}

#: pandera's reason when a required column is not in the frame.
COLUMN_NOT_IN_DATAFRAME = "COLUMN_NOT_IN_DATAFRAME"

#: How many places a refusal names before it says there are more.
NAMED = 8

_Failure = Tuple[str, Optional[int]]


@dataclass(frozen=True)
class Outcome:
    """One thing the schema says, reported by its own label.

    Attributes:
        label: What the result is reported under.
        column: The column it is about.
        key: What a failure of it carries: a check's name, or a property's.
        criticality: ``error``, or ``warn`` for a check pandera is told to
            treat as a warning.
        details: What the document declared, carried by every result.
    """

    label: str
    column: str
    key: str
    criticality: str
    details: Dict[str, Any]


def missing_keys(given, kept, path: str = "$") -> List[str]:
    """Return where ``given`` says something that ``kept`` does not keep.

    Every key of a mapping must be in the other mapping at the same place,
    and a list is walked element by element. One spelling is equated: where
    the document writes a check's argument as a one-key mapping, pandera
    writes the bare value.

    Args:
        given: The rule's document.
        kept: The same document after pandera read it and wrote it out.
        path: Where the two are being compared, for the message.

    Returns:
        Places in ``given`` that ``kept`` does not have, as ``$.a.b``.
    """
    if isinstance(given, Mapping):
        if not isinstance(kept, Mapping):
            for key, value in given.items():
                if value == kept:
                    others = [other for other in given if other != key]
                    return [f"{path}.{other}" for other in others]
            return [path]
        found = []
        for key, value in given.items():
            if key not in kept:
                found.append(f"{path}.{key}")
            else:
                found.extend(missing_keys(value, kept[key], f"{path}.{key}"))
        return found
    if isinstance(given, list) and isinstance(kept, list) and len(given) == len(kept):
        return [
            found
            for index, (left, right) in enumerate(zip(given, kept))
            for found in missing_keys(left, right, f"{path}[{index}]")
        ]
    return []


def _names(places: List[str]) -> str:
    named = ", ".join(places[:NAMED])
    if len(places) <= NAMED:
        return named
    return f"{named} and {len(places) - NAMED} more"


def _text_of(payload) -> str:
    """Return the document as text, or refuse what was configured."""
    if not payload or (isinstance(payload, str) and not payload.strip()):
        raise ConfigurationError(
            "The pandera engine requires a 'schema': the document pandera's "
            "to_json() writes"
        )
    try:
        return payload if isinstance(payload, str) else json.dumps(payload)
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            f"The pandera engine's 'schema' cannot be written as a document: {error}"
        ) from error


def _document(text: str) -> Mapping:
    """Return the document in ``text``, or refuse it."""
    try:
        document = json.loads(text)
    except ValueError as error:
        raise ConfigurationError(
            f"The pandera engine's 'schema' is not a document: {error}"
        ) from error
    if not isinstance(document, Mapping):
        raise ConfigurationError(
            "The pandera engine's 'schema' must be a mapping, or the text of one"
        )
    return document


def _why(key: str) -> str:
    return UNSUPPORTED.get(key, "it is not something this engine reports on")


def _unsupported(given: Mapping, schema) -> List[str]:
    """Return what pandera kept and this engine cannot vouch for, with why."""
    found = [f"$.{key} ({_why(key)})" for key in given if key not in DOCUMENT_KEYS]
    for name, said in given["columns"].items():
        kinds = {check.registry_name for check in schema.columns[name].checks}
        found.extend(
            f"$.columns.{name}.{key} ({_why(key)})"
            for key in said
            if key not in COLUMN_KEYS and key not in kinds
        )
    return found


def _declared(document: Mapping, schema) -> List[Outcome]:
    """Return one outcome per check and per enforced property of ``schema``."""
    outcomes: List[Outcome] = []
    for name, column in schema.columns.items():
        said = document["columns"].get(name, None) or {}
        outcomes.append(
            Outcome(
                f"{DTYPE}_{name}", name, DTYPE, "error", {"dtype": said.get("dtype")}
            )
        )
        if not column.nullable:
            outcomes.append(
                Outcome(f"{NOT_NULLABLE}_{name}", name, NOT_NULLABLE, "error", {})
            )
        if column.unique:
            outcomes.append(Outcome(f"{UNIQUE}_{name}", name, UNIQUE, "error", {}))
        checks = [
            (
                check,
                {
                    "name": None if check.name == check.registry_name else check.name,
                    "kind": check.registry_name,
                    "column": name,
                },
            )
            for check in column.checks
        ]
        labels = checkspec.labels_for_checks(
            [said_check for _, said_check in checks], ("name",), "kind", ("column",)
        )
        for (check, _), label in zip(checks, labels):
            # Severity crosses the check seam from pandera's own warning mark.
            criticality = "warn" if check.raise_warning else "error"
            outcomes.append(
                Outcome(
                    label,
                    name,
                    check.name,
                    criticality,
                    {
                        "check": check.registry_name,
                        "statistics": to_strict_json(check.statistics),
                    },
                )
            )
    return outcomes


def _key_of(check) -> str:
    """Return what a failure's ``check`` names: a check's name, a property's."""
    if not isinstance(check, str):
        return check.name
    if check.startswith("dtype("):
        return DTYPE
    return _PROPERTY.get(check, check)


def _records(failures) -> Tuple[Dict[Tuple[Any, str], List[_Failure]], List[str]]:
    """Return each failure by ``(column, key)``, and the required columns absent.

    A failure's row count is kept only where pandera gave one: elsewhere
    pandera returns a string, a column's name or a dtype, not failing rows.
    """
    by_key: Dict[Tuple[Any, str], List[_Failure]] = {}
    absent: List[str] = []
    for failure in failures:
        cases = failure.failure_cases
        reason = failure.reason_code.name
        if failure.column_name is None and reason == COLUMN_NOT_IN_DATAFRAME:
            absent.append(str(cases))
            continue
        rows = len(cases) if hasattr(cases, "columns") else None
        by_key.setdefault((failure.column_name, _key_of(failure.check)), []).append(
            (reason, rows)
        )
    return by_key, absent


def _present(frame) -> Optional[set]:
    """Return the names of the frame's columns, if the frame says."""
    try:
        return set(frame.columns)
    except Exception:  # noqa: BLE001 - pandera judges what a frame is
        return None


class PanderaSchema:
    """A rule's schema, read once and ready to validate a frame.

    Args:
        payload: The rule's ``schema``: a mapping, or the text of one.
        load: Returns the pandera module that reads a document's
            ``schema_type``.

    Raises:
        ConfigurationError: When there is no document, when pandera cannot
            read it, or when it says something that would be ignored or
            that this engine cannot report on.
        ValueError: When two outcomes would share a label.
    """

    def __init__(self, payload, load):
        text = _text_of(payload)
        given = _document(text)
        kind = given.get("schema_type", None)
        if kind not in BACKENDS:
            raise ConfigurationError(
                f"The 'schema' document says schema_type {kind!r}; this engine "
                f"reads {sorted(BACKENDS)}"
            )
        if not given.get("columns", None):
            raise ConfigurationError("The 'schema' document declares no column")
        # Before the try below: a backend that cannot be imported says so
        # itself, in the words of what is missing.
        backend = load(kind)
        try:
            self._schema = backend.DataFrameSchema.from_json(text)
            kept = json.loads(self._schema.to_json())
        except Exception as error:  # noqa: BLE001 - pandera's parser raises bare errors
            raise ConfigurationError(
                f"pandera could not read the 'schema' document: "
                f"{type(error).__name__}: {error}"
            ) from error
        missing = missing_keys(given, kept)
        if missing:
            raise ConfigurationError(
                f"{_names(missing)} of the 'schema' document is not recognised by "
                "pandera: it would be ignored, and the schema would then validate "
                "anything"
            )
        unsupported = _unsupported(given, self._schema)
        if unsupported:
            raise ConfigurationError(
                f"the pandera engine does not take {_names(unsupported)}"
            )
        self._outcomes = _declared(given, self._schema)
        checkspec.require_unique_labels([outcome.label for outcome in self._outcomes])
        for column in self._schema.columns.values():
            for check in column.checks:
                # Under lazy validation pandera says nothing about a
                # warning-level check that fails. Made strict, it is
                # reported, and the outcome keeps its own severity.
                check.raise_warning = False

    def validate(self, frame) -> List[Dict[str, Any]]:
        """Return one result per outcome of the schema, about ``frame``.

        Raises:
            ConfigurationError: When pandera reports a failure that is not
                one of the schema's own outcomes.
        """
        try:
            self._schema.validate(frame, lazy=True)
        except Exception as error:  # noqa: BLE001 - read first, then judged
            failures = getattr(error, "schema_errors", None)
            if failures is None:
                raise
        else:
            failures = []
        by_key, absent = _records(failures)
        return self._results(by_key, absent, _present(frame))

    def _results(self, by_key, absent: List[str], present) -> List[Dict[str, Any]]:
        """Return the outcome of every thing the schema says."""
        results = []
        answered = set()
        for outcome in self._outcomes:
            hits = by_key.get((outcome.column, outcome.key), [])
            if len(hits) > 1:
                raise ConfigurationError(
                    f"pandera reported {len(hits)} failures for one check, "
                    f"{outcome.label!r}: each result goes with its own check, "
                    "and these cannot be told apart"
                )
            details = dict(outcome.details)
            if hits:
                answered.add((outcome.column, outcome.key))
                reason, rows = hits[0]
                details["reason"] = reason
                if rows is not None:
                    details["failed_rows"] = rows
            elif outcome.column in absent:
                details["reason"] = COLUMN_NOT_IN_DATAFRAME
            elif present is not None and outcome.column not in present:
                # An optional column that is absent: pandera raises nothing.
                details["present"] = False
            passed = not hits and outcome.column not in absent
            results.append(
                checkspec.build_result(
                    outcome.label, passed, details, criticality=outcome.criticality
                )
            )
        unattributed = sorted((key for key in by_key if key not in answered), key=repr)
        if unattributed:
            raise ConfigurationError(
                "pandera reported a failure that is not one of the schema's own "
                "checks: "
                + _names([f"column {column!r}, {key}" for column, key in unattributed])
            )
        return results
