# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Harvester contract, and what every importer shares.

A harvester turns another tool's results into this framework's outcome
dictionaries, so they reach the same report, gate and sinks as its own. A
signal's origin is metadata, not a silo (ADR-011), and importing never
widens the outcome contract: a record either normalizes into it or the
source is rejected.

Every harvester reads bytes and imports no vendor package. An importer
overrides three things: how it recognises its document (:meth:`recognises`),
how it reads the records out of one (:meth:`_import`), and, for a source
that is not JSON, how it turns text into a document (:meth:`_document`).
:meth:`harvest` does the rest, and does it the same way for all of them:
the bytes are read once and digested, every outcome carries where it came
from, and no batch leaves without unique labels and a pass through the
outcome gate.
"""

from __future__ import annotations

import hashlib
import json
import math
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Iterable, List, NamedTuple, Optional

from dq.engine import checkspec
from dq.exceptions import HarvestError, ValidationError
from dq.outcomes import CheckOutcome, normalize_outcomes

#: The largest source that is read whole.
MAX_SOURCE_BYTES = 64 * 1024 * 1024

#: Bounds on the measurements copied from one record: how many, and how
#: long a measurement's name may be.
MAX_OBSERVED = 32
MAX_NAME_LENGTH = 64


class Imported(NamedTuple):
    """One check as its source reports it.

    Attributes:
        label: The check's name, unique in the source.
        success: Whether the source says the check passed.
        criticality: ``"error"`` or ``"warn"``.
        status: The source's own word for the outcome, when it has one.
        observed: Counts and measurements. Never a row of data.
        identifiers: What else names the check in the source: an
            expectation's type, the column it reads.
        position: The record's index in the source, from 0.
        where: Where a person finds it: ``record 3``, ``row 5``.
    """

    label: str
    success: bool
    criticality: str = "error"
    status: Optional[str] = None
    observed: Optional[dict] = None
    identifiers: Optional[dict] = None
    position: int = 0
    where: str = ""


class Harvester(ABC):
    """Reads one external result source and yields outcome dictionaries."""

    #: The source system's name, as ``details.origin.system`` reports it.
    system: ClassVar[str]

    def harvest(self, source: bytes, *, reference: str) -> List[dict]:
        """Turn a source's bytes into outcome dictionaries, or reject it.

        Args:
            source: The source's bytes, read once; the digest in every
                outcome's provenance is of exactly these.
            reference: What the source is called in its provenance: a path,
                as the caller wrote it.

        Returns:
            One ``{"check", "success", "details"}`` dictionary per check,
            each with ``details.origin``, in the source's order.

        Raises:
            HarvestError: If the source cannot be imported: it is not the
                kind of document the harvester reads, a record is malformed
                or holds a status it does not know, two checks share a name,
                there is not one check in it, or an outcome will not cross
                the outcome gate. The message names the record and its
                position.
        """
        origin = self._origin(source, reference)
        document = self._document(_text(source, self.system, reference), reference)
        imported = list(self._import(document, reference))
        if not imported:
            reject(
                self.system,
                reference,
                "holds no check results, so there is nothing to import",
            )
        run = self._run(document)
        if isinstance(run, str) and run:
            origin["run"] = run
        outcomes = []
        for item in imported:
            # The keys this contract defines are set last, so nothing an
            # importer adds can stand in one of their places.
            details = dict(item.identifiers or {})
            if item.status is not None:
                details["status"] = item.status
            if item.observed:
                details["observed"] = item.observed
            details["origin"] = dict(origin)
            outcome = checkspec.build_result(
                item.label, item.success, details, criticality=item.criticality
            )
            try:
                CheckOutcome(outcome)
            except ValidationError as error:
                reject(
                    self.system,
                    reference,
                    f"cannot be recorded as evidence: {error}",
                    position=item.position,
                    record=item.label,
                    where=item.where or None,
                )
            outcomes.append(outcome)
        try:
            checkspec.require_unique_labels([outcome["check"] for outcome in outcomes])
        except ValueError as error:
            reject(self.system, reference, str(error))
        try:
            normalize_outcomes(outcomes)
        except ValidationError as error:
            reject(self.system, reference, f"cannot be recorded as evidence: {error}")
        return outcomes

    @classmethod
    @abstractmethod
    def recognises(cls, source: bytes) -> bool:
        """Return whether ``source`` is a document this harvester reads."""

    @abstractmethod
    def _import(self, document, reference: str) -> Iterable[Imported]:
        """Yield the checks one document holds, in the document's order."""

    def _document(self, text: str, reference: str):
        """Return a document's text as the structure :meth:`_import` reads."""
        return load_json(text, self.system, reference)

    def _run(self, document) -> Optional[str]:
        """Return the name of the run that produced the document, if it has one."""
        return None

    def _origin(self, source: bytes, reference: str) -> dict:
        """Return the provenance every outcome of this source carries."""
        if not isinstance(reference, str) or not reference.strip():
            raise HarvestError(
                "A source is imported under a reference, which names where its "
                f"evidence came from; got {reference!r}",
                system=self.system,
            )
        if len(source) > MAX_SOURCE_BYTES:
            reject(
                self.system,
                reference,
                f"is {len(source)} bytes, more than the {MAX_SOURCE_BYTES} a "
                "source may be",
            )
        return {
            "system": self.system,
            "reference": reference,
            "sha256": hashlib.sha256(source).hexdigest(),
        }


# --------------------------------------------------------------- refusing


def reject(
    system: str,
    reference: str,
    problem: str,
    *,
    position: Optional[int] = None,
    record: Optional[str] = None,
    where: Optional[str] = None,
) -> None:
    """Refuse a source: the one place an import is rejected.

    Args:
        system: The source system being read.
        reference: What the source is called.
        problem: What is wrong, as a predicate: ``holds no check results``.
        position: The offending record's index in the source, from 0.
        record: What the source calls the offending record.
        where: Where a person finds that record; ``record N`` by default.

    Raises:
        HarvestError: Always.
    """
    if where is None:
        where = f"record {position}" if position is not None else "the source"
    named = f": {record!r}" if record else ""
    raise HarvestError(
        f"{system} source {reference!r}: {where}{named} {problem}",
        system=system,
        position=position,
        record=record,
    )


# ----------------------------------------------------------------- reading


def _text(source: bytes, system: str, reference: str) -> str:
    try:
        return source.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        reject(system, reference, f"is not UTF-8 text ({error.reason})")


def parse_json(text: str):
    """Parse JSON, refusing what JSON parsers can disagree about.

    ``NaN`` and ``Infinity`` are not JSON, and a key that repeats in one
    object means one thing to the parser that keeps the first and another
    to the one that keeps the last.

    Raises:
        ValueError: If ``text`` is not JSON, or holds one of those.
    """

    def constant(name):
        raise ValueError(f"{name} is not a JSON value")

    def object_of(pairs):
        seen = set()
        for key, _value in pairs:
            if key in seen:
                raise ValueError(f"the key {key!r} repeats in one object")
            seen.add(key)
        return dict(pairs)

    return json.loads(text, parse_constant=constant, object_pairs_hook=object_of)


def load_json(text: str, system: str, reference: str):
    """Return a source's text parsed as JSON, or refuse the source."""
    try:
        return parse_json(text)
    except ValueError as error:
        reject(system, reference, f"is not valid JSON ({error})")


def peek_json(source: bytes):
    """Return ``source`` parsed as JSON, or ``None`` if it is not JSON.

    For telling sources apart. What a harvester then reads goes through
    :func:`load_json`, which refuses what this lets by.
    """
    try:
        return json.loads(source.decode("utf-8-sig"))
    except (ValueError, UnicodeDecodeError):
        return None


def is_number(value: Any) -> bool:
    """Return whether ``value`` is a finite number that is not a boolean."""
    if type(value) is int:
        return True
    return type(value) is float and math.isfinite(value)


def numbers_of(mapping) -> dict:
    """Return the numbers at the top level of a mapping, within bounds.

    Lists, strings, booleans and nested objects are left out: a number at
    the top level of a result is a count or a measurement, and a list is
    where values live.
    """
    if not isinstance(mapping, dict):
        return {}
    found: dict = {}
    for key, value in mapping.items():
        if len(found) == MAX_OBSERVED:
            break
        if (
            isinstance(key, str)
            and 0 < len(key) <= MAX_NAME_LENGTH
            and is_number(value)
        ):
            found[key] = value
    return found
