# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Importer for the columnar format: any tool's results in four columns.

One record per check, as a JSON array of objects or as a CSV file with a
header row::

    check,success,criticality,details
    orders_id_unique,false,error,"{""failed_rows"": 3}"
    orders_row_count,true,,

The format is small on purpose, and strict about its shape, so that nothing
a tool writes is dropped without a word: a column it does not define is
refused rather than ignored. What a tool puts in ``details`` is its own to
judge -- this format cannot recognise a row of data -- so ``details`` is for
bounded identifiers, counts and measurements, and the outcome gate bounds
its size.
"""

from __future__ import annotations

import csv
import io
from typing import Iterable, Optional

from dq.engine import checkspec
from dq.harvest.base import (
    Harvester,
    Imported,
    load_json,
    parse_json,
    peek_json,
    reject,
)

#: The columns every record has.
REQUIRED = ("check", "success")

#: The columns a record may have.
ALLOWED = frozenset({"check", "success", "criticality", "details"})

_UNDEFINED = (
    "has columns the format does not define: {names}; a record has check and "
    "success, and may have criticality and details"
)


class ColumnarHarvester(Harvester):
    """Reads a columnar file: a JSON array of records, or CSV."""

    system = "columnar"

    @classmethod
    def recognises(cls, source: bytes) -> bool:
        document = peek_json(source)
        if document is not None:
            return isinstance(document, list)
        return cls._has_column_header(source)

    @classmethod
    def _has_column_header(cls, source: bytes) -> bool:
        try:
            text = source.decode("utf-8-sig")
        except UnicodeDecodeError:
            return False
        header = next(csv.reader(io.StringIO(text, newline="")), None)
        return header is not None and all(column in header for column in REQUIRED)

    def _document(self, text: str, reference: str):
        if text.lstrip()[:1] in ("[", "{"):
            # JSON, and so an array of records. A JSON object is some
            # other document -- a vendor's, most likely -- and is not
            # read as a header row.
            document = load_json(text, self.system, reference)
            if not isinstance(document, list):
                reject(
                    self.system,
                    reference,
                    "is not a columnar file: a JSON source is an array of records",
                )
            return document
        return self._rows(text, reference)

    def _import(self, document, reference: str) -> Iterable[Imported]:
        for position, entry in enumerate(document):
            # A CSV document arrives as ``(line, fields)`` pairs.
            line, fields = entry if isinstance(entry, tuple) else (None, entry)
            as_text = line is not None
            where = f"row {line}" if as_text else f"record {position}"
            named = {"position": position, "where": where}
            if not isinstance(fields, dict):
                reject(self.system, reference, "is not an object", where=where)
            unknown = sorted(str(key) for key in fields if key not in ALLOWED)
            if unknown:
                reject(
                    self.system,
                    reference,
                    _UNDEFINED.format(names=", ".join(unknown)),
                    **named,
                )
            name = fields.get("check")
            if not isinstance(name, str) or not name.strip():
                reject(
                    self.system,
                    reference,
                    f"has check {name!r}, which is not a name",
                    **named,
                )
            label = name.strip()
            named["record"] = label
            yield Imported(
                label=label,
                success=self._success(fields, as_text, reference, **named),
                criticality=self._criticality(fields, reference, **named),
                observed=self._observed(fields, as_text, reference, **named),
                position=position,
                where=where,
            )

    def _success(self, fields: dict, as_text: bool, reference: str, **named) -> bool:
        """Return a record's ``success``, which must be a real yes or no."""
        value = fields.get("success")
        if as_text:
            # A CSV cell has no types: the word must be true or false.
            value = str(value).strip().lower()
            if value == "true":
                return True
            if value == "false":
                return False
        elif type(value) is bool:
            return value
        reject(
            self.system,
            reference,
            f"has success {fields.get('success')!r}, which is not true or false",
            **named,
        )

    def _criticality(self, fields: dict, reference: str, **named) -> str:
        """Return a record's criticality: ``error`` unless it says otherwise."""
        value = fields.get("criticality")
        if value is None or (isinstance(value, str) and not value.strip()):
            return "error"
        if not isinstance(value, str):
            reject(
                self.system,
                reference,
                f"has criticality {value!r}, which is not a severity",
                **named,
            )
        try:
            return checkspec.normalize_criticality(value)
        except ValueError:
            reject(
                self.system,
                reference,
                f"has criticality {value.strip()!r}, which is not a severity",
                **named,
            )

    def _observed(
        self, fields: dict, as_text: bool, reference: str, **named
    ) -> Optional[dict]:
        """Return a record's ``details``: an object, or nothing."""
        value = fields.get("details")
        if value is None or (as_text and isinstance(value, str) and not value.strip()):
            return None
        if as_text:
            # One CSV cell holds the object as JSON.
            try:
                value = parse_json(value)
            except ValueError as error:
                reject(
                    self.system,
                    reference,
                    f"has details that are not JSON ({error})",
                    **named,
                )
            if not isinstance(value, dict):
                reject(
                    self.system,
                    reference,
                    "has details that are not a JSON object",
                    **named,
                )
        if not isinstance(value, dict):
            reject(
                self.system,
                reference,
                f"has details {value!r}, which are not an object",
                **named,
            )
        return dict(value)

    def _rows(self, text: str, reference: str) -> list:
        """Return a CSV document as ``(line, fields)`` pairs."""
        reader = csv.reader(io.StringIO(text, newline=""))
        header = next(reader, None)
        if header is None or not any(column.strip() for column in header):
            reject(
                self.system,
                reference,
                "is not a columnar file: it has no header row naming check and "
                "success",
            )
        columns = [column.strip() for column in header]
        unknown = [column for column in columns if column not in ALLOWED]
        if unknown:
            reject(
                self.system,
                reference,
                _UNDEFINED.format(names=", ".join(repr(c) for c in unknown)),
                where="the header",
            )
        missing = [column for column in REQUIRED if column not in columns]
        if missing:
            reject(
                self.system,
                reference,
                "has no " + " or ".join(repr(column) for column in missing) + " column",
                where="the header",
            )
        if len(set(columns)) != len(columns):
            reject(self.system, reference, "names a column twice", where="the header")
        rows = []
        for cells in reader:
            # A blank line carries no record.
            if not any(cell.strip() for cell in cells):
                continue
            if len(cells) != len(columns):
                reject(
                    self.system,
                    reference,
                    f"has {len(cells)} cells and the header has {len(columns)}",
                    where=f"row {reader.line_num}",
                )
            rows.append((reader.line_num, dict(zip(columns, cells))))
        return rows
