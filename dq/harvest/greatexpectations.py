# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Importer for a Great Expectations validation result.

The document is one ``ExpectationSuiteValidationResult`` as its
``to_json_dict()`` writes it. Each entry of ``results`` becomes one
outcome, labelled as the ``greatexpectations`` engine labels the same
expectation: Great Expectations 1.24 and 1.25 have no name of their own for
an expectation.

What a result says about the data is kept to the numbers at the top level
of its ``result``: counts and measurements. Left out, on purpose:

* ``partial_unexpected_list`` and ``partial_unexpected_counts``, which hold
  row values;
* ``partial_unexpected_index_list``, which holds row locators;
* the traceback a raised expectation carries, with local file paths in it.

A raised expectation reports ``success: false`` with an empty ``result``:
nothing was judged, and it is a failed check.

Read from Great Expectations 1.24.1 and 1.25.0's own output.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from dq.engine import checkspec
from dq.harvest.base import Harvester, Imported, numbers_of, peek_json, reject

#: The severity Great Expectations gives an expectation that should only
#: warn. Everything else is read as blocking.
WARNING = "warning"


def _is_validation_result(document) -> bool:
    return (
        isinstance(document, dict)
        and isinstance(document.get("results"), list)
        and (
            isinstance(document.get("suite_name"), str)
            or isinstance(document.get("statistics"), dict)
        )
    )


def _raised(result: dict) -> bool:
    """Return whether the expectation behind ``result`` raised.

    A result that ran says so in ``exception_info.raised_exception``. One
    that raised has no such key: its ``exception_info`` is keyed by metric,
    each holding the traceback.
    """
    info = result.get("exception_info")
    if not isinstance(info, dict):
        return False
    if info.get("raised_exception") is True:
        return True
    return any(
        isinstance(value, dict) and value.get("exception_traceback")
        for value in info.values()
    )


def _told_apart(labels: List[str]) -> List[str]:
    """Give a label that repeats its position, as the engine does.

    The labelling rule names an expectation type by its column, and tells
    two of one type apart by position when they have no column. Two of one
    type on one column need the same: the engine asks for a name, and a
    Great Expectations document has none to give.
    """
    seen: dict = {}
    for index, label in enumerate(labels):
        if label in seen:
            labels[index] = checkspec.derive_name(label, index)
        else:
            seen[label] = index
    return labels


class GreatExpectationsHarvester(Harvester):
    """Reads a Great Expectations validation result."""

    system = "greatexpectations"

    @classmethod
    def recognises(cls, source: bytes) -> bool:
        return _is_validation_result(peek_json(source))

    def _import(self, document, reference: str) -> Iterable[Imported]:
        if not _is_validation_result(document):
            reject(
                self.system,
                reference,
                "is not a Great Expectations validation result: it has no "
                "'results' list of expectation results",
            )
        results = document["results"]
        configurations = [
            self._configuration(result, position, reference)
            for position, result in enumerate(results)
        ]
        # A label is settled for the suite as a whole: an expectation type
        # with no column is named by its position once it repeats.
        labels = _told_apart(
            checkspec.labels_for_checks(
                [
                    {"type": kind, "column": kwargs.get("column", None)}
                    for kind, kwargs in configurations
                ],
                (),
                "type",
                ("column", "columns"),
            )
        )
        for position, result in enumerate(results):
            kind, kwargs = configurations[position]
            success = result.get("success")
            if type(success) is not bool:
                reject(
                    self.system,
                    reference,
                    f"has success {success!r}, which is not true or false",
                    position=position,
                    record=kind,
                )
            raised = _raised(result)
            identifiers = {"expectation": kind}
            if isinstance(kwargs.get("column", None), str):
                identifiers["column"] = kwargs["column"]
            yield Imported(
                label=labels[position],
                success=success,
                criticality=(
                    "warn"
                    if result["expectation_config"].get("severity") == WARNING
                    else "error"
                ),
                status="error" if raised else ("success" if success else "failure"),
                observed=numbers_of(result.get("result")) or None,
                identifiers=identifiers,
                position=position,
            )

    def _run(self, document) -> Optional[str]:
        meta = document.get("meta")
        if not isinstance(meta, dict):
            return None
        validation = meta.get("validation_id")
        if isinstance(validation, str) and validation:
            return validation
        run = meta.get("run_id")
        if isinstance(run, dict) and isinstance(run.get("run_name"), str):
            return run["run_name"] or None
        return None

    def _configuration(self, result, position: int, reference: str) -> Tuple[str, dict]:
        """Return ``(type, kwargs)`` of the expectation behind a result."""
        if not isinstance(result, dict):
            reject(self.system, reference, "is not an object", position=position)
        config = result.get("expectation_config")
        if not isinstance(config, dict):
            reject(
                self.system,
                reference,
                "has no 'expectation_config' object",
                position=position,
            )
        kind = config.get("type")
        if not isinstance(kind, str) or not kind:
            reject(
                self.system,
                reference,
                "has no expectation 'type'",
                position=position,
            )
        kwargs = config.get("kwargs")
        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            reject(
                self.system,
                reference,
                f"has arguments {kwargs!r}, which are not an object",
                position=position,
                record=kind,
            )
        return kind, kwargs
