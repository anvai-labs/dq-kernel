# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Importer for dbt's ``run_results.json``.

Only data tests are evidence about a dataset: records whose ``unique_id``
begins with ``test.``. A model, seed or snapshot is not a quality result,
and a unit test judges a model's logic on fixed inputs.

The artifact does not record a test's configured severity, so a test that
was not evaluated -- its query failed, or ``dbt build`` skipped it because
an earlier test failed -- cannot be read as the advisory kind. No verdict
is not a pass: it is a failed check that blocks.

Read from dbt-core 1.12.7's own output and dbt's published run-results
schema. Only the fields read here are required; the artifact carries others
(``compiled_code``, ``adapter_response``, and a field its own published
schema does not list), and they are ignored.
"""

from __future__ import annotations

from typing import Iterable, Optional

from dq.harvest.base import Harvester, Imported, is_number, peek_json, reject

#: What a data test's ``unique_id`` begins with.
DATA_TEST = "test."

#: dbt's data-test statuses, and what each becomes: ``(success, criticality)``.
OUTCOMES = {
    "pass": (True, "error"),
    "fail": (False, "error"),
    "warn": (False, "warn"),
    "error": (False, "error"),
    "skipped": (False, "error"),
}

#: The statuses of a test that ran, and so can have taken time. A test
#: that errored ran: its query is what failed. Only a skipped one did not.
RAN = frozenset({"pass", "fail", "warn", "error"})


def _is_run_results(document) -> bool:
    return (
        isinstance(document, dict)
        and isinstance(document.get("results"), list)
        and isinstance(document.get("metadata"), dict)
    )


class DbtRunResultsHarvester(Harvester):
    """Reads a dbt ``run_results.json``."""

    system = "dbt"

    @classmethod
    def recognises(cls, source: bytes) -> bool:
        return _is_run_results(peek_json(source))

    def _import(self, document, reference: str) -> Iterable[Imported]:
        if not _is_run_results(document):
            reject(
                self.system,
                reference,
                "is not a dbt run_results.json: it has no 'results' list under "
                "dbt 'metadata'",
            )
        for position, result in enumerate(document["results"]):
            if not isinstance(result, dict):
                reject(self.system, reference, "is not an object", position=position)
            unique_id = result.get("unique_id")
            if not isinstance(unique_id, str) or not unique_id:
                reject(self.system, reference, "has no 'unique_id'", position=position)
            if not unique_id.startswith(DATA_TEST):
                continue
            status = result.get("status")
            if not (isinstance(status, str) and status in OUTCOMES):
                reject(
                    self.system,
                    reference,
                    f"has status {status!r}, which is not a dbt data-test status",
                    position=position,
                    record=unique_id,
                )
            success, criticality = OUTCOMES[status]
            observed = {}
            failures = result.get("failures")
            if failures is not None:
                if type(failures) is not int or failures < 0:
                    reject(
                        self.system,
                        reference,
                        f"reports its failures as {failures!r}, not a count",
                        position=position,
                        record=unique_id,
                    )
                observed["failures"] = failures
            if status in RAN:
                seconds = result.get("execution_time")
                if seconds is not None:
                    if not is_number(seconds):
                        reject(
                            self.system,
                            reference,
                            f"reports its execution time as {seconds!r}, not a number",
                            position=position,
                            record=unique_id,
                        )
                    observed["execution_time"] = seconds
            yield Imported(
                label=unique_id,
                success=success,
                criticality=criticality,
                status=status,
                observed=observed or None,
                position=position,
            )

    def _run(self, document) -> Optional[str]:
        invocation = document["metadata"].get("invocation_id")
        return invocation if isinstance(invocation, str) and invocation else None
