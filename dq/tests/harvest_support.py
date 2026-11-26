# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Sources for the harvester tests (SPEC-013), in the shapes the vendors write.

The shapes are the ones read from the tools' own output: a ``run_results.json``
written by dbt-core 1.12.7 and validation results written by Great
Expectations 1.24.1 and 1.25.0. The checked-in examples under
``examples/import/`` are trimmed copies of those real documents.
"""

import json
from pathlib import Path

from dq.harvest import harvest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "import"
DBT_EXAMPLE = EXAMPLES / "run_results.json"
GX_EXAMPLE = EXAMPLES / "validation_result.json"
CSV_EXAMPLE = EXAMPLES / "checks.csv"
COLUMNAR_JSON_EXAMPLE = EXAMPLES / "checks.json"

EVERY_EXAMPLE = (DBT_EXAMPLE, GX_EXAMPLE, CSV_EXAMPLE, COLUMNAR_JSON_EXAMPLE)

DBT_SCHEMA = "https://schemas.getdbt.com/dbt/run-results/v6.json"
INVOCATION = "11cfc1e8-98df-4b89-9570-fc85ee756525"
VALIDATION = "64f01f7d-b9cb-4b85-a749-0286f9e3f179"

CRITICAL = "critical"


def json_bytes(document) -> bytes:
    return json.dumps(document).encode("utf-8")


def import_bytes(source: bytes, reference: str = "source", system=None):
    return harvest(source, reference=reference, system=system)


def import_json(document, reference: str = "source", system=None):
    return import_bytes(json_bytes(document), reference, system)


# ------------------------------------------------------------------- dbt


def dbt_record(unique_id, status, **fields):
    """One entry of a run_results.json ``results`` list, as dbt writes it."""
    return {
        "status": status,
        "timing": [],
        "thread_id": "Thread-1 (worker)",
        "execution_time": 0.01,
        "adapter_response": {"_message": "OK"},
        "message": None,
        "failures": None,
        "unique_id": unique_id,
        "compiled": True,
        "compiled_code": None,
        "relation_name": None,
        "batch_results": None,
        "overload_results": None,  # not in dbt's published schema
        **fields,
    }


def dbt_document(results, invocation=INVOCATION, **metadata):
    """A run_results.json, as dbt-core 1.12.7 writes it (schema v6)."""
    return {
        "metadata": {
            "dbt_schema_version": DBT_SCHEMA,
            "dbt_version": "1.12.7",
            "invocation_id": invocation,
            "invocation_started_at": "2026-11-26T02:17:32.211975Z",
            "generated_at": "2026-11-26T02:17:34.008138Z",
            "env": {},
            **metadata,
        },
        "elapsed_time": 0.38,
        "args": {"which": "build"},
        "results": list(results),
    }


def data_test(name, status="pass", **fields):
    """A generic data test on a column: its unique_id ends in dbt's hash."""
    return dbt_record(f"test.analytics.{name}.cf6c17daed", status, **fields)


# --------------------------------------------------- great expectations


def gx_result(
    kind,
    success=None,
    *,
    column=None,
    severity=CRITICAL,
    result=None,
    arguments=None,
    raised=False,
    **config,
):
    """One entry of a validation result's ``results`` list, as GX writes it.

    ``success`` is true unless given, and false for a raised expectation:
    that is how GX reports one.
    """
    if success is None:
        success = not raised
    kwargs = dict(arguments or {})
    if column is not None:
        kwargs.setdefault("column", column)
    kwargs.setdefault("batch_id", "analytics-orders")
    if raised:
        exception_info = {
            "MetricConfigurationID(metric_name='column_values.nonnull.condition', "
            "metric_domain_kwargs_id='261e598293b01baf9a685efbe20a5e86', "
            "metric_value_kwargs_id=())": {
                "exception_traceback": "Traceback (most recent call last):\n"
                '  File "/home/someone/venv/lib/great_expectations/x.py", '
                "line 1, in validate\n    ...\nKeyError: 'missing_column'",
                "exception_message": "'missing_column'",
            }
        }
    else:
        exception_info = {
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        }
    return {
        "success": success,
        "exception_info": exception_info,
        "result": {} if result is None else result,
        "meta": {},
        "expectation_config": {
            "id": None,
            "type": kind,
            "kwargs": kwargs,
            "meta": {},
            "severity": severity,
            **config,
        },
    }


def column_result(kind, success=None, *, column="id", **kwargs):
    """A column expectation's result, with the measurements GX reports.

    A raised expectation has none: GX reports it as failed with an empty
    ``result``.
    """
    raised = kwargs.get("raised", False)
    result = kwargs.pop("result", None)
    if result is None:
        result = (
            {}
            if raised
            else {
                "element_count": 5,
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_count": 0 if success is not False else 1,
                "unexpected_percent": 0.0 if success is not False else 20.0,
                "partial_unexpected_list": [] if success is not False else ["weird"],
                "partial_unexpected_counts": (
                    [] if success is not False else [{"value": "weird", "count": 1}]
                ),
                "partial_unexpected_index_list": [] if success is not False else [3],
            }
        )
    return gx_result(kind, success, column=column, result=result, **kwargs)


def gx_document(results, **meta):
    """A validation result, as ``to_json_dict()`` writes it.

    Built around whatever a test hands it, well-formed or not.
    """
    passed = sum(
        1
        for result in results
        if isinstance(result, dict) and result.get("success") is True
    )
    return {
        "id": None,
        "meta": {
            "great_expectations_version": "1.24.1",
            "validation_id": VALIDATION,
            "validation_time": "2026-11-25T17:30:58.038728+00:00",
            "run_id": {
                "run_name": None,
                "run_time": "2026-11-25T11:30:58.038728-06:00",
            },
            **meta,
        },
        "success": passed == len(results),
        "statistics": {
            "evaluated_expectations": len(results),
            "successful_expectations": passed,
            "unsuccessful_expectations": len(results) - passed,
            "success_percent": None,
        },
        "suite_name": "orders_suite",
        "suite_parameters": {},
        "results": list(results),
    }
