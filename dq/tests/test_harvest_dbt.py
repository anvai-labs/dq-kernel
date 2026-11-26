# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The dbt ``run_results.json`` importer (SPEC-013 G1).

The statuses and the record shape are the ones dbt-core 1.12.7 writes. The
table under test is the one in the specification.
"""

import hashlib
import json

import pytest

from dq.exceptions import HarvestError
from dq.harvest import HARVESTERS, harvest
from dq.tests.harvest_support import (
    DBT_EXAMPLE,
    INVOCATION,
    data_test,
    dbt_document,
    dbt_record,
    import_json,
)

dbt = HARVESTERS["dbt"]

#: dbt status -> (success, criticality).
TABLE = {
    "pass": (True, "error"),
    "fail": (False, "error"),
    "warn": (False, "warn"),
    "error": (False, "error"),
    "skipped": (False, "error"),
}


def encoded(document) -> bytes:
    return json.dumps(document).encode("utf-8")


@pytest.mark.parametrize("status, expected", sorted(TABLE.items()))
def test_each_status_maps_as_the_specification_table_says(status, expected):
    (outcome,) = import_json(dbt_document([data_test("not_null_id", status)]))
    assert (outcome["success"], outcome["details"]["criticality"]) == expected
    # dbt's own word is kept: the outcome does not pretend to know more.
    assert outcome["details"]["status"] == status


def test_the_unique_id_is_the_label():
    (outcome,) = import_json(dbt_document([dbt_record("test.a.b", "pass")]))
    assert outcome["check"] == "test.a.b"


def test_only_data_tests_are_imported():
    """A model is not a quality result; a unit test judges a model's logic."""
    document = dbt_document(
        [
            dbt_record("model.analytics.orders", "success"),
            data_test("not_null_id", "pass"),
            dbt_record("unit_test.analytics.orders.labels", "pass"),
            dbt_record("seed.analytics.codes", "success"),
            dbt_record("snapshot.analytics.orders", "success"),
            data_test("unique_id", "fail"),
        ]
    )
    outcomes = import_json(document)
    assert [outcome["check"] for outcome in outcomes] == [
        "test.analytics.not_null_id.cf6c17daed",
        "test.analytics.unique_id.cf6c17daed",
    ]


def test_counts_and_times_are_kept_and_the_message_is_not():
    (outcome,) = import_json(
        dbt_document(
            [
                data_test(
                    "not_null_id",
                    "fail",
                    failures=3,
                    execution_time=0.1875,
                    message="Got 3 results, configured to fail if != 0",
                    compiled_code="select * from orders where id is null",
                )
            ]
        )
    )
    assert outcome["details"]["observed"] == {"failures": 3, "execution_time": 0.1875}
    assert "message" not in outcome["details"]
    assert "select" not in str(outcome)


def test_a_skipped_test_reports_no_time_and_no_count():
    """dbt writes ``execution_time: 0`` for a test that never ran."""
    (outcome,) = import_json(
        dbt_document([data_test("not_null_id", "skipped", execution_time=0)])
    )
    assert "observed" not in outcome["details"]


def test_an_errored_test_reports_no_count():
    (outcome,) = import_json(
        dbt_document(
            [dbt_record("test.a.broken", "error", message="Runtime Error in test ...")]
        )
    )
    assert outcome["details"]["observed"] == {"execution_time": 0.01}


def test_a_passing_test_reports_zero_failures():
    (outcome,) = import_json(
        dbt_document([data_test("not_null_id", "pass", failures=0)])
    )
    assert outcome["details"]["observed"]["failures"] == 0


def test_an_execution_time_may_be_absent():
    record = data_test("not_null_id", "pass")
    del record["execution_time"]
    (outcome,) = import_json(dbt_document([record]))
    assert "observed" not in outcome["details"]


@pytest.mark.parametrize(
    "status", ["skip", "pass ", "PASS", "success", "", None, ["pass"]]
)
def test_a_status_that_is_not_a_data_test_status_is_rejected(status):
    document = dbt_document(
        [data_test("not_null_id", "pass"), data_test("unique_id", status)]
    )
    with pytest.raises(HarvestError) as refused:
        import_json(document)
    error = refused.value
    # The record and its position: the second of dbt's own results list.
    assert "record 1: 'test.analytics.unique_id.cf6c17daed' has status" in str(error)
    assert (error.system, error.position, error.record) == (
        "dbt",
        1,
        "test.analytics.unique_id.cf6c17daed",
    )


def test_a_position_counts_every_record_not_only_tests():
    document = dbt_document(
        [dbt_record("model.a.b", "success"), data_test("not_null_id", "skip")]
    )
    with pytest.raises(HarvestError, match=r"record 1: 'test\.analytics\.not_null_id"):
        import_json(document)


@pytest.mark.parametrize(
    "record, problem",
    [
        ("a string", "record 0 is not an object"),
        ({"status": "pass"}, "record 0 has no 'unique_id'"),
        ({"status": "pass", "unique_id": ""}, "record 0 has no 'unique_id'"),
        ({"status": "pass", "unique_id": 7}, "record 0 has no 'unique_id'"),
    ],
)
def test_a_record_that_is_malformed_is_rejected(record, problem):
    with pytest.raises(HarvestError, match=problem):
        import_json(dbt_document([record]))


@pytest.mark.parametrize("failures", ["3", 3.0, True, -1])
def test_failures_that_are_not_a_count_are_rejected(failures):
    document = dbt_document([data_test("not_null_id", "fail", failures=failures)])
    with pytest.raises(HarvestError, match="reports its failures as"):
        import_json(document)


@pytest.mark.parametrize("seconds", ["0.1", True])
def test_an_execution_time_that_is_not_a_number_is_rejected(seconds):
    document = dbt_document([data_test("not_null_id", "pass", execution_time=seconds)])
    with pytest.raises(HarvestError, match="reports its execution time as"):
        import_json(document)


def test_the_invocation_is_the_run_in_the_provenance():
    (outcome,) = import_json(dbt_document([data_test("not_null_id", "pass")]))
    assert outcome["details"]["origin"]["run"] == INVOCATION


@pytest.mark.parametrize("invocation", [None, "", 7])
def test_without_an_invocation_there_is_no_run(invocation):
    (outcome,) = import_json(
        dbt_document([data_test("not_null_id", "pass")], invocation=invocation)
    )
    assert "run" not in outcome["details"]["origin"]


def test_the_artifact_of_a_run_holds_no_checks_and_is_rejected():
    """`dbt run` writes models only. Nothing was checked."""
    document = dbt_document([dbt_record("model.a.b", "success")])
    with pytest.raises(HarvestError, match="holds no check results"):
        import_json(document)


def test_an_empty_results_list_is_rejected():
    with pytest.raises(HarvestError, match="holds no check results"):
        import_json(dbt_document([]))


def test_a_document_that_is_not_run_results_is_rejected():
    with pytest.raises(HarvestError, match="is not a dbt run_results.json"):
        dbt.harvest(b'{"results": []}', reference="run_results.json")


@pytest.mark.parametrize(
    "source",
    [
        b'{"results": [], "suite_name": "s"}',  # no dbt metadata
        b"[]",
        b"not json",
        b"\xff\xfe",
    ],
)
def test_nothing_but_a_run_results_is_recognised(source):
    assert dbt.recognises(source) is False


def test_a_run_results_is_recognised():
    assert dbt.recognises(encoded(dbt_document([]))) is True


def test_the_example_dbt_wrote_imports():
    source = DBT_EXAMPLE.read_bytes()
    outcomes = harvest(source, reference="run_results.json")
    # Seven records, one a model: six checks, in the order dbt wrote them.
    assert [
        (o["success"], o["details"]["criticality"], o["details"]["status"])
        for o in outcomes
    ] == [
        (False, "warn", "warn"),
        (False, "error", "error"),
        (False, "warn", "warn"),
        (True, "error", "pass"),
        (False, "error", "fail"),
        (False, "error", "skipped"),
    ]
    assert outcomes[0]["check"] == (
        "test.probe.accepted_values_orders_status__new__shipped__cancelled.e6f11ca23e"
    )
    assert outcomes[-1]["details"] == {
        "status": "skipped",
        "origin": {
            "system": "dbt",
            "reference": "run_results.json",
            "sha256": hashlib.sha256(source).hexdigest(),
            "run": "11cfc1e8-98df-4b89-9570-fc85ee756525",
        },
        "criticality": "error",
    }
