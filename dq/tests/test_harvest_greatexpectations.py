# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Great Expectations validation-result importer (SPEC-013 G2).

The document shape is the one Great Expectations 1.24.1 and 1.25.0 write.
"""

import hashlib
import json

import pytest

from dq.exceptions import HarvestError
from dq.harvest import HARVESTERS, harvest
from dq.tests.harvest_support import (
    GX_EXAMPLE,
    VALIDATION,
    column_result,
    gx_document,
    gx_result,
    import_bytes,
    import_json,
)

gx = HARVESTERS["greatexpectations"]

NOT_NULL = "expect_column_values_to_not_be_null"
ROW_COUNT = "expect_table_row_count_to_equal"
IN_SET = "expect_column_values_to_be_in_set"


def text_with_result(result_json: str) -> bytes:
    """A one-result validation result whose ``result`` is written by hand."""
    return (
        '{"suite_name": "s", "meta": {"validation_id": "v"}, "results": ['
        '{"success": true, "result": ' + result_json + ", "
        '"exception_info": {"raised_exception": false}, '
        '"expectation_config": {"type": "' + ROW_COUNT + '", "kwargs": {}}}]}'
    ).encode("utf-8")


def test_each_result_becomes_one_outcome_with_its_type_and_measurements():
    outcomes = import_json(
        gx_document(
            [
                column_result(NOT_NULL, False, column="id"),
                gx_result(ROW_COUNT, False, result={"observed_value": 5}),
                column_result(IN_SET, True, column="status"),
            ]
        )
    )
    assert [outcome["check"] for outcome in outcomes] == [
        "expect_column_values_to_not_be_null_id",
        "expect_table_row_count_to_equal",
        "expect_column_values_to_be_in_set_status",
    ]
    assert [o["details"]["expectation"] for o in outcomes] == [
        NOT_NULL,
        ROW_COUNT,
        IN_SET,
    ]
    assert outcomes[0]["details"]["column"] == "id"
    assert "column" not in outcomes[1]["details"]  # table-level
    assert outcomes[0]["details"]["observed"] == {
        "element_count": 5,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 1,
        "unexpected_percent": 20.0,
    }
    assert outcomes[1]["details"]["observed"] == {"observed_value": 5}
    assert [o["success"] for o in outcomes] == [False, False, True]


def test_row_values_row_locators_and_tracebacks_are_never_copied():
    """The source holds all three; the evidence holds none."""
    outcomes = import_json(
        gx_document(
            [
                column_result(IN_SET, False, column="status"),
                column_result(NOT_NULL, column="missing_column", raised=True),
            ]
        )
    )
    evidence = json.dumps(outcomes)
    assert "weird" not in evidence  # a row value
    assert "partial_unexpected" not in evidence  # where GX keeps them
    assert "Traceback" not in evidence and "/home/someone" not in evidence
    assert "missing_column" in evidence  # the column is an identifier: it stays


@pytest.mark.parametrize(
    "result, success, status",
    [
        (column_result(NOT_NULL, True), True, "success"),
        (column_result(NOT_NULL, False), False, "failure"),
        (column_result(NOT_NULL, raised=True), False, "error"),
    ],
)
def test_the_status_is_gx_s_own_word_for_it(result, success, status):
    (outcome,) = import_json(gx_document([result]))
    assert outcome["success"] is success
    assert outcome["details"]["status"] == status


def test_a_raised_expectation_is_a_failed_check_with_no_verdict():
    """GX reports it as ``success: false`` with an empty result."""
    (outcome,) = import_json(gx_document([column_result(NOT_NULL, raised=True)]))
    assert outcome["success"] is False
    assert outcome["details"]["criticality"] == "error"
    assert "observed" not in outcome["details"]


def test_a_raised_exception_flag_is_read_the_same_way():
    result = gx_result(NOT_NULL, success=False, column="id")
    result["exception_info"] = {"raised_exception": True}
    (outcome,) = import_json(gx_document([result]))
    assert outcome["details"]["status"] == "error"


@pytest.mark.parametrize(
    "severity, criticality",
    [("warning", "warn"), ("critical", "error"), (None, "error"), ("WARNING", "error")],
)
def test_only_a_warning_severity_is_advisory(severity, criticality):
    (outcome,) = import_json(
        gx_document([gx_result(ROW_COUNT, False, severity=severity)])
    )
    assert outcome["details"]["criticality"] == criticality


@pytest.mark.parametrize("success", ["yes", "true", 1, 0, None, [True]])
def test_success_must_be_a_json_boolean(success):
    result = gx_result(ROW_COUNT)
    result["success"] = success  # written as it is, null included
    document = gx_document([column_result(NOT_NULL, True), result])
    with pytest.raises(HarvestError) as refused:
        import_json(document)
    error = refused.value
    assert f"record 1: '{ROW_COUNT}' has success" in str(error)
    assert (error.position, error.record, error.system) == (
        1,
        ROW_COUNT,
        "greatexpectations",
    )


def test_a_result_without_success_is_rejected():
    result = gx_result(ROW_COUNT)
    del result["success"]
    with pytest.raises(
        HarvestError,
        match="record 0: 'expect_table_row_count_to_equal' has success None",
    ):
        import_json(gx_document([result]))


@pytest.mark.parametrize(
    "result, problem",
    [
        ("a string", "record 0 is not an object"),
        ({"success": True}, "record 0 has no 'expectation_config' object"),
        (
            {"success": True, "expectation_config": "x"},
            "has no 'expectation_config' object",
        ),
        (
            {"success": True, "expectation_config": {}},
            "record 0 has no expectation 'type'",
        ),
        (
            {"success": True, "expectation_config": {"type": 7}},
            "record 0 has no expectation 'type'",
        ),
        (
            {"success": True, "expectation_config": {"type": "t", "kwargs": []}},
            "record 0: 't' has arguments \\[\\], which are not an object",
        ),
    ],
)
def test_a_result_that_is_malformed_is_rejected(result, problem):
    with pytest.raises(HarvestError, match=problem):
        import_json(gx_document([result]))


def test_arguments_may_be_absent():
    result = {"success": True, "expectation_config": {"type": ROW_COUNT}}
    (outcome,) = import_json(gx_document([result]))
    assert outcome["check"] == ROW_COUNT


def test_two_expectations_of_one_type_on_one_column_are_told_apart_by_position():
    """GX has no name for an expectation, and the engine would ask for one."""
    between = "expect_column_values_to_be_between"
    outcomes = import_json(
        gx_document(
            [
                gx_result(between, column="amount"),
                gx_result(IN_SET, column="status"),
                gx_result(between, column="amount", arguments={"max_value": 100}),
            ]
        )
    )
    assert [outcome["check"] for outcome in outcomes] == [
        "expect_column_values_to_be_between_amount",
        "expect_column_values_to_be_in_set_status",
        "expect_column_values_to_be_between_amount_2",
    ]


def test_two_table_expectations_of_one_type_are_told_apart_by_position():
    outcomes = import_json(gx_document([gx_result(ROW_COUNT), gx_result(ROW_COUNT)]))
    assert [outcome["check"] for outcome in outcomes] == [
        ROW_COUNT,
        ROW_COUNT + "_1",
    ]


def test_an_empty_suite_is_rejected_though_gx_calls_it_a_success():
    document = gx_document([])
    assert document["success"] is True
    with pytest.raises(HarvestError, match="holds no check results"):
        import_json(document)


def test_a_document_that_is_not_a_validation_result_is_rejected():
    with pytest.raises(
        HarvestError, match="is not a Great Expectations validation result"
    ):
        gx.harvest(b'{"results": [1, 2]}', reference="validation.json")


@pytest.mark.parametrize(
    "source",
    [
        b'{"metadata": {"dbt_version": "1.12.7"}, "results": []}',  # dbt's shape
        b"[]",
        b"check,success\na,true\n",
        b"not json",
        b"\xff",
    ],
)
def test_nothing_but_a_validation_result_is_recognised(source):
    assert gx.recognises(source) is False


def test_a_validation_result_is_recognised():
    assert gx.recognises(GX_EXAMPLE.read_bytes()) is True
    assert gx.recognises(b'{"results": [], "statistics": {}}') is True


def test_the_validation_is_the_run_in_the_provenance():
    (outcome,) = import_json(gx_document([gx_result(ROW_COUNT)]))
    assert outcome["details"]["origin"]["run"] == VALIDATION


def test_the_run_name_stands_in_when_there_is_no_validation_id():
    document = gx_document(
        [gx_result(ROW_COUNT)],
        validation_id=None,
        run_id={"run_name": "a-run", "run_time": "2026-11-25T11:30:58-06:00"},
    )
    (outcome,) = import_json(document)
    assert outcome["details"]["origin"]["run"] == "a-run"


@pytest.mark.parametrize(
    "meta",
    [
        {"validation_id": None, "run_id": {"run_name": None}},
        {"validation_id": None, "run_id": {"run_name": ""}},
        {"validation_id": None, "run_id": "not-an-object"},
        {"validation_id": 7},
    ],
)
def test_without_a_run_the_provenance_has_none(meta):
    (outcome,) = import_json(gx_document([gx_result(ROW_COUNT)], **meta))
    assert "run" not in outcome["details"]["origin"]


def test_a_document_without_meta_has_no_run():
    document = gx_document([gx_result(ROW_COUNT)])
    del document["meta"]
    (outcome,) = import_json(document)
    assert "run" not in outcome["details"]["origin"]


def test_measurements_are_the_numbers_at_the_top_level_and_nothing_else():
    source = text_with_result(
        '{"unexpected_count": 2, "ratio": 0.5, "observed_value": "float64", '
        '"flag": true, "list": [1, 2], "nested": {"n": 1}, '
        '"overflow": 1e400, "": 0, "' + "x" * 65 + '": 1}'
    )
    (outcome,) = import_bytes(source)
    assert outcome["details"]["observed"] == {"unexpected_count": 2, "ratio": 0.5}


def test_at_most_32_measurements_are_copied():
    measurements = ", ".join(f'"m{index}": {index}' for index in range(40))
    (outcome,) = import_bytes(text_with_result("{" + measurements + "}"))
    assert len(outcome["details"]["observed"]) == 32
    assert outcome["details"]["observed"]["m31"] == 31


@pytest.mark.parametrize("result", ["null", "[1, 2]", '"text"', "3"])
def test_a_result_that_holds_no_object_holds_no_measurements(result):
    (outcome,) = import_bytes(text_with_result(result))
    assert "observed" not in outcome["details"]


def test_an_empty_result_holds_no_measurements():
    (outcome,) = import_json(gx_document([column_result(NOT_NULL, result={})]))
    assert "observed" not in outcome["details"]


def test_the_example_gx_wrote_imports():
    source = GX_EXAMPLE.read_bytes()
    outcomes = harvest(source, reference="validation_result.json")
    assert [outcome["check"] for outcome in outcomes] == [
        "expect_column_values_to_be_in_set_status",
        "expect_column_values_to_not_be_null_order_id",
        "expect_column_values_to_be_of_type_order_id",
        "expect_table_row_count_to_equal",
        "expect_compound_columns_to_be_unique",
        "expect_table_columns_to_match_ordered_list",
    ]
    by_check = {outcome["check"]: outcome for outcome in outcomes}
    # Row values, a row locator and a list of columns: none of them copied.
    assert by_check["expect_column_values_to_be_in_set_status"]["details"][
        "observed"
    ] == {
        "element_count": 5,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 1,
        "unexpected_percent": 20.0,
        "unexpected_percent_nonmissing": 20.0,
        "unexpected_percent_total": 20.0,
    }
    # Its observed value is a type name, not a number.
    assert (
        "observed"
        not in by_check["expect_column_values_to_be_of_type_order_id"]["details"]
    )
    assert (
        "observed"
        not in by_check["expect_table_columns_to_match_ordered_list"]["details"]
    )
    assert by_check["expect_table_row_count_to_equal"]["details"]["observed"] == {
        "observed_value": 5
    }
    assert by_check["expect_compound_columns_to_be_unique"]["success"] is True
    assert outcomes[0]["details"]["origin"] == {
        "system": "greatexpectations",
        "reference": "validation_result.json",
        "sha256": hashlib.sha256(source).hexdigest(),
        "run": "a1cd5a63-3ef7-4233-9b58-590b45b21096",
    }
