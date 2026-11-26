# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Evidence is attributable: each result has its own name and own verdict.

The engine conformance suite (SPEC-011) found that four built-in engines
reported several checks under one name, and that wherever several
constraints shared a Deequ Check, a passing constraint was reported as
failed as soon as a sibling failed. These tests pin the fixes.
"""

import pytest
from py4j.protocol import Py4JJavaError
from pyhocon import ConfigFactory

from dq.dq_framework import DQFramework
from dq.engine.businessrules.businessrules_engine import BusinessrulesEngine
from dq.engine.checkspec import require_unique_labels
from dq.engine.deequ.deequ_engine import DeequEngine
from dq.engine.deequ.verification import metrics_from_check_results
from dq.engine.greatexpectations.greatexpectations_check import (
    GreatexpectationsCheck,
)
from dq.engine.schemavalidation.schemavalidation_engine import SchemavalidationEngine
from dq.exceptions import ConfigurationError
from dq.notifier import InMemoryNotifier

pytestmark = pytest.mark.spark


def labels_of(outcomes):
    return [outcome["check"] for outcome in outcomes]


def verdicts_of(outcomes):
    return {outcome["check"]: outcome["success"] for outcome in outcomes}


# ------------------------------------------------------------------- deequ

# Both columns are incomplete.
INCOMPLETE = [(1, "ann"), (2, None), (None, "cy")]


@pytest.fixture
def frame(spark):
    return spark.createDataFrame(INCOMPLETE, "id long, name string")


def test_deequ_labels_a_check_by_its_configured_name(frame):
    config = ConfigFactory.from_dict(
        {
            "name": "labels",
            "engine": "deequ",
            "checks": [
                {
                    "constraint_name": "id_complete",
                    "constraint": "isComplete",
                    "column": "id",
                },
                {
                    "constraint_name": "name_complete",
                    "constraint": "isComplete",
                    "column": "name",
                },
            ],
        }
    )
    assert verdicts_of(DeequEngine(config).apply(frame)) == {
        "id_complete": False,
        "name_complete": False,
    }


def test_deequ_derives_a_label_from_the_constraint_and_its_columns(frame):
    config = ConfigFactory.from_dict(
        {
            "name": "derived",
            "engine": "deequ",
            "checks": [
                {"constraint": "isComplete", "column": "id"},
                {"constraint": "isComplete", "column": "name"},
                {"constraint": "isNonNegative", "column": "id"},
            ],
        }
    )
    assert labels_of(DeequEngine(config).apply(frame)) == [
        "isComplete_id",
        "isComplete_name",
        "isNonNegative_id",
    ]


def test_deequ_refuses_two_checks_that_would_share_a_label(frame):
    config = ConfigFactory.from_dict(
        {
            "name": "twins",
            "engine": "deequ",
            "checks": [
                {"constraint": "isComplete", "column": "id"},
                {"alias": "isComplete_id", "constraint": "isUnique", "column": "id"},
            ],
        }
    )
    with pytest.raises(
        ValueError, match="Duplicate check label 'isComplete_id': checks 0 and 1"
    ):
        DeequEngine(config).apply(frame)


def test_a_deequ_rule_with_no_checks_is_refused_by_the_engine_itself():
    """The validator refuses an empty ``checks`` payload before this point.
    This is the engine's own lock, for a rule built directly: a rule that
    validates nothing must not look like a rule that passed."""
    from dq.engine.deequ.deequ_check import DeequCheck

    # Built as the engine builds it: on the rule's ``checks`` list.
    with pytest.raises(ValueError, match="needs at least one check"):
        DeequCheck([]).apply_checks(None, None)


def test_deequ_single_check_mode_keeps_each_constraint_s_own_verdict(spark):
    """One Check holds every constraint; a failing one must not sink the rest."""
    # Both columns complete, but id repeats.
    frame = spark.createDataFrame([(1, "ann"), (1, "bob")], ["id", "name"])
    config = ConfigFactory.from_dict(
        {
            "name": "single",
            "engine": "deequ",
            "single_check_mode": True,
            "checks": [
                {
                    "constraint_name": "id_complete",
                    "constraint": "isComplete",
                    "column": "id",
                },
                {
                    "constraint_name": "name_complete",
                    "constraint": "isComplete",
                    "column": "name",
                },
                {
                    "constraint_name": "ids_unique",
                    "constraint": "isUnique",
                    "column": "id",
                },
            ],
        }
    )
    assert verdicts_of(DeequEngine(config).apply(frame)) == {
        "id_complete": True,
        "name_complete": True,
        "ids_unique": False,
    }


# ---------------------------------------------------------- schemavalidation

# id is complete and unique; name is incomplete.
SCHEMA_ROWS = [(1, "ann"), (2, None), (3, "cy")]
SCHEMA_CASES = {
    "not-null": {"not_null_columns": ["id", "name"]},
    "unique": {"unique_constraints": [["id"]]},
    "both": {"not_null_columns": ["id", "name"], "unique_constraints": [["id"]]},
}


def schema_probe(spark, case, single_check_mode):
    frame = spark.createDataFrame(SCHEMA_ROWS, ["id", "name"])
    frame.createOrReplaceTempView("label_probe")
    config = ConfigFactory.from_dict(
        {
            "name": "schema",
            "engine": "schemavalidation",
            "schema": {
                "catalog_type": "spark",
                "table": "label_probe",
                **SCHEMA_CASES[case],
            },
        }
    )
    config.put("single_check_mode", single_check_mode)
    return SchemavalidationEngine(config).apply(frame)


@pytest.mark.parametrize(
    "single_check_mode", [True, False], ids=["single-check", "multi-check"]
)
@pytest.mark.parametrize("case", ["not-null", "unique"])
def test_schema_validation_names_each_check_and_keeps_its_own_verdict(
    spark, case, single_check_mode
):
    outcomes = schema_probe(spark, case, single_check_mode)
    labels = labels_of(outcomes)
    assert len(labels) == len(set(labels)), f"labels repeat: {labels}"
    assert len(labels) > 1, "one Check's constraints are reported individually"
    if case == "not-null":
        # The passing constraint must not be reported as failed because its
        # sibling in the same Check failed.
        assert verdicts_of(outcomes)["isComplete_id"] is True
        assert verdicts_of(outcomes)["isComplete_name"] is False
    else:
        assert verdicts_of(outcomes)["hasUniqueness_id"] is True


def test_schema_validation_single_check_names_not_null_and_unique_together(spark):
    verdicts = verdicts_of(schema_probe(spark, "both", True))
    assert verdicts["isComplete_id"] is True
    assert verdicts["isComplete_name"] is False
    assert verdicts["hasUniqueness_id"] is True


# Seven lambda-bearing Checks is past the threshold of the known upstream
# pydeequ defect (see PYDEEQU_MULTI_CHECK_CRASH in the uniqueness tests): it
# crashes with the old Check names too, so it is pinned, not worked around.
@pytest.mark.xfail(
    strict=True,
    raises=Py4JJavaError,
    reason="pydeequ: Scala Map hash collision over Checks reaches the "
    "unhashable ScalaFunction1 assertion wrapper",
)
def test_schema_validation_multi_check_names_not_null_and_unique_together(spark):
    verdicts = verdicts_of(schema_probe(spark, "both", False))
    assert verdicts["isComplete_name"] is False


def test_schema_validation_refuses_to_attribute_results_it_cannot_match():
    """A labelled constraint with no result row is an error, never a guess."""
    with pytest.raises(ConfigurationError, match="cannot be attributed"):
        metrics_from_check_results([], {"Schema Validation": ["isComplete_id"]})


# ----------------------------------------------------------------------- gx

NULL = "expect_column_values_to_not_be_null"
ROWS = "expect_table_row_count_to_be_between"


def gx_labels(config):
    """The labels a rule's expectations would report, after the uniqueness check."""
    labels = GreatexpectationsCheck(config.get("expectations")).rule_labels()
    require_unique_labels(labels)
    return labels


@pytest.mark.parametrize(
    ("expectations", "labels"),
    [
        ([{"type": NULL, "column": "id"}], [f"{NULL}_id"]),
        ([{"type": NULL, "column": "id", "name": "ids"}], ["ids"]),
        # A check with no columns is named by its kind alone...
        ([{"type": ROWS}], [ROWS]),
        # ...unless an earlier check has the same kind.
        (
            [
                {"type": ROWS, "min_value": 1},
                {"type": NULL, "column": "id"},
                {"type": ROWS, "min_value": 5},
            ],
            [ROWS, f"{NULL}_id", f"{ROWS}_2"],
        ),
        (
            [{"type": NULL, "column": "id"}, {"type": NULL, "column": "name"}],
            [f"{NULL}_id", f"{NULL}_name"],
        ),
    ],
    ids=["column", "explicit", "table-level", "two-table-level", "two-columns"],
)
def test_gx_labels_by_name_then_type_and_column(expectations, labels):
    assert gx_labels(ConfigFactory.from_dict({"expectations": expectations})) == labels


def test_gx_refuses_two_expectations_that_would_share_a_label():
    twins = ConfigFactory.from_dict(
        {
            "expectations": [
                {"type": NULL, "column": "id"},
                {"type": NULL, "column": "id", "mostly": 0.9},
            ]
        }
    )
    with pytest.raises(ValueError, match="Duplicate check label.*checks 0 and 1"):
        gx_labels(twins)


# ------------------------------------------------------------- businessrules


def test_businessrules_names_each_outcome_for_its_constraint(spark):
    frame = spark.createDataFrame(
        [("east", 1, 10), ("east", 1, 20), ("west", 3, 5), ("west", 4, -1)],
        ["region", "id", "amount"],
    )
    config = ConfigFactory.from_dict(
        {
            "name": "rules",
            "engine": "businessrules",
            "checks": [
                {"constraint": "NoNegativeValues"},
                {
                    "constraint": "GroupedDistinctBounds",
                    "columns": ["id"],
                    "group_by": ["region"],
                    "min": 3,
                },
            ],
        }
    )
    outcomes = BusinessrulesEngine(config).apply(frame)
    # A constraint reporting one row per column names each column, without
    # the legacy "None" placeholder for an unconfigured source; a constraint
    # reporting one row is named for itself.
    assert verdicts_of(outcomes) == {
        "NoNegativeValues for region": True,
        "NoNegativeValues for id": True,
        "NoNegativeValues for amount": False,
        "GroupedDistinctBounds": False,
    }
    # The frozen instance strings are still reported in the evidence.
    assert {outcome["details"]["instance"] for outcome in outcomes} == {
        "NoNegativeValues for region None",
        "NoNegativeValues for id None",
        "NoNegativeValues for amount None",
        "GroupedDistinctBounds ['region'] for id",
    }


# ------------------------------------------------- through the framework


def test_the_gate_event_names_each_failing_check(spark):
    """Two Warning-level checks on two columns are told apart in the event."""
    frame = spark.createDataFrame([(1, None), (None, "x")], "id long, name string")
    recorder = InMemoryNotifier()
    framework = DQFramework.__new__(DQFramework)
    framework._config = {
        "dqframework": {
            "dqrules": [
                {
                    "name": "gate",
                    "engine": "deequ",
                    "checks": [
                        {
                            "constraint": "isComplete",
                            "column": "id",
                            "level": "Warning",
                        },
                        {
                            "constraint": "isComplete",
                            "column": "name",
                            "level": "Warning",
                        },
                    ],
                }
            ]
        }
    }
    framework._spark = spark
    framework.get_dataframe = lambda name: frame
    framework._notifier = recorder
    results = framework.run()
    assert [result["check"] for result in results] == [
        "isComplete_id",
        "isComplete_name",
    ]
    (event,) = recorder.events
    assert event.warned == ("isComplete_id", "isComplete_name")
    assert event.blocked == ()
