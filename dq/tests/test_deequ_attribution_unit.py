# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Deequ result rows are matched to labels, never guessed (SPEC-011).

Where several constraints share one pydeequ Check, each result row is
attributed to its constraint by the Check's description and the row's place
among that Check's rows. These tests pin the matcher and the recording proxy
with fakes; the Spark lane runs both against real Deequ
(``test_check_labels.py``).
"""

import pytest

from dq.engine.deequ.verification import (
    RecordingCheck,
    metrics_from_check_results,
    recording_check,
)
from dq.exceptions import ConfigurationError

DESCRIPTION = "Schema Validation"
UNIQUENESS = "uniqueness"


class ResultRow:
    """One row of pydeequ's ``checkResultsAsDataFrame``."""

    def __init__(self, check, constraint, status="Success"):
        self.fields = {
            "check": check,
            "constraint": constraint,
            "constraint_status": status,
            "check_status": "Error",
            "check_level": "Warning",
        }

    def asDict(self, recursive=True):
        return dict(self.fields)


LABELS = {DESCRIPTION: ["id_complete", "name_complete"], UNIQUENESS: ["ids_unique"]}


def test_each_row_takes_the_label_at_its_place_under_its_check():
    rows = [
        ResultRow(DESCRIPTION, "Completeness(id)", "Success"),
        ResultRow(DESCRIPTION, "Completeness(name)", "Failure"),
        ResultRow(UNIQUENESS, "Uniqueness(id)", "Success"),
    ]
    results = metrics_from_check_results(rows, LABELS)
    assert [(result["check"], result["success"]) for result in results] == [
        ("id_complete", True),
        ("name_complete", False),
        ("ids_unique", True),
    ]


def test_a_passing_constraint_passes_under_a_failing_check():
    """The second row's parent Check failed, as ``check_status`` says; the
    constraint's own status is what its verdict is read from."""
    rows = [
        ResultRow(DESCRIPTION, "Completeness(id)", "Success"),
        ResultRow(DESCRIPTION, "Completeness(name)", "Failure"),
    ]
    # Reading `check_status` instead, as the engines once did, would call the
    # passing constraint failed too.
    first, second = metrics_from_check_results(
        rows, {DESCRIPTION: ["id_complete", "name_complete"]}
    )
    assert first["success"] is True and second["success"] is False
    assert first["details"]["check_status"] == "Error"  # kept as evidence


def test_a_row_no_label_expects_is_refused():
    """A Check reporting more rows than it has constraints is not a match."""
    rows = [
        ResultRow(DESCRIPTION, "Completeness(id)"),
        ResultRow(DESCRIPTION, "Uniqueness(id)"),
    ]
    with pytest.raises(
        ConfigurationError, match=r"constraint 'Uniqueness\(id\)'.*cannot be attributed"
    ):
        metrics_from_check_results(rows, {DESCRIPTION: ["id_complete"]})


def test_a_row_from_an_unknown_check_is_refused():
    rows = [ResultRow("Somewhere else", "Completeness(id)")]
    with pytest.raises(
        ConfigurationError,
        match="does not match any configured check of 'Somewhere else'",
    ):
        metrics_from_check_results(rows, LABELS)


def test_a_labelled_constraint_with_no_row_is_refused():
    rows = [ResultRow(DESCRIPTION, "Completeness(id)")]
    with pytest.raises(
        ConfigurationError,
        match=r"2 check\(s\) are labelled under 'Schema Validation' but 1 result row",
    ):
        metrics_from_check_results(rows, LABELS)


def test_labels_that_are_not_a_mapping_are_refused_before_any_row_is_read():
    with pytest.raises(ConfigurationError, match="labels by Check description"):
        metrics_from_check_results([], ["id_complete"])


# ------------------------------------------------------- the recording proxy


class FakeCheck:
    """What the proxy relies on: a description, private state, and
    constraint methods that return a new Check."""

    description = DESCRIPTION

    def __init__(self, constraints=()):
        self._java_obj = object()
        self.constraints = tuple(constraints)

    def isComplete(self, column, hint=None):
        return FakeCheck(self.constraints + (("isComplete", column),))

    def satisfies(self, assertion, constraintName=None):
        return FakeCheck(self.constraints + (("satisfies", constraintName),))

    def hasConstraint(self):
        return bool(self.constraints)


def test_the_proxy_records_one_label_per_constraint_and_nothing_else():
    labels = {}
    built = recording_check(FakeCheck(), labels).isComplete("id").isComplete("name")
    assert labels == {DESCRIPTION: ["isComplete_id", "isComplete_name"]}
    # Reading the Check, and its private state, is not adding a constraint.
    assert built.description == DESCRIPTION
    assert built.hasConstraint() is True
    assert built._java_obj is built.unwrap()._java_obj
    assert type(built.unwrap()) is FakeCheck
    assert labels == {DESCRIPTION: ["isComplete_id", "isComplete_name"]}


def test_a_constraint_that_takes_no_column_is_labelled_by_its_own_name():
    """``satisfies`` takes an expression, not a column; Deequ is told the
    constraint's name, so that name is the label -- twice over, made distinct."""

    def ends_with_code(column):
        return column.endswith("_code")

    labels = {}
    proxy = recording_check(FakeCheck(), labels)
    proxy.satisfies(ends_with_code, constraintName="codes_only")
    proxy.satisfies(ends_with_code, constraintName="codes_only")
    assert labels == {DESCRIPTION: ["codes_only", "codes_only_2"]}


def test_an_unwrapped_check_is_the_original_object():
    original = FakeCheck()
    proxy = RecordingCheck(original, [])
    assert proxy.unwrap() is original
