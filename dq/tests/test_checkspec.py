# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the shared check seam (engine-neutral rule axes).

The seam is the coupling decision of the plugin architecture: identity and
severity normalize here once, engine vocabularies pass through untouched.
"""

import pytest

from dq.engine import checkspec
from dq.engine.checkspec import (
    build_result,
    derive_name,
    labels_for_checks,
    normalize_criticality,
)

# ------------------------------------------------------------- criticality


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("error", "error"),
        ("err", "error"),
        ("Error", "error"),
        ("ERROR", "error"),
        ("warn", "warn"),
        ("Warning", "warn"),
        ("warning", "warn"),
        (" warn ", "warn"),
        (None, "error"),
        ("", "error"),
    ],
)
def test_normalize_criticality_accepts_all_engine_spellings(raw, expected):
    assert normalize_criticality(raw) == expected


def test_normalize_criticality_honors_custom_default():
    assert normalize_criticality(None, default="warn") == "warn"


def test_normalize_criticality_fails_closed_on_unknown():
    with pytest.raises(ValueError, match="Unknown criticality"):
        normalize_criticality("fatal")


# ------------------------------------------------------------------ naming


def test_derive_name_is_positional_and_stable():
    assert derive_name("is_not_null", 0) == "is_not_null_0"
    assert derive_name("is_unique", 12) == "is_unique_12"


# -------------------------------------------------------------- build_result


def test_build_result_basic_shape_and_success_coercion():
    assert build_result("a", 1, {"x": 1}) == {
        "check": "a",
        "success": True,
        "details": {"x": 1},
    }
    assert build_result("a", "", None) == {
        "check": "a",
        "success": False,
        "details": {},
    }


def test_build_result_merges_criticality_without_mutating_caller_details():
    details = {"failed_rows": 3}
    result = build_result("a", False, details, criticality="Warning")
    assert result["details"] == {"failed_rows": 3, "criticality": "warn"}
    assert details == {"failed_rows": 3}  # caller dict untouched


def test_build_result_criticality_none_keeps_details_identity():
    details = {"x": 1}
    result = build_result("a", True, details)
    assert result["details"] is details  # no copy when nothing to merge


# ---------------------------------------------------------- the labelling rule


def test_labels_prefer_the_configured_name_then_kind_and_columns():
    checks = [
        {"constraint": "isComplete", "column": "id"},
        {"constraint": "isComplete", "column": "id", "constraint_name": "ids"},
        {"constraint": "isUnique", "columns": ["a", "b"]},
        {"constraint": "hasSize"},
    ]
    assert labels_for_checks(
        checks, ("constraint_name",), "constraint", ("column", "columns")
    ) == [
        "isComplete_id",
        "ids",
        "isUnique_a_b",
        "hasSize",
    ]


def test_the_first_configured_name_key_wins():
    checks = [{"constraint": "c", "alias": "second", "description": "third"}]
    assert labels_for_checks(checks, ("alias", "description"), "constraint", ()) == [
        "second"
    ]


def test_a_second_check_of_the_same_kind_without_columns_takes_its_position():
    checks = [
        {"constraint": "hasSize"},
        {"constraint": "hasSize"},
        {"constraint": "hasSize"},
    ]
    assert labels_for_checks(checks, (), "constraint", ("column", "columns")) == [
        "hasSize",
        "hasSize_1",
        "hasSize_2",
    ]


def test_labels_for_checks_does_not_change_the_configuration():
    from pyhocon import ConfigFactory

    checks = ConfigFactory.from_dict(
        {"checks": [{"constraint": "isComplete", "column": "id"}]}
    ).get("checks")
    labels_for_checks(checks, ("name",), "constraint", ("column", "columns"))
    assert list(checks[0].keys()) == ["constraint", "column"]
