# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the shared check seam (engine-neutral rule axes).

The seam is the coupling decision of the plugin architecture: identity and
severity normalize here once, engine vocabularies pass through untouched.
"""

import pytest

from dq.engine import checkspec
from dq.engine.checkspec import (
    CheckSpec,
    build_result,
    derive_name,
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


def test_require_unique_names_passes_distinct_names():
    checkspec.require_unique_names(["a", "b"])  # must not raise


def test_require_unique_names_fails_closed_on_duplicate():
    with pytest.raises(ValueError, match="Duplicate check name 'dup'"):
        checkspec.require_unique_names(["dup", "dup"])


# --------------------------------------------------------------- CheckSpec


def test_checkspec_freezes_rule_payload():
    spec = CheckSpec(
        name="a",
        criticality="error",
        function="is_not_null",
        native={"column": "id"},
    )
    with pytest.raises(Exception):
        spec.name = "b"
    with pytest.raises(TypeError):
        spec.native["column"] = "other"


def test_checkspec_validates_criticality_at_construction():
    with pytest.raises(ValueError, match="normalize_criticality"):
        CheckSpec(name="a", criticality="fatal", function="f")


def test_checkspec_detail_carries_severity_and_columns():
    plain = CheckSpec(name="a", criticality="warn", function="f")
    scoped = CheckSpec(name="b", criticality="warn", function="f", columns=("id",))
    assert plain.as_detail() == {"criticality": "warn"}
    assert scoped.as_detail() == {"criticality": "warn", "columns": ["id"]}


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
