# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.sql.types import LongType

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    people_df,
    schema_validation_config,
)

# (case id, rows, per-column nullability, expected outcome). Nullability
# is the not-null contract: with it disabled the null-bearing rows pass;
# with it enabled the same shape only passes because no nulls arrive.
NOT_NULL_CASES = [
    (
        "with_nullvalue_disabled_notnull",
        [("John", 25, "2021-01-01"), ("Doe", None, "2021-01-02"), ("Jane", 40, None)],
        (False, True, True),
    ),
    (
        "without_nullvalue_enabled_notnull",
        [
            ("John", 25, "2021-01-01"),
            ("Doe", 30, "2021-01-02"),
            ("Jane", 40, "2024-05-05"),
        ],
        (False, False, False),
    ),
]


@pytest.mark.parametrize(
    "case,rows,nullable",
    NOT_NULL_CASES,
    ids=[case[0] for case in NOT_NULL_CASES],
)
def test_schemavalidation_notnull_constraints(spark, case, rows, nullable):
    df = people_df(spark, rows=rows, age_type=LongType(), nullable=nullable)
    config = schema_validation_config(
        name=f"schemavalidation notnull case {case}",
        single_check_mode=True,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=True,
        context=case,
        register_users=False,
        register_signups=False,
    )


pytestmark = pytest.mark.spark
