# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    people_df,
    schema_validation_config,
)

# LATENT DEFECT (flag-fix commit pending): the tests historically named
# "multiple_check_mode" also ran with single_check_mode = True, so both
# labels exercise the single-check path today.
CHECK_MODE_FLAGS = {
    "single_check_mode": True,
    "multiple_check_mode": True,
}

# (case id, unique key groups, expected outcome). PEOPLE_ROWS carries a
# duplicate (30, "2021-01-02") pair: keys covering age or
# age+signup_date trip the constraint, name-based keys stay unique.
UNIQUE_CASES = [
    ("single_key_name", [["name"]], True),
    ("single_key_age", [["age"]], False),
    ("composite_key_name_age", [["name", "age"]], True),
    ("composite_key_age_signup_date", [["age", "signup_date"]], False),
]


@pytest.mark.parametrize("check_mode", list(CHECK_MODE_FLAGS))
@pytest.mark.parametrize(
    "case,key_groups,expected",
    UNIQUE_CASES,
    ids=[case[0] for case in UNIQUE_CASES],
)
def test_unique_constraint(spark, case, key_groups, expected, check_mode):
    df = people_df(spark)
    config = schema_validation_config(
        name=f"unique constraint {case} {check_mode}",
        single_check_mode=CHECK_MODE_FLAGS[check_mode],
        unique_constraints=key_groups,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=expected,
        context=f"{case}-{check_mode}",
        register_users=False,
        register_signups=False,
    )


pytestmark = pytest.mark.spark
