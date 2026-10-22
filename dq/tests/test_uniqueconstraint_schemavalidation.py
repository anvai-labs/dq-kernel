# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from py4j.protocol import Py4JJavaError

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    people_df,
    schema_validation_config,
)

# pydeequ crashes when uniqueness assertion lambdas are split across
# multiple Checks: "'ScalaFunction1' object has no attribute
# 'hashCode'". The multi-check axis runs the real multi-check config
# and is marked xfail(strict) so the day pydeequ fixes the collision
# the suite flags the stale expectation instead of silently passing.
PYDEEQU_MULTI_CHECK_CRASH = pytest.mark.xfail(
    strict=True,
    raises=Py4JJavaError,
    reason="pydeequ: uniqueness assertion lambdas collide as JVM proxies "
    "across multiple Checks ('ScalaFunction1' object has no attribute "
    "'hashCode')",
)

CHECK_MODES = [
    pytest.param(True, id="single_check_mode"),
    pytest.param(False, id="multiple_check_mode", marks=PYDEEQU_MULTI_CHECK_CRASH),
]

# (case id, unique key groups, expected outcome). PEOPLE_ROWS carries a
# duplicate (30, "2021-01-02") pair: keys covering age or
# age+signup_date trip the constraint, name-based keys stay unique.
UNIQUE_CASES = [
    ("single_key_name", [["name"]], True),
    ("single_key_age", [["age"]], False),
    ("composite_key_name_age", [["name", "age"]], True),
    ("composite_key_age_signup_date", [["age", "signup_date"]], False),
]


@pytest.mark.parametrize("single_check_mode", CHECK_MODES)
@pytest.mark.parametrize(
    "case,key_groups,expected",
    UNIQUE_CASES,
    ids=[case[0] for case in UNIQUE_CASES],
)
def test_unique_constraint(spark, case, key_groups, expected, single_check_mode):
    df = people_df(spark)
    config = schema_validation_config(
        name=f"unique constraint {case} single_check_mode_{single_check_mode}",
        single_check_mode=single_check_mode,
        unique_constraints=key_groups,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=expected,
        context=f"{case}-single_check_mode_{single_check_mode}",
        register_users=False,
        register_signups=False,
    )


pytestmark = pytest.mark.spark
