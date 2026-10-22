# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Datatype-detection scenarios for the schemavalidation engine.

Each scenario is crossed with single_check_mode True/False; the two
modes previously existed as separate, otherwise identical test bodies.
"""

from collections import namedtuple

import pytest
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    schema_validation_config,
)

YYYYMMDD_PATTERN = "^20[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|(1|2)[0-9]|3[01])$"

YYYYMMDD_ROWS = [
    ("John", "20230101"),
    ("Joe", "20230303"),
    ("Jane", "20230401"),
    ("James", "20230903"),
]

DatatypeScenario = namedtuple(
    "DatatypeScenario", ["case", "build", "overrides", "expected"]
)


def _string_signup_df(spark):
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    return spark.createDataFrame(data=YYYYMMDD_ROWS, schema=schema)


def _name_age_df(spark):
    data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    return spark.createDataFrame(data=data, schema=schema)


def _varchar10_df(spark):
    return _name_age_df(spark).withColumn("name", expr("CAST( name as VARCHAR(10))"))


def _short_df(spark):
    # Original scenario data: the 45-character first name plus the
    # discarded withColumn (kept verbatim — the schema check sees plain
    # IntegerType, never ShortType).
    data = [
        ("John's name is greater than allowed 10 characters", 25),
        ("Doe", 30),
        ("Jane", 40),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.withColumn("age", expr("CAST(age as SHORT)"))
    return df


def _char10_df(spark):
    return _name_age_df(spark).withColumn("name", expr("CAST( name as CHAR(10))"))


def _boolean_df(spark):
    data = [
        ("John's name is greater than allowed 10 characters", False),
        ("Doe", False),
        ("Jane", True),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("isteenager", BooleanType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    return df.withColumn("name", expr("CAST( name as CHAR(10))"))


# (case id, dataframe builder, schema overrides, expected outcome).
# Without an override the YYYYMMDD strings are inferred as integers and
# the StringType expectation fails; a replace=True override rewrites the
# column to a matching pattern, replace=False leaves it failing.
DATATYPE_SCENARIOS = [
    DatatypeScenario(
        "stringtype_yyyymmdd_without_override", _string_signup_df, None, False
    ),
    DatatypeScenario(
        "stringtype_yyyymmdd_override_pattern_replace_true",
        _string_signup_df,
        [{"column": "signup_date", "pattern": YYYYMMDD_PATTERN}],
        True,
    ),
    DatatypeScenario(
        "stringtype_yyyymmdd_override_pattern_replace_false",
        _string_signup_df,
        [{"column": "signup_date", "pattern": YYYYMMDD_PATTERN, "replace": False}],
        False,
    ),
    DatatypeScenario("varchartype10", _varchar10_df, None, True),
    DatatypeScenario("shorttype", _short_df, None, True),
    DatatypeScenario("chartype10", _char10_df, None, True),
    DatatypeScenario("booleantype", _boolean_df, None, True),
]


@pytest.mark.parametrize("single_check_mode", [True, False], ids=["true", "false"])
@pytest.mark.parametrize(
    "scenario",
    DATATYPE_SCENARIOS,
    ids=[scenario.case for scenario in DATATYPE_SCENARIOS],
)
def test_schemavalidation_datatype(scenario, single_check_mode, spark):
    df = scenario.build(spark)
    config = schema_validation_config(
        name=f"schemavalidation datatype {scenario.case}",
        single_check_mode=single_check_mode,
        overrides=scenario.overrides,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=scenario.expected,
        context=f"{scenario.case}-single_check_mode_{single_check_mode}",
        register_users=False,
        register_signups=False,
    )


pytestmark = pytest.mark.spark
