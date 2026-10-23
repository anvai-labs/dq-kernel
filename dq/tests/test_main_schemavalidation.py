# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end schemavalidation engine tests.

Consolidates the former test_main_nullable_schemavalidation.py fork:
the nullable variant of every scenario is now a parametrize axis
(``required``/``nullable``) instead of a second copy of the file.
"""

import pytest
from pyhocon import ConfigFactory
from pyspark.sql import DataFrame
from pyspark.sql.functions import round as spark_round, to_date, to_timestamp
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    schema_validation_config,
)

INFERRED_ROWS = [
    ("John", 25, "2021-01-01"),
    ("Doe", 30, "2021-01-02"),
    ("Jane", 40, "2021-01-03"),
]

INT_AGE_ROWS = [
    ("John", 25, "2021-01-01"),
    ("Doe", 30, "2021-01-02"),
    ("Jane", 40, "2021-01-03"),
]

FLOAT_AGE_ROWS = [
    ("John", 25.002, "2021-01-01"),
    ("Doe", 30.93, "2021-01-02"),
    ("Jane", 40.03, "2023-03-03"),
]

DOUBLE_AGE_ROWS = [
    ("John", 25.1, "2021-01-01"),
    ("Doe", 30.9, "2021-01-02"),
    ("Jane", 40.03, "2021-01-03"),
]

NULL_BEARING_ROWS = [
    ("John", None, "2021-01-01"),
    (None, 30.1, "2021-01-02"),
    ("Jane", 40.1, None),
]

NOT_NULL_ROWS = [
    ("John", 2.4, "2021-01-01"),
    ("Joe", 30.1, "2021-01-02"),
    ("Jane", 40.1, "2023-03-03"),
]

COLUMNS = ["name", "age", "signup_date"]


def _round_age(df):
    return df.withColumn("age", spark_round("age", 2))


def _decimal_age(df):
    return df.withColumn("age", df["age"].cast(DecimalType(5, 2)))


def _date_signup(df):
    return df.withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd"))


def _timestamp_signup(df):
    return df.withColumn("signup_date", to_timestamp("signup_date", "yyyy-MM-dd"))


# dtype -> (rows, age Spark type, post-build transform or None).
# float is deliberately NOT rounded: Spark's round() promotes FloatType
# to DoubleType, which would corrupt the datatype expectation. (The old
# df_float_incoming fixture rounded, but no passing test ever used it.)
DTYPE_TABLE = {
    "long": (INT_AGE_ROWS, LongType, None),
    "integer": (INT_AGE_ROWS, IntegerType, None),
    "float": (FLOAT_AGE_ROWS, FloatType, None),
    "double": (DOUBLE_AGE_ROWS, DoubleType, _round_age),
    "decimal": (DOUBLE_AGE_ROWS, StringType, _decimal_age),
    "date": (DOUBLE_AGE_ROWS, DoubleType, _date_signup),
    "timestamp": (DOUBLE_AGE_ROWS, DoubleType, _timestamp_signup),
}


def incoming_df(spark, dtype: str, nullable: bool) -> DataFrame:
    """Build the typed incoming DataFrame for a dtype scenario.

    Args:
        spark: Active SparkSession.
        dtype: Key from DTYPE_TABLE; selects the age column's Spark
            type and any post-build transform.
        nullable: Nullability stamped on every schema field (the old
            nullable-file variant used True).

    Returns:
        DataFrame with an explicit three-column schema.
    """
    rows, age_type, transform = DTYPE_TABLE[dtype]
    schema = StructType(
        [
            StructField("name", StringType(), nullable),
            StructField("age", age_type(), nullable),
            StructField("signup_date", StringType(), nullable),
        ]
    )
    df = spark.createDataFrame(data=rows, schema=schema)
    return transform(df) if transform else df


def unique_fk_config(single_check_mode: bool = True):
    """Build the unique + foreign-key config the type scenarios share.

    Args:
        single_check_mode: Check granularity flag for the engine.

    Returns:
        Parsed ConfigTree for SchemavalidationEngine.
    """
    return schema_validation_config(
        name="schema-validation-single_check_mode",
        single_check_mode=single_check_mode,
        unique_constraints=[["name", "signup_date"], ["age"]],
        foreign_key_constraints=[
            {
                "src_column": "name",
                "ref_table": "temp_users",
                "ref_column": "name",
                # Explicit override (ignored when row count exceeds the
                # list-check threshold).
                "use_list_check": True,
            },
            {
                "src_column": "signup_date",
                "ref_table": "temp_signup_dates",
                "ref_column": "signup_date",
            },
            {
                "src_column": "age",
                "ref_table": "temp_users",
                "ref_column": "age",
            },
        ],
    )


# "long" activates the LongType scenario that previously existed only
# as a dead fixture (df_long_incoming) on main.
def test_schemavalidation_rejects_unknown_catalog_type(spark):
    df = spark.createDataFrame(INFERRED_ROWS, COLUMNS)
    df.createOrReplaceTempView("temp_data_table")
    config = ConfigFactory.from_dict(
        {
            "name": "schemavalidation unknown catalog type",
            "engine": "schemavalidation",
            "schema": {"catalog_type": "bogus", "table": "temp_data_table"},
        }
    )
    with pytest.raises(ValueError, match="Unsupported catalog_type"):
        apply_and_assert(spark, df, config, expected_success=True)


# "long" activates the LongType scenario that previously existed only
# as a dead fixture (df_long_incoming) on main.
DTYPES = ["long", "integer", "double", "decimal", "date", "timestamp"]


# The failure axis (reference-view starvation) triggers the same engine
# path for every dtype -- the FK tests pin the mechanism -- so it runs
# for one representative dtype only; success crosses every dtype x
# nullability so both datatype-constraint branches stay covered.
DTYPE_OUTCOMES = [(True, dtype) for dtype in DTYPES] + [(False, "integer")]


@pytest.mark.parametrize("nullable", [False, True], ids=["required", "nullable"])
@pytest.mark.parametrize(
    "expected_success,dtype",
    DTYPE_OUTCOMES,
    ids=[f"{ok}-{d}" for ok, d in DTYPE_OUTCOMES],
)
def test_schemavalidation_dtype_constraints(spark, expected_success, dtype, nullable):
    df = incoming_df(spark, dtype, nullable)
    apply_and_assert(
        spark,
        df,
        unique_fk_config(),
        expected_success=expected_success,
        context=f"{dtype}-{nullable}",
        # Failure mode starves the reference views so the foreign-key
        # constraints trip on the rows they no longer cover.
        limit_references=not expected_success,
    )


@pytest.mark.parametrize(
    "expected_success,nullable",
    [(True, False), (True, True), (False, False)],
    ids=["success-required", "success-nullable", "failure-required"],
)
def test_schemavalidation_float_type(spark, expected_success, nullable):
    # Float keeps its historical shape: the success path validates a
    # plain schema (no unique/foreign-key config), matching the
    # original self-contained test; the failure path starves the
    # reference views under the constraint config.
    df = incoming_df(spark, "float", nullable)
    if expected_success:
        config = schema_validation_config(
            name="schemavalidation float type success",
            single_check_mode=True,
        )
        apply_and_assert(
            spark, df, config, expected_success=True, context=f"float-{nullable}"
        )
    else:
        apply_and_assert(
            spark,
            df,
            unique_fk_config(),
            expected_success=False,
            context=f"float-{nullable}",
            limit_references=True,
        )


@pytest.mark.parametrize("expected_success", [True, False], ids=["success", "failure"])
def test_schemavalidation_inferred_schema(spark, expected_success):
    df = spark.createDataFrame(INFERRED_ROWS, COLUMNS)
    apply_and_assert(
        spark,
        df,
        unique_fk_config(),
        expected_success=expected_success,
        context="inferred-schema",
        limit_references=not expected_success,
    )


@pytest.mark.parametrize(
    "single_check_mode", [True, False], ids=["single_check_mode", "multi_check_mode"]
)
def test_schemavalidation_nullable_data_check_modes(spark, single_check_mode):
    # Null-bearing inferred data passes in both check modes; in
    # multi-check mode hasDataType is replaced by satisfies for the
    # nullable columns, which is why the axis is exercised explicitly.
    df = spark.createDataFrame(NULL_BEARING_ROWS, COLUMNS)
    config = schema_validation_config(
        name=f"schemavalidation nullable data {single_check_mode} SUCCESS",
        single_check_mode=single_check_mode,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=True,
        context=f"nullable-data-{single_check_mode}",
    )


@pytest.mark.parametrize(
    "case,rows,expected_success",
    [
        ("no_nulls_success", NOT_NULL_ROWS, True),
        ("nulls_failure", NULL_BEARING_ROWS, False),
    ],
)
def test_schemavalidation_not_null_columns(spark, case, rows, expected_success):
    df = spark.createDataFrame(rows, COLUMNS)
    config = schema_validation_config(
        name=f"schemavalidation not-null columns {case}",
        single_check_mode=True,
        not_null_columns=COLUMNS,
    )
    apply_and_assert(
        spark,
        df,
        config,
        expected_success=expected_success,
        context=case,
        register_users=False,
        register_signups=False,
    )


pytestmark = pytest.mark.spark
