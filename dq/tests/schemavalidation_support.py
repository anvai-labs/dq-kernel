# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared dataframe/config factories and runners for schemavalidation tests.

Consolidates the per-file ``process_schemavalidation_success`` /
``process_schemavalidation_failure`` helpers and the repeated HOCON
config blocks behind single definitions.  Assertion primitives live in
``dq.tests.helpers``; this module owns the Spark-dependent pieces.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple

from pyhocon import ConfigFactory, ConfigTree
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DataType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dq.engine.schemavalidation.schemavalidation_engine import (
    SchemavalidationEngine,
)
from dq.tests.helpers import assert_overall_success

PEOPLE_COLUMNS = ("name", "age", "signup_date")

# Rows shared by the unique/foreign-key constraint scenarios: the
# duplicate (30, "2021-01-02") pair is what drives the failure cases.
PEOPLE_ROWS: List[Tuple[Any, Any, Any]] = [
    ("John", 25, "2021-01-01"),
    ("Doe", 30, "2021-01-02"),
    ("Jane", 30, "2021-01-02"),
]


def people_df(
    spark,
    rows: Sequence[Tuple[Any, Any, Any]] = PEOPLE_ROWS,
    age_type: DataType = IntegerType(),
    nullable: Sequence[bool] = (False, True, True),
) -> DataFrame:
    """Build the canonical (name, age, signup_date) test DataFrame.

    Args:
        spark: Active SparkSession.
        rows: Row values in PEOPLE_COLUMNS order.
        age_type: Spark datatype instance for the ``age`` column
            (e.g. ``IntegerType()``).
        nullable: Per-column nullability flags in PEOPLE_COLUMNS order.

    Returns:
        DataFrame with an explicit schema (no inference).
    """
    schema = StructType(
        [
            StructField("name", StringType(), nullable[0]),
            StructField("age", age_type, nullable[1]),
            StructField("signup_date", StringType(), nullable[2]),
        ]
    )
    return spark.createDataFrame(list(rows), schema)


def schema_validation_config(
    *,
    name: str,
    single_check_mode: Optional[bool] = None,
    table: str = "temp_data_table",
    unique_constraints: Optional[List[List[str]]] = None,
    foreign_key_constraints: Optional[List[Dict[str, Any]]] = None,
    not_null_columns: Optional[List[str]] = None,
    overrides: Optional[List[Dict[str, Any]]] = None,
) -> ConfigTree:
    """Build a schemavalidation engine config from keyword parts.

    Mirrors the HOCON blocks previously copy-pasted per test: omitted
    keys are left out so engine defaults (e.g. ``single_check_mode``
    defaulting to True) behave exactly as before.

    Args:
        name: Rule name used in logs.
        single_check_mode: True for one Check per validation, False for
            one Check per constraint; omit for the engine default.
        table: Temp view the engine validates.
        unique_constraints: Column groups that must be jointly unique.
        foreign_key_constraints: FK dicts with src_column / ref_table /
            ref_column and optional use_list_check.
        not_null_columns: Columns that must not contain nulls.
        overrides: Per-column override dicts (column / pattern / replace).

    Returns:
        Parsed ConfigTree ready for ``SchemavalidationEngine``.
    """
    config: Dict[str, Any] = {
        "name": name,
        "engine": "schemavalidation",
        "schema": {"catalog_type": "spark", "table": table},
    }
    if single_check_mode is not None:
        config["single_check_mode"] = single_check_mode
    if unique_constraints is not None:
        config["schema"]["unique_constraints"] = unique_constraints
    if foreign_key_constraints is not None:
        config["schema"]["foreign_key_constraints"] = foreign_key_constraints
    if not_null_columns is not None:
        config["schema"]["not_null_columns"] = not_null_columns
    if overrides is not None:
        config["schema"]["overrides"] = overrides
    return ConfigFactory.from_dict(config)


def apply_and_assert(
    spark,
    df: DataFrame,
    config: ConfigTree,
    expected_success: bool,
    context: str = "",
    register_users: bool = True,
    register_signups: bool = True,
    limit_references: bool = False,
) -> List[Dict[str, Any]]:
    """Run schema validation and assert the overall outcome.

    Replaces the per-file ``process_schemavalidation_success`` /
    ``process_schemavalidation_failure`` helpers: registers the temp
    views each scenario needs, applies the engine, and asserts either
    all-success or at-least-one-failure.

    Args:
        spark: Active SparkSession.
        df: Incoming DataFrame under validation.
        config: Engine config from :func:`schema_validation_config`.
        expected_success: True asserts every metric passed; False
            asserts at least one metric failed.
        context: Test-scenario label for assertion messages.
        register_users: Create the ``temp_users`` reference view.
        register_signups: Create the ``temp_signup_dates`` reference view.
        limit_references: Restrict reference views (``limit 2`` users,
            ``limit 1`` signups) to force foreign-key failures.

    Returns:
        The engine's summary metrics.
    """
    df.createOrReplaceTempView("temp_data_table")
    if register_users:
        users_limit = " limit 2" if limit_references else ""
        spark.sql(
            f"select name, age from temp_data_table{users_limit}"
        ).createOrReplaceTempView("temp_users")
    if register_signups:
        signups_limit = " limit 1" if limit_references else ""
        spark.sql(
            f"SELECT signup_date from temp_data_table{signups_limit}"
        ).createOrReplaceTempView("temp_signup_dates")

    metrics = SchemavalidationEngine(config).apply(df, repository=None)
    if expected_success:
        assert_overall_success(metrics, context)
    else:
        failures = [metric for metric in metrics if metric["success"] is not True]
        assert failures, f"{context}: expected a failing check, got {metrics}"
    return metrics
