# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from dq.tests.schemavalidation_support import (
    apply_and_assert,
    schema_validation_config,
)

# The single_check_mode label now actually runs single-check; before
# the flag-fix commit both labels ran multi-check (latent defect from
# the copy-pasted fixtures).
CHECK_MODE_FLAGS = {
    "single_check_mode": True,
    "multiple_check_mode": False,
}

FOREIGN_KEY = ("age", "temp_users", "age")


@pytest.fixture
def df_long_incoming(spark):
    # Example DataFrame with incoming data
    incoming_data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    incoming_data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
        ]
    )
    df = spark.createDataFrame(data=incoming_data, schema=incoming_data_schema)
    return df


def fk_config(use_list_check: bool, check_mode: str):
    """Build the single-column FK validation config for a case.

    Args:
        use_list_check: Value for the constraint's use_list_check flag.
        check_mode: Label from CHECK_MODE_FLAGS driving the flag value.

    Returns:
        Parsed ConfigTree for SchemavalidationEngine.
    """
    src_column, ref_table, ref_column = FOREIGN_KEY
    return schema_validation_config(
        name=f"list_check_{str(use_list_check).lower()}_{check_mode}_schemavalidation",
        single_check_mode=CHECK_MODE_FLAGS[check_mode],
        foreign_key_constraints=[
            {
                "src_column": src_column,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "use_list_check": use_list_check,
            }
        ],
    )


@pytest.mark.parametrize("use_list_check", [True, False], ids=["true", "false"])
@pytest.mark.parametrize("check_mode", list(CHECK_MODE_FLAGS))
def test_schemavalidation_fk_use_list_check_success(
    spark, df_long_incoming, use_list_check, check_mode
):
    apply_and_assert(
        spark,
        df_long_incoming,
        fk_config(use_list_check, check_mode),
        expected_success=True,
        context=f"use_list_check-{use_list_check}-{check_mode}",
        register_signups=False,
    )


@pytest.mark.parametrize("use_list_check", [True, False], ids=["true", "false"])
@pytest.mark.parametrize("check_mode", list(CHECK_MODE_FLAGS))
def test_schemavalidation_fk_use_list_check_failure(
    spark, df_long_incoming, use_list_check, check_mode
):
    # Limiting the reference view to two rows starves the third
    # incoming row's foreign key, forcing the constraint to fail.
    apply_and_assert(
        spark,
        df_long_incoming,
        fk_config(use_list_check, check_mode),
        expected_success=False,
        context=f"use_list_check-{use_list_check}-{check_mode}",
        register_signups=False,
        limit_references=True,
    )


pytestmark = pytest.mark.spark
