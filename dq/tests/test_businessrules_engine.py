# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os

os.environ["SPARK_VERSION"] = "3.5"
import pytest
from pyhocon import ConfigFactory

from dq.engine.businessrules.businessrules_engine import BusinessrulesEngine
from dq.engine.businessrules.strategies import NoNegativeValuesStrategy
from dq.tests.helpers import assert_overall_success
import pydeequ

from pyspark.sql import SparkSession


def test_wide_col_negative_values(
    spark, businessrules_config_wide_col_negative_values, numeric_dataframe
):
    engine = BusinessrulesEngine(businessrules_config_wide_col_negative_values)
    results = engine.apply(numeric_dataframe, None)
    assert_overall_success(results, "wide-table negative values")


def test_no_negative_values_messages_blank_the_default_source(
    spark, numeric_dataframe_with_negatives
):
    # Legacy output contract: the default timeSeries source renders as
    # an empty suffix in verification messages, never the literal word.
    _, verifications = NoNegativeValuesStrategy().apply(
        numeric_dataframe_with_negatives,
        "WideTablesNegativeValuesCheck",
        "Validity",
        "Warning",
    )
    assert verifications
    for row in verifications:
        assert "timeSeries" not in row[5], row


pytestmark = pytest.mark.spark
