# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark frames for the shared semantic corpus (``dq.sdk.fixtures``).

The corpus and its loader are vendor-free; building a frame for one is the
adapter's side of the contract, so the Spark builder lives with the tests
that exercise the Spark adapter. Another adapter's tests write their own.
"""

from dq.sdk.fixtures import SemanticCase, python_rows


def spark_type(kind):
    """Return the Spark SQL type a corpus column type names."""
    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        DoubleType,
        LongType,
        StringType,
    )

    if kind == "string":
        return StringType()
    if kind == "long":
        return LongType()
    if kind == "double":
        return DoubleType()
    if kind == "boolean":
        return BooleanType()
    precision, scale = kind[len("decimal(") : -1].split(",")
    return DecimalType(int(precision), int(scale))


def spark_frame(spark, case: SemanticCase):
    """Materialize a corpus case as a Spark DataFrame with an exact schema."""
    from pyspark.sql.types import StructField, StructType

    schema = StructType(
        [StructField(name, spark_type(kind)) for name, kind in case.columns]
    )
    return spark.createDataFrame(list(python_rows(case)), schema)
