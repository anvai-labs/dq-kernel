# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Frames for the shared semantic corpus (``dq.sdk.fixtures``).

The corpus and its loader are vendor-free; building a frame for one is the
adapter's side of the contract, so each builder lives here, next to the
tests that exercise its adapter. Every builder must give a case's rows
exactly the types its columns name, because an engine is not bound to
refuse a value that does not fit.
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


def arrow_type(kind):
    """Return the Arrow type a corpus column type names."""
    import pyarrow as pa

    if kind == "string":
        return pa.string()
    if kind == "long":
        return pa.int64()
    if kind == "double":
        return pa.float64()
    if kind == "boolean":
        return pa.bool_()
    precision, scale = kind[len("decimal(") : -1].split(",")
    return pa.decimal128(int(precision), int(scale))


def arrow_table(case: SemanticCase, *, chunks: int = 1, encoded=()):
    """Materialize a corpus case as a pyarrow table with an exact schema.

    Args:
        case: The corpus case.
        chunks: Split the rows into at most this many record batches (each
            holds at least one row). A table is seldom one chunk: it is read
            in batches, and handed over by another library in several.
        encoded: Columns to store dictionary-encoded. A Parquet reader
            returns a column that way when the file stored it so, and the
            values are the same either way.
    """
    import pyarrow as pa

    rows = python_rows(case)
    columns = {
        name: pa.array([row[index] for row in rows], type=arrow_type(kind))
        for index, (name, kind) in enumerate(case.columns)
    }
    for name in encoded:
        columns[name] = columns[name].dictionary_encode()
    table = pa.table(columns)
    if chunks <= 1 or not rows:
        return table
    size = max(1, -(-len(rows) // chunks))
    return pa.Table.from_batches(
        table.to_batches(max_chunksize=size), schema=table.schema
    )
