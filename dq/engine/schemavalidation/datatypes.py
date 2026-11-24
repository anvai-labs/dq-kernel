# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Vendor datatype maps for the schemavalidation engine.

The only module that imports pydeequ for datatype classification; keeping
it engine-local is what lets ``dq.utils.keys`` (and everything upstream of
the engines) stay vendor-free. Used exclusively by
``dq.engine.schemavalidation.schemavalidation_check``.
"""

from pydeequ.checks import ConstrainableDataTypes

SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP = {
    "IntegerType": ConstrainableDataTypes.Integral,
    "LongType": ConstrainableDataTypes.Integral,
    "StringType": ConstrainableDataTypes.String,
    "CharType": ConstrainableDataTypes.String,
    "VarcharType": ConstrainableDataTypes.String,
    "DoubleType": ConstrainableDataTypes.Fractional,
    "FloatType": ConstrainableDataTypes.Fractional,
    "BooleanType": ConstrainableDataTypes.Boolean,
    "DecimalType": ConstrainableDataTypes.Numeric,
}
SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP = {
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "TimestampNTZType": "TIMESTAMPNTZ",
    "BinaryType": "BINARY",
    "ShortType": "SHORT",
    "ByteType": "BYTE",
}
SCHEMA_VALIDATION_TYPEOF_DATATYPE_MAP = {
    "IntegerType": "int",
    "LongType": "bigint",
    "StringType": "string",
    "CharType": "string",
    "VarcharType": "string",
    "DoubleType": "double",
    "FloatType": "float",
    "BooleanType": "boolean",
    "DecimalType": "decimal",
    "DateType": "date",
    "TimestampType": "timestamp",
}
