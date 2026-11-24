# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""HOCON configuration key names and string constants.

Vendor-free by construction (stdlib strings only): this module is safe to
import from the foundation layer, the engine contract, and the orchestrator
without any optional engine dependency installed. Vendor-specific datatype
maps live with the engine that owns them (e.g.
``dq.engine.schemavalidation.datatypes``).
"""

DQ_ENGINE_NAME = "engine"
DQ_RULE_NAME = "name"
SCHEMA_VALIDATION_FK_CONSTRAINTS = "foreign_key_constraints"
SCHEMA_VALIDATION_UNQK_CONSTRAINTS = "unique_constraints"
SCHEMA_VALIDATION_REF_DB = "ref_db"
SCHEMA_VALIDATION_REF_TABLE = "ref_table"
SCHEMA_VALIDATION_REF_COLUMN = "ref_column"
SCHEMA_VALIDATION_SCHEMA = "schema"
SCHEMA_VALIDATION_DATABASE_KEY = "database"
SCHEMA_VALIDATION_TABLE_KEY = "table"
SCHEMA_VALIDATION_SRC_COLUMN = "src_column"
DQ_REPOSITORY_METRICS = "metrics"
DQ_REPOSITORY_VERIFICATIONS = "verifications"
SCHEMA_VALIDATION_CATALOG_TYPE_KEY = "catalog_type"
SCHEMA_VALIDATION_CATALOG_TYPE_UNITY = "unity"
SCHEMA_VALIDATION_CATALOG_TYPE_DELTA = "delta"
SCHEMA_VALIDATION_CATALOG_TYPE_GLUE = "glue"
SCHEMA_VALIDATION_CATALOG_TYPE_HIVE = "hive"
SCHEMA_VALIDATION_CATALOG_TYPE_SPARK = "spark"
DQ_SINGLE_CHECK_MODE = "single_check_mode"
SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_KEY = "threshold_list_count"
SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_VALUE = 1024
SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY = "use_list_check"
DQ_METRICS_RESULT_SUCCESS_KEY = "success"
SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY = "pattern"
SCHEMA_VALIDATION_OVERRIDE_KEY = "overrides"
SCHEMA_VALIDATION_OVERRIDE_CONFIG_REPLACE_KEY = "replace"
SCHEMA_VALIDATION_NOT_NULL_COLUMNS_KEY = "not_null_columns"
