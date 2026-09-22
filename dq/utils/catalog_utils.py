# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Catalog utility functions for resolving table references."""

import logging

from dq.catalog.catalog_factory import CatalogFactory

logger = logging.getLogger(__name__)


def get_from_catalog(
    spark, dataframe_name, catalog_type=None, database=None, catalog=None, **kwargs
):
    """Load a DataFrame from a catalog.

    Uses the CatalogFactory to resolve table references across different
    metastores (Spark, Unity Catalog, Glue, Hive).

    Args:
        spark: Active SparkSession.
        dataframe_name: Table name or fully-qualified reference.
        catalog_type: Optional catalog type ("spark", "unity", "glue", "hive").
            Auto-detected from SparkSession config if None.
        database: Optional database/schema name.
        catalog: Optional catalog name (for Unity Catalog three-part names).
        **kwargs: Additional args passed to the catalog provider (e.g., region_name for Glue).

    Returns:
        Spark DataFrame.
    """
    provider = CatalogFactory.get_provider(spark, catalog_type=catalog_type, **kwargs)
    return provider.get_dataframe(dataframe_name, database=database, catalog=catalog)


def get_table_schema(
    spark, table_name, catalog_type=None, database=None, catalog=None, **kwargs
):
    """Fetch table schema from a catalog.

    Args:
        spark: Active SparkSession.
        table_name: Table name or fully-qualified reference.
        catalog_type: Optional catalog type.
        database: Optional database/schema name.
        catalog: Optional catalog name (for Unity Catalog).
        **kwargs: Additional args passed to the catalog provider.

    Returns:
        pyspark.sql.types.StructType.
    """
    provider = CatalogFactory.get_provider(spark, catalog_type=catalog_type, **kwargs)
    return provider.get_table_schema(table_name, database=database, catalog=catalog)


def table_exists(
    spark, table_name, catalog_type=None, database=None, catalog=None, **kwargs
):
    """Check if a table exists in a catalog.

    Args:
        spark: Active SparkSession.
        table_name: Table name or fully-qualified reference.
        catalog_type: Optional catalog type.
        database: Optional database/schema name.
        catalog: Optional catalog name (for Unity Catalog).
        **kwargs: Additional args passed to the catalog provider.

    Returns:
        bool.
    """
    provider = CatalogFactory.get_provider(spark, catalog_type=catalog_type, **kwargs)
    return provider.table_exists(table_name, database=database, catalog=catalog)
