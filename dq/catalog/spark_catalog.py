# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark catalog provider - default catalog using Spark's built-in catalog."""

import logging

from dq.catalog.base import CatalogProvider

logger = logging.getLogger(__name__)


class SparkCatalogProvider(CatalogProvider):
    """Catalog provider using Spark's built-in catalog (Hive metastore or in-memory).

    This is the default catalog provider. It resolves tables via
    ``spark.table()`` and ``spark.catalog.tableExists()``.
    """

    def get_dataframe(self, table_reference, database=None, catalog=None):
        """Load a table as a Spark DataFrame via Spark catalog.

        Args:
            table_reference: Table name or database.table.
            database: Optional database name (prepended if not already in table_reference).
            catalog: Ignored for Spark catalog.

        Returns:
            Spark DataFrame.
        """
        full_name = self._build_full_table_name(table_reference, database)
        logger.debug("Loading DataFrame from Spark catalog: %s", full_name)
        return self._spark.table(full_name)

    def get_table_schema(self, table_reference, database=None, catalog=None):
        """Fetch table schema via Spark catalog.

        Args:
            table_reference: Table name or database.table.
            database: Optional database name.
            catalog: Ignored for Spark catalog.

        Returns:
            pyspark.sql.types.StructType.
        """
        full_name = self._build_full_table_name(table_reference, database)
        logger.debug("Fetching schema from Spark catalog: %s", full_name)
        return self._spark.table(full_name).schema

    def table_exists(self, table_reference, database=None, catalog=None):
        """Check table existence in Spark catalog.

        Args:
            table_reference: Table name or database.table.
            database: Optional database name.
            catalog: Ignored for Spark catalog.

        Returns:
            bool.
        """
        full_name = self._build_full_table_name(table_reference, database)
        try:
            return self._spark._jsparkSession.catalog().tableExists(full_name)
        except Exception:
            return False
