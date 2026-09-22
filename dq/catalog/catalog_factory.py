# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Factory for creating catalog provider instances."""

import logging

from dq.catalog.base import CatalogProvider
from dq.catalog.spark_catalog import SparkCatalogProvider
from dq.catalog.unity_catalog import UnityCatalogProvider
from dq.catalog.glue_catalog import GlueCatalogProvider

logger = logging.getLogger(__name__)

# Registry of catalog type names to provider classes
_CATALOG_PROVIDERS = {
    "spark": SparkCatalogProvider,
    "hive": SparkCatalogProvider,  # Hive uses Spark's built-in catalog
    "unity": UnityCatalogProvider,
    "delta": UnityCatalogProvider,  # Delta tables use Unity Catalog on Databricks
    "glue": GlueCatalogProvider,
}


class CatalogFactory:
    """Factory that creates catalog provider instances based on catalog type.

    Supports automatic detection of the catalog type from SparkSession
    configuration when no explicit type is specified.

    Usage::

        factory = CatalogFactory()
        provider = factory.get_provider(spark, catalog_type="unity")
        df = provider.get_dataframe("main.sales.orders")
        schema = provider.get_table_schema("main.sales.orders")
    """

    @staticmethod
    def get_provider(spark_session, catalog_type=None, **kwargs):
        """Create a catalog provider for the given type.

        Args:
            spark_session: Active SparkSession.
            catalog_type: Catalog type string ("spark", "unity", "glue", "hive").
                If None, attempts to auto-detect from SparkSession configuration.
            **kwargs: Additional keyword arguments passed to the provider constructor.
                For Glue: ``region_name``, ``glue_client``.

        Returns:
            CatalogProvider instance.

        Raises:
            ValueError: If the catalog type is not recognized.
        """
        if catalog_type is None:
            catalog_type = CatalogFactory._detect_catalog_type(spark_session)

        catalog_type = catalog_type.lower().strip()
        logger.info("Using catalog provider: %s", catalog_type)

        provider_class = _CATALOG_PROVIDERS.get(catalog_type)
        if provider_class is None:
            raise ValueError(
                f"Unknown catalog_type '{catalog_type}'. "
                f"Supported types: {', '.join(_CATALOG_PROVIDERS.keys())}"
            )

        if provider_class == GlueCatalogProvider:
            return provider_class(
                spark_session,
                region_name=kwargs.get("region_name"),
                glue_client=kwargs.get("glue_client"),
            )

        return provider_class(spark_session)

    @staticmethod
    def _detect_catalog_type(spark_session):
        """Auto-detect catalog type from SparkSession configuration.

        Detection heuristics:
        1. If ``spark.sql.catalog.spark_catalog`` contains "DeltaCatalog"
           or "UnityCatalog", return "unity".
        2. If ``spark.hadoop.hive.metastore.client.factory.class`` contains
           "AWSGlue", return "glue".
        3. Otherwise, return "spark" (default).

        Args:
            spark_session: Active SparkSession.

        Returns:
            Detected catalog type string.
        """
        try:
            conf = spark_session.sparkContext.getConf()

            # Check for Unity Catalog / Delta Catalog
            spark_catalog = conf.get("spark.sql.catalog.spark_catalog", "")
            if "DeltaCatalog" in spark_catalog or "UnityCatalog" in spark_catalog:
                logger.info("Auto-detected Unity Catalog from SparkSession config")
                return "unity"

            # Check for Glue metastore
            metastore_class = conf.get(
                "spark.hadoop.hive.metastore.client.factory.class", ""
            )
            if "AWSGlue" in metastore_class:
                logger.info("Auto-detected Glue Catalog from SparkSession config")
                return "glue"

        except Exception as e:
            logger.debug("Could not auto-detect catalog type: %s", e)

        logger.debug("Defaulting to Spark catalog")
        return "spark"

    @staticmethod
    def supported_types():
        """Return list of supported catalog type names.

        Returns:
            List of strings.
        """
        return list(_CATALOG_PROVIDERS.keys())
