# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from pyhocon import ConfigTree
from dq.utils import config_utils

logger = logging.getLogger(__name__)


class GreatexpectationsCheck:
    """Applies Great Expectations checks dynamically based on configuration.

    Uses reflection to call the appropriate GE expectation method based on
    the expectation type specified in each configuration entry.
    """

    def __init__(self, config: ConfigTree):
        self._expectations_config = config

    def apply_checks(self, ge_df):
        """Apply a list of expectations dynamically using reflection.

        Args:
            ge_df: A Great Expectations ``SparkDFDataset``.
        """
        for expectation in self._expectations_config:
            expectation_type = expectation.get("type")
            column = expectation.get("column")
            kwargs_config = expectation.get("kwargs", {})
            kwargs = config_utils.config_tree_to_python(kwargs_config)

            expectation_method = getattr(ge_df, expectation_type, None)
            if expectation_method:
                logger.debug(
                    "Applying %s on column %s with args: %s",
                    expectation_type,
                    column,
                    kwargs,
                )
                expectation_method(column, **kwargs)
            else:
                logger.warning("Unknown expectation type: %s", expectation_type)
