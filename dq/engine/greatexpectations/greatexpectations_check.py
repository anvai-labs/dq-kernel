# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging

from pyhocon import ConfigTree

from dq.utils import config_utils

logger = logging.getLogger(__name__)

_TYPE_KEY = "type"
_KWARGS_KEY = "kwargs"


def expectation_class(gx, expectation_type: str):
    """Resolve a snake-case expectation type to its GX Core class.

    Args:
        gx: The great_expectations module.
        expectation_type: Expectation type as written in HOCON, e.g.
            ``expect_column_values_to_not_be_null``.

    Returns:
        The ``gx.expectations.Expect*`` class for the type.

    Raises:
        ValueError: If the type is not a GX Core expectation.
    """
    camel = "Expect" + "".join(part.title() for part in expectation_type.split("_")[1:])
    klass = getattr(gx.expectations, camel, None)
    if klass is None:
        raise ValueError(f"Unsupported expectation type: {expectation_type!r}")
    return klass


class GreatexpectationsCheck:
    """Builds a GX Core ExpectationSuite from the ``expectations`` config.

    Each entry carries a snake-case ``type`` (resolved against
    ``gx.expectations`` — unknown types fail closed), optional top-level
    keyword arguments (``column`` among them; omit it for table-level
    expectations), and an optional nested ``kwargs`` subtree for the
    rest of the constructor arguments.
    """

    def __init__(self, config: ConfigTree):
        self._expectations_config = config

    def build_suite(self, gx, name: str):
        """Build the expectation suite for this configuration.

        Args:
            gx: The great_expectations module.
            name: Rule name used as the suite identity.

        Returns:
            A ``gx.ExpectationSuite`` with one expectation per entry.
        """
        suite = gx.ExpectationSuite(name=name)
        for entry in self._expectations_config:
            expectation_type = entry.get(_TYPE_KEY, None)
            if not expectation_type:
                raise ValueError(
                    "Every expectation requires a 'type', got " f"{dict(entry)!r}"
                )
            kwargs = {
                key: config_utils.config_tree_to_python(value)
                for key, value in entry.items()
                if key not in (_TYPE_KEY, _KWARGS_KEY)
            }
            kwargs.update(
                config_utils.config_tree_to_python(entry.get(_KWARGS_KEY, {}))
            )
            klass = expectation_class(gx, expectation_type)
            suite.add_expectation(klass(**kwargs))
        return suite
