# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging

from pyhocon import ConfigTree

from dq.engine import checkspec
from dq.utils import config_utils

logger = logging.getLogger(__name__)

_TYPE_KEY = "type"
_KWARGS_KEY = "kwargs"
_NAME_KEY = "name"
_CRITICALITY_KEY = "criticality"


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
    expectations), and an optional nested ``kwargs`` subtree for the rest
    of the constructor arguments.

    The common axes cross the shared check seam (``dq.engine.checkspec``).
    An optional ``name`` labels the metric result; without one the label
    is the expectation ``type`` and its column (``<type>_<column>``), or
    the ``type`` alone for a table-level expectation. GX has no rule
    identity of its own, and a bare type gives two expectations of one
    type on different columns the same name. An optional ``criticality``
    (Deequ-style spellings accepted, defaulting to ``error``) surfaces in
    every result's ``details``. GX has no native severity, so criticality
    is reporting metadata here, not execution routing. Labels must be
    unique: a rule whose two expectations would report the same name is
    refused.
    """

    def __init__(self, config: ConfigTree):
        self._expectations_config = config
        self._labels = []

    def build_suite(self, gx, name: str):
        """Build the expectation suite for this configuration.

        Args:
            gx: The great_expectations module.
            name: Rule name used as the suite identity.

        Returns:
            A ``gx.ExpectationSuite`` with one expectation per entry.

        Raises:
            ValueError: On an unknown type, missing ``type``, invalid
                criticality, or two expectations that would report the
                same label.
        """
        suite = gx.ExpectationSuite(name=name)
        labels = self.rule_labels()
        self._labels = []
        for index, entry in enumerate(self._expectations_config):
            expectation_type = entry.get(_TYPE_KEY, None)
            if not expectation_type:
                raise ValueError(
                    "Every expectation requires a 'type', got " f"{dict(entry)!r}"
                )
            kwargs = {
                key: config_utils.config_tree_to_python(value)
                for key, value in entry.items()
                if key not in (_TYPE_KEY, _KWARGS_KEY, _NAME_KEY, _CRITICALITY_KEY)
            }
            kwargs.update(
                config_utils.config_tree_to_python(entry.get(_KWARGS_KEY, {}))
            )
            klass = expectation_class(gx, expectation_type)
            suite.add_expectation(klass(**kwargs))
            criticality = checkspec.normalize_criticality(
                entry.get(_CRITICALITY_KEY, None)
            )
            self._labels.append((labels[index], criticality))
        # Evidence is attributed by label: two expectations may not share one.
        checkspec.require_unique_labels(label for label, _ in self._labels)
        return suite

    def rule_labels(self) -> list:
        """Return the label of every expectation, in configuration order."""
        return checkspec.labels_for_checks(
            self._expectations_config, (_NAME_KEY,), _TYPE_KEY, ("column", "columns")
        )

    def labels(self):
        """Return ``(result label, criticality)`` pairs in suite order.

        Validation results come back in expectation order, so the engine
        zips these with ``validation_output.results`` by index.

        Returns:
            List of ``(label, criticality)`` tuples, one per entry.
        """
        return list(self._labels)
