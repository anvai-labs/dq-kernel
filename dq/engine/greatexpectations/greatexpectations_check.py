# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, List, NamedTuple, Set, Tuple

from pyhocon import ConfigTree

from dq.engine import checkspec
from dq.exceptions import ConfigurationError
from dq.utils import config_utils

logger = logging.getLogger(__name__)

_TYPE_KEY = "type"
_KWARGS_KEY = "kwargs"
_NAME_KEY = "name"
_CRITICALITY_KEY = "criticality"

#: Stands in for a keyword argument a result's configuration does not carry.
_ABSENT = object()


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


class _Expectation(NamedTuple):
    """One expectation as GX normalised it: what its result carries back."""

    type: str
    kwargs: dict
    severity: Any


def _describes(configuration, expectation: _Expectation) -> bool:
    """Return whether a result's configuration is ``expectation``'s.

    GX hands each result its expectation's configuration -- type, arguments
    and severity, as it normalised them -- and adds keyword arguments of
    its own at run time (``batch_id`` among them). So a result's arguments
    contain its expectation's, and say nothing about the others.
    """
    if (
        getattr(configuration, "type", None) != expectation.type
        or getattr(configuration, "severity", None) != expectation.severity
    ):
        return False
    kwargs = getattr(configuration, "kwargs", None) or {}
    return all(
        kwargs.get(key, _ABSENT) == value for key, value in expectation.kwargs.items()
    )


class GreatexpectationsCheck:
    """Builds a GX Core ExpectationSuite from the ``expectations`` config.

    Each entry carries a snake-case ``type`` (resolved against
    ``gx.expectations`` -- unknown types fail closed), optional top-level
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
    every result's ``details``. GX has a ``severity`` of its own, which
    governs only GX's own success flag; the kernel keeps its own
    ``criticality`` and does not read it.

    A result is attributed to its expectation by what it carries, never by
    where it sits (:meth:`attribute`). So labels must be unique, and no
    two expectations may be the same type with the same arguments and
    severity: their results could not be told apart.
    """

    def __init__(self, config: ConfigTree):
        self._expectations_config = config
        self._labels = []
        self._expectations: List[_Expectation] = []

    def build_suite(self, gx, name):
        """Build the expectation suite for this configuration.

        Args:
            gx: The great_expectations module.
            name: Rule name used as the suite identity.

        Returns:
            A ``gx.ExpectationSuite`` with one expectation per entry.

        Raises:
            ValueError: On an unknown type, missing ``type``, invalid
                criticality, two expectations that would report the same
                label, or two whose results could not be told apart.
        """
        labels = self.rule_labels()
        # Evidence is attributed by label: two expectations may not share one.
        checkspec.require_unique_labels(labels)
        suite = gx.ExpectationSuite(name=name)
        self._labels = []
        self._expectations = []
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
            added = suite.add_expectation(klass(**kwargs))
            configuration = added.configuration
            expectation = _Expectation(
                configuration.type,
                dict(configuration.kwargs),
                getattr(configuration, "severity", None),
            )
            # Two expectations the same in type, arguments and severity come
            # back as two results nothing could tell apart. GX sometimes
            # keeps that from being seen: it keeps one copy of an expectation
            # it already holds (``meta`` and ``notes`` do not count) and
            # drops the rest without an error, so that one never runs.
            twin = next(
                (
                    other
                    for other, seen in enumerate(self._expectations)
                    if seen == expectation
                ),
                None,
            )
            if twin is not None or len(suite.expectations) != index + 1:
                pair = (
                    f"Checks {twin} and {index}"
                    if twin is not None
                    else (f"Check {index} and an earlier check")
                )
                raise ValueError(
                    f"{pair} are the same expectation to GX "
                    f"({expectation.type} with the same arguments), so their "
                    "results could not be told apart; give them different "
                    "arguments or remove one"
                )
            self._expectations.append(expectation)
            criticality = checkspec.normalize_criticality(
                entry.get(_CRITICALITY_KEY, None)
            )
            self._labels.append((labels[index], criticality))
        return suite

    def rule_labels(self) -> list:
        """Return the label of every expectation, in configuration order."""
        return checkspec.labels_for_checks(
            self._expectations_config, (_NAME_KEY,), _TYPE_KEY, ("column", "columns")
        )

    def attribute(self, validation_results) -> List[Tuple[str, str, Any]]:
        """Attribute each validation result to the expectation that produced it.

        GX returns one result per expectation, but not in suite order: it
        validates expectations grouped by column, table-level ones among
        them, so where a result sits says nothing about which expectation
        it belongs to. Each result carries the configuration of its own
        expectation and is matched on that.

        Args:
            validation_results: ``validation_output.results``, the GX
                expectation validation results of one run.

        Returns:
            ``(label, criticality, result)`` triples, one per expectation,
            in configuration order.

        Raises:
            ConfigurationError: If a result matches no expectation, two
                match it equally, or an expectation produced no result.
                Evidence is never attributed to a label by position or by
                guesswork.
        """
        taken: Set[int] = set()
        attributed: dict = {}
        for position, exp_result in enumerate(validation_results):
            index = self._attribute_one(exp_result.expectation_config, position, taken)
            attributed[index] = exp_result
        missing = [
            self._labels[index][0]
            for index in range(len(self._expectations))
            if index not in attributed
        ]
        if missing:
            raise ConfigurationError(
                f"GX returned {len(validation_results)} result(s) for "
                f"{len(self._expectations)} expectation(s); "
                f"{', '.join(missing)} produced none, so their evidence "
                "cannot be attributed"
            )
        return [
            (*self._labels[index], attributed[index])
            for index in range(len(self._expectations))
        ]

    def _attribute_one(self, configuration, position: int, taken: Set[int]) -> int:
        """Return the index of the expectation the result at ``position`` belongs to."""
        candidates = [
            index
            for index, expectation in enumerate(self._expectations)
            if index not in taken and _describes(configuration, expectation)
        ]
        if not candidates:
            raise ConfigurationError(
                f"GX result {position} ({getattr(configuration, 'type', None)!r}) "
                f"matches none of the {len(self._expectations)} configured "
                "expectations; its evidence cannot be attributed"
            )
        # One expectation's arguments can contain another's; a result
        # belongs to the one it describes in full.
        widest = max(len(self._expectations[index].kwargs) for index in candidates)
        candidates = [
            index
            for index in candidates
            if len(self._expectations[index].kwargs) == widest
        ]
        if len(candidates) > 1:
            raise ConfigurationError(
                f"GX result {position} matches checks "
                f"{', '.join(str(index) for index in candidates)} equally; "
                "its evidence cannot be attributed"
            )
        taken.add(candidates[0])
        return candidates[0]
