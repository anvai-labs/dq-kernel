# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""DQX check-builder: kernel HOCON ``checks`` payload to DQX metadata dicts.

DQX validates Spark DataFrames against metadata checks (lists of dicts,
schema documented at https://databrickslabs.github.io/dqx/). The kernel
passes each HOCON entry through verbatim -- DQX's own schema is already
JSON-native -- with kernel guarantees layered on top through the shared
check seam (``dq.engine.checkspec``):

* ``criticality`` defaults to ``error``; Deequ-style spellings
  (``Error``/``Warning``) are accepted and normalized.
* Every entry carries an explicit ``name`` so results map 1:1 back to
  configured checks without relying on DQX's internal auto-naming, which is
  a private implementation detail. A missing name follows the labelling
  rule every engine shares (``checkspec.labels_for_checks``): the function
  and the columns it targets -- ``is_not_null_id``, ``is_unique_a_b`` for
  ``for_each_column`` -- or, for a check with no columns, the function
  alone. The name therefore does not change when checks are reordered.
  Names must be unique; duplicates fail closed so evidence stays
  unambiguous.
* ``check.function`` must be present.

``for_each_column`` is passed through untouched -- DQX expands it into one
rule per column (row- or dataset-level per the function's registry), and
every generated rule shares the entry's name, so a multi-column check
reports one aggregated result under that name.
"""

import logging
from typing import Any, Dict, List

from pyhocon import ConfigTree

from dq.engine import checkspec
from dq.utils import config_utils

logger = logging.getLogger(__name__)

_TYPE_KEY = "function"
_NAME_KEY = "name"
_CRITICALITY_KEY = "criticality"

#: The keys ``checkspec.labels_for_checks`` reads a check's columns from.
_COLUMN_KEYS = ("column", "columns")


class DqxCheck:
    """Builds DQX metadata checks from the ``checks`` config payload.

    Each entry is a DQX metadata check dict: a ``check`` block
    (``function`` plus ``arguments`` / ``for_each_column``), optional
    ``name``, ``criticality``, ``filter``, and ``user_metadata``.
    """

    def __init__(self, config: ConfigTree):
        self._checks_config = config.get("checks", None)

    def build_checks(self) -> List[Dict[str, Any]]:
        """Build the validated list of DQX metadata check dicts.

        Returns:
            One metadata dict per configured entry, each carrying an
            explicit ``name`` and ``criticality``.

        Raises:
            ValueError: If ``checks`` is missing or empty, an entry is not
                a check dict, ``check.function`` is absent, ``criticality``
                is invalid, or a name is duplicated.
        """
        if not self._checks_config:
            raise ValueError(
                "The dqx engine requires a 'checks' list of DQX metadata "
                "check entries (e.g. checks = [{check = {function = "
                '"is_not_null", arguments = {column = "id"}}}]'
            )
        entries = [
            config_utils.config_tree_to_python(entry) for entry in self._checks_config
        ]
        for check_dict in entries:
            if not isinstance(check_dict, dict) or not isinstance(
                check_dict.get("check"), dict
            ):
                raise ValueError(
                    f"Every dqx check requires a 'check' block, got {check_dict!r}"
                )
            function = check_dict["check"].get(_TYPE_KEY, None)
            if not function or not isinstance(function, str):
                raise ValueError(
                    f"Every dqx check requires a 'check.function' name, got "
                    f"{check_dict['check']!r}"
                )
        # The labelling rule every engine follows, so a check's name means
        # the same check whatever position it is configured in.
        labels = checkspec.labels_for_checks(
            [_as_check(check_dict) for check_dict in entries],
            (_NAME_KEY,),
            _TYPE_KEY,
            _COLUMN_KEYS,
        )
        checks: List[Dict[str, Any]] = []
        for check_dict, label in zip(entries, labels, strict=True):
            # The common axes (severity, identity) normalize through the
            # shared seam; DQX accepts Deequ-style spellings for free.
            criticality = checkspec.normalize_criticality(
                check_dict.get(_CRITICALITY_KEY)
            )
            check_dict[_CRITICALITY_KEY] = criticality
            check_dict[_NAME_KEY] = label
            checks.append(check_dict)
        # Evidence is attributed by label: an explicit name that equals a
        # derived one would make two checks indistinguishable.
        checkspec.require_unique_labels(labels)
        return checks


def _as_check(check_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Return a DQX entry under the keys the labelling rule reads.

    A DQX check names its columns in one of three places: one
    ``arguments.column``, a list of ``arguments.columns``, or
    ``for_each_column``, which DQX expands into one rule per column. Only
    what is there is read: DQX judges the arguments themselves.
    """
    check = check_dict["check"]
    arguments = check.get("arguments", None)
    columns = check.get("for_each_column", None)
    return {
        _NAME_KEY: check_dict.get(_NAME_KEY, None),
        _TYPE_KEY: check.get(_TYPE_KEY, None),
        "column": (
            arguments.get("column", None) if isinstance(arguments, dict) else None
        ),
        "columns": columns
        or (arguments.get("columns", None) if isinstance(arguments, dict) else None),
    }
