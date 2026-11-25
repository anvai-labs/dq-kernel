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
* Every entry carries an explicit ``name`` (kernel-generated
  ``<function>_<index>`` when absent) so results map 1:1 back to configured
  checks without relying on DQX's internal auto-naming, which is a private
  implementation detail. Explicitly configured names must be unique;
  duplicates fail closed so evidence stays unambiguous.
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
        checks: List[Dict[str, Any]] = []
        for index, entry in enumerate(self._checks_config):
            check_dict = config_utils.config_tree_to_python(entry)
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
            # The common axes (severity, identity) normalize through the
            # shared seam; DQX accepts Deequ-style spellings for free.
            criticality = checkspec.normalize_criticality(
                check_dict.get(_CRITICALITY_KEY)
            )
            check_dict[_CRITICALITY_KEY] = criticality
            name = check_dict.get(_NAME_KEY, None)
            if not name:
                check_dict[_NAME_KEY] = checkspec.derive_name(function, index)
            checks.append(check_dict)
        # Evidence is attributed by label: an explicit name that equals a
        # derived one would make two checks indistinguishable.
        checkspec.require_unique_labels([check[_NAME_KEY] for check in checks])
        return checks
