# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Typed, validated snapshot of a rule pack (SPEC-010).

ADR-001 requires that configuration produce a validated structure before
any Spark work begins. A rule pack is parsed once here and fails closed,
naming the rule index and the offending key, so an invalid rule is
reported before the first engine runs rather than after the previous
rules' work is done. The framework, the validator and the CLI all read
this one snapshot.

Engine payloads are opaque here: each engine interprets its own
vocabulary (ADR-012).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

from dq.exceptions import ConfigurationError
from dq.gate import policy_from_config
from dq.utils.config_utils import config_get

#: The dataframe a rule validates when it names none.
DEFAULT_DATAFRAME = "default"


@dataclass(frozen=True)
class RuleRef:
    """One rule as the framework executes it.

    Attributes:
        index: Zero-based position of the rule in ``dqframework.dqrules``.
        engine: The engine identifier, as written.
        dataframes: Logical dataframe names the rule validates.
        name: Optional rule name.
        definition: The rule's own mapping; the engine reads its payload
            from here.
    """

    index: int
    engine: str
    dataframes: Tuple[str, ...]
    name: Optional[str]
    definition: Any = field(repr=False)

    @property
    def label(self) -> str:
        """The rule's name, or ``Rule <index>`` when it has none."""
        return self.name if self.name else f"Rule {self.index}"


@dataclass(frozen=True)
class RulePack:
    """A validated rule pack: its rules plus the run-level settings."""

    rules: Tuple[RuleRef, ...]
    config: Any = field(repr=False)

    @property
    def engines(self) -> Tuple[str, ...]:
        """The engines the rules name, each once, in rule order."""
        return tuple(dict.fromkeys(rule.engine for rule in self.rules))

    @property
    def gate_policy(self) -> str:
        """The gate policy (``dqframework.gate.fail_on``)."""
        return policy_from_config(self.config)

    @property
    def notifiers(self):
        """The ``dqframework.notifiers`` configuration, or ``None``."""
        return config_get(self.config, "dqframework.notifiers")

    @property
    def observers(self):
        """The ``dqframework.observers`` configuration, or ``None``."""
        return config_get(self.config, "dqframework.observers")

    @property
    def repository(self):
        """The ``dqframework.repository`` configuration, or an empty mapping."""
        return config_get(self.config, "dqframework.repository", {})

    @property
    def dataset(self) -> Optional[str]:
        """The ``dqframework.dataset`` identity, or ``None``."""
        return config_get(self.config, "dqframework.dataset")

    @property
    def catalog_type(self) -> Optional[str]:
        """The configured catalog type, or ``None`` for the default."""
        return config_get(self.config, "dqframework.catalog_type")


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _dataframes_problems(rule: Any, index: int) -> List[str]:
    """Return what is wrong with a rule's ``dataframes`` key (empty if fine)."""
    names = rule.get("dataframes", None)
    if names is None:
        return []
    if isinstance(names, (str, bytes)) or not hasattr(names, "__iter__"):
        return [f"Rule {index}: key 'dataframes' must be a list of dataframe names"]
    if not isinstance(names, (list, tuple)):
        names = list(names)
    if not names or any(
        not isinstance(name, str) or not name.strip() for name in names
    ):
        return [
            f"Rule {index}: key 'dataframes' must name at least one dataframe, "
            "each with a non-empty name"
        ]
    return []


def _rule_problems(rule: Any, index: int) -> List[str]:
    """Return what is wrong with one rule (empty if it is well formed)."""
    if not hasattr(rule, "get"):
        return [f"Rule {index} must be a mapping with at least an 'engine' key"]
    problems: List[str] = []
    engine = rule.get("engine", None)
    if engine is None:
        problems.append(f"Rule {index} is missing required key 'engine'")
    elif not isinstance(engine, str) or not engine.strip():
        problems.append(f"Rule {index}: key 'engine' must be a non-empty engine name")
    name = rule.get("name", None)
    if name is not None and (not isinstance(name, str) or not name.strip()):
        problems.append(
            f"Rule {index}: key 'name' must be a non-empty string when given"
        )
    problems.extend(_dataframes_problems(rule, index))
    return problems


def rule_pack_problems(config: Any) -> List[str]:
    """Return every problem a rule pack has, in rule order.

    A problem with the rule list itself is reported alone: nothing else can
    be checked without it.

    Args:
        config: A ``ConfigTree`` or a plain mapping.

    Returns:
        Messages that name the rule index and the offending key; empty when
        the pack is well formed.
    """
    rules_config = config_get(config, "dqframework.dqrules")
    if not rules_config:
        return ["No dqrules defined in configuration"]
    if isinstance(rules_config, (str, bytes, Mapping)) or not hasattr(
        rules_config, "__iter__"
    ):
        return [
            "dqframework.dqrules must be a list of rules, each with an 'engine' key"
        ]
    problems: List[str] = []
    for index, rule in enumerate(rules_config):
        problems.extend(_rule_problems(rule, index))
    return problems


def _build_rule_pack(config: Any) -> RulePack:
    rules_config = config_get(config, "dqframework.dqrules")
    rules = tuple(
        RuleRef(
            index=index,
            engine=rule.get("engine"),
            dataframes=tuple(rule.get("dataframes", None) or (DEFAULT_DATAFRAME,)),
            name=rule.get("name", None),
            definition=rule,
        )
        for index, rule in enumerate(rules_config)
    )
    return RulePack(rules=rules, config=config)


def parse_rule_pack(config: Any) -> RulePack:
    """Validate a configuration and return its :class:`RulePack`.

    Args:
        config: A ``ConfigTree`` or a plain mapping.

    Returns:
        The validated snapshot.

    Raises:
        ConfigurationError: With the first problem found, when
            ``dqframework.dqrules`` is missing or empty, or a rule lacks an
            ``engine``, or a rule's ``name`` or ``dataframes`` has the wrong
            shape. Messages name the rule index and the key.
    """
    problems = rule_pack_problems(config)
    if problems:
        raise ConfigurationError(problems[0])
    return _build_rule_pack(config)


__all__ = [
    "DEFAULT_DATAFRAME",
    "RulePack",
    "RuleRef",
    "parse_rule_pack",
    "rule_pack_problems",
]
