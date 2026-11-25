# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate policy: which failed checks stop a pipeline (ADR-013).

A run's outcomes are facts about the data; whether those facts should halt
a pipeline is the operator's decision. This module is the one place that
decision is made. The report and the gate event keep saying whether any
check failed; :func:`evaluate_gate` says whether that failure blocks under
the configured policy.

Severity comes from ``details.criticality``, which every engine reports
since ADR-012: ``"warn"`` is advisory, anything else (including a missing
value) counts as ``error``.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

from dq.exceptions import ConfigurationError
from dq.utils.config_utils import config_get

#: Accepted values of ``dqframework.gate.fail_on``.
POLICIES = ("any", "error", "never")

#: Preserves the exit behaviour the CLI always had: any failure blocks.
DEFAULT_POLICY = "any"

#: The one advisory severity.
WARN = "warn"

_GATE_CONFIG_KEY = "dqframework.gate.fail_on"

EXIT_PASSED = 0
EXIT_GATE_FAILED = 1
EXIT_CONFIGURATION = 2


def criticality_of(outcome: Dict[str, Any]) -> str:
    """Return ``"warn"`` or ``"error"`` for one outcome.

    An outcome with no criticality, or with details that are not a mapping,
    is an error: the conservative reading, so nothing fails open.
    """
    details = outcome.get("details")
    if isinstance(details, dict) and details.get("criticality") == WARN:
        return WARN
    return "error"


def severity_split(
    outcomes: Iterable[Dict[str, Any]],
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Split failed checks by severity, in outcome order.

    Returns:
        ``(error_failures, warn_failures)`` as tuples of check names.
    """
    errors: list = []
    warns: list = []
    for outcome in outcomes:
        if outcome.get("success") is True:
            continue
        name = outcome.get("check", "unnamed")
        if criticality_of(outcome) == WARN:
            warns.append(name)
        else:
            errors.append(name)
    return tuple(errors), tuple(warns)


@dataclass(frozen=True)
class GateDecision:
    """The outcome of applying a gate policy to a run's outcomes.

    Attributes:
        policy: The policy that was applied.
        passed: Whether the gate opened.
        blocking: Failed checks that stop the pipeline under this policy.
        advisory: Failed checks reported without stopping it.
    """

    policy: str
    passed: bool
    blocking: Tuple[str, ...]
    advisory: Tuple[str, ...]

    @property
    def exit_code(self) -> int:
        """0 when the gate passed, 1 when it failed."""
        return EXIT_PASSED if self.passed else EXIT_GATE_FAILED

    def to_dict(self) -> Dict[str, Any]:
        """Return the decision as strict-JSON native types."""
        return {
            "policy": self.policy,
            "passed": self.passed,
            "blocking": list(self.blocking),
            "advisory": list(self.advisory),
        }


def evaluate_gate(
    outcomes: Iterable[Dict[str, Any]], policy: str = DEFAULT_POLICY
) -> GateDecision:
    """Decide whether a run's failed checks block under ``policy``.

    Args:
        outcomes: Legacy outcome dictionaries (``check``, ``success``,
            optional ``details``).
        policy: One of :data:`POLICIES`.

    Returns:
        The :class:`GateDecision`.

    Raises:
        ConfigurationError: If the policy is not one of :data:`POLICIES`.
    """
    if policy not in POLICIES:
        raise ConfigurationError(
            f"Unknown gate policy {policy!r}: {_GATE_CONFIG_KEY} must be one of {POLICIES}"
        )
    blocking: list = []
    advisory: list = []
    for outcome in outcomes:
        if outcome.get("success") is True:
            continue
        name = outcome.get("check", "unnamed")
        advisory_failure = criticality_of(outcome) == WARN
        if policy == "never" or (policy == "error" and advisory_failure):
            advisory.append(name)
        else:
            blocking.append(name)
    return GateDecision(
        policy=policy,
        passed=not blocking,
        blocking=tuple(blocking),
        advisory=tuple(advisory),
    )


def policy_from_config(config: Any) -> str:
    """Return the configured gate policy, or the default when none is set.

    Raises:
        ConfigurationError: If the configured policy is unknown or not a
            string.
    """
    policy = config_get(config, _GATE_CONFIG_KEY, DEFAULT_POLICY)
    if policy is None:
        return DEFAULT_POLICY
    if policy not in POLICIES:
        raise ConfigurationError(
            f"{_GATE_CONFIG_KEY} must be one of {POLICIES}, got {policy!r}"
        )
    return policy
