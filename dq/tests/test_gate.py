# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the gate policy (ADR-013, SPEC-012).

The gate decision is made in exactly one place; these tests pin the whole
policy matrix, the conservative reading of a missing severity, and the
configuration reader for both pyhocon trees and plain dictionaries.
"""

import pytest

from pyhocon import ConfigFactory

from dq.exceptions import ConfigurationError
from dq.gate import (
    DEFAULT_POLICY,
    EXIT_CONFIGURATION,
    EXIT_GATE_FAILED,
    EXIT_PASSED,
    POLICIES,
    config_get,
    evaluate_gate,
    policy_from_config,
    severity_split,
)

PASS = {"check": "fine", "success": True}
ERROR = {"check": "broken", "success": False, "details": {"criticality": "error"}}
WARN = {"check": "stale", "success": False, "details": {"criticality": "warn"}}

# (outcomes, expected decision per policy: policy -> (passed, blocking, advisory))
MATRIX = [
    ([PASS], {"any": (True, (), ()), "error": (True, (), ()), "never": (True, (), ())}),
    (
        [PASS, ERROR],
        {
            "any": (False, ("broken",), ()),
            "error": (False, ("broken",), ()),
            "never": (True, (), ("broken",)),
        },
    ),
    (
        [PASS, WARN],
        {
            "any": (False, ("stale",), ()),
            "error": (True, (), ("stale",)),
            "never": (True, (), ("stale",)),
        },
    ),
    (
        [WARN, PASS, ERROR],
        {
            "any": (False, ("stale", "broken"), ()),
            "error": (False, ("broken",), ("stale",)),
            "never": (True, (), ("stale", "broken")),
        },
    ),
]


@pytest.mark.parametrize(
    "outcomes,expected", MATRIX, ids=["none", "error", "warn", "both"]
)
@pytest.mark.parametrize("policy", POLICIES)
def test_policy_matrix(outcomes, expected, policy):
    passed, blocking, advisory = expected[policy]
    decision = evaluate_gate(outcomes, policy)
    assert decision.policy == policy
    assert decision.passed is passed
    assert decision.blocking == blocking
    assert decision.advisory == advisory
    assert decision.exit_code == (EXIT_PASSED if passed else EXIT_GATE_FAILED)


def test_default_policy_is_any_and_blocks_every_failure():
    assert DEFAULT_POLICY == "any"
    assert POLICIES == ("any", "error", "never")
    assert evaluate_gate([WARN]).passed is False


@pytest.mark.parametrize(
    "outcome",
    [
        {"check": "legacy", "success": False},
        {"check": "legacy", "success": False, "details": {}},
        {"check": "legacy", "success": False, "details": "a plain string"},
        {"check": "legacy", "success": False, "details": {"criticality": "fatal"}},
    ],
    ids=["no-details", "empty-details", "string-details", "unknown-severity"],
)
def test_a_missing_or_unknown_severity_counts_as_error(outcome):
    assert evaluate_gate([outcome], "error").passed is False
    assert severity_split([outcome]) == (("legacy",), ())


@pytest.mark.parametrize("success", [False, None, 0, "yes"])
def test_anything_but_true_is_a_failure(success):
    """Only a literal True passes, matching the outcome boundary's rule."""
    outcome = {"check": "ambiguous", "success": success}
    assert evaluate_gate([outcome], "never").advisory == ("ambiguous",)


def test_only_warn_spelled_exactly_is_advisory():
    uppercase = {"check": "a", "success": False, "details": {"criticality": "Warn"}}
    # Engines normalize through the check seam; the gate reads the canonical
    # spelling and treats anything else as blocking.
    assert evaluate_gate([uppercase], "error").passed is False


def test_decision_keeps_outcome_order_and_is_json_native():
    decision = evaluate_gate([WARN, PASS, ERROR], "any")
    assert decision.blocking == ("stale", "broken")
    assert decision.to_dict() == {
        "policy": "any",
        "passed": False,
        "blocking": ["stale", "broken"],
        "advisory": [],
    }


def test_passing_outcomes_are_ignored():
    assert evaluate_gate([PASS, {"check": "ok", "success": True}], "any").passed


@pytest.mark.parametrize("policy", ["sometimes", "", "ANY", None, 3])
def test_unknown_policy_fails_closed(policy):
    with pytest.raises(ConfigurationError, match="Unknown gate policy"):
        evaluate_gate([], policy)


# ------------------------------------------------------- configuration reading


def test_policy_defaults_to_any_when_unset():
    assert policy_from_config(ConfigFactory.from_dict({"dqframework": {}})) == "any"
    assert policy_from_config({}) == "any"
    assert policy_from_config(None) == "any"


@pytest.mark.parametrize(
    "config",
    [
        ConfigFactory.parse_string("dqframework { gate { fail_on = error } }"),
        {"dqframework": {"gate": {"fail_on": "error"}}},
        {"dqframework.gate.fail_on": "error"},
    ],
    ids=["pyhocon", "nested-dict", "flat-dotted-dict"],
)
def test_policy_is_read_from_trees_and_plain_dictionaries(config):
    assert policy_from_config(config) == "error"


@pytest.mark.parametrize(
    "config",
    [
        ConfigFactory.parse_string("dqframework { gate { fail_on = maybe } }"),
        {"dqframework": {"gate": {"fail_on": "maybe"}}},
        {"dqframework": {"gate": {"fail_on": 3}}},
    ],
    ids=["pyhocon", "nested-dict", "non-string"],
)
def test_unknown_policy_in_configuration_fails_closed(config):
    with pytest.raises(
        ConfigurationError, match="dqframework.gate.fail_on must be one of"
    ):
        policy_from_config(config)


def test_config_get_walks_nested_mappings_and_falls_back_to_dotted_keys():
    assert config_get({"a": {"b": {"c": 1}}}, "a.b.c") == 1
    assert config_get({"a.b.c": 2}, "a.b.c") == 2
    assert config_get({"a": {}}, "a.b.c", "fallback") == "fallback"
    assert config_get(object(), "anything", "fallback") == "fallback"
    assert config_get(None, "anything", None) is None


def test_exit_codes_are_the_documented_three():
    assert (EXIT_PASSED, EXIT_GATE_FAILED, EXIT_CONFIGURATION) == (0, 1, 2)
