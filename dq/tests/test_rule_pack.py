# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the typed rule-pack snapshot (SPEC-010).

The snapshot is parsed once and fails closed, naming the rule index and the
offending key, so a broken rule is reported before the first engine runs.
"""

import pytest
from pyhocon import ConfigFactory, ConfigTree

from dq.config import DEFAULT_DATAFRAME, RulePack, RuleRef, parse_rule_pack
from dq.exceptions import ConfigurationError
from dq.utils.config_utils import config_tree_to_python

VALID = {
    "dqframework": {
        "catalog_type": "glue",
        "dataset": "orders",
        "gate": {"fail_on": "error"},
        "notifiers": [{"type": "logging"}],
        "repository": {"dataset": "orders"},
        "dqrules": [
            {
                "name": "first",
                "engine": "deequ",
                "dataframes": ["orders"],
                "checks": [1],
            },
            {"engine": "portable", "plan": []},
        ],
    }
}


def test_valid_pack_exposes_typed_rules():
    pack = parse_rule_pack(VALID)
    assert isinstance(pack, RulePack)
    assert pack.rules == (
        RuleRef(
            index=0,
            engine="deequ",
            dataframes=("orders",),
            name="first",
            definition=pack.rules[0].definition,
        ),
        RuleRef(
            index=1,
            engine="portable",
            dataframes=(DEFAULT_DATAFRAME,),
            name=None,
            definition=pack.rules[1].definition,
        ),
    )
    assert [rule.label for rule in pack.rules] == ["first", "Rule 1"]


def test_rule_definition_carries_the_engine_payload_untouched():
    pack = parse_rule_pack(VALID)
    assert list(pack.rules[0].definition) == ["name", "engine", "dataframes", "checks"]
    assert config_tree_to_python(pack.rules[0].definition["checks"]) == [1]


def test_run_level_settings_are_read_from_the_pack():
    pack = parse_rule_pack(VALID)
    assert pack.gate_policy == "error"
    assert pack.dataset == "orders"
    assert pack.catalog_type == "glue"
    assert [entry["type"] for entry in pack.notifiers] == ["logging"]
    assert pack.repository == {"dataset": "orders"}


def test_run_level_settings_default_when_unset():
    pack = parse_rule_pack({"dqframework.dqrules": [{"engine": "deequ"}]})
    assert pack.gate_policy == "any"
    assert pack.dataset is None and pack.catalog_type is None
    assert pack.notifiers is None and pack.repository == {}


@pytest.mark.parametrize(
    "config",
    [
        ConfigFactory.from_dict(VALID),
        VALID,
        {"dqframework.dqrules": VALID["dqframework"]["dqrules"]},
        {"dqframework": {"dqrules": VALID["dqframework"]["dqrules"]}},
    ],
    ids=["pyhocon", "nested-dict", "flat-dotted-dict", "nested-rules-only"],
)
def test_trees_and_plain_dictionaries_parse_to_equal_rules(config):
    reference = parse_rule_pack(VALID)
    pack = parse_rule_pack(config)
    assert [(r.index, r.engine, r.dataframes, r.name) for r in pack.rules] == [
        (r.index, r.engine, r.dataframes, r.name) for r in reference.rules
    ]


@pytest.mark.parametrize(
    "rules",
    [
        [],
        {},
        "deequ",
        {"engine": "deequ"},
        None,
    ],
    ids=["empty-list", "empty-mapping", "string", "mapping", "none"],
)
def test_dqrules_must_be_a_list_of_rules(rules):
    with pytest.raises(ConfigurationError) as caught:
        parse_rule_pack({"dqframework.dqrules": rules})
    assert "dqrules" in str(caught.value)


def test_a_missing_rule_list_is_named():
    with pytest.raises(ConfigurationError, match="No dqrules defined in configuration"):
        parse_rule_pack({"dqframework": {}})


@pytest.mark.parametrize(
    ("rule", "message"),
    [
        ("just-a-string", "Rule 1 must be a mapping"),
        ({}, "Rule 1 is missing required key 'engine'"),
        ({"engine": None}, "Rule 1 is missing required key 'engine'"),
        ({"engine": ""}, "Rule 1: key 'engine' must be a non-empty engine name"),
        ({"engine": "  "}, "Rule 1: key 'engine' must be a non-empty engine name"),
        ({"engine": 3}, "Rule 1: key 'engine' must be a non-empty engine name"),
        (
            {"engine": "deequ", "name": ""},
            "Rule 1: key 'name' must be a non-empty string",
        ),
        (
            {"engine": "deequ", "name": 7},
            "Rule 1: key 'name' must be a non-empty string",
        ),
        (
            {"engine": "deequ", "dataframes": []},
            "Rule 1: key 'dataframes' must name at least one dataframe",
        ),
        (
            {"engine": "deequ", "dataframes": "orders"},
            "Rule 1: key 'dataframes' must be a list of dataframe names",
        ),
        (
            {"engine": "deequ", "dataframes": [""]},
            "Rule 1: key 'dataframes' must name at least one dataframe",
        ),
        (
            {"engine": "deequ", "dataframes": ["a", None]},
            "Rule 1: key 'dataframes' must name at least one dataframe",
        ),
    ],
    ids=[
        "not-a-mapping",
        "engine-absent",
        "engine-none",
        "engine-empty",
        "engine-blank",
        "engine-not-text",
        "name-empty",
        "name-not-text",
        "dataframes-empty",
        "dataframes-string",
        "dataframes-blank-name",
        "dataframes-none-name",
    ],
)
def test_a_broken_rule_names_its_index_and_key(rule, message):
    # A valid rule at index 0 proves the reported index is the rule's own.
    with pytest.raises(ConfigurationError, match=message):
        parse_rule_pack({"dqframework.dqrules": [{"engine": "deequ"}, rule]})


def test_all_rules_are_checked_before_any_is_accepted():
    """The third rule is broken; nothing partial is returned."""
    rules = [{"engine": "deequ"}, {"engine": "gx"}, {"engine": ""}]
    with pytest.raises(ConfigurationError, match="Rule 2"):
        parse_rule_pack({"dqframework.dqrules": rules})


def test_dataframes_default_to_the_default_frame():
    (rule,) = parse_rule_pack({"dqframework.dqrules": [{"engine": "deequ"}]}).rules
    assert rule.dataframes == (DEFAULT_DATAFRAME,)


def test_a_pyhocon_tree_parses_and_keeps_each_rule_tree_intact():
    tree = ConfigFactory.parse_string(
        "dqframework { dqrules = [{ engine = deequ, dataframes = [a, b] }] }"
    )
    assert isinstance(tree, ConfigTree)
    (rule,) = parse_rule_pack(tree).rules
    assert rule.dataframes == ("a", "b")
    # The engine reads its payload from the rule's own tree, untouched.
    assert rule.definition is tree.get("dqframework.dqrules")[0]
    assert rule.definition.get("engine") == "deequ"
