# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Profiling and suggestion, with no Spark and no pyarrow (SPEC-019).

The profile document, the suggesters, the three formats and the two
commands are held here with a profile written by hand; what the real
profilers say about real data is in test_profile_arrow.py and
test_profile_spark.py.
"""

import hashlib
import decimal
import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyhocon import ConfigFactory

from dq.engine.registry import BUILT_IN_ENGINES, find_spec
from dq.exceptions import ConfigurationError
from dq.gate import evaluate_gate, policy_from_config
from dq.profile import cli, profile as documents, suggest
from dq.profile.profile import (
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    NESTED,
    TEXT,
    TIMESTAMP,
    Column,
    Profile,
)
from dq.profile.suggest import COMPLETE, NON_NEGATIVE, UNIQUE
from dq.utils.config_utils import config_tree_to_python, load_config

#: Every package the kernel treats as optional.
EXTRAS = {
    "pyspark",
    "pydeequ",
    "great_expectations",
    "datafusion",
    "databricks",
    "delta",
    "boto3",
    "pyarrow",
    "sqlalchemy",
    "opentelemetry",
    "polars",
    "pandas",
    "pandera",
}


def a_profile(*columns, rows=8, dataset="orders", digest="a" * 64, source="arrow"):
    return Profile(dataset, digest, rows, columns, source)


ID = Column("id", INTEGER, 0, 8, True, 1, 8)
#: Eight rows: one null, a not-a-number, and a least value below zero.
AMOUNT = Column("amount", FLOAT, 1, 5, True, -2.0, 3.25, None, None, None, None, 1)
#: Eight rows: two nulls, and a least value below zero.
PRICE = Column("price", DECIMAL, 2, 2, True, "-0.05", "1.50", None, None, 10, 2)
NAME = Column("name", TEXT, 2, 5, True, None, None, 0, 3)
TAGS = Column("tags", NESTED, 2, None, True)
THE_FIXTURE = a_profile(ID, AMOUNT, PRICE, NAME, TAGS)


def loads_back(path) -> dict:
    """Return the rule pack a written draft holds, by the package's own loader."""
    return config_tree_to_python(load_config(str(path)))


# ------------------------------------------------------------ the document


def test_every_value_has_one_spelling():
    assert documents.spell(1, INTEGER) == 1
    assert documents.spell(1.5, FLOAT) == 1.5
    assert documents.spell(float("inf"), FLOAT) == "Infinity"
    assert documents.spell(float("-inf"), FLOAT) == "-Infinity"
    assert documents.spell(Decimal("-0.05"), DECIMAL) == "-0.05"
    assert documents.spell(date(2026, 1, 1), DATE) == "2026-01-01"
    assert documents.spell(datetime(2026, 1, 1, 10), TIMESTAMP) == "2026-01-01T10:00:00"
    assert documents.spell(None, INTEGER) is None


def test_a_decimal_handed_for_a_float_bound_is_a_float():
    assert documents.spell(decimal.Decimal("1.50"), FLOAT) == 1.5
    assert documents.unspell("ten", DECIMAL) is None


def test_a_document_that_is_not_an_object_is_refused():
    for not_a_profile in ([1, 2], "dq-profile/v1", None, 7):
        with pytest.raises(ConfigurationError, match="must be a JSON object"):
            Profile.from_document(not_a_profile)
    for not_a_summary in ([1, 2], "id", None, 7):
        with pytest.raises(ConfigurationError, match="must be a mapping"):
            Column.from_document(not_a_summary)


def test_a_not_a_number_is_never_a_bound():
    with pytest.raises(ConfigurationError, match="cannot be a not-a-number"):
        documents.spell(float("nan"), FLOAT)


def test_a_profile_round_trips_through_its_own_json():
    again = Profile.from_json(THE_FIXTURE.to_json())
    assert again == THE_FIXTURE
    assert again.to_document() == THE_FIXTURE.to_document()


def test_the_document_is_strict_json():
    document = json.loads(THE_FIXTURE.to_json())
    assert document == THE_FIXTURE.to_document()
    assert document["schema_version"] == "dq-profile/v1"
    assert document["dataset"] == {"name": "orders", "snapshot_sha256": "a" * 64}


def test_which_profiler_measured_does_not_make_two_profiles_different():
    assert a_profile(ID, source="arrow") == a_profile(ID, source="spark")
    assert a_profile(ID, rows=8) != a_profile(ID, rows=9)


def test_a_summary_carries_every_measure_even_where_it_has_none():
    assert set(ID.to_document()) == {
        "name",
        "type",
        "nulls",
        "distinct",
        "distinct_exact",
        "min",
        "max",
        "min_length",
        "max_length",
        "precision",
        "scale",
        "not_a_number",
    }
    assert ID.min_length is None and ID.not_a_number is None


def a_summary(**said) -> dict:
    """Return a column summary with every measure; those not named are null.

    Every key of a summary is always present in a profile -- a measure a
    type has none of is null, never omitted -- and a reader refuses one
    that is missing.
    """
    whole = {
        "name": "x",
        "type": "integer",
        "nulls": 0,
        "distinct": 1,
        "distinct_exact": True,
        "min": None,
        "max": None,
        "min_length": None,
        "max_length": None,
        "precision": None,
        "scale": None,
        "not_a_number": None,
    }
    whole.update(said)
    return whole


@pytest.mark.parametrize(
    "change",
    [
        {"min": "2026-01-01", "max": "2026-11-25"},
        {"min": 1.5, "max": 2},
        {"min": True, "max": 5},
        {"min": 5, "max": 1},
        {"min": 1, "max": None},
        {"min": 1, "max": "two"},
        {"min_length": 1, "max_length": 2},
        {"mystery": 1},
        {"not_a_number": 0},
        {"precision": 10, "scale": 2},
        {"nulls": -1},
        {"nulls": True},
        {"distinct": -1},
        {"distinct_exact": "yes"},
        {"type": "string"},
        {"name": " "},
    ],
    ids=[
        "a-date-where-an-integer-belongs",
        "not-an-integer",
        "a-boolean-is-no-count",
        "least-above-greatest",
        "one-bound",
        "a-word-for-a-number",
        "an-integer-has-no-length",
        "no-such-measure",
        "an-integer-is-never-a-not-a-number",
        "only-a-decimal-has-a-precision",
        "negative-nulls",
        "boolean-nulls",
        "negative-distinct",
        "exactness-is-true-or-false",
        "a-type-that-is-not-one",
        "a-blank-name",
    ],
)
def test_a_summary_that_cannot_be_is_refused(change):
    said = {**ID.to_document(), **change}
    with pytest.raises(ConfigurationError) as raised:
        Column.from_document(said)
    assert str(raised.value)


@pytest.mark.parametrize(
    "said",
    [
        a_summary(type="decimal"),
        a_summary(type="decimal", min="-0.05", max="1.50", precision=10, scale=12),
        a_summary(type="float", min=1, max="NaN"),
        a_summary(type="date", min="2026-1-1", max="2026-11-25"),
        a_summary(
            type="timestamp",
            min="2026-01-01T00:00:00+00:00",
            max="2026-11-25T10:00:00",
        ),
        a_summary(type="text", min_length=3, max_length=2),
        [1, 2],
    ],
    ids=[
        "a-decimal-without-its-precision",
        "a-scale-above-the-precision",
        "not-a-number-as-a-bound",
        "a-date-not-written-as-one",
        "a-timestamp-with-an-offset",
        "least-length-above-greatest",
        "not-a-mapping",
    ],
)
def test_whole_summaries_that_cannot_be_are_refused(said):
    with pytest.raises(ConfigurationError):
        Column.from_document(said)


@pytest.mark.parametrize(
    ("said", "refused"),
    [
        (dict(type="text", min_length=3), "a length bound comes with its other bound"),
        (dict(type="text", max_length=3), "a length bound comes with its other bound"),
        (dict(type="text", min_length=-1, max_length=3), "min_length must be a count"),
        (
            dict(type="text", min_length="one", max_length=3),
            "min_length must be a count",
        ),
        (
            dict(type="decimal", min="ten", max="1.50", precision=10, scale=2),
            "not spelled",
        ),
        (
            dict(type="decimal", min="-0.05", max="", precision=10, scale=2),
            "not spelled",
        ),
    ],
    ids=[
        "a-least-length-alone",
        "a-greatest-length-alone",
        "a-negative-length",
        "a-length-as-text",
        "a-decimal-that-is-not-one",
        "a-decimal-of-nothing",
    ],
)
def test_lengths_and_decimals_that_cannot_be_are_refused_with_their_reason(
    said, refused
):
    with pytest.raises(ConfigurationError, match=refused):
        Column.from_document(a_summary(**said))


@pytest.mark.parametrize(
    "measure", ["min", "max_length", "precision", "scale", "not_a_number"]
)
def test_a_summary_that_omits_a_measure_is_refused(measure):
    """Every measure is said, null where the type has none: a reader never
    tells absence from none."""
    said = {key: value for key, value in ID.to_document().items() if key != measure}
    with pytest.raises(
        ConfigurationError, match=f"a column summary has no {measure!r}"
    ):
        Column.from_document(said)


def test_a_dataset_that_omits_its_digest_is_refused():
    document = json.loads(json.dumps(GOOD))
    del document["dataset"]["snapshot_sha256"]
    with pytest.raises(ConfigurationError, match="has no 'snapshot_sha256'"):
        Profile.from_document(document)
    document["dataset"]["snapshot_sha256"] = None  # said: no file was read
    assert Profile.from_document(document).snapshot_sha256 is None


def test_an_infinite_float_bound_is_a_spelling():
    column = Column(
        "amount", FLOAT, 0, 1, True, "-Infinity", "Infinity", None, None, None, None, 0
    )
    assert Column.from_document(column.to_document()) == column


@pytest.mark.parametrize("not_finite", [float("inf"), float("-inf"), float("nan")])
def test_a_float_that_is_not_finite_is_spelled_as_text_never_given_as_a_number(
    not_finite,
):
    said = {**AMOUNT.to_document(), "min": not_finite}
    with pytest.raises(ConfigurationError, match="not spelled as a value"):
        Column.from_document(said)


@pytest.mark.parametrize("no_value", [[3.25], {"value": 3.25}, None, True])
def test_a_bound_that_is_no_sort_of_value_is_refused_not_a_crash(no_value):
    said = {**AMOUNT.to_document(), "max": no_value}
    with pytest.raises(ConfigurationError) as raised:
        Column.from_document(said)
    assert str(raised.value)


@pytest.mark.parametrize(
    ("family", "least", "most"),
    [
        ("date", "2026-11-25", "2026-01-01"),
        ("timestamp", "2026-11-25T10:00:00", "2026-01-01T00:00:00"),
    ],
    ids=["a-date", "a-timestamp"],
)
def test_a_least_moment_above_the_greatest_is_refused(family, least, most):
    def said(low, high):
        return a_summary(type=family, min=low, max=high)

    with pytest.raises(ConfigurationError, match="above the greatest"):
        Column.from_document(said(least, most))
    assert Column.from_document(said(most, least)).min == most


GOOD = a_profile(ID).to_document()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document.pop("schema_version"),
        lambda document: document.update(schema_version="dq-profile/v2"),
        lambda document: document.pop("dataset"),
        lambda document: document["dataset"].pop("name"),
        lambda document: document["dataset"].update(snapshot_sha256="xyz"),
        lambda document: document.update(rows=-1),
        lambda document: document.pop("rows"),
        lambda document: document.update(columns={}),
        lambda document: document.update(source="excel"),
        lambda document: document["columns"].append(dict(document["columns"][0])),
    ],
    ids=[
        "no-version",
        "another-version",
        "no-dataset",
        "no-name",
        "bad-digest",
        "negative-rows",
        "no-rows",
        "columns-not-a-list",
        "another-source",
        "a-column-twice",
    ],
)
def test_a_document_that_is_not_a_profile_is_refused(mutate):
    document = json.loads(json.dumps(GOOD))
    mutate(document)
    with pytest.raises(ConfigurationError):
        Profile.from_document(document)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document.update(profiled_at="2026-11-28"),
        lambda document: document["dataset"].update(catalog="unity"),
        lambda document: document["columns"][0].update(nulls=9),
    ],
    ids=["an-unknown-key", "an-unknown-key-of-the-dataset", "more-nulls-than-rows"],
)
def test_a_document_that_says_what_no_profile_says_is_refused(mutate):
    document = json.loads(json.dumps(GOOD))
    mutate(document)
    with pytest.raises(ConfigurationError) as raised:
        Profile.from_document(document)
    assert str(raised.value)


def test_text_that_is_not_json_is_refused():
    with pytest.raises(ConfigurationError, match="not JSON"):
        Profile.from_json("{not json")


@pytest.mark.parametrize("spelled", [float("nan"), float("inf"), float("-inf")])
def test_text_that_a_json_reader_would_take_is_not_strict_json(spelled):
    """Python's reader takes NaN and Infinity for numbers; a profile spells
    an infinite bound as text, and a not-a-number never."""
    document = json.loads(THE_FIXTURE.to_json())
    document["columns"][1]["min"] = spelled
    text = json.dumps(document)  # writes NaN, Infinity or -Infinity bare
    with pytest.raises(ConfigurationError, match="strict JSON"):
        Profile.from_json(text)


def test_the_names_a_dataset_holds_twice_are_found_in_order():
    assert documents.named_twice(["id", "x", "id", "y", "x", "id"]) == ["id", "x"]
    assert documents.named_twice(["id", "x"]) == []


# ------------------------------------------------------------ what is warranted


def test_a_column_says_what_it_warrants():
    """The fixture's id is complete, exactly distinct and never below 0. Its
    price has a least value of -0.05, so nothing is said of it."""
    assert suggest.warranted(THE_FIXTURE) == (
        suggest.Suggestion("id", COMPLETE),
        suggest.Suggestion("id", UNIQUE),
        suggest.Suggestion("id", NON_NEGATIVE),
    )


def test_nothing_is_warranted_of_an_empty_dataset():
    assert suggest.warranted(a_profile(ID, rows=0)) == ()


def test_a_null_is_not_completeness_and_an_approximate_count_is_not_uniqueness():
    every_value_null = Column("x", INTEGER, 8, 0, True, None, None)
    assert suggest.warranted(a_profile(every_value_null)) == ()
    almost = Column("x", INTEGER, 0, 8, False, 1, 8)
    assert suggest.warranted(a_profile(almost)) == (
        suggest.Suggestion("x", COMPLETE),
        suggest.Suggestion("x", NON_NEGATIVE),
    )


def test_nothing_is_said_of_a_nested_column():
    assert suggest.warranted(a_profile(TAGS)) == ()


#: How each vocabulary says a column is complete.
COMPLETENESS = ("isComplete", "expect_column_values_to_not_be_null", "is_not_null")
#: A float column holding a not-a-number, with no null and nothing below zero.
WITH_A_NOT_A_NUMBER = Column(
    "c", FLOAT, 0, 4, True, 0.5, 2.0, None, None, None, None, 1
)
NESTED_WHOLE = Column("c", NESTED, 0, None, True)
SIGNED_ZEROS = Column("c", FLOAT, 0, 1, True, 0.0, 0.0, None, None, None, None, 0)
WHOLE_TEXT = Column("c", TEXT, 0, 2, True, None, None, 0, 1)


def completeness_of(profile, target) -> list:
    """Return the completeness checks a draft holds for ``target``, by name."""
    rules = suggest.suggest(profile, target, snapshot_sha256="a" * 64)
    key = find_spec(target).payload_key  # the registry's word for a rule's checks
    named = []
    for rule in rules["dqframework"]["dqrules"]:
        for check in rule[key]:
            if "check" in check:  # DQX nests what it says
                said = check["check"]["function"]
            else:
                said = check.get("constraint") or check.get("type")
            if said in COMPLETENESS:
                named.append(check.get("constraint_name", check.get("name")))
    return named


def test_the_portable_subset_says_nothing_of_completeness_where_it_counts_a_not_a_number_missing():
    """Measured: the portable isComplete counts a float that is not a number
    as a missing value, where the profile counts it a value -- so a draft
    that said it would fail on the data it was drawn from."""
    profile = a_profile(WITH_A_NOT_A_NUMBER, rows=4)
    assert suggest.warranted(profile) == (
        suggest.Suggestion("c", COMPLETE),
        suggest.Suggestion("c", UNIQUE),
        suggest.Suggestion("c", NON_NEGATIVE),
    )
    assert suggest.unconfirmed(profile, "portable") == ("c_complete",)
    assert completeness_of(profile, "portable") == []
    for other in ("deequ", "dqx", "greatexpectations"):
        assert suggest.unconfirmed(profile, other) == ()
        assert completeness_of(profile, other) == ["c_complete"]
    # Without a not-a-number, or not a float, the same suggestion stands.
    for taken in (SIGNED_ZEROS, NESTED_WHOLE, WHOLE_TEXT):
        assert suggest.unconfirmed(a_profile(taken), "portable") == ()
        assert completeness_of(a_profile(taken), "portable") == ["c_complete"]


def test_deequ_says_nothing_of_completeness_of_a_nested_column():
    """Measured: Deequ's isComplete refuses a nested column -- ``Unsupported
    nested column type`` -- and so fails it."""
    profile = a_profile(NESTED_WHOLE)
    assert suggest.warranted(profile) == (suggest.Suggestion("c", COMPLETE),)
    assert suggest.unconfirmed(profile, "deequ") == ("c_complete",)
    assert completeness_of(profile, "deequ") == []
    for other in ("portable", "dqx", "greatexpectations"):
        assert suggest.unconfirmed(profile, other) == ()
        assert completeness_of(profile, other) == ["c_complete"]
    assert suggest.unconfirmed(a_profile(WITH_A_NOT_A_NUMBER), "deequ") == ()


def test_dq_suggest_names_what_the_vocabulary_would_not_confirm(tmp_path, caplog):
    path = tmp_path / "nan.json"
    path.write_text(a_profile(WITH_A_NOT_A_NUMBER).to_json())
    code = cli.suggest_main(
        [str(path), "--engine", "portable", "--snapshot-sha256", "a" * 64]
    )
    assert code == 0
    said = [line for line in caplog.text.splitlines() if "would not confirm" in line]
    assert len(said) == 1, caplog.text
    assert "for portable" in said[0] and "'c_complete'" in said[0], said[0]


def test_nothing_is_suggested_for_a_column_no_rule_can_name():
    """Measured on Spark 3.5: the portable engine refuses such a rule when it
    runs, Deequ raises on a name with a dot, DQX fails every row of one, and
    only Great Expectations takes a dotted name."""
    oddly = a_profile(
        Column("a.b", INTEGER, 0, 8, True, 1, 8),
        Column("order id", INTEGER, 0, 8, True, 1, 8),
        Column("a`b", INTEGER, 0, 8, True, 1, 8),
        Column("9lives", INTEGER, 0, 8, True, 1, 8),
        Column("x" * 129, INTEGER, 0, 8, True, 1, 8),
        ID,
    )
    assert suggest.warranted(oddly) == (
        suggest.Suggestion("id", COMPLETE),
        suggest.Suggestion("id", UNIQUE),
        suggest.Suggestion("id", NON_NEGATIVE),
    )
    assert suggest.passed_over(oddly) == ("a.b", "order id", "a`b", "9lives", "x" * 129)
    assert suggest.passed_over(THE_FIXTURE) == ()
    for target in suggest.TARGETS:
        rules = suggest.suggest(oddly, target)["dqframework"]["dqrules"]
        written = json.dumps(rules)
        assert "id_complete" in written  # the plainly named column is still said
        for name in suggest.passed_over(oddly):
            assert name not in written, (target, name)


@pytest.mark.parametrize(
    ("name", "addressable"),
    [
        ("_", True),
        ("A1_b", True),
        ("x" * 128, True),
        ("", False),
        ("a.b", False),
        ("a b", False),
        ("a-b", False),
        ("1a", False),
        ("x" * 129, False),
    ],
)
def test_a_column_is_named_as_the_kernel_names_one(name, addressable):
    assert suggest.addressable(name) is addressable


@pytest.mark.parametrize(
    "least",
    ["Infinity", 0, 0.0, 3.25],
    ids=["all-infinite", "an-integer-zero", "a-float-zero", "a-positive"],
)
def test_a_least_value_of_zero_or_more_warrants_non_negativity(least):
    column = Column("f", FLOAT, 0, 8, True, least, 3.25, None, None, None, None, 0)
    assert suggest.warranted(a_profile(column)) == (
        suggest.Suggestion("f", COMPLETE),
        suggest.Suggestion("f", UNIQUE),
        suggest.Suggestion("f", NON_NEGATIVE),
    )


@pytest.mark.parametrize(
    ("least", "family", "said"),
    [
        ("-Infinity", FLOAT, {}),
        (-2.0, FLOAT, {}),
        ("-0.05", DECIMAL, {"precision": 10, "scale": 2}),
    ],
    ids=["all-minus-infinite", "a-negative-float", "a-negative-decimal"],
)
def test_a_least_value_below_zero_does_not_warrant_non_negativity(least, family, said):
    column = Column("c", family, 0, 8, True, least, 3.25, **said)
    assert suggest.warranted(a_profile(column)) == (
        suggest.Suggestion("c", COMPLETE),
        suggest.Suggestion("c", UNIQUE),
    )


def test_a_boolean_a_date_and_text_are_never_non_negative():
    for column in (
        Column("b", BOOLEAN, 0, 1, True),
        Column("d", DATE, 0, 1, True, "2026-01-01", "2026-01-01"),
        Column("t", TEXT, 0, 1, True, None, None, 1, 1),
    ):
        assert all(s.kind != NON_NEGATIVE for s in suggest.warranted(a_profile(column)))


# ------------------------------------------------------------ each vocabulary


def rules_of(profile, target):
    return suggest.suggest(profile, target)["dqframework"]["dqrules"]


def test_the_deequ_draft_is_written_in_deequ_s_words():
    (rule,) = rules_of(THE_FIXTURE, "deequ")
    assert rule["name"] == "orders_draft"
    assert rule["engine"] == "deequ"
    assert rule["dataframes"] == ["orders"]
    assert rule["checks"] == [
        {
            "constraint_name": "id_complete",
            "constraint": "isComplete",
            "column": "id",
            "level": "Warning",
        },
        {
            "constraint_name": "id_unique",
            "constraint": "isUnique",
            "column": "id",
            "level": "Warning",
        },
        {
            "constraint_name": "id_non_negative",
            "constraint": "isNonNegative",
            "column": "id",
            "level": "Warning",
        },
    ]


def test_the_dqx_draft_uses_the_registry_s_argument_names():
    (rule,) = rules_of(THE_FIXTURE, "dqx")
    assert rule["engine"] == "dqx"
    assert rule["checks"] == [
        {
            "name": "id_complete",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "id_unique",
            "criticality": "warn",
            "check": {"function": "is_unique", "arguments": {"columns": ["id"]}},
        },
        {
            "name": "id_non_negative",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "id", "limit": 0},
            },
        },
    ]


def test_the_great_expectations_draft_puts_only_arguments_on_the_expectation():
    (rule,) = rules_of(THE_FIXTURE, "greatexpectations")
    assert rule["engine"] == "greatexpectations"
    assert rule["expectations"] == [
        {
            "name": "id_complete",
            "type": "expect_column_values_to_not_be_null",
            "column": "id",
            "criticality": "warn",
        },
        {
            "name": "id_unique",
            "type": "expect_column_values_to_be_unique",
            "column": "id",
            "criticality": "warn",
        },
        {
            "name": "id_non_negative",
            "type": "expect_column_values_to_be_between",
            "column": "id",
            "criticality": "warn",
            "min_value": 0,
        },
    ]


def test_the_portable_draft_has_no_uniqueness_and_one_rule_per_semantic_version():
    rules = rules_of(THE_FIXTURE, "portable")
    assert [rule["name"] for rule in rules] == [
        "orders_draft_complete",
        "orders_draft_non_negative",
    ]
    for rule in rules:
        assert rule["engine"] == "portable"
        assert rule["dataframes"] == ["orders"]
        assert rule["dataset"] == "orders"
        assert rule["snapshot_sha256"] == "a" * 64
        assert rule["adapter"] == "arrow"
    assert rules[0]["plan"] == [
        {
            "constraint_name": "id_complete",
            "constraint": "isComplete",
            "column": "id",
            "level": "Warning",
        }
    ]
    assert rules[1]["plan"] == [
        {
            "constraint_name": "id_non_negative",
            "constraint": "isNonNegative",
            "column": "id",
            "level": "Warning",
        }
    ]


def test_a_portable_draft_of_one_semantic_version_keeps_the_plain_name():
    """Complete, exactly distinct, and with a least value below zero."""
    column = Column("x", INTEGER, 0, 8, True, -5, 3)
    (rule,) = rules_of(a_profile(column), "portable")
    assert rule["name"] == "orders_draft"
    assert [check["constraint"] for check in rule["plan"]] == ["isComplete"]


def test_a_portable_rule_needs_the_snapshot_it_certifies():
    with pytest.raises(ConfigurationError, match="--snapshot-sha256"):
        suggest.suggest(a_profile(ID, digest=None), "portable")


def test_where_nothing_is_warranted_no_digest_is_asked_for():
    for target in suggest.TARGETS:
        assert suggest.suggest(a_profile(TAGS, digest=None), target) == {
            "dqframework": {"gate": {"fail_on": "error"}, "dqrules": []}
        }


def test_a_draft_is_written_under_the_gate_policy_where_a_warning_blocks_nothing():
    """Measured: the kernel's default policy, "any", blocks on a failed
    warning as on any failure -- so a warning alone would let a draft gate."""
    a_failed_warning = {
        "check": "id_complete",
        "success": False,
        "details": {"criticality": "warn"},
    }
    assert evaluate_gate([a_failed_warning]).passed is False  # the default
    for target in suggest.TARGETS:
        config = suggest.suggest(THE_FIXTURE, target)
        decision = evaluate_gate([a_failed_warning], policy_from_config(config))
        assert decision.policy == "error"
        assert decision.passed is True, target
        assert decision.advisory == ("id_complete",)
        assert decision.blocking == ()


@pytest.mark.parametrize("target", ["excel", "Portable", "", None])
def test_a_vocabulary_that_is_not_one_is_refused(target):
    with pytest.raises(ConfigurationError, match="suggested for one of"):
        suggest.suggest(THE_FIXTURE, target)


@pytest.mark.parametrize("target", suggest.TARGETS)
def test_the_same_profile_always_yields_the_same_draft(target):
    once = suggest.suggest(THE_FIXTURE, target)
    assert [suggest.suggest(THE_FIXTURE, target) for _ in range(3)] == [once] * 3


def test_the_registry_knows_each_vocabulary():
    assert set(suggest.TARGETS) <= set(BUILT_IN_ENGINES)


# --------------------------------------------------------------- the formats


@pytest.mark.parametrize("target", suggest.TARGETS)
@pytest.mark.parametrize("fmt", suggest.FORMATS)
def test_every_draft_loads_back_equal_in_every_format(tmp_path, target, fmt):
    pack = suggest.suggest(THE_FIXTURE, target)
    path = tmp_path / f"draft.{fmt}"
    path.write_text(suggest.render(pack, fmt))
    assert suggest.format_of(path) == fmt
    assert loads_back(path) == pack


def test_a_name_with_a_space_or_a_quote_in_it_survives_every_format(tmp_path):
    """A dataset's name is a rule's name, and no identifier constrains it."""
    odd = a_profile(ID, dataset='odd "orders"')
    for fmt in suggest.FORMATS:
        path = tmp_path / f"draft.{fmt}"
        path.write_text(suggest.render(suggest.suggest(odd, "dqx"), fmt))
        (rule,) = loads_back(path)["dqframework"]["dqrules"]
        assert rule["name"] == 'odd "orders"_draft'
        assert rule["dataframes"] == ['odd "orders"']


def test_a_value_with_a_quote_a_backslash_or_a_newline_reads_back(tmp_path):
    """Values a draft never writes, to show what the writer's quoting is worth."""
    pack = {
        "dqframework": {"gate": {"fail_on": "never"}, "dqrules": []},
        "an odd key": ['quote"inside', "back\\slash", "line\nbreak", "braces {}"],
        "scalars": [None, True, False, 7, -1.5, "null"],
        "empty": {},
        "none": [],
    }
    for fmt in suggest.FORMATS:
        path = tmp_path / f"draft.{fmt}"
        path.write_text(suggest.render(pack, fmt))
        assert loads_back(path) == pack, fmt


def test_hocon_is_written_as_hocon_with_quoted_strings():
    text = suggest.render(suggest.suggest(a_profile(ID), "deequ"), "hocon")
    assert text == (
        "{\n"
        "  dqframework {\n"
        '    gate { fail_on = "error" }\n'
        "    dqrules = [\n"
        "      {\n"
        '        name = "orders_draft"\n'
        '        engine = "deequ"\n'
        '        dataframes = ["orders"]\n'
        "        checks = [\n"
        '          { constraint_name = "id_complete", constraint = "isComplete",'
        ' column = "id", level = "Warning" }\n'
        '          { constraint_name = "id_unique", constraint = "isUnique",'
        ' column = "id", level = "Warning" }\n'
        '          { constraint_name = "id_non_negative",'
        ' constraint = "isNonNegative", column = "id", level = "Warning" }\n'
        "        ]\n"
        "      }\n"
        "    ]\n"
        "  }\n"
        "}\n"
    )


def test_an_empty_object_and_an_empty_list_have_a_spelling():
    pack = {"a": {}, "b": []}
    assert suggest.render(pack, "hocon") == "{\n  a = {}\n  b = []\n}\n"
    assert config_tree_to_python(
        ConfigFactory.parse_string(suggest.render(pack, "hocon"))
    ) == {
        "a": {},
        "b": [],
    }


@pytest.mark.parametrize(
    ("name", "fmt"),
    [
        ("draft.conf", "hocon"),
        ("draft.hocon", "hocon"),
        ("draft.YAML", "yaml"),
        ("draft.yml", "yaml"),
        ("draft.json", "json"),
    ],
)
def test_a_path_s_extension_names_the_format(name, fmt):
    assert suggest.format_of(name) == fmt


@pytest.mark.parametrize("name", ["draft.txt", "draft", "draft.parquet"])
def test_an_extension_that_names_no_format_is_refused(name):
    with pytest.raises(ConfigurationError, match="names no format"):
        suggest.format_of(name)


def test_a_format_that_is_not_one_is_refused():
    with pytest.raises(ConfigurationError, match="written as one of"):
        suggest.render({}, "toml")


# ------------------------------------------------------------ the digest


def test_the_digest_is_of_the_bytes_read(tmp_path):
    data = tmp_path / "orders.parquet"
    data.write_bytes(b"not really parquet")
    assert (
        cli.snapshot_digest(data) == hashlib.sha256(b"not really parquet").hexdigest()
    )


def test_a_directory_s_digest_covers_every_file_in_name_order(tmp_path):
    data = tmp_path / "dataset"
    (data / "part-b").mkdir(parents=True)
    (data / "part-b" / "c.parquet").write_bytes(b"c")
    (data / "a.parquet").write_bytes(b"a")
    assert cli.snapshot_digest(data) == hashlib.sha256(b"ac").hexdigest()
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ConfigurationError, match="holds no file"):
        cli.snapshot_digest(empty)


# --------------------------------------------------------- the two commands


def stand_in_for_reading(monkeypatch, profile=THE_FIXTURE):
    """Stand in for the reading; return what each call was asked."""
    asked = SimpleNamespace(asked=[])

    def profile_file(path, name, profiler, *, approximate=False):
        asked.asked.append((path, name, profiler, approximate))
        return profile

    monkeypatch.setattr(cli, "profile_file", profile_file)
    return asked


@pytest.fixture
def data(tmp_path):
    path = tmp_path / "orders.parquet"
    path.write_bytes(b"x")
    return path


def test_dq_profile_writes_the_profile_as_json(tmp_path, monkeypatch, data):
    asked = stand_in_for_reading(monkeypatch)
    out = tmp_path / "orders.profile.json"
    code = cli.profile_main(["--dataset", f"orders={data}", "--profile", str(out)])
    assert code == 0
    assert asked.asked == [(data, "orders", "arrow", False)]
    assert Profile.from_json(out.read_text()) == THE_FIXTURE


def test_dq_profile_prints_the_profile_when_no_file_is_named(monkeypatch, capsys, data):
    stand_in_for_reading(monkeypatch)
    assert cli.profile_main(["--dataset", f"orders={data}"]) == 0
    assert Profile.from_json(capsys.readouterr().out) == THE_FIXTURE


def test_dq_profile_reads_with_spark_and_can_approximate(tmp_path, monkeypatch, data):
    asked = stand_in_for_reading(monkeypatch)
    out = tmp_path / "orders.profile.json"
    code = cli.profile_main(
        [
            "--dataset",
            f"orders={data}",
            "--profiler",
            "spark",
            "--approximate-distinct",
            "--profile",
            str(out),
        ]
    )
    assert code == 0
    assert asked.asked == [(data, "orders", "spark", True)]


def test_dq_profile_refuses_an_approximation_asked_of_arrow(monkeypatch, data):
    asked = stand_in_for_reading(monkeypatch)
    code = cli.profile_main(["--dataset", f"orders={data}", "--approximate-distinct"])
    assert code == 2
    assert asked.asked == []


def test_verbose_shows_the_traceback_of_a_refusal(tmp_path, monkeypatch, capsys):
    assert cli.profile_main(["--dataset", f"orders={tmp_path / 'no.parquet'}"]) == 2
    assert "Traceback" not in capsys.readouterr().err
    assert (
        cli.profile_main(
            ["--dataset", f"orders={tmp_path / 'no.parquet'}", "--verbose"]
        )
        == 2
    )
    shown = capsys.readouterr().err
    assert "Traceback (most recent call last)" in shown
    assert "no such file or directory" in shown


def test_dq_profile_refuses_a_dataset_that_is_not_there(tmp_path, monkeypatch):
    asked = stand_in_for_reading(monkeypatch)
    code = cli.profile_main(["--dataset", f"orders={tmp_path / 'no.parquet'}"])
    assert code == 2
    assert asked.asked == []


def test_verbose_shows_the_traceback_of_an_error(tmp_path, monkeypatch, capsys, data):
    def broken(*arguments, **options):
        raise RuntimeError("the kernel of Arrow gave up")

    monkeypatch.setattr(cli, "profile_file", broken)
    code = cli.profile_main(["--dataset", f"orders={data}", "--verbose"])
    assert code == 1
    assert "RuntimeError: the kernel of Arrow gave up" in capsys.readouterr().err


def test_dq_profile_is_of_one_dataset(monkeypatch, data):
    stand_in_for_reading(monkeypatch)
    with pytest.raises(SystemExit) as raised:
        cli.profile_main(["--dataset", f"a={data}", "--dataset", f"b={data}"])
    assert raised.value.code == 2
    with pytest.raises(SystemExit):
        cli.profile_main(["--dataset", "no-separator"])
    with pytest.raises(SystemExit):
        cli.profile_main([])


def test_dq_profile_refuses_a_directory_that_is_not_there_to_write_into(
    tmp_path, monkeypatch, data
):
    stand_in_for_reading(monkeypatch)
    out = tmp_path / "nowhere" / "orders.profile.json"
    code = cli.profile_main(["--dataset", f"orders={data}", "--profile", str(out)])
    assert code == 2
    assert not out.exists()


def test_a_missing_dependency_is_exit_two_and_names_the_extra(
    caplog, monkeypatch, data
):
    def missing(*arguments, **options):
        raise ImportError("The arrow runner requires the optional pyarrow package")

    monkeypatch.setattr(cli, "profile_file", missing)
    assert cli.profile_main(["--dataset", f"orders={data}"]) == 2
    assert "pyarrow" in caplog.text


def test_a_dataset_whose_bytes_cannot_be_read_is_a_refusal(caplog, monkeypatch, data):
    def unreadable(*arguments, **options):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(cli, "profile_file", unreadable)
    assert cli.profile_main(["--dataset", f"orders={data}"]) == 2
    assert "Cannot read the dataset" in caplog.text


def test_a_profiler_failure_is_not_a_refusal_and_is_exit_one(caplog, monkeypatch, data):
    def broken(*arguments, **options):
        raise RuntimeError("the kernel of Arrow gave up")

    monkeypatch.setattr(cli, "profile_file", broken)
    assert cli.profile_main(["--dataset", f"orders={data}"]) == 1
    assert "Error profiling: the kernel of Arrow gave up" in caplog.text


#: What a corrupt Parquet file makes Spark say, stack frames and all.
SPARK_S_TRACE = (
    "An error occurred while calling o28.parquet.\n"
    ": org.apache.spark.SparkException: Job aborted due to stage failure: Task 0\n"
    "\tat org.apache.spark.util.SparkThreadUtils$.awaitResult(SparkThreadUtils.scala:56)\n"
    "Caused by: org.apache.spark.SparkException: Exception thrown in awaitResult: \n"
    "\tat org.apache.spark.util.ThreadUtils$.$anonfun$parmap$2(ThreadUtils.scala:384)\n"
    "Caused by: org.apache.spark.SparkException: [CANNOT_READ_FILE_FOOTER] Could not\n"
    "read   footer for file: /tmp/bad.parquet. If it is, you can choose to either\n"
    "ignore it or fix the corrupt file.\n"
    "\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(T.java:1144)\n"
)


def test_what_spark_said_is_its_own_last_word_without_the_stack_trace():
    assert cli._said(Exception(SPARK_S_TRACE)) == (
        "[CANNOT_READ_FILE_FOOTER] Could not read footer for file: /tmp/bad.parquet. "
        "If it is, you can choose to either ignore it or fix the corrupt file."
    )


def test_a_failure_spark_states_without_a_class_keeps_its_first_line():
    said = cli._said(Exception("first line\n\tat a.b.C(D.java:1)\n\tat x"))
    assert said == "first line"


def test_what_spark_said_is_bounded():
    said = cli._said(Exception("[A_CLASS] " + "word " * 1000))
    assert len(said) == cli.SAID_AT_MOST
    assert said.endswith("...")


@pytest.fixture
def profile_on_disk(tmp_path):
    path = tmp_path / "orders.profile.json"
    path.write_text(THE_FIXTURE.to_json())
    return path


@pytest.mark.parametrize("target", suggest.TARGETS)
@pytest.mark.parametrize("suffix", [".conf", ".hocon", ".yaml", ".yml", ".json"])
def test_dq_suggest_writes_a_draft_that_validates(
    tmp_path, profile_on_disk, target, suffix
):
    out = tmp_path / f"draft{suffix}"
    code = cli.suggest_main(
        [str(profile_on_disk), "--engine", target, "--output", str(out)]
    )
    assert code == 0
    assert loads_back(out) == suggest.suggest(THE_FIXTURE, target)


def test_dq_suggest_counts_rules_and_checks_for_its_summary(
    tmp_path, caplog, profile_on_disk
):
    caplog.set_level(logging.INFO)
    out = tmp_path / "draft.conf"
    code = cli.suggest_main(
        [str(profile_on_disk), "--engine", "portable", "--output", str(out)]
    )
    assert code == 0
    assert "wrote 2 suggested rules for portable" in caplog.text
    # The validator's own line would read as if the user had asked it.
    assert "Configuration is valid" not in caplog.text


def test_dq_suggest_prints_the_draft_when_no_file_is_named(capsys, profile_on_disk):
    assert cli.suggest_main([str(profile_on_disk), "--engine", "deequ"]) == 0
    printed = capsys.readouterr().out
    assert printed == suggest.render(suggest.suggest(THE_FIXTURE, "deequ"), "hocon")


def test_dq_suggest_takes_a_format_where_the_extension_names_none(
    tmp_path, profile_on_disk
):
    out = tmp_path / "draft.txt"
    code = cli.suggest_main(
        [
            str(profile_on_disk),
            "--engine",
            "dqx",
            "--output",
            str(out),
            "--format",
            "hocon",
        ]
    )
    assert code == 0
    assert loads_back(out) == suggest.suggest(
        THE_FIXTURE, "dqx"
    )  # HOCON is read by default
    assert out.read_text().startswith("{\n  dqframework {")  # HOCON, not JSON


def test_dq_suggest_refuses_a_format_that_contradicts_the_extension(
    tmp_path, caplog, profile_on_disk
):
    """A rule pack is read by its extension: HOCON in a .json file would fail."""
    caplog.set_level(logging.INFO)
    out = tmp_path / "draft.json"
    code = cli.suggest_main(
        [
            str(profile_on_disk),
            "--engine",
            "dqx",
            "--output",
            str(out),
            "--format",
            "hocon",
        ]
    )
    assert code == 2
    assert not out.exists()
    assert "contradicts" in caplog.text


def test_dq_suggest_passes_the_snapshot_a_portable_rule_certifies(tmp_path):
    path = tmp_path / "digest.json"
    path.write_text(a_profile(ID, digest=None).to_json())
    out = tmp_path / "draft.conf"
    code = cli.suggest_main(
        [
            str(path),
            "--engine",
            "portable",
            "--output",
            str(out),
            "--snapshot-sha256",
            "c" * 64,
        ]
    )
    assert code == 0
    assert loads_back(out)["dqframework"]["dqrules"][0]["snapshot_sha256"] == "c" * 64


def test_dq_suggest_refuses_a_portable_draft_with_no_digest(tmp_path):
    path = tmp_path / "digest.json"
    path.write_text(a_profile(ID, digest=None).to_json())
    assert cli.suggest_main([str(path), "--engine", "portable"]) == 2


def test_dq_suggest_says_so_when_nothing_is_warranted_and_writes_nothing(
    tmp_path, caplog
):
    caplog.set_level(logging.INFO)
    path = tmp_path / "none.json"
    path.write_text(a_profile(TAGS).to_json())
    out = tmp_path / "draft.conf"
    code = cli.suggest_main([str(path), "--engine", "deequ", "--output", str(out)])
    assert code == 0
    assert not out.exists()
    assert "warrants no rule" in caplog.text


def test_dq_suggest_fails_when_what_it_wrote_does_not_validate(
    tmp_path, monkeypatch, profile_on_disk, caplog
):
    import dq.validate

    out = tmp_path / "draft.conf"

    def refusing(reference, *, report=True):
        return SimpleNamespace(valid=False, errors=("Rule 0: a reason of its own",))

    monkeypatch.setattr(dq.validate, "validate_config_document", refusing)
    code = cli.suggest_main(
        [str(profile_on_disk), "--engine", "deequ", "--output", str(out)]
    )
    assert code == 1
    said = [line for line in caplog.text.splitlines() if "does not validate" in line]
    assert len(said) == 1 and "a reason of its own" in said[0], caplog.text


def test_dq_suggest_checks_a_printed_draft_too(
    capsys, monkeypatch, profile_on_disk, caplog
):
    import dq.validate

    monkeypatch.setattr(
        dq.validate,
        "validate_config_document",
        lambda reference, *, report=True: SimpleNamespace(valid=False, errors=()),
    )
    assert cli.suggest_main([str(profile_on_disk), "--engine", "deequ"]) == 1
    assert "does not validate" in caplog.text
    assert "isComplete" in capsys.readouterr().out


def test_a_printed_draft_is_checked_through_a_copy_that_is_removed(
    monkeypatch, profile_on_disk
):
    import dq.validate

    checked = []
    answer = dq.validate.validate_config_document
    monkeypatch.setattr(
        dq.validate,
        "validate_config_document",
        lambda reference, *, report=True: checked.append(Path(reference))
        or answer(reference, report=report),
    )
    assert cli.suggest_main([str(profile_on_disk), "--engine", "deequ"]) == 0
    (copy,) = checked
    assert copy.suffix == ".conf"  # the format a printed draft is in
    assert not copy.exists()


def test_dq_suggest_refuses_a_profile_that_is_not_text(tmp_path, caplog):
    path = tmp_path / "profile.json"
    path.write_bytes(b"\xff\xfe\x00garbage")
    assert cli.suggest_main([str(path), "--engine", "deequ"]) == 2
    assert "Cannot read the profile" in caplog.text


def test_dq_suggest_reports_an_error_as_exit_one(monkeypatch, caplog, profile_on_disk):
    def broken(*arguments, **options):
        raise RuntimeError("the suggester gave up")

    monkeypatch.setattr(cli, "suggest", broken)
    assert cli.suggest_main([str(profile_on_disk), "--engine", "deequ"]) == 1
    assert "Error suggesting: the suggester gave up" in caplog.text


def test_dq_suggest_names_the_columns_it_passes_over(tmp_path, caplog):
    path = tmp_path / "passed.json"
    path.write_text(a_profile(Column("a.b", INTEGER, 0, 8, True, 1, 8)).to_json())
    assert cli.suggest_main([str(path), "--engine", "deequ"]) == 0
    passed = [line for line in caplog.text.splitlines() if "plain identifier" in line]
    assert len(passed) == 1 and "'a.b'" in passed[0], caplog.text


def test_dq_suggest_refuses_a_profile_that_is_not_there(tmp_path):
    assert cli.suggest_main([str(tmp_path / "no.json"), "--engine", "deequ"]) == 2


def test_dq_suggest_refuses_a_profile_that_is_not_one(tmp_path):
    path = tmp_path / "profile.json"
    path.write_text('{"schema_version": "dq-profile/v2"}')
    assert cli.suggest_main([str(path), "--engine", "deequ"]) == 2


def test_dq_suggest_refuses_a_directory_that_is_not_there_to_write_into(
    tmp_path, caplog, profile_on_disk
):
    out = tmp_path / "nowhere" / "draft.conf"
    code = cli.suggest_main(
        [str(profile_on_disk), "--engine", "deequ", "--output", str(out)]
    )
    assert code == 2
    assert "Cannot write the draft" in caplog.text
    assert not out.exists()


def test_dq_suggest_verbose_names_its_error(
    tmp_path, monkeypatch, capsys, profile_on_disk
):
    def broken(*arguments, **options):
        raise RuntimeError("the suggester gave up")

    monkeypatch.setattr(cli, "suggest", broken)
    code = cli.suggest_main([str(profile_on_disk), "--engine", "deequ", "--verbose"])
    assert code == 1
    assert "RuntimeError: the suggester gave up" in capsys.readouterr().err


def test_dq_suggest_refuses_an_output_whose_extension_names_no_format(
    tmp_path, profile_on_disk
):
    out = tmp_path / "draft.txt"
    code = cli.suggest_main(
        [str(profile_on_disk), "--engine", "deequ", "--output", str(out)]
    )
    assert code == 2
    assert not out.exists()


def test_dq_suggest_needs_a_vocabulary(profile_on_disk):
    with pytest.raises(SystemExit) as raised:
        cli.suggest_main([str(profile_on_disk)])
    assert raised.value.code == 2


# ------------------------------------------------ nothing optional installed


def test_the_whole_of_profiling_imports_with_every_extra_blocked():
    code = f"""
import sys

BLOCKED = {sorted(EXTRAS)!r}


class Block:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise AssertionError(fullname + " was imported")

sys.meta_path.insert(0, Block())
from dq.profile import arrow, cli, profile, spark, suggest

assert cli.PROFILERS == ("arrow", "spark")
assert suggest.TARGETS == ("portable", "deequ", "dqx", "greatexpectations")
one = profile.Profile("orders", None, 3, (profile.Column("id", "integer", 0, 3, True, 1, 3),), "arrow")
assert suggest.render(suggest.suggest(one, "deequ"), "hocon").count("isComplete") == 1
assert not [n for n in sys.modules if n.split(".")[0] in BLOCKED]
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        env={
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("COV_CORE_", "COVERAGE_"))
        },
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
