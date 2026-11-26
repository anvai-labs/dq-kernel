# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Spark adapter against the shared semantic corpus (SPEC-011).

Two layers are checked, so a divergence is located precisely: the metrics
the adapter computes (``check_metrics``) and the decisions it reaches
(``check_case``). For counts/v1 the corpus's decisions are also cross-checked
against Deequ wherever the two semantics coincide; the rules where they do
not are named in the corpus with the reason.
"""

import operator

import pytest
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from dq.sdk.fixtures import (
    DATASET_NAME,
    SEMANTICS_VERSION,
    build_plan,
    check_case,
    check_metrics,
    load_cases,
)
from dq.spark_adapter import compute_plan_values, execute_plan
from dq.tests.fixture_frames import spark_frame

pytestmark = pytest.mark.spark

EVERY_CASE = load_cases()
DECIDABLE = [case for case in EVERY_CASE if not case.expect_error]
FAIL_CLOSED = [case for case in EVERY_CASE if case.expect_error]
COUNTS = [case for case in EVERY_CASE if case.semantics == SEMANTICS_VERSION]

_OPERATORS = {
    "ge": operator.ge,
    "gt": operator.gt,
    "le": operator.le,
    "lt": operator.lt,
    "eq": operator.eq,
}


# ------------------------------------------------------------- metrics


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_spark_computes_the_documented_metrics(spark, case):
    frames = {DATASET_NAME: spark_frame(spark, case)}

    def compute(plan, bindings):
        from dq.spark_adapter import _validated_bindings

        return compute_plan_values(plan, _validated_bindings(plan, bindings))

    assert check_metrics(case, compute, frames) == []


# ------------------------------------------------------------ decisions


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_spark_decides_every_case_as_the_corpus_documents(spark, case):
    frames = {DATASET_NAME: spark_frame(spark, case)}
    assert check_case(case, execute_plan, frames) == []
    assert build_plan(case).fingerprint  # a plan always carries its identity


@pytest.mark.parametrize("case", FAIL_CLOSED, ids=lambda case: case.id)
def test_spark_fails_closed_where_the_corpus_documents_an_error(spark, case):
    frames = {DATASET_NAME: spark_frame(spark, case)}
    assert check_case(case, execute_plan, frames) == []


# ------------------------------------------------------ Deequ cross-check


def deequ_decisions(spark, dataframe, specs):
    """Run one Deequ check per ``(rule_id, builder)`` and map id to success."""
    builder = VerificationSuite(spark).onData(dataframe)
    for rule_id, chain in specs:
        builder = builder.addCheck(chain(Check(spark, CheckLevel.Error, rule_id)))
    result = builder.run()
    return {
        row["check"]: row["constraint_status"] == "Success"
        for row in VerificationResult.checkResultsAsDataFrame(spark, result).collect()
    }


def deequ_builder(rule):
    """The Deequ assertion a corpus counts/v1 rule corresponds to."""
    compare = _OPERATORS[rule["operator"]]
    threshold = float(rule["threshold"])
    assertion = lambda value: compare(value, threshold)  # noqa: E731
    if rule["kind"] == "size":
        return lambda check: check.hasSize(assertion)
    return lambda check: check.hasCompleteness(rule["column"], assertion)


def comparable_rules(case):
    """The counts/v1 rules whose verdict Deequ and the kernel must share.

    A rule is left out when the corpus names it as a known divergence, or
    when its column carries a NaN: Deequ counts NaN as present and the
    kernel does not.
    """
    return [
        rule
        for rule in case.rules
        if rule["rule_id"] not in case.deequ_diverges
        and not (rule.get("column") and case.carries_nan(rule["column"]))
    ]


@pytest.mark.parametrize("case", COUNTS, ids=lambda case: case.id)
def test_deequ_agrees_with_the_corpus_where_the_semantics_coincide(spark, case):
    specs = [(rule["rule_id"], deequ_builder(rule)) for rule in comparable_rules(case)]
    if not specs:
        pytest.skip("every rule of this case is a named divergence")
    deequ = deequ_decisions(spark, spark_frame(spark, case), specs)
    assert set(deequ) == {rule_id for rule_id, _ in specs}
    for rule_id, _ in specs:
        assert deequ[rule_id] is case.expected[rule_id], (
            f"{case.id}.{rule_id}: Deequ says {deequ[rule_id]}, "
            f"the corpus says {case.expected[rule_id]}. If this is a real "
            "divergence, name it in the corpus with the reason."
        )


def test_the_cross_check_is_not_vacuous():
    compared = sum(len(comparable_rules(case)) for case in COUNTS)
    divergent = sum(len(case.deequ_diverges) for case in COUNTS)
    assert compared >= 20, f"only {compared} rules are compared with Deequ"
    assert divergent >= 4, "the corpus should name the known divergences"
    assert compared + divergent < sum(
        len(case.rules) for case in COUNTS
    ), "some rules must be excluded for NaN, or the NaN rule is not exercised"
