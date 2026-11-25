# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Property-based tests for the pure kernel (SPEC-011 G6).

Example-based tests pin behaviour for inputs someone thought of. These hold
the pure kernel functions to invariants that must hold for any input: a
sanitized payload always serializes, a gate decision partitions the
failures, labels are unique when checks are distinguishable, and exact
comparisons stay exact where doubles would not.
"""

import json
import math
from decimal import Decimal
from fractions import Fraction

import pytest
from hypothesis import given, settings, strategies as st
from pyhocon import ConfigFactory

from dq.engine.checkspec import labels_for_checks, normalize_criticality
from dq.gate import POLICIES, evaluate_gate
from dq.notifier import GateEvent
from dq.plan import ColumnRef, Comparison, DatasetRef, MetricKey, MetricKind, Predicate
from dq.sdk.fixtures import metric_label
from dq.utils.sanitization import to_strict_json

settings.register_profile("dq", max_examples=120, deadline=None)
settings.load_profile("dq")

identifier = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,12}", fullmatch=True)

json_data = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**53), max_value=2**53)
    | st.floats(allow_nan=True, allow_infinity=True, width=32)
    | st.text(max_size=12),
    lambda children: st.lists(children, max_size=4)
    | st.dictionaries(st.text(max_size=6), children, max_size=4),
    max_leaves=12,
)


class VendorScalar:
    """Stands in for a vendor scalar: exposes ``item()`` and little else."""

    def __init__(self, value):
        self.value = value

    def item(self):
        if self.value == "explode":
            raise RuntimeError("not a scalar")
        return self.value


any_payload = st.recursive(
    json_data
    | st.builds(VendorScalar, json_data | st.just("explode"))
    | st.builds(object),
    lambda children: st.lists(children, max_size=3)
    | st.dictionaries(st.text(max_size=4) | st.integers(), children, max_size=3)
    | st.tuples(children)
    | st.builds(VendorScalar, children),
    max_leaves=10,
)


# -------------------------------------------------------------- sanitizer


@settings(max_examples=300)
@given(any_payload)
def test_a_sanitized_payload_always_serializes_as_strict_json(payload):
    sanitized = to_strict_json(payload)
    encoded = json.dumps(sanitized, allow_nan=False)
    assert json.loads(encoded) == sanitized


@settings(max_examples=300)
@given(any_payload)
def test_sanitizing_is_idempotent(payload):
    once = to_strict_json(payload)
    assert to_strict_json(once) == once


def _json_view(value):
    """What strict JSON can hold: non-finite floats become null, everywhere."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _json_view(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_view(item) for item in value]
    return value


@given(json_data)
def test_sanitizing_plain_json_data_changes_only_what_json_cannot_write(data):
    assert to_strict_json(data) == _json_view(data)


def _leaves(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _leaves(item)
    elif isinstance(value, list):
        for item in value:
            yield from _leaves(item)
    else:
        yield value


@settings(max_examples=300)
@given(any_payload)
def test_every_leaf_of_a_sanitized_payload_is_a_json_scalar(payload):
    for leaf in _leaves(to_strict_json(payload)):
        assert leaf is None or isinstance(leaf, (bool, int, float, str))
        assert not (isinstance(leaf, float) and not math.isfinite(leaf))
        assert not (isinstance(leaf, str) and len(leaf) > 70_000)


def test_the_engines_use_the_one_sanitizer():
    import dq.engine.greatexpectations.greatexpectations_engine as gx

    assert gx._to_strict_json is to_strict_json


# ------------------------------------------------------------- exactness


@settings(max_examples=300)
@given(st.integers(min_value=-(10**12), max_value=10**12))
def test_a_predicate_threshold_round_trips_through_its_text(value):
    text = Predicate(Comparison.GE, Decimal(value)).threshold_text
    again = Predicate(Comparison.GE, Decimal(text))
    assert again.threshold_text == text and again.threshold == Decimal(value)


@settings(max_examples=300)
@given(
    st.integers(min_value=-(10**20), max_value=10**20),
    st.integers(min_value=1, max_value=10**6),
)
def test_a_threshold_is_compared_exactly_not_as_a_double(numerator, denominator):
    """A ratio equals a threshold only when it does, exactly.

    ``n/d`` is converted to a decimal with 30 significant digits; when that
    rounding lands above the true ratio the kernel reports it as below the
    threshold, where a double comparison would call the two equal.
    """
    ratio = Fraction(numerator, denominator)
    threshold = Decimal(numerator) / Decimal(denominator)
    exact_equal = ratio == Fraction(threshold)
    assert Predicate(Comparison.EQ, threshold).matches(ratio) is exact_equal
    assert Predicate(Comparison.GE, threshold).matches(ratio) is (
        ratio >= Fraction(threshold)
    )
    assert Predicate(Comparison.LT, threshold).matches(ratio) is (
        ratio < Fraction(threshold)
    )


def test_two_values_a_double_cannot_tell_apart_are_told_apart():
    below = Decimal("0.10000000000000000001")
    above = Decimal("0.1")
    assert float(below) == float(above), "the premise: doubles see one value"
    assert Predicate(Comparison.EQ, above).matches(Fraction(above)) is True
    assert Predicate(Comparison.EQ, above).matches(Fraction(below)) is False


# -------------------------------------------------------------------- gate


rows = st.lists(
    st.tuples(
        identifier, st.booleans(), st.sampled_from(["error", "warn", None, "fatal"])
    ),
    max_size=12,
    unique_by=lambda row: row[0],
)


def as_outcomes(specs):
    return [
        {"check": name, "success": success, "details": {"criticality": criticality}}
        for name, success, criticality in specs
    ]


@given(rows)
def test_a_gate_decision_partitions_the_failed_checks(specs):
    outcomes = as_outcomes(specs)
    failed = [name for name, success, _ in specs if not success]
    for policy in POLICIES:
        decision = evaluate_gate(outcomes, policy)
        assert sorted(decision.blocking + decision.advisory) == sorted(failed), policy
        assert set(decision.blocking).isdisjoint(decision.advisory), policy
        assert decision.passed is (not decision.blocking)
        assert decision.exit_code == (0 if decision.passed else 1)
        assert json.loads(json.dumps(decision.to_dict())) == decision.to_dict()


@given(rows)
def test_the_default_policy_blocks_every_failure_and_advises_none(specs):
    decision = evaluate_gate(as_outcomes(specs), "any")
    assert decision.blocking == tuple(name for name, success, _ in specs if not success)
    assert decision.advisory == ()


@given(rows)
def test_under_the_error_policy_only_warn_failures_are_advisory(specs):
    decision = evaluate_gate(as_outcomes(specs), "error")
    assert decision.advisory == tuple(
        name
        for name, success, criticality in specs
        if not success and criticality == "warn"
    )
    assert decision.blocking == tuple(
        name
        for name, success, criticality in specs
        if not success and criticality != "warn"
    )


@given(rows)
def test_the_gate_event_names_every_failed_check_once(specs):
    outcomes = as_outcomes(specs)
    event = GateEvent.from_outcomes(1, "ds", outcomes, 1, "job")
    assert event.failed == tuple(name for name, success, _ in specs if not success)
    assert sorted(event.blocked + event.warned) == sorted(event.failed)
    assert set(event.blocked).isdisjoint(event.warned)
    assert event.status == ("passed" if not event.failed else "failed")
    assert event.total == len(specs)


def test_severity_spellings_outside_the_vocabulary_are_refused():
    for raw in (
        "fatal",
        "critical",
        "errors",
        "warning!",
        "warn!",
        "erro",
        3,
        True,
        [],
    ):
        with pytest.raises(ValueError, match="Unknown criticality"):
            normalize_criticality(raw)


def test_the_accepted_severity_spellings_normalize_to_the_canonical_two():
    for raw in (
        "Error",
        "ERROR",
        "error",
        " err ",
        "Warning",
        "WARNING",
        "warn",
        "warning",
    ):
        assert normalize_criticality(raw) in ("error", "warn")
    assert normalize_criticality("Warning") == "warn"
    assert normalize_criticality(None, default="warn") == "warn"


# ------------------------------------------------------------------ labels


specs = st.lists(
    st.fixed_dictionaries({"kind": identifier, "column": identifier | st.none()}),
    max_size=8,
)


@given(specs)
def test_labels_are_unique_when_the_checks_are_distinguishable(checks):
    if any(check["column"] is None for check in checks):
        return  # covered below: same-kind checks without columns take positions
    if len({(check["kind"], check["column"]) for check in checks}) != len(checks):
        return  # the same check twice: no rule could tell them apart
    labels = labels_for_checks(checks, (), "kind", ("column",))
    assert len(labels) == len(set(labels))
    assert labels == [f"{check['kind']}_{check['column']}" for check in checks]


@given(identifier, st.integers(min_value=2, max_value=12))
def test_same_kind_checks_without_columns_take_positions_after_the_first(kind, count):
    checks = [{"kind": kind}] * (count + 1)
    labels = labels_for_checks(checks, (), "kind", ("column",))
    assert labels == [kind] + [f"{kind}_{index}" for index in range(1, count + 1)]


@given(st.lists(st.tuples(identifier, identifier), min_size=1, max_size=8, unique=True))
def test_an_explicit_name_is_always_the_label(pairs):
    checks = [{"kind": kind, "column": "c", "name": name} for name, kind in pairs]
    assert labels_for_checks(checks, ("name",), "kind", ("column",)) == [
        name for name, _ in pairs
    ]


GROUP_KINDS = (
    MetricKind.GROUP_COUNT,
    MetricKind.GROUP_MIN_DISTINCT,
    MetricKind.GROUP_MAX_DISTINCT,
)
RANGE_KINDS = (MetricKind.COLUMN_COUNT, MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX)


def build_metric(kind, column, groups):
    """Build a valid MetricKey, or ``None`` for a refused combination."""
    version = (
        "groups/v1"
        if kind in GROUP_KINDS
        else "ranges/v1" if kind in RANGE_KINDS else "counts/v1"
    )
    needs_column = kind not in (MetricKind.ROW_COUNT, MetricKind.GROUP_COUNT)
    if needs_column != (column is not None):
        return None
    if bool(groups) != (kind in GROUP_KINDS):
        return None
    try:
        return MetricKey(
            DatasetRef("ds", "d" * 64),
            kind,
            ColumnRef(column) if column else None,
            version,
            tuple(ColumnRef(group) for group in groups),
        )
    except Exception:  # noqa: BLE001 - the kernel refuses invalid combinations
        return None


@st.composite
def metrics(draw):
    return (
        draw(st.sampled_from(list(MetricKind))),
        draw(identifier | st.none()),
        draw(st.lists(identifier, max_size=3, unique=True)),
    )


@given(metrics(), metrics())
def test_distinct_metrics_never_share_a_label(first, second):
    a = build_metric(*first)
    b = build_metric(*second)
    if a is None or b is None or a == b:
        return
    assert metric_label(a) != metric_label(b), (metric_label(a), metric_label(b))


@given(metrics())
def test_a_metric_label_names_its_kind_and_has_no_empty_segment(spec):
    metric = build_metric(*spec)
    if metric is None:
        return
    label = metric_label(metric)
    assert label.startswith(metric.kind.value)
    assert "::" not in label and not label.endswith(":") and not label.startswith(":")
    assert label == metric_label(
        MetricKey(
            metric.dataset,
            metric.kind,
            metric.column,
            metric.semantic_version,
            metric.group_by,
        )
    )


# ------------------------------------------------------ configuration reading


@given(identifier, st.text(max_size=20), st.integers(min_value=0, max_value=5))
def test_a_dotted_key_reads_the_same_from_a_tree_and_a_nested_mapping(
    section, value, depth
):
    nested = value
    for _ in range(depth):
        nested = {"inner": nested}
    document = {"top": {section: nested}}
    tree = ConfigFactory.from_dict(document)
    from dq.utils.config_utils import config_get

    key = "top." + section + "".join(".inner" for _ in range(depth))
    assert config_get(tree, key, "missing") == config_get(document, key, "missing")
