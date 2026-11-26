# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contracts of the Arrow adapter beyond the corpus (SPEC-015).

The corpus says the adapter computes the documented values for the column
types a corpus case has. These say the rest: how it refuses, what it
declares, and what it computes for a column no corpus case has -- every
integer width, half- and single-precision floats, the narrow decimals, view
and dictionary encodings, and the nested types Arrow cannot count at all.
Each type is measured through real plans, so a row asserts what the adapter
computes and not what a helper returns.
"""

import decimal
import sys
from datetime import date, datetime
from fractions import Fraction

import pytest

from dq import arrow_adapter
from dq.arrow_adapter import execute_plan, validated_bindings
from dq.compute_support import ADAPTER_MODULES, adapter_named
from dq.envelope import RulesetEnvelope
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    Severity,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.tests.helpers import legacy_by_rule

pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")

D = decimal.Decimal
DATASET = DatasetRef("orders", "a" * 64)
OTHER = DatasetRef("returns", "b" * 64)
GE, LE, EQ = Comparison.GE, Comparison.LE, Comparison.EQ
MIN, MAX, GROUPS = (
    MetricKind.GROUP_MIN_DISTINCT,
    MetricKind.GROUP_MAX_DISTINCT,
    MetricKind.GROUP_COUNT,
)
COUNT, LOW, HIGH = (
    MetricKind.COLUMN_COUNT,
    MetricKind.COLUMN_MIN,
    MetricKind.COLUMN_MAX,
)
PRESENT = MetricKind.PRESENT_COUNT


def table(columns):
    import pyarrow as pa

    return pa.table(columns)


def frame():
    return table({"id": ["a", None, "c", "a"], "n": [1, None, 3, 3]})


# ------------------------------------------------------------- plans


def _rule(identifier, kind, **options):
    options.setdefault("operator", GE)
    options.setdefault("threshold", "0")
    return RuleSpec(
        identifier,
        DATASET,
        kind,
        Predicate(options.pop("operator"), D(options.pop("threshold"))),
        ColumnRef(options["column"]) if options.get("column") else None,
        Severity.ERROR,
        tuple(ColumnRef(name) for name in options.pop("group_by", ())),
        options.pop("target", None),
    )


def counts_plan(column="id"):
    return ExecutionPlan(
        (
            _rule("size", RuleKind.SIZE),
            _rule("complete", RuleKind.COMPLETENESS, column=column),
        )
    )


def groups_plan(column="id", key="k"):
    return ExecutionPlan(
        (
            _rule(
                "narrowest",
                RuleKind.GROUPED_DISTINCT,
                target=MIN,
                column=column,
                group_by=(key,),
            ),
            _rule(
                "widest",
                RuleKind.GROUPED_DISTINCT,
                target=MAX,
                column=column,
                group_by=(key,),
            ),
        )
    )


def ranges_plan(column="id"):
    return ExecutionPlan(
        (
            _rule(
                "lowest",
                RuleKind.VALUE_RANGE,
                target=LOW,
                column=column,
                operator=LE,
                threshold="1e30",
            ),
            _rule(
                "highest",
                RuleKind.VALUE_RANGE,
                target=HIGH,
                column=column,
                operator=LE,
                threshold="1e30",
            ),
        )
    )


def measured(plan, bindings):
    """``{metric kind: value}``: what the adapter computed for a one-column plan."""
    values = arrow_adapter.compute_plan_values(plan, validated_bindings(plan, bindings))
    return {metric.kind: value for metric, value in values.items()}


# ------------------------------------------------------------ bindings


def test_bindings_must_cover_exactly_the_plan_datasets():
    plan = counts_plan()
    with pytest.raises(ConfigurationError, match="missing"):
        execute_plan(plan, {})
    with pytest.raises(ConfigurationError, match="unknown"):
        execute_plan(plan, {"orders": frame(), "other": frame()})
    with pytest.raises(ConfigurationError, match="mapping"):
        execute_plan(plan, [("orders", frame())])


@pytest.mark.parametrize("bound", [object(), None, 7, "orders.parquet", [1, 2]])
def test_a_binding_arrow_cannot_read_is_refused_by_its_type(bound):
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(counts_plan(), {"orders": bound})
    assert str(raised.value) == (
        f"dataset binding 'orders' must be an Arrow table or export one; "
        f"got {type(bound).__name__}"
    )


def test_an_unreadable_binding_is_refused_whichever_dataset_it_belongs_to():
    plan = ExecutionPlan(
        (
            RuleSpec("a", DATASET, RuleKind.SIZE, Predicate(GE, D(0))),
            RuleSpec("b", OTHER, RuleKind.SIZE, Predicate(GE, D(0))),
        )
    )
    with pytest.raises(ConfigurationError, match="dataset binding 'returns'"):
        execute_plan(plan, {"orders": frame(), "returns": object()})


# ------------------------------------------------------------- columns


@pytest.mark.parametrize("column", ["Id", "ID"])
def test_rejects_columns_without_exact_case_sensitive_match(column):
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(counts_plan(column), {"orders": frame()})


def test_missing_column_error_lists_available_columns():
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(counts_plan("email"), {"orders": frame()})
    assert str(raised.value) == (
        "column 'email' in dataset 'orders' requires an exact case-sensitive "
        "match; available columns: id, n"
    )


@pytest.mark.parametrize("column", ["value", "VALUE"])
def test_rejects_case_insensitive_ambiguous_columns(column):
    with pytest.raises(ConfigurationError, match="ambiguous"):
        execute_plan(
            counts_plan(column), {"orders": table({"value": [1], "VALUE": [2]})}
        )


def test_rejects_a_duplicated_column_name():
    """Arrow can build a table with one name twice; neither column is meant."""
    import pyarrow as pa

    twice = pa.table([pa.array([1]), pa.array([2])], names=["id", "id"])
    with pytest.raises(ConfigurationError, match="ambiguous") as raised:
        execute_plan(counts_plan(), {"orders": twice})
    assert "column 'id' in dataset 'orders'" in str(raised.value)


def test_a_grouping_key_is_resolved_as_exactly_as_a_measured_column():
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(
            groups_plan("id", key="k"), {"orders": table({"id": ["a"], "K": ["g"]})}
        )
    twins = table({"id": ["a"], "k": ["g"], "K": ["g"]})
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(groups_plan("id", key="k"), {"orders": twins})
    assert str(raised.value).startswith("column 'k' in dataset 'orders' is ambiguous")


# -------------------------------------------------------- capabilities


@pytest.mark.parametrize(
    "declared, semantic, kinds",
    [
        (arrow_adapter.CAPABILITIES, SEMANTICS_VERSION, {"row_count", "present_count"}),
        (
            arrow_adapter.GROUP_CAPABILITIES,
            SEMANTIC_GROUPS_VERSION,
            {"group_count", "group_min_distinct", "group_max_distinct"},
        ),
        (
            arrow_adapter.RANGES_CAPABILITIES,
            SEMANTIC_RANGES_VERSION,
            {"column_count", "column_min", "column_max"},
        ),
    ],
)
def test_capabilities_declare_every_metric_of_each_semantic_version(
    declared, semantic, kinds
):
    assert declared.adapter == arrow_adapter.ADAPTER_NAME == "arrow"
    assert declared.version == arrow_adapter.ADAPTER_VERSION
    assert declared.semantic_version == semantic
    assert {kind.value for kind in declared.metrics} == kinds


def test_execute_plan_refuses_a_plan_the_declared_capabilities_cannot_hold():
    """Declared absent means refused: a plan is never approximated."""
    original = arrow_adapter.CAPABILITIES
    arrow_adapter.CAPABILITIES = CapabilitySet(
        "arrow", "1", frozenset({MetricKind.ROW_COUNT})
    )
    try:
        with pytest.raises(ConfigurationError, match="adapter capabilities cannot"):
            execute_plan(counts_plan(), {"orders": frame()})
    finally:
        arrow_adapter.CAPABILITIES = original


# ------------------------------------------------------------ outcomes


def test_multiple_datasets_execute_against_their_own_bindings():
    plan = ExecutionPlan(
        (
            RuleSpec("orders_sized", DATASET, RuleKind.SIZE, Predicate(GE, D(1))),
            RuleSpec(
                "orders_ids",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(EQ, D("0.75")),
                ColumnRef("id"),
            ),
            RuleSpec("returns_sized", OTHER, RuleKind.SIZE, Predicate(GE, D(1))),
            RuleSpec(
                "returns_ids",
                OTHER,
                RuleKind.COMPLETENESS,
                Predicate(EQ, D(1)),
                ColumnRef("id"),
            ),
        )
    )
    by_rule = legacy_by_rule(
        execute_plan(
            plan, {"orders": frame(), "returns": table({"id": ["x", "y"], "n": [1, 2]})}
        )
    )
    assert by_rule["orders_ids"]["success"] is True
    assert by_rule["returns_ids"]["success"] is True
    assert by_rule["orders_ids"]["details"]["observed"] == {
        "numerator": 3,
        "denominator": 4,
    }


def test_a_missing_column_of_the_second_dataset_names_that_dataset():
    plan = ExecutionPlan(
        (
            RuleSpec(
                "orders_ids",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(EQ, D(1)),
                ColumnRef("id"),
            ),
            RuleSpec(
                "returns_labels",
                OTHER,
                RuleKind.COMPLETENESS,
                Predicate(EQ, D(1)),
                ColumnRef("label"),
            ),
        )
    )
    with pytest.raises(ConfigurationError, match="column 'label' in dataset 'returns'"):
        execute_plan(plan, {"orders": frame(), "returns": table({"id": ["x"]})})


def test_outcomes_are_bounded_immutable_snapshots_naming_the_adapter():
    outcome = execute_plan(counts_plan(), {"orders": frame()})[0]
    assert isinstance(outcome.success, bool)
    assert outcome.serialized_size < 1_048_576
    legacy = outcome.to_legacy()
    assert CheckOutcome.from_legacy(legacy).to_legacy() == legacy
    assert legacy["details"]["adapter"] == {"name": "arrow", "version": "1"}


def test_execute_envelope_runs_every_semantics_of_a_ruleset_over_one_table():
    grouped = ExecutionPlan(
        (
            _rule(
                "regions",
                RuleKind.GROUPED_DISTINCT,
                target=MIN,
                column="id",
                group_by=("k",),
            ),
        )
    )
    envelope = RulesetEnvelope((counts_plan(), grouped, ranges_plan("n")))
    bindings = {
        "orders": table(
            {"id": ["a", "b", "a", None], "n": [1, 2, 3, 4], "k": ["g", "g", "g", "g"]}
        )
    }
    by_rule = legacy_by_rule(arrow_adapter.execute_envelope(envelope, bindings))
    assert set(by_rule) == {"size", "complete", "regions", "lowest", "highest"}
    assert by_rule["regions"]["details"]["observed"] == {
        "numerator": 2,
        "denominator": 1,
    }
    assert by_rule["highest"]["success"] is True  # 4 is at most 1e30


def test_execute_envelope_refuses_a_bare_plan():
    with pytest.raises(
        ConfigurationError, match="execute_envelope requires a RulesetEnvelope"
    ):
        arrow_adapter.execute_envelope(counts_plan(), {"orders": frame()})


# ------------------------------------------------- the form it computes on


def _type_of(pa, label):
    """The Arrow type a matrix label names, or ``None`` when this pyarrow has none."""
    simple = {
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint64": pa.uint64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "bool": pa.bool_(),
        "string": pa.string(),
        "large_string": pa.large_string(),
        "binary": pa.binary(),
        "date32": pa.date32(),
        "timestamp": pa.timestamp("s"),
        "null": pa.null(),
        "list": pa.list_(pa.int64()),
        "struct": pa.struct([pa.field("a", pa.int64())]),
        "decimal128": pa.decimal128(10, 2),
        "decimal256": pa.decimal256(38, 2),
    }
    if label == "float16":
        return getattr(pa, "float16", lambda: None)()
    if label in ("decimal32", "decimal64"):
        maker = getattr(pa, label, None)
        return (
            None
            if maker is None
            else maker(9, 2) if label == "decimal32" else maker(18, 2)
        )
    return simple[label]


def _cells(label):
    """Four cells of a column, with a null and a repeat in them."""
    numbers, zeros, text = (
        [5, -3, None, 9],
        [0.0, -0.0, None, 1.0],
        ["b", "a", None, "a"],
    )
    cells = {
        "int8": numbers,
        "int16": numbers,
        "int32": numbers,
        "int64": numbers,
        "uint64": [5, 3, None, 2**64 - 1],
        "float16": zeros,
        "float32": zeros,
        "float64": zeros,
        "bool": [True, None, False, True],
        "string": text,
        "large_string": text,
        "dictionary": text,
        "string_view": text,
        "binary": [b"b", b"a", None, b"a"],
        "date32": [date(2026, 1, 2), None, date(2026, 1, 1), date(2026, 1, 1)],
        "timestamp": [
            datetime(2026, 1, 2),
            None,
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
        ],
        "null": [None, None, None, None],
        "decimal32": [D("1.25"), None, D("9.50"), D("1.25")],
        "decimal64": [D("1.25"), None, D("9.50"), D("1.25")],
        "decimal128": [D("1.25"), None, D("9.50"), D("1.25")],
        "decimal256": [D("1.25"), None, D("9.50"), D("1.25")],
        "list": [[1, 2], [1], None, [1]],
        "struct": [{"a": 1}, {"a": 2}, None, {"a": 1}],
    }
    return cells[label]


def _half_floats(pa, cells):
    """A half-float column. pyarrow 16 has the type but builds one only from
    numpy's own instances, so that is the second way this is tried -- with a
    null kept a null, which numpy would otherwise make a NaN of."""
    cells = list(cells)
    try:
        return pa.array(cells, type=pa.float16())
    except pa.ArrowException:
        import numpy as np

        return pa.array(
            np.array(
                [0.0 if cell is None else cell for cell in cells], dtype=np.float16
            ),
            type=pa.float16(),
            mask=np.array([cell is None for cell in cells]),
        )


def _column_of(pa, label):
    """The column as a file or another library hands it over."""
    if label == "float16":
        return _half_floats(pa, _cells(label))
    if label == "dictionary":
        return pa.array(_cells(label)).dictionary_encode()
    if label == "string_view":
        return pa.array(_cells(label), pa.string()).cast(pa.string_view())
    return pa.array(_cells(label), type=_type_of(pa, label))


_INTEGERS = {
    "present": 3,
    "distinct": (3, 3),
    "count": 3,
    "low": Fraction(-3),
    "high": Fraction(9),
}
_UNCOUNTABLE = {
    # Three cells hold a value; a nested column has no NaN to be absent.
    "present": 3,
    "groups": "has no group_min_distinct in Arrow",
    "ranges": "has no column_min in Arrow",
}

#: label -> what the adapter must find: how many values are present, the
#: smallest and largest distinct count over the groups, and the bounds as
#: exact fractions -- or, instead, the refusal a semantic version must give.
MATRIX = {
    "int8": _INTEGERS,
    "int16": _INTEGERS,
    "int32": _INTEGERS,
    "int64": _INTEGERS,
    "uint64": {
        "present": 3,
        "distinct": (3, 3),
        "count": 3,
        "low": Fraction(3),
        "high": Fraction(2**64 - 1),
    },
    "float16": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(0),
        "high": Fraction(1),
    },
    "float32": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(0),
        "high": Fraction(1),
    },
    "float64": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(0),
        "high": Fraction(1),
    },
    "bool": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(0),
        "high": Fraction(1),
    },
    "decimal32": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(5, 4),
        "high": Fraction(19, 2),
    },
    "decimal64": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(5, 4),
        "high": Fraction(19, 2),
    },
    "decimal128": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(5, 4),
        "high": Fraction(19, 2),
    },
    "decimal256": {
        "present": 3,
        "distinct": (2, 2),
        "count": 3,
        "low": Fraction(5, 4),
        "high": Fraction(19, 2),
    },
    "string": {"present": 3, "distinct": (2, 2), "ranges": "unsupported type: str"},
    "large_string": {
        "present": 3,
        "distinct": (2, 2),
        "ranges": "unsupported type: str",
    },
    "dictionary": {"present": 3, "distinct": (2, 2), "ranges": "unsupported type: str"},
    "string_view": {
        "present": 3,
        "distinct": (2, 2),
        "ranges": "unsupported type: str",
    },
    "binary": {"present": 3, "distinct": (2, 2), "ranges": "unsupported type: bytes"},
    "date32": {"present": 3, "distinct": (2, 2), "ranges": "unsupported type: date"},
    "timestamp": {
        "present": 3,
        "distinct": (2, 2),
        "ranges": "unsupported type: datetime",
    },
    "null": {
        "present": 0,
        "distinct": (0, 0),
        "count": 0,
        "low": Fraction(0),
        "high": Fraction(0),
    },
    "list": _UNCOUNTABLE,
    "struct": _UNCOUNTABLE,
}


def _bindings_over(column):
    """A table holding ``column`` as ``x``, every row in the one group ``g``."""
    import pyarrow as pa

    return {
        "orders": pa.table(
            {"x": column, "k": pa.array(["g"] * len(column), pa.string())}
        )
    }


def _refusal_over(column, plan):
    """The adapter's refusal for ``plan`` over ``column``, as a message."""
    with pytest.raises(ConfigurationError) as raised:
        measured(plan, _bindings_over(column))
    return str(raised.value)


@pytest.mark.parametrize("label", sorted(MATRIX))
def test_every_column_type_is_measured_exactly_or_refused(label):
    pa = pytest.importorskip("pyarrow")
    if label not in ("dictionary", "string_view") and _type_of(pa, label) is None:
        pytest.skip(f"this pyarrow has no {label}")
    try:
        column = _column_of(pa, label)
    except pa.ArrowException:
        pytest.skip(f"this pyarrow cannot hold a {label} column")
    want = MATRIX[label]
    bindings = _bindings_over(column)

    assert measured(counts_plan("x"), bindings)[PRESENT] == want["present"], label
    if "groups" in want:
        assert _refusal_over(column, groups_plan("x")).startswith(
            f"column 'x' in dataset 'orders' {want['groups']}"
        ), label
    else:
        groups = measured(groups_plan("x"), bindings)
        assert (groups[MIN], groups[MAX]) == want["distinct"], label
        assert groups[GROUPS] == 1, "every row is in the one group"
    if "ranges" in want:
        assert want["ranges"] in _refusal_over(column, ranges_plan("x")), label
    else:
        ranges = measured(ranges_plan("x"), bindings)
        assert (ranges[COUNT], ranges[LOW], ranges[HIGH]) == (
            want["count"],
            want["low"],
            want["high"],
        ), label


def _ranges_over(column):
    return measured(ranges_plan("x"), _bindings_over(column))


def test_a_half_float_is_measured_as_the_double_it_exactly_is():
    pa = pytest.importorskip("pyarrow")
    if _type_of(pa, "float16") is None:
        pytest.skip("this pyarrow has no half-float")
    pc = pytest.importorskip("pyarrow.compute")
    column = _half_floats(pa, [0.1, None, 0.3])
    # What the half-float 0.1 is as a double, read from Arrow itself.
    widened = str(pc.cast(column[0], pa.float64()).as_py())
    assert _ranges_over(column)[LOW] == Fraction(D(widened))


def test_a_single_precision_bound_is_the_shortest_decimal_of_the_widened_value():
    """0.1 as a float32 is 0.10000000149011612 as a double, and that is the
    number Spark reports for a float column as well."""
    pa = pytest.importorskip("pyarrow")
    ranges = _ranges_over(pa.array([0.1, None, 0.3], pa.float32()))
    assert ranges[LOW] == Fraction(D("0.10000000149011612"))
    assert ranges[HIGH] == Fraction(D("0.30000001192092896"))


def test_a_narrow_decimal_keeps_every_digit_when_it_is_widened():
    pa = pytest.importorskip("pyarrow")
    for name, precision, scale, low in (
        ("decimal32", 9, 2, D("0.25")),
        ("decimal64", 18, 6, D("0.000001")),
    ):
        maker = getattr(pa, name, None)
        if maker is None:
            continue
        column = pa.array([low, None, D("9.50")], type=maker(precision, scale))
        assert _ranges_over(column)[LOW] == Fraction(low), name


def test_an_infinite_bound_is_refused_and_a_nan_is_not_a_bound():
    pa = pytest.importorskip("pyarrow")
    infinite = pa.array([1.0, None, float("inf")], pa.float64())
    assert "infinite bound" in _refusal_over(infinite, ranges_plan("x"))
    column = pa.array([float("nan"), None, -2.5, 1.5], pa.float64())
    assert _ranges_over(column) == {
        COUNT: 2,
        LOW: Fraction(D("-2.5")),
        HIGH: Fraction(D("1.5")),
    }


def test_a_float_key_groups_its_signed_zeros_and_its_nans():
    pa = pytest.importorskip("pyarrow")
    bindings = {
        "orders": pa.table(
            {
                "x": pa.array(["a", "a", "b", "b", "b"], pa.string()),
                "k": pa.array(
                    [0.0, -0.0, float("nan"), float("nan"), 1.0], pa.float64()
                ),
            }
        )
    }
    # Three groups -- the two zeros, the two NaN, and 1.0 -- of one value each.
    assert measured(groups_plan("x", key="k"), bindings) == {MIN: 1, MAX: 1, GROUPS: 3}


def test_a_column_that_is_both_key_and_measured_counts_one_per_group():
    pa = pytest.importorskip("pyarrow")
    bindings = {"orders": pa.table({"id": pa.array(["a", "a", "b", None])})}
    # The null key's group holds no value, so its distinct count is zero.
    assert measured(groups_plan("id", key="id"), bindings) == {
        MIN: 0,
        MAX: 1,
        GROUPS: 3,  # a, b and the null key
    }


def test_a_refusal_names_the_column_it_is_about():
    """A list is neither a value to count nor a key to group by. Arrow reports
    a measured list as a key it cannot group on, so the adapter keeps the two
    steps apart for the refusal to name the right column."""
    pa = pytest.importorskip("pyarrow")
    nested = pa.array([[1], [2], None])
    plain = pa.array(["g", "g", "g"], pa.string())
    counted = pa.array([1, 2, None])
    assert (
        measured(groups_plan("n"), {"orders": pa.table({"n": counted, "k": plain})})[
            MAX
        ]
        == 2
    )
    assert _refusal_over(nested, groups_plan("x")).startswith(
        "column 'x' in dataset 'orders' has no group_min_distinct in Arrow: "
    )
    assert _refusal_over(nested, ranges_plan("x")).startswith(
        "column 'x' in dataset 'orders' has no column_min in Arrow: "
    )
    with pytest.raises(ConfigurationError) as raised:
        measured(
            groups_plan("n", key="x"), {"orders": pa.table({"n": counted, "x": nested})}
        )
    assert str(raised.value).startswith(
        "column 'x' in dataset 'orders' has no group_count in Arrow: Keys of type list"
    )


def test_a_refusal_does_not_mask_a_fault_that_is_not_arrows():
    def broken(column):
        raise ValueError("a bug in the adapter, not in Arrow")

    original = arrow_adapter.canonical
    arrow_adapter.canonical = broken
    try:
        with pytest.raises(ValueError, match="a bug in the adapter"):
            execute_plan(counts_plan(), {"orders": frame()})
    finally:
        arrow_adapter.canonical = original


# --------------------------------------------------------- the mapping


def test_an_adapter_is_found_by_its_name():
    assert ADAPTER_MODULES == {"spark": "dq.spark_adapter", "arrow": "dq.arrow_adapter"}
    assert adapter_named("arrow") is arrow_adapter
    assert adapter_named("spark").__name__ == "dq.spark_adapter"
    for name in ADAPTER_MODULES:
        # What a rule's adapter is asked for, whoever runs the plan.
        assert callable(adapter_named(name).execute_plan)


@pytest.mark.parametrize(
    "name", ["Arrow", "ARROW", "arrow2", "", " arrow", None, 7, ["arrow"]]
)
def test_a_name_no_adapter_has_is_refused_before_anything_is_read(name):
    with pytest.raises(ConfigurationError) as raised:
        adapter_named(name)
    assert str(raised.value) == (
        f"adapter {name!r} is not a portable adapter; choose one of ['arrow', 'spark']"
    )


# ------------------------------------------------------ without pyarrow


def test_the_module_needs_no_pyarrow_to_import_and_names_the_extra_when_used(
    monkeypatch,
):
    bindings = {"orders": frame()}  # built while pyarrow can still be imported
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError) as raised:
        execute_plan(counts_plan(), bindings)
    message = str(raised.value)
    assert "The arrow adapter requires the optional pyarrow package" in message
    assert "poetry install -E arrow" in message
    assert "pyarrow>=16,<27" in message
    with pytest.raises(ImportError, match="arrow adapter"):
        arrow_adapter.table_of(object())


class Unreadable:
    """A binding that must never be touched."""

    def __getattr__(self, name):
        raise AssertionError(f"{name} was read")


def test_without_pyarrow_the_extra_is_named_before_a_binding_is_read(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match="arrow adapter"):
        validated_bindings(counts_plan(), {"orders": Unreadable()})
