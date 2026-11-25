# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared semantic fixtures: one corpus, every compute adapter (SPEC-011).

The portable kernel defines three semantic versions. Until now each
adapter's tests carried their own data and expectations, so a new adapter
had nothing to be measured against. This module loads one corpus and
checks any adapter against it.

A case is a two-layer specification. Its ``metrics`` state what an adapter
must compute (row and present counts, grouped distinct bounds, value-range
bounds); its ``expected`` state the decision each rule must produce. The
decisions follow from the metrics through the kernel's own evaluation, so
:func:`documented_decisions` can check that arithmetic with no engine at
all -- which keeps a wrong expectation from hiding behind an adapter.

This module imports the kernel and nothing else: no engine, no vendor
library.
"""

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from dq.exceptions import ConfigurationError
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKey,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
    Severity,
)

FIXTURE_PATH = Path(__file__).with_name("fixtures") / "semantic-cases.json"

#: The dataset name every corpus plan binds to.
DATASET_NAME = "ds"

SEMANTICS = (SEMANTICS_VERSION, SEMANTIC_GROUPS_VERSION, SEMANTIC_RANGES_VERSION)

#: Floating-point values that JSON cannot express, named by these strings.
DOUBLE_MARKERS = {
    "NaN": float("nan"),
    "Infinity": float("inf"),
    "-Infinity": -float("inf"),
}

_DECIMAL_TYPE = re.compile(r"^decimal\((\d{1,2}),(\d{1,2})\)$")
_SIMPLE_TYPES = ("string", "long", "double", "boolean")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
#: Case ids are lowercase words joined by hyphens (they become test ids).
_CASE_ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_RULE_KINDS = {
    "size": RuleKind.SIZE,
    "completeness": RuleKind.COMPLETENESS,
    "grouped_distinct": RuleKind.GROUPED_DISTINCT,
    "value_range": RuleKind.VALUE_RANGE,
}
_OPERATORS = {tag.value: tag for tag in Comparison}
_GROUP_TARGETS = ("group_min_distinct", "group_max_distinct")
_RANGE_TARGETS = ("column_min", "column_max")
_KIND_SEMANTICS = {
    "size": SEMANTICS_VERSION,
    "completeness": SEMANTICS_VERSION,
    "grouped_distinct": SEMANTIC_GROUPS_VERSION,
    "value_range": SEMANTIC_RANGES_VERSION,
}


@dataclass(frozen=True)
class SemanticCase:
    """One corpus case: data, rules, the metrics required, and the decisions."""

    id: str
    semantics: str
    why: str
    columns: Tuple[Tuple[str, str], ...]
    rows: Tuple[Tuple[Any, ...], ...]
    rules: Tuple[Mapping[str, Any], ...]
    metrics: Mapping[str, Any]
    expected: Mapping[str, bool]
    expect_error: Optional[str] = None
    deequ_diverges: Tuple[str, ...] = field(default=())

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def carries_nan(self, column: str) -> bool:
        """Whether a floating-point column holds a NaN value in this case.

        Only a ``double`` column can: the string ``"NaN"`` in a text column
        is an ordinary present value, not a NaN.
        """
        index = self.column_names.index(column)
        if self.columns[index][1] != "double":
            return False
        return any(row[index] == "NaN" for row in self.rows)

    def digest(self) -> str:
        """A SHA-256 over the case's schema and rows: a fixture's identity."""
        canonical = json.dumps(
            {"columns": [list(column) for column in self.columns], "rows": self.rows},
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ loading


def load_document(path=FIXTURE_PATH) -> dict:
    """Return the parsed corpus document."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _case_from(document: Mapping[str, Any]) -> SemanticCase:
    return SemanticCase(
        id=document["id"],
        semantics=document["semantics"],
        why=document.get("why", ""),
        columns=tuple((str(name), str(kind)) for name, kind in document["columns"]),
        rows=tuple(tuple(row) for row in document["rows"]),
        rules=tuple(dict(rule) for rule in document["rules"]),
        metrics=dict(document.get("metrics", {})),
        expected=dict(document.get("expected", {})),
        expect_error=document.get("expect_error", None),
        deequ_diverges=tuple(sorted((document.get("deequ_diverges") or {}).keys())),
    )


def load_cases(
    semantics: Optional[str] = None, path=FIXTURE_PATH
) -> Tuple[SemanticCase, ...]:
    """Return every corpus case, optionally of one semantic version.

    Raises:
        ValueError: When the corpus itself is malformed.
    """
    document = load_document(path)
    problems = validate_corpus(document)
    if problems:
        raise ValueError("the semantic corpus is malformed: " + "; ".join(problems))
    cases = tuple(_case_from(entry) for entry in document["cases"])
    if semantics is not None:
        cases = tuple(case for case in cases if case.semantics == semantics)
    return cases


# -------------------------------------------------------------- value decoding


def column_type_problem(name: str, kind: str) -> Optional[str]:
    """Return what is wrong with a column type, or ``None`` when it is one
    of ``string``, ``long``, ``double``, ``boolean`` or ``decimal(p,s)``."""
    if kind in _SIMPLE_TYPES:
        return None
    match = _DECIMAL_TYPE.match(kind) if isinstance(kind, str) else None
    if (
        match
        and 1 <= int(match.group(1)) <= 38
        and int(match.group(2)) <= int(match.group(1))
    ):
        return None
    return f"column {name!r} has unsupported type {kind!r}"


def decode_cell(value: Any, kind: str) -> Any:
    """Return a corpus cell as the Python value an adapter should see.

    Decimals become :class:`~decimal.Decimal`, the named floating-point
    markers become the floats they name, and everything else passes through.
    """
    if value is None:
        return None
    if kind.startswith("decimal("):
        return Decimal(value)
    if kind == "double" and isinstance(value, str):
        return DOUBLE_MARKERS[value]
    return value


def python_rows(case: SemanticCase) -> Tuple[Tuple[Any, ...], ...]:
    """Return the case's rows with every cell decoded."""
    return tuple(
        tuple(decode_cell(cell, kind) for cell, (_, kind) in zip(row, case.columns))
        for row in case.rows
    )


# ------------------------------------------------------------------- plans


def rule_spec(rule: Mapping[str, Any], dataset: DatasetRef) -> RuleSpec:
    """Build one :class:`RuleSpec` from a corpus rule."""
    return RuleSpec(
        rule["rule_id"],
        dataset,
        _RULE_KINDS[rule["kind"]],
        Predicate(_OPERATORS[rule["operator"]], Decimal(str(rule["threshold"]))),
        ColumnRef(rule["column"]) if rule.get("column") else None,
        Severity.ERROR,
        tuple(ColumnRef(column) for column in rule.get("group_by", ())),
        MetricKind(rule["target"]) if rule.get("target") else None,
    )


def build_plan(case: SemanticCase, dataset_name: str = DATASET_NAME) -> ExecutionPlan:
    """Return the canonical plan a corpus case describes."""
    dataset = DatasetRef(dataset_name, case.digest())
    return ExecutionPlan(tuple(rule_spec(rule, dataset) for rule in case.rules))


# ----------------------------------------------------------------- metrics


def metric_label(metric: MetricKey) -> str:
    """Return the stable text label of a metric key.

    ``row_count``, ``present_count:id``, ``group_count:region+day``,
    ``group_min_distinct:id:region``, ``column_max:amount``. A metric
    without a column has no column segment.
    """
    kind = metric.kind.value
    if metric.kind is MetricKind.ROW_COUNT:
        return kind
    groups = "+".join(column.name for column in metric.group_by)
    column = metric.column.name if metric.column else None
    if groups:
        return f"{kind}:{column}:{groups}" if column else f"{kind}:{groups}"
    return f"{kind}:{column}"


def documented_metrics(plan: ExecutionPlan, case: SemanticCase) -> Dict[MetricKey, Any]:
    """Return the case's documented metric values keyed as the plan expects.

    Range bounds become exact :class:`~decimal.Fraction` values, as an
    adapter must supply them.
    """
    values = {}
    for metric in plan.metrics:
        text = case.metrics[metric_label(metric)]
        if metric.kind in (MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX):
            values[metric] = Fraction(Decimal(str(text)))
        else:
            values[metric] = int(text)
    return values


def kernel_capabilities(plan: ExecutionPlan) -> CapabilitySet:
    """A capability set covering exactly a plan's metrics.

    This is the kernel's own reference declaration -- every metric kind the
    plan requires, for its semantic version -- and needs no adapter.
    """
    return CapabilitySet(
        "corpus",
        "1",
        frozenset(metric.kind for metric in plan.metrics),
        plan.semantic_version,
    )


def documented_decisions(plan: ExecutionPlan, case: SemanticCase) -> Dict[str, bool]:
    """Decide a case's rules from its documented metrics, with no adapter.

    This is the kernel's own evaluation: it proves the case's decisions are
    the consequence of its metrics.
    """
    values = documented_metrics(plan, case)
    return decisions(plan.evaluate(values, kernel_capabilities(plan)))


def decisions(outcomes) -> Dict[str, bool]:
    """Reduce kernel outcomes to ``{rule_id: success}``."""
    legacy = [outcome.to_legacy() for outcome in outcomes]
    return {outcome["check"]: outcome["success"] for outcome in legacy}


# ----------------------------------------------------------------- checking


def check_case(
    case: SemanticCase,
    execute: Callable[[ExecutionPlan, Mapping[str, Any]], Any],
    frames: Mapping[str, Any],
) -> List[str]:
    """Run one case through ``execute`` and return every decision mismatch.

    Args:
        case: The corpus case.
        execute: ``execute(plan, frames) -> outcomes``, an adapter's entry
            point.
        frames: Dataset bindings for the plan.

    Returns:
        Empty when the adapter's decisions match the case, including when
        the case documents an error and the adapter raised a matching one.
    """
    plan = build_plan(case)
    try:
        outcomes = execute(plan, frames)
    except ConfigurationError as error:
        if case.expect_error and re.search(case.expect_error, str(error)):
            return []
        return [f"{case.id}: raised instead of deciding: {error}"]
    if case.expect_error:
        return [
            f"{case.id}: expected an error matching {case.expect_error!r}, got outcomes"
        ]
    got = decisions(outcomes)
    return [
        f"{case.id}.{rule_id}: expected {want}, got {got.get(rule_id, 'no outcome')}"
        for rule_id, want in case.expected.items()
        if got.get(rule_id) is not want
    ]


def check_metrics(
    case: SemanticCase,
    compute: Callable[[ExecutionPlan, Mapping[str, Any]], Dict[MetricKey, Any]],
    frames: Mapping[str, Any],
) -> List[str]:
    """Compare an adapter's metric values with the case's documented ones.

    Stronger than the decisions: a mismatch here names the metric that an
    adapter computes differently, not just the rule that flipped.
    """
    plan = build_plan(case)
    want = documented_metrics(plan, case)
    got = compute(plan, frames)
    problems = []
    for metric in plan.metrics:
        label = metric_label(metric)
        if metric not in got:
            problems.append(f"{case.id}: {label} was not computed")
        elif got[metric] != want[metric]:
            problems.append(
                f"{case.id}: {label} expected {want[metric]}, got {got[metric]}"
            )
    return problems


# ---------------------------------------------------------- corpus integrity


def _cell_problem(value: Any, kind: str) -> Optional[str]:
    """Return what is wrong with one cell, or ``None`` when it fits its type.

    A cell must be storable in its column as written, because an engine is
    not bound to refuse one that is not: Spark turns a decimal ``"NaN"``
    into null and rounds ``"1.234"`` stored as ``decimal(10,2)`` to 1.23.
    Either would run a case on data other than the one it documents.
    """
    if value is None:
        return None
    if kind == "string":
        return None if isinstance(value, str) else "expected a string"
    if kind == "long":
        if isinstance(value, bool) or not isinstance(value, int):
            return "expected an integer"
        return None if -(2**63) <= value < 2**63 else "expected a 64-bit integer"
    if kind == "double":
        marker = "expected a number or one of " + ", ".join(sorted(DOUBLE_MARKERS))
        if isinstance(value, bool):
            return marker
        if isinstance(value, (int, float)):
            return None
        # Only a string can name a marker; a list or object is simply wrong.
        return None if isinstance(value, str) and value in DOUBLE_MARKERS else marker
    if kind == "boolean":
        return None if isinstance(value, bool) else "expected a boolean"
    # decimal(p,s): a string only, as a JSON number is a binary float and
    # not the exact decimal the corpus means. It must be finite and hold no
    # more than s fractional and p-s integer digits.
    column = _DECIMAL_TYPE.fullmatch(kind)
    if column is None:
        # An unsupported column type; `column_type_problem` reports it.
        return None
    try:
        exact = Decimal(value) if isinstance(value, str) else None
    except InvalidOperation:
        exact = None
    if exact is None or not exact.is_finite():
        return "expected a decimal string"
    precision, scale = int(column[1]), int(column[2])
    exponent = exact.as_tuple().exponent
    if max(0, -exponent) > scale or max(0, exact.adjusted() + 1) > precision - scale:
        return f"expected a decimal that fits {kind}"
    return None


def _rule_problem(rule: Any, columns: set) -> List[str]:
    where = f"rule {rule.get('rule_id', '<no id>')!r}"
    if not isinstance(rule, dict) or not _IDENTIFIER.fullmatch(
        str(rule.get("rule_id", ""))
    ):
        return [f"{where}: a rule needs a valid rule_id"]
    problems = []
    kind = rule.get("kind")
    if kind not in _RULE_KINDS:
        problems.append(f"{where}: unknown kind {kind!r}")
    if rule.get("operator") not in _OPERATORS:
        problems.append(f"{where}: operator must be one of {sorted(_OPERATORS)}")
    try:
        Decimal(str(rule.get("threshold")))
    except (InvalidOperation, TypeError, ValueError):
        problems.append(f"{where}: threshold must be a decimal string")
    column = rule.get("column")
    if kind in ("completeness", "grouped_distinct", "value_range"):
        if column not in columns:
            problems.append(f"{where}: kind {kind!r} needs a column that exists")
    elif column is not None:
        problems.append(f"{where}: kind {kind!r} takes no column")
    group_by = rule.get("group_by", [])
    if kind == "grouped_distinct":
        if (
            not group_by
            or not set(group_by) <= columns
            or len(set(group_by)) != len(group_by)
        ):
            problems.append(
                f"{where}: grouped_distinct needs distinct grouping columns that exist"
            )
        if rule.get("target") not in _GROUP_TARGETS:
            problems.append(f"{where}: target must be one of {_GROUP_TARGETS}")
    if kind == "value_range" and rule.get("target") not in _RANGE_TARGETS:
        problems.append(f"{where}: target must be one of {_RANGE_TARGETS}")
    return problems


def validate_corpus(document: Mapping[str, Any]) -> List[str]:
    """Return every way the corpus document is malformed (empty when sound).

    Checks that each case is well formed, that its expectations name every
    rule and nothing else, and that its metrics are exactly the ones its
    plan requires. A malformed document is reported, never raised from:
    the cross-checks that need a buildable plan run only for a case that
    has passed the structural checks.
    """
    problems: List[str] = []
    if not isinstance(document, Mapping) or document.get("version") != 1:
        problems.append("corpus version must be 1")
    if problems:
        return problems
    seen = set()
    for entry in document.get("cases", []):
        case_id = str(entry.get("id", "<no id>"))
        if case_id in seen:
            problems.append(f"{case_id}: duplicate case id")
        seen.add(case_id)
        if not _CASE_ID.fullmatch(case_id):
            case_problems = [
                f"{case_id}: a case id is lowercase words joined by hyphens"
            ]
        else:
            case_problems = _case_structure_problems(entry, case_id)
        problems.extend(case_problems)
        if case_problems or entry.get("expect_error"):
            continue
        problems.extend(_case_metric_problems(entry, case_id))
    return problems


def _case_structure_problems(entry: Mapping[str, Any], case_id: str) -> List[str]:
    """Return the structural problems of one case (rows, rules, decisions)."""
    problems: List[str] = []
    semantics = entry.get("semantics")
    if semantics not in SEMANTICS:
        return [f"{case_id}: semantics must be one of {SEMANTICS}"]
    columns = entry.get("columns", [])
    names = [column[0] for column in columns]
    if not columns or len(set(names)) != len(names):
        problems.append(f"{case_id}: columns must be non-empty with unique names")
    for name, kind in columns:
        problem = column_type_problem(name, kind)
        if problem:
            problems.append(f"{case_id}: {problem}")
    for index, row in enumerate(entry.get("rows", [])):
        if len(row) != len(columns):
            problems.append(
                f"{case_id}: row {index} has {len(row)} cells, expected {len(columns)}"
            )
            continue
        for (name, kind), cell in zip(columns, row):
            problem = _cell_problem(cell, kind)
            if problem:
                problems.append(f"{case_id}: row {index}, column {name!r}: {problem}")
    rules = entry.get("rules", [])
    if not rules:
        problems.append(f"{case_id}: a case needs at least one rule")
    for rule in rules:
        problems.extend(
            f"{case_id}: {problem}" for problem in _rule_problem(rule, set(names))
        )
        if semantics != _KIND_SEMANTICS.get(
            rule.get("kind") if isinstance(rule, dict) else None
        ):
            problems.append(
                f"{case_id}: rule kind {rule.get('kind')!r} does not belong to {semantics}"
            )
    expected = entry.get("expected", {})
    rule_ids = {rule.get("rule_id") for rule in rules if isinstance(rule, dict)}
    if entry.get("expect_error"):
        if expected:
            problems.append(
                f"{case_id}: a case that documents an error carries no decisions"
            )
    elif set(expected) != rule_ids or not all(
        type(value) is bool for value in expected.values()
    ):
        problems.append(
            f"{case_id}: expected must decide exactly every rule, as booleans"
        )
    for rule_id in entry.get("deequ_diverges") or {}:
        if rule_id not in rule_ids or semantics != SEMANTICS_VERSION:
            problems.append(
                f"{case_id}: deequ_diverges names {rule_id!r}, which is not a counts/v1 rule"
            )
    return problems


def _case_metric_problems(entry: Mapping[str, Any], case_id: str) -> List[str]:
    """Return how a case's metrics differ from what its plan requires."""
    try:
        required = {
            metric_label(metric) for metric in build_plan(_case_from(entry)).metrics
        }
    except (ConfigurationError, KeyError, TypeError, ValueError) as error:
        return [f"{case_id}: its plan cannot be built: {error}"]
    documented = set(entry.get("metrics", {}))
    if documented != required:
        return [
            f"{case_id}: metrics must be exactly {sorted(required)}, got {sorted(documented)}"
        ]
    return []
