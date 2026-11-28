# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""A rule asks for a column by a name that is a plain identifier (SPEC-025).

Measured on Spark 3.5: a not-null check on a column literally called ``s.t``,
beside a struct ``s`` with a field ``t``, was answered "passed" by DQX and
by the business-rules engine over data where that column held a null. Spark
had read the name as the struct's field. The kernel now refuses such a name
for every engine that hands one to Spark: before the engine is imported and
before any data is read. The loader and ``dq-validate`` share one function,
and each engine declares where its payload names a column.

The loader and validator tests need no engine installed: the refusal comes
before the engine is imported, which is the point, and a test fails if one
is imported to judge a name.
"""

import inspect
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dq.engine.checkspec import column_name_problems
from dq.engine.engine_loader import EngineLoader
from dq.engine.registry import BUILT_IN_ENGINES, _DQX_COLUMN_ARGUMENTS, find_spec
from dq.exceptions import ConfigurationError
from dq.utils.config_utils import config_tree_to_python, load_config
from dq.tests.columns_support import (
    NOT_A_NAME,
    engines_that_name_a_column,
    rule_for,
    trap_frame,
)
from dq.validate import main as validate_main
from dq.validate import validate_config_document

ROOT = Path(__file__).resolve().parents[2]
THE_ENGINES = engines_that_name_a_column()

#: How a refusal ends, in the kernel's one message for such a name.
WHY = "Spark reads any other string as an expression, not as a name"

#: Modules of the kernel itself that judge a rule before an engine is
#: imported: anything else under ``dq.engine`` is an engine, or a vendor's.
KERNEL = {
    "dq.engine.registry",
    "dq.engine.engine_loader",
    "dq.engine.dq_engine",
    "dq.engine.checkspec",
}


# ------------------------------------------------------ what the function reads


def a_spec(keys, payload_key="checks", payload_type="list"):
    return SimpleNamespace(
        column_keys=tuple(keys), payload_key=payload_key, payload_type=payload_type
    )


def test_a_name_is_read_where_the_engine_says_it_is():
    spec = a_spec(("column", "check.arguments.column", "columns", "groups.name"))
    definition = {
        "name": "r",
        "engine": "an_engine",
        "checks": [
            {"column": "id"},
            {"check": {"arguments": {"column": "s.t"}}},
            {"columns": ["a", "x-y", "b"]},
            {"groups": [{"name": "plain"}, {"name": "order id"}]},
            {"column": 7, "columns": None},
        ],
    }
    said = column_name_problems(spec, definition, "Rule 3: ")

    assert len(said) == 3
    assert said[0].startswith("Rule 3: check 2: check.arguments.column = 's.t': ")
    assert said[1].startswith("Rule 3: check 3: columns = 'x-y': ")
    assert said[2].startswith("Rule 3: check 4: groups.name = 'order id': ")
    assert all(problem.endswith(WHY) for problem in said)
    assert (
        "must hold no dot" in said[0] and "letters, digits, and underscores" in said[1]
    )


def test_a_payload_that_is_one_mapping_is_read_the_same_way():
    spec = a_spec(("not_null_columns", "overrides.column"), payload_key="schema")
    definition = {
        "schema": {"not_null_columns": ["id", "x-y"], "overrides": [{"column": "a`b"}]}
    }
    said = column_name_problems(spec, definition)
    assert len(said) == 2
    assert said[0].startswith("check 1: not_null_columns = 'x-y': ")
    assert said[1].startswith("check 1: overrides.column = 'a`b': ")


def test_what_is_no_name_is_left_to_the_engine():
    """A number where a name belongs, a missing list, an empty one: the
    engine refuses each for its own reason, in its own words."""
    spec = a_spec(("column", "columns", "check.arguments.columns"))
    definition = {
        "checks": [
            {"column": 7, "columns": None, "check": {"arguments": {}}},
            {"columns": []},
        ]
    }
    assert column_name_problems(spec, definition) == []


@pytest.mark.parametrize(
    ("spec", "definition"),
    [
        (a_spec(()), {"checks": [{"column": "s.t"}]}),
        (a_spec(("column",)), "not a mapping"),
        (a_spec(("column",)), {"name": "r"}),
        (a_spec(("column",), payload_type="string"), {"checks": "an expression"}),
    ],
    ids=["no-keys-declared", "no-rule", "no-payload", "text-the-author-wrote"],
)
def test_what_the_declaration_does_not_reach_is_not_judged(spec, definition):
    assert column_name_problems(spec, definition) == []


def test_every_declared_key_is_read():
    """A rule that names a bad column at every declared key is refused for
    each: a key nothing reads would be a key nothing guards."""
    for engine in THE_ENGINES:
        spec = find_spec(engine)
        bad = "s.t"
        entry = {
            "column": bad,
            "check": {"arguments": {}, "for_each_column": [bad]},
            "kwargs": {},
        }
        for key in spec.column_keys:
            at = entry
            *lead, last = key.split(".")
            for segment in lead:
                at = at.setdefault(segment, {})
            at[last] = [bad]
        payload = (
            [entry]
            if spec.payload_type == "list" and engine != "schemavalidation"
            else entry
        )
        definition = {"name": "r", "engine": engine, spec.payload_key: payload}
        said = column_name_problems(spec, definition)
        assert len(said) == len(spec.column_keys), (engine, said[:3])
        assert [_the_key(problem) for problem in said] == list(spec.column_keys)


def _the_key(problem: str) -> str:
    """Return which declared key a problem names."""
    return re.search(r"check \d+: (.+?) = 's\.t': ", problem).group(1)


# ------------------------------------------------------------------------ G1


class NoEngineImport:
    """An import finder that fails the test if an engine is imported under it."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith("dq.engine.") and fullname not in KERNEL:
            raise AssertionError(f"{fullname} was imported to judge a name")
        return None


def engine_modules():
    return {name for name in sys.modules if name.startswith("dq.engine.")}


@pytest.mark.parametrize("column", NOT_A_NAME)
@pytest.mark.parametrize("engine", THE_ENGINES)
def test_g1_a_name_spark_would_not_read_is_refused_before_the_engine_is_imported(
    monkeypatch, engine, column
):
    monkeypatch.setattr(sys, "meta_path", [NoEngineImport(), *sys.meta_path])
    before = engine_modules()

    with pytest.raises(ConfigurationError) as refused:
        EngineLoader().load_engine(engine, rule_for(engine, column), 1)

    said = str(refused.value)
    assert said.startswith(f"Engine '{engine}': check 1: ")
    assert f"= {column!r}: " in said and said.endswith(WHY)
    assert engine_modules() == before


@pytest.mark.parametrize("engine", THE_ENGINES)
def test_g1_a_plain_name_is_not_this_check_s_to_refuse(engine):
    """A plain name goes on to the engine: it loads, or refuses for a reason
    of its own -- a missing extra, a demand of its vendor."""
    outcome = None
    try:
        EngineLoader().load_engine(engine, rule_for(engine, "plain"), 1)
    except Exception as error:  # noqa: BLE001 - whatever the engine said is the finding
        outcome = error
    assert not (isinstance(outcome, ConfigurationError) and WHY in str(outcome))


@pytest.mark.parametrize("engine", THE_ENGINES)
def test_g1_dq_validate_reports_the_name_in_the_same_words(
    tmp_path, capsys, monkeypatch, engine
):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"dqframework": {"dqrules": [rule_for(engine, "x-y")]}}))

    checked = validate_config_document(str(path))

    assert checked.errors
    assert all(
        problem.startswith("Rule 0: check 1: ") and problem.endswith(WHY)
        for problem in checked.errors
    ), checked.errors
    assert all("'x-y'" in problem for problem in checked.errors)
    monkeypatch.setattr(sys, "argv", ["dq-validate", str(path)])
    assert validate_main() == 1
    assert "is invalid" in capsys.readouterr().out


# ------------------------------------------------------------------------ G2


def test_g2_the_roster_of_engines_that_name_a_column():
    assert THE_ENGINES == [
        "businessrules",
        "deequ",
        "dqx",
        "portable",
        "greatexpectations",
        "schemavalidation",
    ]
    # dqdl's payload is an expression its author writes; pandera's columns
    # are labels of a dataframe that never reach Spark.
    for engine in ("dqdl", "pandera"):
        assert not find_spec(engine).column_keys, engine
    assert find_spec("dqdl").payload_type == "string"
    assert find_spec("pandera").payload_type == "document"
    assert set(THE_ENGINES) | {"dqdl", "pandera"} == set(BUILT_IN_ENGINES)


def test_g2_dqx_declares_what_its_own_check_functions_take():
    pytest.importorskip("databricks.labs.dqx", reason="the dqx engine needs its extra")
    from databricks.labs.dqx import check_funcs
    from databricks.labs.dqx import rule as dqx_rule
    from databricks.labs.dqx.geo import check_funcs as geo_check_funcs

    check_funcs.is_not_null  # loads DQX's check modules, and so its registry
    naming = set()
    for name in dqx_rule.CHECK_FUNC_REGISTRY:
        # Resolved as DQX resolves one: its own checks first, its geo checks after.
        function = getattr(check_funcs, name, None) or getattr(geo_check_funcs, name)
        parameters = inspect.signature(function).parameters
        naming |= {parameter for parameter in parameters if "col" in parameter.lower()}

    declared = find_spec("dqx").column_keys
    arguments = {
        key.removeprefix("check.arguments.")
        for key in declared
        if key.startswith("check.arguments.")
    }
    # ``convert_column`` is a flag of DQX's geo checks that happens to hold
    # the word: when true, the column's values are turned into geometries.
    assert naming - {"convert_column"} == arguments == set(_DQX_COLUMN_ARGUMENTS)
    assert declared[0] == "check.for_each_column"


def test_g2_great_expectations_declares_what_its_own_expectations_take():
    gx = pytest.importorskip(
        "great_expectations", reason="the gx engine needs its extra"
    )
    naming = set()
    for name in dir(gx.expectations):
        klass = getattr(gx.expectations, name)
        if not (name.startswith("Expect") and inspect.isclass(klass)):
            continue
        said = set()
        for attribute in ("domain_keys", "success_keys", "metric_keys"):
            value = getattr(klass, attribute, None)
            if isinstance(value, (list, tuple, set, frozenset)):
                said |= {key for key in value if isinstance(key, str)}
        try:
            said |= set(inspect.signature(klass.__init__).parameters)
        except (TypeError, ValueError):
            pass
        naming |= {key for key in said if "col" in key.lower()}

    declared = {
        key.removeprefix("kwargs.")
        for key in find_spec("greatexpectations").column_keys
    }
    # ``column_index`` is a position in the frame, not a name.
    assert naming - {"column_index"} == declared
    assert {
        key
        for key in find_spec("greatexpectations").column_keys
        if key.startswith("kwargs.")
    } == {"kwargs." + key for key in declared}


def test_g2_deequ_declares_what_its_wrapper_reads():
    source = (ROOT / "dq/engine/deequ/deequ_check.py").read_text(encoding="utf-8")
    read = set(re.findall(r'check_config\.get\("(\w+)"', source))
    assert set(find_spec("deequ").column_keys) == {"column", "columns"}
    assert {"column", "columns"} <= read


def test_g2_businessrules_declares_what_its_engine_hands_its_strategies():
    source = (ROOT / "dq/engine/businessrules/businessrules_engine.py").read_text(
        encoding="utf-8"
    )
    read = set(re.findall(r'check_config\.get\("(\w+)"', source))
    declared = set(find_spec("businessrules").column_keys)
    assert declared == {"columns", "group_by", "sort_by"}
    assert declared <= read


def test_g2_portable_declares_what_its_translation_reads():
    source = (ROOT / "dq/portable_config.py").read_text(encoding="utf-8")
    read = set(re.findall(r'check\.get\("(column|columns|group_by)"', source))
    assert (
        set(find_spec("portable").column_keys)
        == read
        == {"column", "columns", "group_by"}
    )


def test_g2_schema_validation_declares_the_keys_it_reads():
    from dq.utils import keys

    declared = find_spec("schemavalidation").column_keys
    assert keys.SCHEMA_VALIDATION_NOT_NULL_COLUMNS_KEY in declared
    assert keys.SCHEMA_VALIDATION_UNQK_CONSTRAINTS in declared
    assert f"{keys.SCHEMA_VALIDATION_OVERRIDE_KEY}.column" in declared
    assert (
        f"{keys.SCHEMA_VALIDATION_FK_CONSTRAINTS}.{keys.SCHEMA_VALIDATION_SRC_COLUMN}"
        in declared
    )


def test_g4_every_checked_in_example_names_plain_columns():
    """A plain name changes nothing: the repository's own rule packs name
    no column Spark would not read as one."""
    checked = 0
    for path in sorted((ROOT / "examples").rglob("*")):
        # `examples/import/` holds result sources for dq-import, not rules.
        if (
            not path.is_file()
            or path.suffix not in (".conf", ".hocon", ".yaml", ".yml", ".json")
            or "import" in path.parts
        ):
            continue
        pack = config_tree_to_python(load_config(str(path)))
        rules = (pack.get("dqframework") or {}).get("dqrules") or []
        for rule in rules:
            spec = find_spec(str(rule.get("engine", "")))
            if spec is not None and spec.column_keys:
                assert column_name_problems(spec, rule) == [], (path.name, rule)
                checked += 1
    assert checked >= 10


# ------------------------------------------------------------------------ G3


@pytest.mark.spark
@pytest.mark.parametrize("engine", ["dqx", "businessrules"])
def test_g3_a_check_that_passed_over_a_null_is_refused(spark, engine):
    """The measurement, held. On this frame a not-null check on ``s.t``
    answered "passed" for each of these engines: Spark had read the name as
    the struct's field. The control shows the frame holds what it says, and
    that each engine sees a null when it is asked for the column plainly."""
    if engine == "dqx":
        pytest.importorskip(
            "databricks.labs.dqx", reason="the dqx engine needs its extra"
        )
    frame = trap_frame(spark)
    assert [row[0] for row in frame.select("`s.t`").collect()] == [None, 3]

    with pytest.raises(ConfigurationError) as refused:
        EngineLoader().load_engine(engine, rule_for(engine, "s.t"), 1)
    assert str(refused.value).endswith(WHY)

    engine_built = EngineLoader().load_engine(engine, rule_for(engine, "plain"), 1)
    (outcome,) = engine_built.apply(frame)
    assert outcome["success"] is False
    (outcome,) = (
        EngineLoader()
        .load_engine(engine, rule_for(engine, "plain"), 1)
        .apply(trap_frame(spark, with_nulls=False))
    )
    assert outcome["success"] is True
