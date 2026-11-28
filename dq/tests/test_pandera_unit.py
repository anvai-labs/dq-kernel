# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The pandera engine's contracts, with no Spark and no pandera.

pandera is stood in for by a backend that answers from a document: what its
schema is read back as, and what it fails. That reaches every refusal and
every shape a failure can take. The real package is exercised in
test_pandera.py.
"""

import copy
import json
import sys
from types import SimpleNamespace

import pytest
from pyhocon import ConfigFactory

from dq.engine.engine_loader import EngineLoader
from dq.engine.pandera import pandera_check, pandera_engine
from dq.engine.pandera.pandera_check import (
    COLUMN_NOT_IN_DATAFRAME,
    PanderaSchema,
    missing_keys,
)
from dq.engine.pandera.pandera_engine import PanderaEngine
from dq.engine.registry import find_spec, missing_vendor_imports
from dq.exceptions import ConfigurationError
from dq.tests.test_spark_free import EXTRAS, child_env, run_python
from dq.validate import validate_config, validate_config_document

#: What pandera keeps of a document: the answers the fake backend gives.
KEPT = {
    "schema_type": "dataframe",
    "api": "pandas",
    "name": "orders",
    "columns": {
        "id": {"dtype": "int64", "nullable": False, "unique": True},
        "amount": {
            "dtype": "float64",
            "greater_than_or_equal_to": {"min_value": 0, "name": "non-negative"},
            "less_than_or_equal_to": 1000,
        },
        "note": {"dtype": "str", "nullable": True, "required": False},
    },
}

#: Which of the kept document's column keys pandera reads as checks.
KINDS = {"amount": ("greater_than_or_equal_to", "less_than_or_equal_to")}

LABELS = [
    "dtype_id",
    "not_nullable_id",
    "unique_id",
    "dtype_amount",
    "not_nullable_amount",
    "non-negative",
    "less_than_or_equal_to_amount",
    "dtype_note",
]


class Rows(list):
    """What pandera hands back when it counted the failing rows."""

    columns = ["index", "failure_case"]


class FakeCheck:
    def __init__(self, kind, said):
        self.registry_name = kind
        self.raise_warning = isinstance(said, dict) and said.get("raise_warning", False)
        self.name = said.get("name", kind) if isinstance(said, dict) else kind
        self.statistics = (
            {
                key: value
                for key, value in said.items()
                if key not in ("name", "raise_warning")
            }
            if isinstance(said, dict)
            else said
        )


class FakeColumn:
    def __init__(self, said, kinds):
        self.dtype = said["dtype"]
        # pandera's defaults: a column admits no null and must be there,
        # unless the document says otherwise.
        self.nullable = said.get("nullable", False)
        self.unique = said.get("unique", False)
        self.required = said.get("required", True)
        self.checks = [FakeCheck(kind, said[kind]) for kind in kinds if kind in said]


class FakeSchemaErrors(Exception):
    def __init__(self, failures):
        super().__init__("the frame does not satisfy the schema")
        self.schema_errors = failures


class FakeSchema:
    """A schema as pandera read it: it writes back the document it keeps."""

    def __init__(self, document, kinds):
        self.document = document
        self.columns = {
            name: FakeColumn(said, kinds.get(name, ()))
            for name, said in document["columns"].items()
        }
        self.frames = []
        self.failures = []

    def to_json(self):
        return json.dumps(self.document)

    def validate(self, frame, lazy):
        assert (
            lazy is True
        ), "the engine validates lazily, or one failure hides the rest"
        self.frames.append(frame)
        if self.failures:
            raise FakeSchemaErrors(self.failures)
        return frame


def kept(**changes):
    """Return the document pandera keeps, with ``changes`` applied to its columns."""
    document = copy.deepcopy(KEPT)
    for name, said in changes.items():
        document["columns"][name].update(said)
    return document


def schema(document=KEPT, keeps=None, failures=(), kinds=None):
    """Return ``(the rule's schema, the fake pandera keeps behind it)``.

    pandera is read as keeping ``KEPT``, whatever document it is given,
    unless ``keeps`` says what it keeps of that one.
    """
    fake = FakeSchema(keeps or KEPT, kinds or KINDS)
    fake.failures = list(failures)
    backend = SimpleNamespace(
        DataFrameSchema=SimpleNamespace(from_json=lambda text: fake)
    )
    return PanderaSchema(document, lambda kind: backend), fake


def failure(column, check, reason="DATAFRAME_CHECK", cases=Rows([1])):
    """Return one failure as pandera reports it."""
    return SimpleNamespace(
        column_name=column,
        check=check,
        reason_code=SimpleNamespace(name=reason),
        failure_cases=cases,
    )


def check_named(name):
    """Return a check object a failure may carry: only its name identifies it."""
    return SimpleNamespace(name=name)


def by_label(results):
    return {
        result["check"]: (result["success"], result["details"]) for result in results
    }


# --------------------------------------------------------- the comparison


def test_nothing_is_missing_when_pandera_keeps_everything():
    assert missing_keys(KEPT, KEPT) == []
    assert (
        missing_keys({"a": {"b": [1, {"c": 2}]}}, {"a": {"b": [9, {"c": 2, "d": 3}]}})
        == []
    )


@pytest.mark.parametrize(
    ("given", "kept", "missing"),
    [
        ({"a": 1}, {}, ["$.a"]),
        ({"a": {"b": {"c": 1}}}, {"a": {"b": {}}}, ["$.a.b.c"]),
        ({"a": [{"b": 1}, {"b": 2}]}, {"a": [{"b": 1}, {}]}, ["$.a[1].b"]),
        # A check's argument: pandera writes the bare value, the document a key.
        ({"ge": {"value": 0}}, {"ge": 0}, []),
        ({"ge": {"min_value": 0}}, {"ge": 0}, []),
        (
            {"ge": {"value": 0, "raise_warning": True}},
            {"ge": 0},
            ["$.ge.raise_warning"],
        ),
        ({"ge": {"value": 0}}, {"ge": 1}, ["$.ge"]),
        # A list of a different length is left to pandera.
        ({"isin": ["a", "b"]}, {"isin": ["a"]}, []),
    ],
)
def test_what_pandera_does_not_keep_is_found(given, kept, missing):
    assert missing_keys(given, kept) == missing


# ------------------------------------------------------------ the reading


def test_a_schema_is_read_from_its_mapping_or_from_the_text_of_one():
    from_mapping, _ = schema()
    fake = FakeSchema(KEPT, KINDS)
    backend = SimpleNamespace(
        DataFrameSchema=SimpleNamespace(from_json=lambda text: fake)
    )
    from_text = PanderaSchema(json.dumps(KEPT), lambda kind: backend)
    assert [outcome.label for outcome in from_mapping._outcomes] == LABELS
    assert [outcome.label for outcome in from_text._outcomes] == LABELS


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (None, "requires a 'schema'"),
        ("", "requires a 'schema'"),
        ("   ", "requires a 'schema'"),
        ("[1, 2]", "must be a mapping"),
        ("not json", "is not a document"),
        ({"schema_type": "dataframe"}, "declares no column"),
        ({"schema_type": "dataframe", "columns": {}}, "declares no column"),
        ({"schema_type": "spider", "columns": KEPT["columns"]}, "schema_type 'spider'"),
    ],
    ids=[
        "absent",
        "empty",
        "blank",
        "a-list",
        "not-json",
        "no-columns",
        "no-columns",
        "unknown-backend",
    ],
)
def test_what_is_no_schema_document_is_refused(payload, message):
    with pytest.raises(ConfigurationError, match=message):
        PanderaSchema(payload, lambda kind: None)


def test_a_payload_that_cannot_be_written_as_a_document_is_refused():
    odd = {"schema_type": "dataframe", "columns": {"id": {"dtype": object()}}}
    with pytest.raises(ConfigurationError, match="cannot be written as a document"):
        PanderaSchema(odd, lambda kind: None)


def test_an_unknown_backend_is_refused_naming_the_two_it_reads():
    document = {"schema_type": "spider", "columns": KEPT["columns"]}
    with pytest.raises(ConfigurationError, match="dataframe.*polars_dataframe"):
        PanderaSchema(document, lambda kind: None)


def test_a_parser_error_of_pandera_s_is_a_refusal_that_quotes_it():
    def unreadable(text):
        raise KeyError("options")

    backend = SimpleNamespace(DataFrameSchema=SimpleNamespace(from_json=unreadable))
    with pytest.raises(ConfigurationError, match="could not read.*KeyError: 'options'"):
        PanderaSchema(KEPT, lambda kind: backend)


@pytest.mark.parametrize(
    ("given", "place"),
    [
        (dict(KEPT, mystery=True), "$.mystery"),
        (dict(KEPT, columns=dict(KEPT["columns"], mystery=True)), "$.columns.mystery"),
        (
            kept(amount=dict(KEPT["columns"]["amount"], between_the_lines={})),
            "$.columns.amount.between_the_lines",
        ),
        (
            kept(
                amount=dict(
                    KEPT["columns"]["amount"],
                    greater_than_or_equal_to={
                        "min_value": 0,
                        "name": "non-negative",
                        "almost_unique": True,
                    },
                )
            ),
            "$.columns.amount.greater_than_or_equal_to.almost_unique",
        ),
    ],
    ids=["at-the-top", "a-column", "a-check", "inside-a-check"],
)
def test_what_pandera_drops_is_refused_naming_where_it_was_written(given, place):
    """A dropped key would leave a schema that validates anything."""
    with pytest.raises(
        ConfigurationError, match="is not recognised by pandera"
    ) as raised:
        schema(given)
    assert place in str(raised.value)


def test_a_refusal_names_eight_places_and_counts_the_rest():
    given = dict(KEPT, **{f"mystery{index}": True for index in range(11)})
    with pytest.raises(ConfigurationError, match=r"\$\.mystery7 and 3 more"):
        schema(given)


def test_one_kept_spelling_of_a_check_s_argument_is_not_refused():
    # pandera writes the bare 1000 where the document spelled a key.
    schema(
        kept(
            amount=dict(
                KEPT["columns"]["amount"], less_than_or_equal_to={"min_value": 1000}
            )
        )
    )


@pytest.mark.parametrize(
    ("given", "place", "why"),
    [
        (dict(KEPT, strict=True), "$.strict", "no column to carry it"),
        (dict(KEPT, coerce=True), "$.coerce", "a cast copy"),
        (dict(KEPT, unique=["id", "amount"]), "$.unique", "several columns"),
        (
            kept(amount=dict(KEPT["columns"]["amount"], coerce=True)),
            "$.columns.amount.coerce",
            "a cast copy",
        ),
        (
            kept(id=dict(KEPT["columns"]["id"], drop_invalid_rows=True)),
            "$.columns.id.drop_invalid_rows",
            "not something",
        ),
    ],
    ids=[
        "strict",
        "coerce-the-frame",
        "unique-over-columns",
        "coerce-a-column",
        "unknown-but-kept",
    ],
)
def test_what_pandera_keeps_but_this_engine_cannot_vouch_for_is_refused(
    given, place, why
):
    with pytest.raises(ConfigurationError, match="does not take") as raised:
        schema(given, keeps=given)  # pandera keeps it; the engine does not take it
    assert place in str(raised.value) and why in str(raised.value)


# ------------------------------------------------------ what is declared


def test_one_outcome_per_check_and_per_enforced_property():
    rule, _ = schema()
    assert [outcome.label for outcome in rule._outcomes] == LABELS


def test_a_check_is_named_by_its_kind_and_its_column_unless_the_document_names_it():
    rule, _ = schema()
    keys = {outcome.label: outcome.key for outcome in rule._outcomes}
    assert keys["less_than_or_equal_to_amount"] == "less_than_or_equal_to"
    assert keys["non-negative"] == "non-negative"
    assert keys["not_nullable_id"] == "not_nullable"


def test_two_outcomes_that_would_share_a_label_are_refused():
    document = kept(
        id=dict(
            KEPT["columns"]["id"],
            greater_than_or_equal_to={"min_value": 0, "name": "valid"},
        ),
        amount=dict(
            KEPT["columns"]["amount"],
            less_than_or_equal_to={"max_value": 5, "name": "valid"},
        ),
    )
    kinds = {"amount": KINDS["amount"], "id": ("greater_than_or_equal_to",)}
    with pytest.raises(ValueError, match="Duplicate check label 'valid'"):
        schema(document, keeps=document, kinds=kinds)


def test_a_warning_level_check_keeps_its_severity_and_is_made_strict():
    document = kept(
        amount=dict(
            KEPT["columns"]["amount"],
            less_than_or_equal_to={"max_value": 1000, "raise_warning": True},
        )
    )
    read = FakeSchema(document, KINDS)
    backend = SimpleNamespace(
        DataFrameSchema=SimpleNamespace(from_json=lambda text: read)
    )
    assert read.columns["amount"].checks[1].raise_warning is True  # as pandera read it
    rule = PanderaSchema(document, lambda kind: backend)
    warning = next(
        o for o in rule._outcomes if o.label == "less_than_or_equal_to_amount"
    )
    assert warning.criticality == "warn"
    # Under lazy validation pandera says nothing about a warning that fails.
    assert read.columns["amount"].checks[1].raise_warning is False
    others = [o.criticality for o in rule._outcomes if o is not warning]
    assert others == ["error"] * (len(LABELS) - 1)


# ---------------------------------------------------------- the validation


def test_a_frame_that_satisfies_the_schema_passes_every_outcome():
    rule, fake = schema()
    frame = object()
    results = rule.validate(frame)
    assert [result["check"] for result in results] == LABELS
    assert all(result["success"] for result in results)
    assert fake.frames == [frame]
    assert by_label(results)["dtype_id"][1] == {
        "dtype": "int64",
        "criticality": "error",
    }
    assert by_label(results)["non-negative"][1]["check"] == "greater_than_or_equal_to"


def test_each_failure_marks_its_own_outcome_and_nothing_else():
    failures = [
        failure("amount", check_named("non-negative"), cases=Rows([1, 2])),
        failure("id", "not_nullable", "SERIES_CONTAINS_NULLS", Rows([3])),
        failure("id", "field_uniqueness", "SERIES_CONTAINS_DUPLICATES", Rows([1, 1])),
        failure("note", "dtype('str')", "WRONG_DATATYPE", "float64"),
    ]
    rule, _ = schema(failures=failures)
    results = by_label(rule.validate(object()))
    assert sorted(label for label, (passed, _) in results.items() if not passed) == [
        "dtype_note",
        "non-negative",
        "not_nullable_id",
        "unique_id",
    ]
    assert results["non-negative"][1] == {
        "check": "greater_than_or_equal_to",
        "statistics": {"min_value": 0},
        "criticality": "error",
        "reason": "DATAFRAME_CHECK",
        "failed_rows": 2,
    }
    assert results["not_nullable_id"][1] == {
        "criticality": "error",
        "reason": "SERIES_CONTAINS_NULLS",
        "failed_rows": 1,
    }
    # A wrong dtype is a whole column: pandera gives no rows to count.
    assert "failed_rows" not in results["dtype_note"][1]
    assert results["dtype_note"][1]["reason"] == "WRONG_DATATYPE"


def test_a_passing_outcome_carries_no_reason_and_no_count():
    rule, _ = schema()
    assert by_label(rule.validate(object()))["dtype_id"] == (
        True,
        {"dtype": "int64", "criticality": "error"},
    )


def test_a_required_column_that_is_absent_fails_every_outcome_of_that_column():
    absent = failure(None, "column_in_dataframe", COLUMN_NOT_IN_DATAFRAME, "amount")
    rule, _ = schema(failures=[absent])
    results = by_label(rule.validate(SimpleNamespace(columns=["id", "note"])))
    assert sorted(label for label, (passed, _) in results.items() if not passed) == [
        "dtype_amount",
        "less_than_or_equal_to_amount",
        "non-negative",
        "not_nullable_amount",
    ]
    assert results["non-negative"][1] == {
        "check": "greater_than_or_equal_to",
        "statistics": {"min_value": 0},
        "criticality": "error",
        "reason": COLUMN_NOT_IN_DATAFRAME,
    }


def test_an_optional_column_that_is_absent_passes_and_says_it_was_not_there():
    rule, _ = schema()
    results = by_label(rule.validate(SimpleNamespace(columns=["id", "amount"])))
    assert results["dtype_note"] == (
        True,
        {"dtype": "str", "criticality": "error", "present": False},
    )


def test_a_frame_that_does_not_say_its_columns_changes_no_outcome():
    rule, _ = schema()
    assert by_label(rule.validate(object()))["dtype_note"] == (
        True,
        {"dtype": "str", "criticality": "error"},
    )


def test_a_failure_that_is_no_outcome_of_the_schema_refuses_the_run():
    stray = failure("id", "column_in_schema", "COLUMN_NOT_IN_SCHEMA", "extra")
    rule, _ = schema(failures=[stray])
    with pytest.raises(
        ConfigurationError,
        match="not one of the schema's own checks.*'id', column_in_schema",
    ):
        rule.validate(object())


def test_two_failures_for_one_check_are_refused():
    twice = [failure("amount", check_named("non-negative"))] * 2
    rule, _ = schema(failures=twice)
    with pytest.raises(
        ConfigurationError, match="2 failures for one check, 'non-negative'"
    ):
        rule.validate(object())


def test_an_error_that_is_not_a_validation_failure_is_raised_as_it_came():
    rule, fake = schema()

    def explode(frame, lazy):
        raise TypeError("not a frame")

    fake.validate = explode
    with pytest.raises(TypeError, match="not a frame"):
        rule.validate(object())


# --------------------------------------------------------------- the engine


def stub_pandera(monkeypatch, document=KEPT, failures=()):
    """Stand in for pandera, and return the schema it keeps."""
    fake = FakeSchema(document, KINDS)
    fake.failures = list(failures)
    backend = SimpleNamespace(
        DataFrameSchema=SimpleNamespace(from_json=lambda text: fake)
    )
    monkeypatch.setattr(pandera_engine, "_backend", lambda kind: backend)
    monkeypatch.setitem(sys.modules, "pandera", SimpleNamespace(__name__="pandera"))
    return fake


def engine_for(schema_document=KEPT):
    return PanderaEngine(ConfigFactory.from_dict({"schema": schema_document}), 1)


def test_the_engine_validates_a_frame_and_refuses_a_repository(monkeypatch):
    stub_pandera(monkeypatch)
    with pytest.raises(ConfigurationError, match="persistence=False"):
        engine_for().apply(object(), repository=object())
    assert [result["check"] for result in engine_for().apply(object())] == LABELS


def test_the_schema_is_read_once_and_used_for_every_frame(monkeypatch):
    fake = stub_pandera(monkeypatch)
    engine = engine_for()
    first, second = object(), object()
    engine.apply(first)
    engine.apply(second)
    assert fake.frames == [first, second]


def test_the_capabilities_say_what_the_engine_does():
    assert PanderaEngine.CAPABILITIES == {
        "criticality": "derived",
        "row_filter": False,
        "failed_rows": True,
        "persistence": False,
    }


def test_asked_to_work_without_pandera_it_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "pandera", None)
    with pytest.raises(ImportError, match="The pandera engine requires.*-E pandera"):
        engine_for().apply(object())


def test_a_backend_that_cannot_be_imported_is_refused_saying_what_it_needs(monkeypatch):
    # The module the backend names is not installed, as polars would not be.
    monkeypatch.setitem(
        pandera_engine.BACKENDS, "polars_dataframe", "no.such.pandera.backend"
    )
    monkeypatch.setitem(sys.modules, "pandera", SimpleNamespace(__name__="pandera"))
    polars = {"schema_type": "polars_dataframe", "columns": KEPT["columns"]}
    # The backend's own words come first. They must not trail behind "pandera
    # could not read the document", which is not what went wrong.
    with pytest.raises(
        ConfigurationError,
        match=r"^pandera's polars_dataframe schema cannot be used here: No module named 'no'\.",
    ):
        engine_for(polars).apply(object())


def test_the_registry_declares_the_engine_and_its_extra(monkeypatch):
    spec = find_spec("pandera")
    assert (spec.module, spec.class_name, spec.payload_key) == (
        "dq.engine.pandera.pandera_engine",
        "PanderaEngine",
        "schema",
    )
    assert (spec.extras, spec.vendor_imports, spec.payload_type) == (
        ("pandera",),
        ("pandera",),
        "document",
    )
    # What dq-engines reads to say the engine is not installed here.
    monkeypatch.setitem(sys.modules, "pandera", None)
    assert missing_vendor_imports(spec) == ("pandera",)


def test_the_module_imports_with_pandera_absent_and_loading_is_refused():
    """Importing the module is one thing -- pandera is not so much as looked
    up -- and being asked for the engine is another. For that, pandera is
    absent -- a missing module the loader can report -- not forbidden:
    asking whether a library is installed is a lookup, which the block
    forbids. The refusal comes at loading, before the engine's work, and
    names the extra: it used to come when the first frame was applied."""
    code = f"""
import sys

BLOCKED = {sorted(EXTRAS)!r}


class Block:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise AssertionError(fullname + " was imported")

sys.meta_path.insert(0, Block())
import dq.engine.pandera.pandera_engine
from dq.engine.engine_loader import EngineLoader
from dq.exceptions import EngineNotFoundError
from pyhocon import ConfigFactory

sys.meta_path.pop(0)
sys.modules["pandera"] = None
schema = {{"schema_type": "dataframe", "columns": {{"id": {{"dtype": "int64"}}}}}}
rule = ConfigFactory.from_dict({{"schema": schema}})
try:
    EngineLoader().load_engine("pandera", rule, 1)
except EngineNotFoundError as error:
    assert str(error) == (
        "Engine 'pandera' cannot run here: 'pandera' is not installed. "
        "Install it with: pip install 'dq-kernel[pandera]'  "
        "(or: poetry install -E pandera)."
    ), error
else:
    raise AssertionError("asked for without pandera, it was loaded")
loaded = [n for n, m in sys.modules.items() if m is not None and n.split(".")[0] in BLOCKED]
assert not loaded, loaded
print("ok")
"""
    result = run_python(["-c", code], child_env())
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


# ------------------------------------------------------------ the validator

RULE = """dqframework {{
  dqrules = [
    {{
      name = "orders"
      engine = "pandera"
      dataframes = ["orders"]
      schema = {payload}
    }}
  ]
}}
"""

AS_MAPPING = """{
        schema_type = "dataframe"
        columns {
          id {
            dtype = "int64"
          }
        }
      }"""
AS_TEXT = (
    '"{\\"schema_type\\": \\"dataframe\\", '
    '\\"columns\\": {\\"id\\": {\\"dtype\\": \\"int64\\"}}}"'
)


@pytest.mark.parametrize(
    "payload", [AS_MAPPING, AS_TEXT], ids=["a-mapping", "the-text-of-one"]
)
def test_the_validator_takes_a_mapping_or_the_text_of_one(tmp_path, payload):
    path = tmp_path / "rules.conf"
    path.write_text(RULE.format(payload=payload), encoding="utf-8")
    assert validate_config(str(path))


@pytest.mark.parametrize(
    ("payload", "problem"),
    [
        ("42", "payload 'schema' must be a mapping, or the text of one"),
        ('"   "', "payload 'schema' must be a mapping, or the text of one"),
        # An empty list is refused as no payload at all, one rule earlier.
        ("[]", "requires a non-empty 'schema' payload"),
    ],
    ids=["a-number", "blank-text", "a-list"],
)
def test_the_validator_refuses_a_payload_that_is_no_document(
    tmp_path, payload, problem
):
    path = tmp_path / "rules.conf"
    path.write_text(RULE.format(payload=payload), encoding="utf-8")
    assert validate_config_document(str(path)).errors == (
        f"Rule 0: Engine 'pandera' {problem}",
    )
