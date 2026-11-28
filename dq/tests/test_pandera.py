# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Live round trips for the pandera engine, on pandas and on Polars (SPEC-017).

Skipped entirely when the optional extra is absent. The contracts that need
no pandera are in test_pandera_unit.py; what runs here is the real package,
including the facts the spec's design rests on.
"""

import importlib.metadata
import json
import os
import warnings
from datetime import datetime, timedelta, timezone

import pytest
import yaml

pytest.importorskip("pandera")

import pandera.errors  # noqa: E402
import pandera.pandas  # noqa: E402
import pandera.polars  # noqa: E402

from dq.engine.engine_loader import EngineLoader  # noqa: E402
from dq.engine.pandera.pandera_engine import PanderaEngine  # noqa: E402
from dq.exceptions import ConfigurationError  # noqa: E402
from dq.gate import evaluate_gate  # noqa: E402
from dq.sdk.conformance import assert_engine_conforms  # noqa: E402
from dq.sinks import InMemorySink  # noqa: E402
from dq.tests.pandera_support import (  # noqa: E402
    BACKENDS,
    CLEAN,
    DOCUMENTS,
    EXAMPLE,
    LABELS,
    ROWS,
    document,
    frame,
    reordered,
    rule,
)
from dq.utils.config_utils import config_tree_to_python, load_config  # noqa: E402
from dq.validate import validate_config  # noqa: E402

#: Only pandera's warning-level check fails here.
OVER_THE_LIMIT = {
    "id": [1, 2, 3],
    "amount": [3.0, 2000.0, 4.0],
    "note": ["a", "b", "c"],
}
UNDER_ZERO = {"id": [1, 2, 3], "amount": [-5.0, 3.0, 4.0], "note": ["a", "b", "c"]}
TEXT_IDS = {"id": ["1", "2", "3"], "amount": [1.0, 2.0, 3.0], "note": ["a", "b", "c"]}
A_NULL_ID = {"id": [1, None, 3], "amount": [1.0, 2.0, 3.0], "note": ["a", "b", "c"]}


def results(backend, rows=None, document=None, without=()):
    """Return ``{label: (passed, details)}`` for a frame on ``backend``."""
    engine = PanderaEngine(rule(backend, document), 1)
    return {
        result["check"]: (result["success"], result["details"])
        for result in engine.apply(frame(backend, rows, without))
    }


# --------------------------------------------------------------------- G1


def test_g1_the_facts_the_spec_rests_on_hold_in_the_installed_pandera():
    """SPEC-017's note, as a test. If pandera changes under any of these,
    the note and the engine's design are revisited together."""
    assert importlib.metadata.version("pandera").startswith("0.36.")

    read = {
        backend: getattr(pandera, backend).DataFrameSchema.from_json(
            json.dumps(DOCUMENTS[backend])
        )
        for backend in BACKENDS
    }
    written = {
        backend: json.loads(schema.to_json()) for backend, schema in read.items()
    }
    # A check's argument is written bare when the statistic is all there
    # is, on both backends, and as a mapping when the check says more:
    # keyed `min_value` and `max_value` by pandas, `value` by Polars.
    for backend in BACKENDS:
        assert written[backend]["columns"]["amount"]["greater_than_or_equal_to"] == 0
    assert written["pandas"]["columns"]["amount"]["less_than_or_equal_to"] == {
        "max_value": 1000,
        "raise_warning": True,
    }
    assert written["polars"]["columns"]["amount"]["less_than_or_equal_to"] == {
        "value": 1000,
        "raise_warning": True,
    }
    # A document keys a column's checks by their kind, and a failure carries
    # the check's name, on both backends.
    for backend in BACKENDS:
        checks = read[backend].columns["amount"].checks
        assert [check.registry_name for check in checks] == [
            "greater_than_or_equal_to",
            "less_than_or_equal_to",
        ]
        assert [check.name for check in checks] == [
            check.registry_name for check in checks
        ]
        assert [check.raise_warning for check in checks] == [False, True]

    # A `checks` list on a column is not a document pandera reads.
    listed = document(
        "pandas", amount={"checks": [{"check": "greater_than_or_equal_to"}]}
    )
    with pytest.raises(KeyError, match="options"):
        pandera.pandas.DataFrameSchema.from_json(json.dumps(listed))

    # Under lazy validation pandera says nothing about a warning-level check
    # that fails: the frame comes back as if nothing were wrong.
    schema = pandera.pandas.DataFrameSchema.from_json(json.dumps(DOCUMENTS["pandas"]))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        returned = schema.validate(frame("pandas", OVER_THE_LIMIT), lazy=True)
    assert returned is not None

    # Made strict, it is reported, and the failure names the column and the
    # check: that is what a result is matched on.
    for column in schema.columns.values():
        for check in column.checks:
            check.raise_warning = False
    with pytest.raises(pandera.errors.SchemaErrors) as raised:
        schema.validate(frame("pandas", OVER_THE_LIMIT), lazy=True)
    assert [
        (failure.column_name, failure.check.name)
        for failure in raised.value.schema_errors
    ] == [("amount", "less_than_or_equal_to")]


def test_g1_importing_pandera_sets_spark_s_timezone_switch_for_the_process():
    """Why the suite's Arrow tests are written so that nothing moves them.
    Where pyspark is installed, pandera's import sets PYARROW_IGNORE_TIMEZONE
    and SPARK_LOCAL_IP for the whole process, and pyarrow reads the first on
    every conversion of a Python sequence -- any value at all, even "0".
    """
    pytest.importorskip("pyspark", reason="pandera sets the switch only there")
    assert os.environ.get("PYARROW_IGNORE_TIMEZONE") == "1"
    assert os.environ.get("SPARK_LOCAL_IP") == "127.0.0.1"
    # And pyarrow follows it: a zoned value's wall time is read as if UTC.
    import pyarrow as pa

    value = datetime(2026, 11, 25, 10, tzinfo=timezone(timedelta(hours=-6)))
    zoned = pa.array([value], pa.timestamp("us", tz="America/Chicago"))[0].as_py()
    assert (zoned.hour, zoned.utcoffset()) == (4, timedelta(hours=-6))


# --------------------------------------------------------------------- G2


@pytest.mark.parametrize("backend", BACKENDS)
def test_g2_the_engine_conforms_with_its_columns_in_either_order(backend):
    report = assert_engine_conforms(
        lambda dqts=None: PanderaEngine(rule(backend), dqts=dqts),
        frame(backend),
        InMemorySink,
        reordered_factory=lambda dqts=None: PanderaEngine(
            rule(backend, reordered(backend)), dqts=dqts
        ),
    )
    assert report.conforms, report.summary()
    # pandera's columns are a dataframe's labels: no rule hands one to
    # Spark, so the suite has no misnamed rule to offer it (SPEC-025).
    assert report.notes == (
        "column references were not checked: no misnamed_factory was given",
    ), report.notes
    assert "attribution_survives_reordering" in report.checks
    assert "persistence_honours_the_capability" in report.checks


# --------------------------------------------------------------------- G3


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_passing_schema_passes_every_outcome(backend):
    found = results(backend, CLEAN)
    assert list(found) == LABELS
    assert [passed for passed, _ in found.values()] == [True] * len(LABELS)


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_failing_column_check_carries_its_reason_and_its_rows(backend):
    assert results(backend)["greater_than_or_equal_to_amount"] == (
        False,
        {
            "check": "greater_than_or_equal_to",
            # Read back, the argument is `min_value` on both backends; only
            # the Polars document spells it `value`.
            "statistics": {"min_value": 0},
            "criticality": "error",
            "reason": "DATAFRAME_CHECK",
            "failed_rows": 1,
        },
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_failing_warning_level_check_is_reported_as_one(backend):
    """pandera alone reports nothing for it (see G1)."""
    passed, details = results(backend)["less_than_or_equal_to_amount"]
    assert passed is False
    assert (details["criticality"], details["reason"], details["failed_rows"]) == (
        "warn",
        "DATAFRAME_CHECK",
        1,
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_null_in_a_non_nullable_column_is_counted(backend):
    found = results(backend, A_NULL_ID)
    assert found["not_nullable_id"] == (
        False,
        {"criticality": "error", "reason": "SERIES_CONTAINS_NULLS", "failed_rows": 1},
    )
    # One cause, two failures on pandas: a null turns an integer column to
    # float, so its dtype fails too. Polars keeps the dtype.
    assert found["dtype_id"][0] is (backend == "polars")


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_duplicate_where_unique_is_declared_is_counted(backend):
    assert results(backend)["unique_id"] == (
        False,
        {
            "criticality": "error",
            "reason": "SERIES_CONTAINS_DUPLICATES",
            "failed_rows": 2,
        },
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_wrong_dtype_fails_without_a_failing_value(backend):
    """pandera returns the offending values themselves; evidence gets a count."""
    passed, details = results(backend, TEXT_IDS)["dtype_id"]
    assert passed is False
    assert details["reason"] == "WRONG_DATATYPE"
    assert "1" not in json.dumps(details)


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_missing_required_column_fails_every_outcome_of_that_column(backend):
    found = results(backend, without=("amount",))
    for label in (
        "dtype_amount",
        "not_nullable_amount",
        "greater_than_or_equal_to_amount",
        "less_than_or_equal_to_amount",
    ):
        assert found[label][0] is False, label
        assert found[label][1]["reason"] == "COLUMN_NOT_IN_DATAFRAME"
        assert "failed_rows" not in found[label][1]
    assert found["dtype_id"][0] is True and found["dtype_note"][0] is True


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_an_optional_column_that_is_absent_passes_and_says_so(backend):
    base = DOCUMENTS[backend]
    said = dict(base["columns"]["note"], required=False)
    optional = {**base, "columns": {**base["columns"], "note": said}}
    found = results(backend, document=optional, without=("note",))
    assert found["dtype_note"] == (
        True,
        {"dtype": said["dtype"], "criticality": "error", "present": False},
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_check_named_in_the_document_reports_under_that_name(backend):
    said = {
        **DOCUMENTS[backend]["columns"]["amount"]["greater_than_or_equal_to"],
        "name": "amount non-negative",
    }
    found = results(
        backend, document=document(backend, amount={"greater_than_or_equal_to": said})
    )
    assert found["amount non-negative"][0] is False
    assert "greater_than_or_equal_to_amount" not in found


# ---------------------------------------------------------- attribution


@pytest.mark.parametrize("backend", BACKENDS)
def test_each_result_is_attributed_to_its_own_check(backend):
    """Two checks on one column, each failing on a different frame."""
    under = results(backend, UNDER_ZERO)
    over = results(backend, OVER_THE_LIMIT)
    assert under["greater_than_or_equal_to_amount"][0] is False
    assert under["less_than_or_equal_to_amount"][0] is True
    assert over["greater_than_or_equal_to_amount"][0] is True
    assert over["less_than_or_equal_to_amount"][0] is False


@pytest.mark.parametrize("backend", BACKENDS)
def test_the_gate_reads_the_criticality_of_the_check_that_failed(backend):
    """Only the warning-level check fails on ``OVER_THE_LIMIT``."""
    outcomes = PanderaEngine(rule(backend)).apply(frame(backend, OVER_THE_LIMIT))
    advised = evaluate_gate(outcomes, "error")
    assert advised.passed is True
    assert advised.advisory == ("less_than_or_equal_to_amount",)
    assert evaluate_gate(outcomes, "any").passed is False


# --------------------------------------------------------------------- G4


@pytest.mark.parametrize("backend", BACKENDS)
def test_g4_an_unknown_check_is_refused_naming_where_it_was_written(backend):
    with pytest.raises(ConfigurationError, match="not recognised by pandera") as raised:
        PanderaEngine(
            rule(backend, document(backend, amount={"between_the_lines": {}}))
        ).apply(frame(backend))
    assert "$.columns.amount.between_the_lines" in str(raised.value)


@pytest.mark.parametrize("backend", BACKENDS)
def test_g4_coerce_is_refused_saying_why(backend):
    engine = PanderaEngine(rule(backend, document(backend, amount={"coerce": True})))
    with pytest.raises(
        ConfigurationError, match=r"\$\.columns\.amount\.coerce.*a cast copy"
    ):
        engine.apply(frame(backend))


def test_g4_a_document_pandera_cannot_read_is_refused_quoting_its_error():
    listed = document(
        "pandas", amount={"checks": [{"check": "greater_than_or_equal_to"}]}
    )
    with pytest.raises(ConfigurationError, match="could not read.*KeyError: 'options'"):
        PanderaEngine(rule("pandas", listed)).apply(frame("pandas"))


def test_g4_an_unknown_key_at_the_top_is_refused():
    engine = PanderaEngine(rule("pandas", dict(DOCUMENTS["pandas"], mystery=True)))
    with pytest.raises(ConfigurationError, match=r"\$\.mystery"):
        engine.apply(frame("pandas"))


def test_g4_a_document_with_no_column_is_refused():
    engine = PanderaEngine(rule("pandas", {"schema_type": "dataframe", "columns": {}}))
    with pytest.raises(ConfigurationError, match="declares no column"):
        engine.apply(frame("pandas"))


def test_g4_an_unknown_key_inside_a_check_is_refused_naming_it():
    """pandera keeps the argument and drops the key it does not know."""
    said = {"min_value": 0, "almost_unique": True}
    engine = PanderaEngine(
        rule("pandas", document("pandas", amount={"greater_than_or_equal_to": said}))
    )
    with pytest.raises(
        ConfigurationError,
        match=r"\$\.columns\.amount\.greater_than_or_equal_to\.almost_unique",
    ):
        engine.apply(frame("pandas"))


def test_g4_strict_is_refused_saying_why():
    engine = PanderaEngine(rule("pandas", dict(DOCUMENTS["pandas"], strict=True)))
    with pytest.raises(ConfigurationError, match=r"\$\.strict.*no column to carry it"):
        engine.apply(frame("pandas"))


def test_g4_two_checks_named_alike_are_refused():
    columns = {
        "id": dict(
            DOCUMENTS["pandas"]["columns"]["id"],
            greater_than_or_equal_to={"min_value": 0, "name": "valid"},
        ),
        "amount": dict(
            DOCUMENTS["pandas"]["columns"]["amount"],
            less_than_or_equal_to={"max_value": 1000, "name": "valid"},
        ),
        "note": DOCUMENTS["pandas"]["columns"]["note"],
    }
    engine = PanderaEngine(rule("pandas", dict(DOCUMENTS["pandas"], columns=columns)))
    with pytest.raises(ValueError, match="Duplicate check label 'valid'"):
        engine.apply(frame("pandas"))


@pytest.mark.parametrize("backend", BACKENDS)
def test_g3_a_whole_column_of_the_wrong_dtype(backend):
    """pandas counts the values that do not fit; Polars names the dtype."""
    said = {"dtype": "float64" if backend == "pandas" else "Float64"}
    passed, details = results(backend, document=document(backend, note=said))[
        "dtype_note"
    ]
    assert passed is False
    assert details["reason"] == "WRONG_DATATYPE"
    if backend == "pandas":
        assert details["failed_rows"] == 2  # "a" and "c"
    else:
        assert "failed_rows" not in details
    assert set(details) == {"dtype", "criticality", "reason"} | (
        {"failed_rows"} if backend == "pandas" else set()
    )


@pytest.mark.parametrize("backend", BACKENDS)
def test_something_that_is_not_a_frame_is_raised_as_it_came(backend):
    """Not a validation failure: the engine reports nothing about it."""
    with pytest.raises(Exception) as raised:
        PanderaEngine(rule(backend)).apply([1, 2, 3])
    assert not isinstance(raised.value, ConfigurationError)
    if backend == "pandas":
        assert "expected pd.DataFrame" in str(raised.value)


# --------------------------------------------------------------------- G5


def test_g5_the_example_rule_pack_validates():
    assert validate_config(str(EXAMPLE))


def test_g5_the_example_runs_as_a_rule_pack_is_loaded():
    first = load_config(str(EXAMPLE)).get("dqframework").get("dqrules")[0]
    engine = EngineLoader().load_engine("pandera", first, 1)
    found = {
        result["check"]: (result["success"], result["details"]["criticality"])
        for result in engine.apply(frame("pandas"))
    }
    assert list(found) == LABELS
    assert [label for label, (passed, _) in found.items() if not passed] == [
        "unique_id",
        "greater_than_or_equal_to_amount",
        "less_than_or_equal_to_amount",
    ]
    assert found["less_than_or_equal_to_amount"] == (False, "warn")
    assert all(result["success"] for result in engine.apply(frame("pandas", CLEAN)))


# ------------------------------------------- a column label with a dot in it


#: A schema whose column label holds a dot: pyhocon read it as a path from
#: JSON and YAML, and kept its quotes from HOCON (SPEC-017's promise is that
#: a schema already in use travels into a rule pack unchanged).
LABEL_ROWS = {"id": [1, 2, 3], "order.id": [1, None, "x"]}
LABEL_PACK = {
    "dqframework": {
        "dqrules": [
            {
                "name": "labels",
                "engine": "pandera",
                "dataframes": ["default"],
                "schema": {
                    "schema_type": "dataframe",
                    "columns": {
                        "id": {"dtype": "int64"},
                        "order.id": {"dtype": "int64"},
                    },
                },
            }
        ]
    }
}
LABEL_AS_HOCON = """
dqframework {
  dqrules = [{
    name = "labels"
    engine = "pandera"
    dataframes = ["default"]
    schema {
      schema_type = "dataframe"
      columns {
        id { dtype = "int64" }
        "order.id" { dtype = "int64" }
      }
    }
  }]
}
"""
LABEL_OUTCOMES = {
    "dtype_id": True,
    "not_nullable_id": True,
    "dtype_order.id": False,  # a null and a text, where integers were declared
    "not_nullable_order.id": False,
}


@pytest.mark.parametrize("fmt", ["conf", "yaml", "json"])
def test_g5_a_column_label_with_a_dot_in_it_is_read_as_written(tmp_path, fmt):
    text = {
        "json": json.dumps(LABEL_PACK),
        "yaml": yaml.safe_dump(LABEL_PACK, sort_keys=False),
        "conf": LABEL_AS_HOCON,
    }[fmt]
    path = tmp_path / f"labels.{fmt}"
    path.write_text(text)
    assert validate_config(str(path))
    loaded = load_config(str(path)).get("dqframework").get("dqrules")[0]

    # As the framework hands a rule, a tree; and as a plain mapping.
    for given in (loaded, config_tree_to_python(loaded)):
        results = (
            EngineLoader()
            .load_engine("pandera", given, 1)
            .apply(frame("pandas", LABEL_ROWS))
        )
        assert {result["check"]: result["success"] for result in results} == (
            LABEL_OUTCOMES
        ), fmt
