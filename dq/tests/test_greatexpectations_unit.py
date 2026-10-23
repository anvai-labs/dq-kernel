# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""JVM-free, GE-free contract tests for the greatexpectations engine.

The gx module is faked in sys.modules (test_dqdl-style vendor mock) so
the engine contract -- strict-JSON sanitizer, bool coercion, fail-closed
type resolution, single-key persistence, batch-definition caching --
runs in the fast unit lane without the optional extra installed.
"""

import math
import sys
import types
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from pyhocon import ConfigFactory

from dq.engine.greatexpectations.greatexpectations_engine import (
    GreatexpectationsEngine,
    _to_strict_json,
)

# ---------------------------------------------------------------- sanitizer


def test_sanitizer_normalizes_adversarial_payloads():
    class NumpyBool:
        def item(self):
            return True

    class NumpyFloat:
        def item(self):
            return float("nan")

    payload = {
        42: "int-key",
        "tuple": (1, 2),
        "nan": float("nan"),
        "inf": float("inf"),
        "np_bool": NumpyBool(),
        "np_nan": NumpyFloat(),
        "bytes": b"raw",
        "date": date(2026, 10, 22),
        "exotic": object(),
    }
    out = _to_strict_json(payload)
    import json

    assert out == json.loads(json.dumps(out, allow_nan=False))
    assert list(out) == [
        "42",
        "tuple",
        "nan",
        "inf",
        "np_bool",
        "np_nan",
        "bytes",
        "date",
        "exotic",
    ]
    assert out["tuple"] == [1, 2]
    assert out["nan"] is None and out["inf"] is None
    assert out["np_bool"] is True and out["np_nan"] is None
    assert out["bytes"] == "raw"
    assert out["date"] == "2026-10-22"
    assert isinstance(out["exotic"], str)


def test_sanitizer_falls_through_when_item_unwrap_raises():
    class BrokenScalar:
        def item(self):
            raise RuntimeError("not a numpy scalar")

    out = _to_strict_json({"broken": BrokenScalar()})
    assert isinstance(out["broken"], str)


def test_engine_import_error_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "great_expectations", None)
    engine = GreatexpectationsEngine(
        ConfigFactory.from_dict(
            {"name": "gx", "engine": "greatexpectations", "expectations": []}
        )
    )
    with pytest.raises(ImportError, match="greatexpectations engine requires"):
        engine.apply(_FakeFrame())


def test_sanitizer_bounds_recursion_depth():
    deep = current = {}
    for _ in range(30):
        current["child"] = {}
        current = current["child"]
    current["leaf"] = "value"
    out = _to_strict_json(deep)
    assert out == _to_strict_json(out)  # stable, no recursion error


# ------------------------------------------------------------- vendor fake


class FakeExpectation:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _make_fake_gx(validation):
    """Build a great_expectations module double for one apply() run."""
    fake = types.ModuleType("great_expectations")
    fake.expectations = SimpleNamespace(
        ExpectColumnValuesToNotBeNull=FakeExpectation,
        ExpectTableRowCountToBeBetween=FakeExpectation,
    )
    batch = SimpleNamespace(validate=lambda suite: validation)
    batch_def = SimpleNamespace(get_batch=lambda batch_parameters: batch)
    fake.get_context = lambda: SimpleNamespace(
        data_sources=SimpleNamespace(
            add_spark=lambda name: SimpleNamespace(
                add_dataframe_asset=lambda name: SimpleNamespace(
                    add_batch_definition_whole_dataframe=lambda name: batch_def
                )
            )
        )
    )
    fake._batch_def = batch_def
    fake._suite = _Suite(None)
    fake.ExpectationSuite = lambda name=None: fake._suite
    return fake


class _Suite:
    def __init__(self, name):
        self.name = name
        self.added = []

    def add_expectation(self, exp):
        self.added.append(exp)


def _validation(results):
    return SimpleNamespace(
        results=[
            SimpleNamespace(
                expectation_config=SimpleNamespace(type=etype),
                success=success,
                result=result,
            )
            for etype, success, result in results
        ]
    )


def _engine_with(fake_gx, monkeypatch, expectations, **config):
    monkeypatch.setitem(sys.modules, "great_expectations", fake_gx)
    config.setdefault("name", "gx-unit")
    config["engine"] = "greatexpectations"
    config["expectations"] = expectations
    return GreatexpectationsEngine(ConfigFactory.from_dict(config))


class _FakeSpark:
    def createDataFrame(self, rows, schema):
        return SimpleNamespace(rows=rows, schema=schema)


class _FakeFrame:
    sparkSession = _FakeSpark()


# ----------------------------------------------------------------- engine


def test_engine_coerces_results_through_the_sanitizer(monkeypatch):
    validation = _validation(
        [
            ("expect_column_values_to_not_be_null", True, {"unexpected_count": 0}),
            (
                "expect_column_values_to_be_in_set",
                "yes",
                {"partial_unexpected_list": (1, 2), "ratio": float("nan")},
            ),
        ]
    )
    fake = _make_fake_gx(validation)
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    metrics = engine.apply(_FakeFrame())
    assert metrics[0]["success"] is True
    assert metrics[1]["success"] is True  # coerced to bool("yes")
    assert metrics[1]["details"] == {
        "partial_unexpected_list": [1, 2],
        "ratio": None,
    }


def test_engine_fails_closed_on_unknown_type_without_gx_classes(monkeypatch):
    fake = _make_fake_gx(_validation([]))
    engine = _engine_with(
        fake, monkeypatch, [{"type": "expect_never_exists", "column": "id"}]
    )
    with pytest.raises(ValueError, match="Unsupported expectation type"):
        engine.apply(_FakeFrame())


def test_engine_fails_closed_on_missing_type(monkeypatch):
    fake = _make_fake_gx(_validation([]))
    engine = _engine_with(fake, monkeypatch, [{"column": "id"}])
    with pytest.raises(ValueError, match="requires a 'type'"):
        engine.apply(_FakeFrame())


def test_engine_passes_column_only_when_present_and_merges_kwargs(monkeypatch):
    validation = _validation(
        [("expect_table_row_count_to_be_between", True, {"observed_value": 3})]
    )
    fake = _make_fake_gx(validation)
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {"type": "expect_table_row_count_to_be_between", "min_value": 1},
            {
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
                "kwargs": {"mostly": 0.9},
            },
        ],
    )
    engine.apply(_FakeFrame())
    added = fake._suite.added
    assert added[0].kwargs == {"min_value": 1}
    assert added[1].kwargs == {"column": "id", "mostly": 0.9}


def test_engine_persists_both_artifacts_under_one_key(monkeypatch):
    validation = _validation(
        [("expect_column_values_to_not_be_null", True, {"unexpected_count": 0})]
    )
    fake = _make_fake_gx(validation)
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    saved = []

    class FakeSink:
        def save(self, df, metric_type, key):
            saved.append((metric_type, key, df))

    engine.apply(_FakeFrame(), repository=FakeSink())
    assert [kind for kind, _, _ in saved] == ["metrics", "verifications"]
    assert len({key for _, key, _ in saved}) == 1
    assert saved[0][2].rows == [("expect_column_values_to_not_be_null", True)]


def test_engine_caches_batch_definition_across_applies(monkeypatch):
    validation = _validation([("expect_column_values_to_not_be_null", True, {})])
    fake = _make_fake_gx(validation)
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    engine.apply(_FakeFrame())
    first = engine._gx_batch_definition
    engine.apply(_FakeFrame())
    assert engine._gx_batch_definition is first
