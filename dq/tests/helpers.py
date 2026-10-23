# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared outcome assertions, doubles, and sink checks for tests (TD-ARCH-9/U7).

These helpers replace the repeated manual success loops and per-file
outcome-keying logic so that a failing check fails the test with a clear
message instead of relying on printed diagnostics. Pure-python test
doubles also live here; anything Spark-dependent belongs in a
``*_support`` module instead.
"""


def legacy_by_rule(outcomes):
    """Map portable outcomes by their rule_id to legacy dictionaries."""
    return {
        outcome.to_legacy()["details"]["rule_id"]: outcome.to_legacy()
        for outcome in outcomes
    }


def summaries_by_instance(results):
    """Map engine summaries by their details.instance string."""
    return {result["details"]["instance"]: result for result in results}


def assert_overall_success(results, context=""):
    """Assert every summary succeeded, naming the failures otherwise."""
    failures = [result for result in results if result["success"] is not True]
    assert not failures, f"{context}: failing outcomes {failures}"


def assert_any_failure(results, context=""):
    """Assert at least one summary failed, showing all outcomes otherwise."""
    failures = [result for result in results if result["success"] is not True]
    assert failures, f"{context}: expected a failing outcome, got {results}"


def assert_sink_received_both_artifacts(results, sink):
    """Assert an engine run persisted both artifact kinds under one key.

    Both artifacts of a run must share a single ResultKey so downstream
    consumers joining on the run timestamp see the complete run, and
    the metrics payload must carry actual rows.
    """
    assert results
    writes = sink.writes()
    assert set(kind for kind, _ in writes) == {"metrics", "verifications"}
    keys = {key for _, key in writes}
    assert len(keys) == 1, f"run artifacts split across keys {keys}"
    (key,) = keys
    assert sink.rows("metrics", key), "metrics artifact has no rows"


class FakeRow:
    """Minimal Row double exposing asDict for collect-and-convert code."""

    def __init__(self, values):
        self._values = values

    def asDict(self, recursive=True):
        return dict(self._values)


class FakeDataFrame:
    """Stands in for a DataFrame where a sink only needs collect()."""

    def __init__(self, rows):
        self._rows = [FakeRow(row) for row in rows]

    def collect(self):
        return self._rows
