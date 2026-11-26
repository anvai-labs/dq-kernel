# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""A real run, observed (SPEC-014).

Real Spark, a real engine, real time: what the observer is told about a run
that actually happened.
"""

import json
import logging

import pytest
from pyhocon import ConfigFactory

pytest.importorskip("pydeequ")

from dq.dq_framework import DQFramework  # noqa: E402
from dq.observability import LOG_FORMAT_VARIABLE, InMemoryObserver  # noqa: E402
from dq.tests.observability_support import in_memory_observatory  # noqa: E402

pytestmark = pytest.mark.spark

RULES = [
    {
        "name": "names",
        "engine": "deequ",
        "dataframes": ["default"],
        "checks": [{"constraint": "isComplete", "column": "name"}],
    },
    {
        "name": "ages",
        "engine": "deequ",
        "dataframes": ["default"],
        "checks": [{"constraint": "isNonNegative", "column": "age"}],
    },
]
CONFIG = ConfigFactory.from_dict({"dqframework": {"dqrules": RULES}})
#: One name missing and one negative age: each rule fails one check.
ROWS = [("Alice", 34), ("Bob", 45), (None, -1)]


def observed(spark, observer=None):
    """Run the two rules on ``ROWS``; with no observer given, the default one."""
    frame = spark.createDataFrame(ROWS, ["name", "age"])
    injected = {"observer": observer} if observer is not None else {}
    return DQFramework(spark, CONFIG, default_dataframe=frame, **injected).run()


def test_a_real_run_tells_its_observer_what_happened(spark):
    observer = InMemoryObserver()
    results = observed(spark, observer)

    assert observer.notifications == (
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    )
    started, first, done, second, second_done, finished, gate = [
        event for _name, event in observer.events
    ]
    # The run is identified by the Spark application that ran it.
    assert started.job_id == spark.sparkContext.applicationId == finished.job_id
    assert started.rules == 2
    assert (first.rule, first.engine, first.dataset) == ("names", "deequ", "default")
    assert (second.rule, second.engine, second.dataset) == ("ages", "deequ", "default")
    # Each rule produced one check, named as the engine names it (by its
    # constraint and column), and each failed.
    assert (done.outcomes, done.failed) == (1, ("isComplete_name",))
    assert (second_done.outcomes, second_done.failed) == (1, ("isNonNegative_age",))
    assert done.error is None and second_done.error is None
    # Real work took real time, measured by the run core.
    assert done.duration_ms > 0 and second_done.duration_ms > 0
    assert finished.duration_ms >= done.duration_ms + second_done.duration_ms - 1e-6
    assert (finished.outcomes, finished.failed) == (len(results), 2)
    assert (finished.blocking, finished.advisory, finished.gate_passed) == (2, 0, False)
    assert (gate.status, gate.total, gate.job_id) == ("failed", 2, started.job_id)


def test_a_real_run_writes_json_lines_that_parse_and_name_the_run(
    spark, monkeypatch, caplog
):
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, "json")
    with caplog.at_level(logging.INFO, logger="dq.run_events"):
        observed(spark)  # the default logging observer, in JSON because of the variable
    documents = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "dq.run_events"
    ]
    assert [document["event"] for document in documents] == [
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    ]
    assert len({document["run_key"] for document in documents}) == 1
    assert [
        (d["rule"], d["engine"], d["dataset"])
        for d in documents
        if d["event"] == "rule_finished"
    ] == [
        ("names", "deequ", "default"),
        ("ages", "deequ", "default"),
    ]
    assert all(d["duration_ms"] > 0 for d in documents if d["event"] == "rule_finished")


def test_a_real_run_through_the_opentelemetry_observer(spark):
    observatory = in_memory_observatory()
    observed(spark, observatory.observer)

    assert sorted(span.name for span in observatory.spans) == [
        "dq.rule",
        "dq.rule",
        "dq.run",
    ]
    (run,) = observatory.named("dq.run")
    assert run.attributes["dq.job_id"] == spark.sparkContext.applicationId
    assert (run.attributes["dq.gate_passed"], run.attributes["dq.blocking"]) == (
        False,
        2,
    )
    rules = sorted(observatory.named("dq.rule"), key=lambda s: s.attributes["dq.rule"])
    assert [span.attributes["dq.rule"] for span in rules] == ["ages", "names"]
    assert all(span.attributes["dq.duration_ms"] > 0 for span in rules)
    # Two checks ran and both failed at error level.
    assert observatory.outcomes_counted("dq.outcomes") == {
        (("dq.criticality", "error"), ("dq.status", "failed")): 2
    }
