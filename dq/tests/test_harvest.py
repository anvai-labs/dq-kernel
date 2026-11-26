# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Harvester contract (SPEC-013 G5).

What every importer owes: provenance on each outcome, a batch that crosses
the outcome gate, a refusal that names what it was reading, and no vendor
library imported anywhere.
"""

import hashlib
import json

import pytest

from dq.exceptions import ConfigurationError, HarvestError
from dq.gate import evaluate_gate
from dq.harvest import HARVESTERS, Harvester, detect, harvest
from dq.harvest import base
from dq.outcomes import normalize_outcomes
from dq.report import REPORT_SCHEMA_VERSION, build_report
from dq.tests.harvest_support import (
    COLUMNAR_JSON_EXAMPLE,
    CSV_EXAMPLE,
    DBT_EXAMPLE,
    GX_EXAMPLE,
    INVOCATION,
    data_test,
    dbt_document,
    gx_document,
    gx_result,
    import_bytes,
    import_json,
)
from dq.tests.test_package_contract import _run_with_blocked_imports

#: Each example with the system that reads it.
SOURCES = [
    (DBT_EXAMPLE, "dbt"),
    (GX_EXAMPLE, "greatexpectations"),
    (CSV_EXAMPLE, "columnar"),
    (COLUMNAR_JSON_EXAMPLE, "columnar"),
]

#: The run each vendor document names, read from the document itself.
RUN_OF = {
    "dbt": lambda source: json.loads(source)["metadata"]["invocation_id"],
    "greatexpectations": lambda source: json.loads(source)["meta"]["validation_id"],
    "columnar": lambda source: None,
}


def by_name(value):
    return getattr(value, "name", value)


# ------------------------------------------------------------- provenance


@pytest.mark.parametrize("example, system", SOURCES, ids=by_name)
def test_every_imported_outcome_carries_where_it_came_from(example, system):
    reference = f"evidence/{example.name}"
    source = example.read_bytes()
    outcomes = harvest(source, reference=reference)
    assert outcomes
    run = RUN_OF[system](source)
    for outcome in outcomes:
        origin = outcome["details"]["origin"]
        assert origin["system"] == system
        assert origin["reference"] == reference  # as the caller wrote it
        assert origin["sha256"] == hashlib.sha256(source).hexdigest()
        assert origin.get("run") == run
        assert set(origin) == (
            {"system", "reference", "sha256", "run"}
            if run
            else {"system", "reference", "sha256"}
        )


def test_the_digest_is_of_the_bytes_that_were_parsed():
    first = import_bytes(b'[{"check": "a", "success": true}]', "checks.json")
    second = import_bytes(b'[{"check": "a", "success": false}]', "checks.json")
    digests = {o["details"]["origin"]["sha256"] for o in first + second}
    assert digests == {
        hashlib.sha256(b'[{"check": "a", "success": true}]').hexdigest(),
        hashlib.sha256(b'[{"check": "a", "success": false}]').hexdigest(),
    }


def test_one_outcomes_provenance_is_not_anothers():
    outcomes = import_json(
        [{"check": "a", "success": True}, {"check": "b", "success": True}]
    )
    outcomes[0]["details"]["origin"]["reference"] = "somewhere else"
    assert outcomes[1]["details"]["origin"]["reference"] == "source"


# -------------------------------------------------------- outcome contract


@pytest.mark.parametrize("example, system", SOURCES, ids=by_name)
def test_every_imported_outcome_crosses_the_outcome_gate(example, system):
    outcomes = harvest(example.read_bytes(), reference=example.name)
    assert [outcome.to_legacy() for outcome in normalize_outcomes(outcomes)] == outcomes


@pytest.mark.parametrize("example, system", SOURCES, ids=by_name)
def test_importing_never_widens_the_outcome_contract(example, system):
    for outcome in harvest(example.read_bytes(), reference=example.name):
        assert set(outcome) == {"check", "success", "details"}
        assert type(outcome["success"]) is bool
        assert outcome["details"]["criticality"] in ("error", "warn")


def test_an_outcome_that_cannot_be_recorded_is_rejected_naming_its_record():
    too_deep: dict = {}
    node = too_deep
    for _ in range(20):
        node["deeper"] = {}
        node = node["deeper"]
    record = {"check": "too_deep", "success": True, "details": too_deep}
    with pytest.raises(
        HarvestError, match="record 0: 'too_deep' cannot be recorded"
    ) as refused:
        import_json([record])
    assert (refused.value.position, refused.value.record) == (0, "too_deep")


def test_a_batch_over_the_outcome_limits_is_rejected():
    records = [{"check": f"c{index}", "success": True} for index in range(10_001)]
    with pytest.raises(HarvestError, match="the source cannot be recorded as evidence"):
        import_json(records)


def test_two_data_tests_with_one_unique_id_are_rejected():
    document = dbt_document(
        [data_test("not_null_id", "pass"), data_test("not_null_id", "fail")]
    )
    with pytest.raises(HarvestError, match="Duplicate check label"):
        import_json(document)


# ------------------------------------------------------- telling sources apart


@pytest.mark.parametrize("example, system", SOURCES, ids=by_name)
def test_the_system_is_told_from_the_document(example, system):
    assert detect(example.read_bytes()) == system


def test_nothing_else_is_recognised():
    with pytest.raises(HarvestError, match="The source is not recognised"):
        import_bytes(b'{"hello": [1, 2]}')
    with pytest.raises(HarvestError, match="The source is not recognised"):
        import_bytes(b"")


def test_an_unknown_system_is_refused():
    with pytest.raises(HarvestError, match="Unknown source system 'soda'"):
        import_bytes(CSV_EXAMPLE.read_bytes(), system="soda")


def test_stating_the_system_skips_detection_and_its_verdict_stands():
    """A dbt artifact read as Great Expectations says what it lacks."""
    with pytest.raises(
        HarvestError, match="is not a Great Expectations validation result"
    ):
        harvest(DBT_EXAMPLE.read_bytes(), reference="x", system="greatexpectations")
    outcomes = harvest(
        COLUMNAR_JSON_EXAMPLE.read_bytes(), reference="x", system="columnar"
    )
    assert len(outcomes) == 3


def test_the_harvesters_are_found_by_the_name_of_their_system():
    assert sorted(HARVESTERS) == ["columnar", "dbt", "greatexpectations"]
    for system, harvester in HARVESTERS.items():
        assert isinstance(harvester, Harvester)
        assert harvester.system == system
    with pytest.raises(TypeError):
        HARVESTERS["soda"] = None  # a mapping others cannot extend by accident


# ----------------------------------------------------------------- reading


def test_the_shared_reader_keeps_its_wording_for_a_rule_pack():
    """``sha256_file_reference`` is built on it; what it said, it still says."""
    from dq.report import read_local_file, sha256_file_reference

    with pytest.raises(
        ConfigurationError, match="Could not read configuration for evidence"
    ):
        sha256_file_reference("nowhere/at/all.conf")
    with pytest.raises(
        ConfigurationError, match="Could not read configuration for evidence"
    ):
        read_local_file("nowhere/at/all.conf")
    with pytest.raises(ConfigurationError, match="require a local configuration file"):
        read_local_file("https://example.test/rules.conf")


def test_a_source_may_carry_a_byte_order_mark():
    (outcome,) = import_bytes(b'\xef\xbb\xbf[{"check": "a", "success": true}]')
    assert outcome["check"] == "a"


def test_a_source_that_is_not_utf_8_is_rejected():
    with pytest.raises(HarvestError, match="is not UTF-8 text"):
        import_bytes(b"\xff\xfe\x00\x00", system="columnar")


@pytest.mark.parametrize(
    "source, problem",
    [
        (b'[{"check": "a", ', "is not valid JSON"),
        (b'[{"check": "a", "success": NaN}]', "is not valid JSON"),
        (b'[{"check": "a", "success": Infinity}]', "is not valid JSON"),
        (
            b'[{"check": "a", "check": "b", "success": true}]',
            "the key 'check' repeats in one object",
        ),
        (b"[] trailing", "is not valid JSON"),
    ],
)
def test_json_that_parsers_disagree_about_is_rejected(source, problem):
    with pytest.raises(HarvestError, match=problem):
        import_bytes(source, system="columnar")


def test_a_source_over_the_size_limit_is_rejected(monkeypatch):
    monkeypatch.setattr(base, "MAX_SOURCE_BYTES", 16)
    with pytest.raises(HarvestError, match="more than the 16 a source may be"):
        import_bytes(b'[{"check": "a", "success": true}]')


@pytest.mark.parametrize("reference", ["", "   ", None, 7])
def test_a_source_is_imported_under_a_reference(reference):
    with pytest.raises(HarvestError, match="imported under a reference"):
        import_bytes(b'[{"check": "a", "success": true}]', reference)


# --------------------------------------------------------------- refusing


def test_a_rejected_source_is_a_configuration_error():
    """The input is to be fixed, not the data: a CLI that reads that meaning
    exits 2 for it, as `dq-kernel` does for a rule pack it cannot load."""
    assert issubclass(HarvestError, ConfigurationError)


def test_a_refusal_says_what_was_being_read():
    document = gx_document(
        [gx_result("expect_table_row_count_to_equal"), "not an object"]
    )
    with pytest.raises(HarvestError) as refused:
        import_json(document)
    error = refused.value
    assert str(error) == (
        "greatexpectations source 'source': record 1 is not an object"
    )
    assert (error.system, error.position, error.record) == (
        "greatexpectations",
        1,
        None,
    )


def test_a_refusal_of_the_whole_source_points_at_the_source():
    with pytest.raises(
        HarvestError, match="^columnar source 'x': the source holds no check results"
    ):
        HARVESTERS["columnar"].harvest(b"[]", reference="x")


def test_reject_is_the_one_place_an_import_is_refused():
    with pytest.raises(
        HarvestError, match="^dbt source 'r': record 3: 't' has a problem"
    ):
        base.reject("dbt", "r", "has a problem", position=3, record="t")
    with pytest.raises(HarvestError, match="^dbt source 'r': record 3 has a problem"):
        base.reject("dbt", "r", "has a problem", position=3)
    with pytest.raises(HarvestError, match="^dbt source 'r': row 9: 't' has a problem"):
        base.reject("dbt", "r", "has a problem", position=3, record="t", where="row 9")


# ------------------------------------------------ no vendor library at all


def test_the_package_imports_and_harvests_with_every_vendor_library_blocked():
    statement = """
from pathlib import Path
from dq.harvest import HARVESTERS, harvest
for name in ("run_results.json", "validation_result.json", "checks.csv", "checks.json"):
    outcomes = harvest((Path("examples/import") / name).read_bytes(), reference=name)
    assert outcomes and all(o["details"]["origin"]["sha256"] for o in outcomes)
print("harvested", sorted(HARVESTERS))
"""
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "great_expectations",
        "pandas",
        "polars",
        "pyarrow",
        "pydeequ",
        "pyspark",
        "sqlalchemy",
        statement=statement,
    )
    assert result.returncode == 0, result.stderr
    assert "harvested ['columnar', 'dbt', 'greatexpectations']" in result.stdout


# ------------------------------------------- the same gate, report and verdicts


@pytest.mark.parametrize(
    "policy, opens, blocking, advisory",
    [
        ("any", False, 5, 0),
        ("error", False, 3, 2),
        ("never", True, 0, 5),
    ],
)
def test_imported_outcomes_reach_the_gate(policy, opens, blocking, advisory):
    outcomes = harvest(DBT_EXAMPLE.read_bytes(), reference="target/run_results.json")
    decision = evaluate_gate(outcomes, policy)
    assert (decision.passed, len(decision.blocking), len(decision.advisory)) == (
        opens,
        blocking,
        advisory,
    )
    # Under any policy but `never`, the skipped test is among what blocks:
    # it never ran, and no verdict is not a pass.
    skipped = "test.probe.not_null_customers_status.355d9d2596"
    assert (skipped in decision.blocking) is (policy != "never")


def test_imported_outcomes_reach_a_report():
    source = DBT_EXAMPLE.read_bytes()
    outcomes = harvest(source, reference="target/run_results.json")
    report = build_report(
        results=outcomes,
        config_reference="target/run_results.json",
        config_sha256=hashlib.sha256(source).hexdigest(),
        dataset_id="orders",
        dataset_sha256="b" * 64,
        application_id=INVOCATION,
        spark_version=None,
    )
    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["rule_set"] == {
        "reference": "target/run_results.json",
        "sha256": hashlib.sha256(source).hexdigest(),
    }
    assert report["summary"] == {
        "total": 6,
        "passed": 1,
        "failed": 5,
        "blocked": 3,
        "warned": 2,
    }
    assert report["checks"] == outcomes
    assert report["runtime"]["spark"] is None
