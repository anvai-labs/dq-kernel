# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract: specs are tracked honestly (SPEC-008).

The spec files are the source of truth. These tests hold the whole set to
the rules in SPEC-008, hold the tracker table to what the specs generate,
and -- because a validator is only worth trusting if it can fail -- mutate
a real spec to prove each drift class is rejected.
"""

import dataclasses
import re
import subprocess
import sys
from pathlib import Path

import pytest

from dq.tests import spec_support
from dq.tests.spec_support import (
    ADR_DIR,
    CHANGELOG_PATH,
    REPOSITORY_ROOT,
    ROADMAP_PATH,
    SPECS_DIR,
    changelog_versions,
    existing_adr_numbers,
    load_specs,
    parse_spec,
    render_summary,
    render_tracker,
    spec_problems,
    specs_problems,
    tracker_block,
    tracker_summary_block,
)

HISTORICAL_DOCS = (
    "improvements-index.adoc",
    "future-enhancements.adoc",
    "architecture-improvements.adoc",
    "performance-optimization.adoc",
    "technical-debt-analysis.adoc",
)

#: Every proposal the historical documents make. Each must have a row in
#: the triage table of docs/roadmap.adoc.
PROPOSAL_INVENTORY = (
    "Remove executable configuration (`eval`)",
    "SQL injection hardening",
    "Replace driver `collect()`",
    "Input validation",
    "Inheritance hierarchy",
    "Error handling",
    "Configuration validation",
    "Caching system",
    "Plugin architecture",
    "Parallel processing",
    "Real-time monitoring",
    "REST API",
    "Multi-environment configuration",
    "Typed configuration schema",
    "Structured logging",
    "Circuit breaker",
    "Query optimization",
    "Property-based testing",
    "Performance scaling tests",
    "Streaming data quality",
    "Alerting system",
    "Data quality dashboards",
    "ML anomaly detection",
    "AI-assisted rule optimization",
    "Layered microkernel and domain services",
    "Event bus",
    "Partition optimization",
    "Predicate pushdown",
    "Test coverage target (85%)",
    "Code duplication",
    "Control plane and marketplace offers (MVP-2)",
    "Coexistence guides for Lakehouse Monitoring and Glue DQ",
)

DISPOSITIONS = (
    "Shipped",
    "Shipped in part",
    "Shipped in substance",
    "Exceeded",
    "Deployment",
    "SPEC-005",
    "SPEC-010",
    "SPEC-011",
    "SPEC-012, SPEC-020",
    "SPEC-013",
    "SPEC-014",
    "SPEC-022",
    "Deferred",
    "Rejected",
)


@pytest.fixture(scope="module")
def specs():
    return load_specs()


@pytest.fixture(scope="module")
def context(specs):
    numbers = {spec.number for spec in specs}
    return specs, numbers, existing_adr_numbers(), changelog_versions()


def _problems_of(spec, context):
    _, numbers, adrs, versions = context
    return spec_problems(spec, numbers, adrs, versions)


# ------------------------------------------------------------- the real set


def test_spec_set_is_valid():
    assert specs_problems(load_specs()) == []


def test_spec_ids_are_unique_and_contiguous():
    numbers = [spec.number for spec in load_specs()]
    assert numbers == list(range(1, len(numbers) + 1))


def test_tracker_table_is_up_to_date(specs):
    on_disk = tracker_block(ROADMAP_PATH.read_text(encoding="utf-8")).strip("\n")
    assert on_disk == render_tracker(specs).strip("\n")


def test_tracker_summary_is_up_to_date(specs):
    on_disk = tracker_summary_block(ROADMAP_PATH.read_text(encoding="utf-8")).strip(
        "\n"
    )
    assert on_disk == render_summary(specs).strip("\n")


def test_roadmap_check_mode_exits_zero():
    result = subprocess.run(
        [sys.executable, str(REPOSITORY_ROOT / "scripts" / "roadmap.py"), "--check"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PROBLEM" not in result.stdout and "STALE" not in result.stdout


def test_tracker_detects_a_stale_table(specs):
    """A tracker row that no longer matches its spec must not pass."""
    fresh = tracker_block(ROADMAP_PATH.read_text(encoding="utf-8")).strip("\n")
    changed = dataclasses.replace(
        specs[0], fields={**specs[0].fields, "Status": "Proposed"}
    )
    assert render_tracker((changed,) + specs[1:]).strip("\n") != fresh
    # A document without markers yields no table, which can never equal a render.
    assert tracker_block("no markers here") == ""


def test_evidence_citations_name_real_tests(specs):
    """Every `path::test` cited as evidence exists."""
    missing = []
    for spec in specs:
        for checked, gate_id, text in spec.progress:
            if not checked or "evidence:" not in text.lower():
                continue
            path = None
            for token in re.findall(r"`([^`]+)`", text):
                if token.startswith("dq/tests/") and ".py" in token:
                    path, _, test = token.partition("::")
                    target = REPOSITORY_ROOT / path
                    if not target.exists():
                        missing.append(f"{spec.id} {gate_id}: {path} does not exist")
                        continue
                    if test and not re.search(
                        rf"^def {re.escape(test)}\b",
                        target.read_text(encoding="utf-8"),
                        re.M,
                    ):
                        missing.append(
                            f"{spec.id} {gate_id}: {path} has no test {test}"
                        )
                elif token.startswith("::") and path:
                    test = token[2:]
                    source = (REPOSITORY_ROOT / path).read_text(encoding="utf-8")
                    if not re.search(rf"^def {re.escape(test)}\b", source, re.M):
                        missing.append(
                            f"{spec.id} {gate_id}: {path} has no test {test}"
                        )
    assert missing == []


@pytest.mark.parametrize("name", HISTORICAL_DOCS)
def test_historical_docs_point_at_the_tracker(name):
    text = (REPOSITORY_ROOT / "docs" / name).read_text(encoding="utf-8")
    assert "roadmap.adoc" in text, f"{name} does not point at the tracker"


def test_triage_table_covers_the_proposal_inventory():
    text = ROADMAP_PATH.read_text(encoding="utf-8")
    section = text.split("== Historical proposals: triage", 1)[1].split("\n== ", 1)[0]
    body = section.split("|===", 1)[1].rsplit("|===", 1)[0]
    # An AsciiDoc block table: rows are separated by a blank line and carry
    # one cell per line. The disposition is the second cell of a row; a
    # wrapped cell continues on indented lines, which are not cells.
    by_label = {}
    for row in body.strip().split("\n\n"):
        cells = [line[2:].strip() for line in row.split("\n") if line.startswith("| ")]
        if len(cells) >= 2:
            by_label[cells[0]] = cells[1]
    missing = [label for label in PROPOSAL_INVENTORY if label not in by_label]
    assert missing == [], f"proposals without a triage row: {missing}"
    unknown = {
        label: where for label, where in by_label.items() if where not in DISPOSITIONS
    }
    assert unknown == {}, f"triage rows with an unknown disposition: {unknown}"


def test_every_adr_has_a_status_and_a_date():
    problems = []
    for path in sorted(ADR_DIR.glob("ADR-*.adoc")):
        text = path.read_text(encoding="utf-8")
        if not re.search(r"^\*? ?Status: ", text, re.M):
            problems.append(f"{path.name}: no Status line")
        if not re.search(r"^\*? ?Date: \d{4}-\d{2}-\d{2}", text, re.M):
            problems.append(f"{path.name}: no Date line")
    assert problems == []


# ------------------------------------------------- the validator can fail


def _released_spec(specs):
    return next(spec for spec in specs if spec.status == "Released")


def test_validator_rejects_unknown_status(specs, context):
    spec = dataclasses.replace(_released_spec(specs), fields={"Status": "Mostly Done"})
    assert any("unknown status" in problem for problem in _problems_of(spec, context))


def test_validator_rejects_verified_with_an_unchecked_gate(specs, context):
    """A progress line that exists but is unchecked must not pass as Verified."""
    spec = _released_spec(specs)
    second = spec.progress[1]
    progress = (spec.progress[0], (False, second[1], second[2])) + spec.progress[2:]
    mutated = dataclasses.replace(spec, progress=progress)
    assert any(
        f"Released requires gate {second[1]} to be checked" in problem
        for problem in _problems_of(mutated, context)
    )


def test_validator_rejects_verified_with_a_missing_progress_line(specs, context):
    spec = _released_spec(specs)
    mutated = dataclasses.replace(spec, progress=spec.progress[:1])
    assert any(
        "Released requires a Progress line for every gate" in problem
        for problem in _problems_of(mutated, context)
    )


def test_validator_rejects_verified_without_evidence(specs, context):
    spec = _released_spec(specs)
    progress = tuple(
        (checked, gate_id, "done, trust me") for checked, gate_id, _ in spec.progress
    )
    mutated = dataclasses.replace(spec, progress=progress)
    problems = _problems_of(mutated, context)
    assert problems and all("requires an 'evidence:' citation" in p for p in problems)


def test_validator_rejects_missing_section(specs, context):
    spec = _released_spec(specs)
    sections = {
        name: body for name, body in spec.sections.items() if name != "Out of scope"
    }
    mutated = dataclasses.replace(spec, sections=sections)
    assert any(
        "missing or empty section 'Out of scope'" in p
        for p in _problems_of(mutated, context)
    )


def test_validator_rejects_missing_header_field(specs, context):
    spec = _released_spec(specs)
    fields = {name: value for name, value in spec.fields.items() if name != "Milestone"}
    mutated = dataclasses.replace(spec, fields=fields)
    assert any(
        "missing header field 'Milestone'" in p for p in _problems_of(mutated, context)
    )


def test_validator_rejects_unknown_gate_in_progress(specs, context):
    spec = _released_spec(specs)
    progress = ((True, "G9", "evidence: nowhere"),) + spec.progress
    mutated = dataclasses.replace(spec, progress=progress)
    assert any("cites unknown gate G9" in p for p in _problems_of(mutated, context))


def test_validator_rejects_non_contiguous_gates(specs, context):
    spec = _released_spec(specs)
    gates = spec.gates + (dataclasses.replace(spec.gates[0], id="G7"),)
    mutated = dataclasses.replace(
        spec, progress=spec.progress + ((False, "G7", ""),), gates=gates
    )
    assert any("contiguous from G1" in p for p in _problems_of(mutated, context))


def test_validator_rejects_dangling_adr_reference(specs, context, tmp_path):
    source = SPECS_DIR / "SPEC-001-evidence-plane-rollout.adoc"
    copy = tmp_path / source.name
    copy.write_text(
        source.read_text(encoding="utf-8").replace(
            "Links: TD-PROD-1; ADR-006; ADR-007; SPEC-006; SPEC-007.",
            "Links: TD-PROD-1; ADR-099; SPEC-006.",
        ),
        encoding="utf-8",
    )
    problems = _problems_of(parse_spec(copy), context)
    assert any("references missing ADR-099" in p for p in problems)


def test_validator_rejects_dangling_spec_reference(specs, context, tmp_path):
    source = SPECS_DIR / "SPEC-001-evidence-plane-rollout.adoc"
    copy = tmp_path / source.name
    copy.write_text(
        source.read_text(encoding="utf-8").replace(
            "Links: TD-PROD-1; ADR-006; ADR-007; SPEC-006; SPEC-007.",
            "Links: TD-PROD-1; ADR-006; SPEC-099.",
        ),
        encoding="utf-8",
    )
    problems = _problems_of(parse_spec(copy), context)
    assert any("references missing SPEC-099" in p for p in problems)


def test_validator_rejects_released_without_a_changelog_version(specs, context):
    _, numbers, adrs, _ = context
    spec = _released_spec(specs)
    problems = spec_problems(spec, numbers, adrs, released_versions=set())
    assert any("has no heading in CHANGELOG.md" in p for p in problems)


def test_validator_rejects_skipping_verified(specs, context):
    spec = _released_spec(specs)
    fields = {**spec.fields, "Status": "Verified"}
    progress = tuple((False, gate_id, "") for _, gate_id, _ in spec.progress)
    mutated = dataclasses.replace(spec, fields=fields, progress=progress)
    problems = _problems_of(mutated, context)
    assert any("Verified requires gate G1 to be checked" in p for p in problems)


def test_validator_rejects_in_progress_with_no_gate_met(specs, context):
    spec = _released_spec(specs)
    progress = tuple((False, gate_id, "") for _, gate_id, _ in spec.progress)
    mutated = dataclasses.replace(
        spec, fields={**spec.fields, "Status": "In Progress"}, progress=progress
    )
    assert any(
        "In Progress requires at least one checked gate" in p
        for p in _problems_of(mutated, context)
    )


def test_validator_rejects_unreasoned_deferred(specs, context):
    spec = _released_spec(specs)
    mutated = dataclasses.replace(spec, fields={**spec.fields, "Status": "Deferred"})
    assert any("Deferred must state why" in p for p in _problems_of(mutated, context))


def test_validator_rejects_non_contiguous_spec_ids(specs, context):
    numbers = [spec.number for spec in specs]
    problems = spec_support.specs_problems(
        specs[1:], adr_numbers=context[2], released_versions=context[3]
    )
    assert any("contiguous from 001" in p for p in problems)
    assert numbers == sorted(numbers)


def test_validator_rejects_a_malformed_document(tmp_path):
    broken = tmp_path / "SPEC-042-broken.adoc"
    broken.write_text(
        "= Not a spec title\n\n== Purpose\n\nNothing.\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="SPEC-NNN"):
        parse_spec(broken)
