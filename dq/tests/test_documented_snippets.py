# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Python the documents show for an engine or a compute adapter is the Python that runs.

Covers the Arrow adapter (SPEC-015), the SQL adapter (SPEC-016) and the
pandera engine (SPEC-017). A snippet that does not run is worse than none:
one was written into the operator reference binding a frame without a
column its plan needed, and only running it showed that; another showed a
SQL table "declared" with no columns at all. Each Python block named here
is read out of its document and executed, so it cannot go stale unnoticed.
"""

import re
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[2] / "docs"

ADAPTER = {
    "arrow": {"name": "arrow", "version": "1"},
    "sql": {"name": "sql", "version": "1"},
}


def outcomes(named, adapter, passed=None):
    """Check that ``named`` holds outcomes of ``adapter``, decided ``passed``."""

    def check(namespace):
        result = namespace[named]
        found = list(result) if isinstance(result, (list, tuple)) else [result]
        assert found and all(
            outcome.to_legacy()["details"]["adapter"] == ADAPTER[adapter]
            for outcome in found
        )
        if passed is not None:
            assert [outcome.success for outcome in found] == passed

    return check


def a_bind_from_the_environment(namespace):
    dataset = namespace["dataset"]["orders"]
    assert dataset.bind.dialect.name == "duckdb"
    assert [str(column.name) for column in dataset.table.c] == ["id"]


def checks_that_failed(*labels):
    """Check that ``failed`` holds exactly ``labels``, in the schema's order."""

    def check(namespace):
        assert namespace["failed"] == list(labels)

    return check


#: What the adapters' snippets need beyond their own extra.
ADAPTERS = ("sqlalchemy", "duckdb")

#: (document, the line the block follows, the name it must define, its check,
#: the optional packages it needs and skips without)
SNIPPETS = (
    (
        "index.adoc",
        "=== From Python, over any Arrow table",
        "outcomes",
        outcomes("outcomes", "arrow"),
        ("polars", *ADAPTERS),
    ),
    (
        "compute-engines.adoc",
        "== pandas and Polars with the portable semantics",
        "outcome",
        outcomes("outcome", "arrow", [False]),
        ("polars", *ADAPTERS),
    ),
    (
        "compute-engines.adoc",
        "== The SQL adapter",
        "outcome",
        outcomes("outcome", "sql", [False]),
        ADAPTERS,
    ),
    (
        "compute-engines.adoc",
        "A bind is built from a URL the environment holds, never from a rule pack. The",
        "dataset",
        a_bind_from_the_environment,
        ADAPTERS,
    ),
    (
        "index.adoc",
        "loaded like any other, and given its frame:",
        "failed",
        checks_that_failed("unique_id", "greater_than_or_equal_to_amount"),
        ("pandera",),
    ),
)


def python_block_after(document, anchor):
    """The first Python listing that follows the line ``anchor`` in ``document``."""
    lines = document.split("\n")
    start = lines.index(anchor)
    opened = lines.index("[source,python]", start)
    assert lines[opened + 1] == "----", "a Python listing opens with ----"
    closed = lines.index("----", opened + 2)
    return "\n".join(lines[opened + 2 : closed])


@pytest.mark.parametrize("document, anchor, defines, check, needs", SNIPPETS)
def test_the_documented_python_runs(
    document, anchor, defines, check, needs, monkeypatch
):
    for package in needs:
        pytest.importorskip(package, reason=f"this snippet needs {package}")
    # A bind in a document is built from the environment, as it says.
    monkeypatch.setenv("ORDERS_DATABASE_URL", "duckdb:///:memory:")
    source = python_block_after((DOCS / document).read_text(encoding="utf-8"), anchor)
    assert re.search(
        rf"^\(?\b{defines}\b", source, re.M
    ), "the snippet no longer shows its result"
    namespace = {}
    exec(  # noqa: S102 - the repository's own document
        compile(source, f"{document}: {anchor}", "exec"), namespace
    )
    check(namespace)


def test_each_snippet_is_found_where_it_is_said_to_be():
    """A renamed heading or a rewrapped paragraph would leave nothing to run."""
    for document, anchor, _, _, _ in SNIPPETS:
        text = (DOCS / document).read_text(encoding="utf-8")
        assert anchor in text.split("\n"), (document, anchor)
        assert "import" in python_block_after(text, anchor)
