# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Python the documents show over Arrow is the Python that runs (SPEC-015).

A snippet that does not run is worse than none: one was written into the
operator reference binding a frame without a column its plan needed, and
only running it showed that. Each Python block named here is read out of
its document and executed, so it cannot go stale unnoticed.
"""

import re
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[2] / "docs"

#: (document, the heading or anchor the block follows, what it must define)
SNIPPETS = (
    (
        "index.adoc",
        "=== From Python, over any Arrow table",
        "outcomes",
    ),
    (
        "compute-engines.adoc",
        "== pandas and Polars with the portable semantics",
        "outcome",
    ),
)


def python_block_after(document, heading):
    """The first Python listing that follows ``heading`` in ``document``."""
    lines = document.split("\n")
    start = lines.index(heading)
    opened = lines.index("[source,python]", start)
    assert lines[opened + 1] == "----", "a Python listing opens with ----"
    closed = lines.index("----", opened + 2)
    return "\n".join(lines[opened + 2 : closed])


@pytest.mark.parametrize("document, heading, defines", SNIPPETS)
def test_the_documented_python_runs(document, heading, defines):
    pytest.importorskip("polars", reason="the documented frames are Polars frames")
    pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")
    source = python_block_after((DOCS / document).read_text(encoding="utf-8"), heading)
    assert re.search(
        rf"^\(?\b{defines}\b", source, re.M
    ), "the snippet no longer shows its result"
    namespace = {}
    exec(
        compile(source, f"{document}:{heading}", "exec"), namespace
    )  # noqa: S102 - the repo's own document
    result = namespace[defines]
    outcomes = list(result) if isinstance(result, (list, tuple)) else [result]
    assert outcomes and all(
        outcome.to_legacy()["details"]["adapter"] == {"name": "arrow", "version": "1"}
        for outcome in outcomes
    )


def test_each_snippet_is_found_where_it_is_said_to_be():
    """A renamed heading would leave the test above with nothing to run."""
    for document, heading, _ in SNIPPETS:
        text = (DOCS / document).read_text(encoding="utf-8")
        assert heading in text.split("\n"), (document, heading)
        assert "import" in python_block_after(text, heading)
