# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Scaffold a new engine (SPEC-011 G4): `python scripts/new_engine.py NAME`.

Generates an engine on the ADR-012 hooks with a valid capability manifest,
labelled checks, its unit tests and an example rule pack, and updates the
repository's guards so the tree stays green without manual edits: the engine
registry, the registry's own snapshot test, and the error catalog.

The author fills in two things: the vendor call (``_evaluate``) and, when
the vendor library is an optional dependency, the extra and the lazy import.
"""

import re
from pathlib import Path
from typing import List

_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
HEADER = (
    "# Copyright 2024 Data Quality Framework Contributors\n"
    "# SPDX-License-Identifier: Apache-2.0\n\n"
)


class ScaffoldError(ValueError):
    """The scaffold cannot proceed; the message says what to change."""


def class_name(name: str) -> str:
    """Return the engine class name the loader expects (``name.capitalize()``)."""
    return f"{name.capitalize()}Engine"


def engine_module_path(name: str) -> str:
    """Return the engine's dotted module path."""
    return f"dq.engine.{name}.{name}_engine"


ENGINE_TEMPLATE = '''{header}"""{NAME} engine (scaffolded): one line on what it validates.

Two markers are left to fill in, both marked TODO: the vendor call in
``_evaluate`` and, when the vendor library is an optional dependency, the
lazy import in ``_prepare``. The hooks, the capability manifest and the
result shaping already follow the engine contract; see
``docs/writing-an-engine.adoc``.
"""

import logging
from typing import Any, List

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.engine.{name}.{name}_check import {Class}Check



class {Class}(DQEngine):
    """Validates a Spark DataFrame with <the vendor library>."""

    # What this engine can really do. `criticality` is native when the engine
    # has per-rule severity, derived when the kernel maps one onto it, and
    # absent when it has none. `persistence` stays False until _execute
    # exists and the engine can hand artifacts to a repository.
    CAPABILITIES = {{
        "criticality": "absent",
        "row_filter": False,
        "failed_rows": False,
        "persistence": False,
    }}

    def _prepare(self, dataframe) -> List[Any]:
        """Build the checks once per engine.

        An optional vendor library is imported here, lazily, through
        ``dq.utils.imports.require_extra``.
        """
        return {Class}Check(self._config).build_checks()

    def _execute(self, dataframe, prepared) -> List[dict]:
        """Run the checks and return one result per check.

        Build every result with ``dq.engine.checkspec.build_result`` so it
        carries the contract's shape, and give it a canonical ``criticality``
        when the engine reports severity.
        """
        return self._evaluate(dataframe, prepared)

    def _evaluate(self, dataframe, checks) -> List[dict]:
        """TODO: replace the body with the vendor library's call."""
        raise NotImplementedError("{Class}._evaluate is not implemented yet")
'''

CHECK_TEMPLATE = '''{header}"""Check builder for the {name} engine: validates the ``checks`` payload.

The payload is a list of check mappings in this engine's own vocabulary.
The kernel adds only what every engine shares: a label per check, unique
within the rule.
"""

from typing import List

from pyhocon import ConfigTree

from dq.engine import checkspec


class {Class}Check:
    """Validates the rule's ``checks`` payload and labels each check."""

    def __init__(self, config: ConfigTree):
        self._checks = config.get("checks", None)

    def build_checks(self) -> List[dict]:
        """Return one copy of each configured check.

        Raises:
            ValueError: When ``checks`` is empty, or two checks would report
                the same label.
        """
        if not self._checks:
            raise ValueError("The {name} engine requires a non-empty 'checks' list")
        # A check is named by its `name`, else by its kind and columns.
        labels = checkspec.labels_for_checks(
            self._checks, ("name",), "check", ("column", "columns")
        )
        checkspec.require_unique_labels(labels)
        return [dict(check) for check in self._checks]
'''

TEST_TEMPLATE = '''{header}"""Unit contracts for the scaffolded {name} engine.

Runs with no Spark and no vendor library: the vendor call is replaced by a
fixed answer. Replace these with real tests as the engine takes shape.
"""

import pytest
from pyhocon import ConfigFactory

from dq.engine.{name}.{name}_engine import {Class}
from dq.exceptions import ConfigurationError


class FakeEngine({Class}):
    """The scaffold with the vendor call replaced by a fixed answer."""

    def __init__(self, checks):
        super().__init__(ConfigFactory.from_dict({{"checks": checks}}))
        self.prepared = None

    def _evaluate(self, dataframe, checks):
        self.prepared = checks
        return []


def test_each_configured_check_reaches_the_vendor_call_once():
    engine = FakeEngine(
        [
            {{"check": "not_null", "column": "id"}},
            {{"name": "named", "check": "not_null", "column": "id"}},
        ]
    )
    engine._execute(object(), engine._prepare(object()))
    assert [check["check"] for check in engine.prepared] == ["not_null", "not_null"]


def test_a_check_mapping_is_copied_not_shared_with_the_configuration():
    engine = FakeEngine([{{"check": "not_null", "column": "id"}}])
    built = engine._prepare(object())
    built[0]["column"] = "changed"
    assert engine._prepare(object())[0]["column"] == "id"


def test_the_scaffold_refuses_a_repository_until_it_can_persist():
    engine = FakeEngine([{{"check": "not_null", "column": "id"}}])
    with pytest.raises(ConfigurationError, match="persistence=False"):
        engine.apply(object(), repository=object())


def test_the_vendor_call_is_not_implemented_yet():
    engine = {Class}(ConfigFactory.from_dict({{"checks": [1]}}))
    with pytest.raises(NotImplementedError):
        engine._execute(object(), [])


def test_an_empty_checks_payload_is_refused():
    with pytest.raises(ValueError, match="non-empty 'checks'"):
        {Class}(ConfigFactory.from_dict({{"checks": []}}))._prepare(object())


def test_two_checks_that_would_share_a_label_are_refused():
    twins = [
        {{"check": "not_null", "column": "id"}},
        {{"name": "not_null_id", "check": "not_null", "column": "name"}},
    ]
    with pytest.raises(ValueError, match="Duplicate check label"):
        {Class}(ConfigFactory.from_dict({{"checks": twins}}))._prepare(object())
'''

EXAMPLE_TEMPLATE = """# {NAME} engine example (scaffolded). The checks below are placeholders in
# this engine's vocabulary; replace them once the vendor call exists.
dqframework {{
  dqrules = [
    {{
      name = "{name}_checks"
      engine = "{name}"
      dataframes = ["default"]
      checks = [
        {{ check = "not_null", column = "id" }}
        {{ name = "names_present", check = "not_null", column = "name" }}
      ]
    }}
  ]
}}
"""

NEXT_STEPS = """
Scaffolded the '{name}' engine:

  dq/engine/{name}/{name}_engine.py   hooks, manifest, and a TODO in _evaluate
  dq/engine/{name}/{name}_check.py    the checks payload and its labels
  dq/tests/test_{name}_unit.py        unit contracts, no Spark needed
  examples/{name}_validation.conf     a placeholder rule pack

and updated the registry, its snapshot test, and the error catalog.

Next:
  1. Implement {Class}Engine._evaluate: one build_result() per check.
  2. If the vendor library is optional: add it to pyproject.toml as an
     optional dependency, declare the extra, import it in _prepare through
     dq.utils.imports.require_extra, and set `extra` and `vendor_imports`
     on the engine's EngineSpec.
  3. Set CAPABILITIES to what the engine really does.
  4. Prove it: run dq.sdk.conformance.assert_engine_conforms against a frame.
  5. Add a section for the engine to docs/index.adoc.

The check ({name} on checks) validates and loads already; running it
raises NotImplementedError until step 1 is done.
"""


def _format(source: str) -> str:
    """Format generated Python with black when it is importable."""
    try:
        import black

        return black.format_str(source, mode=black.Mode())
    except Exception:  # noqa: BLE001 - formatting is a convenience, not a gate
        return source


def _insert_registry_spec(source: str, name: str, spec: str) -> str:
    """Insert an EngineSpec at the end of the registry's tuple literal."""
    opener = "BUILT_IN_ENGINES: Dict[str, EngineSpec] = {"
    if opener not in source:
        raise ScaffoldError(
            "dq/engine/registry.py has no BUILT_IN_ENGINES literal to extend"
        )
    close = source.find("\n    )\n}", source.index(opener))
    if close == -1:
        raise ScaffoldError(
            "dq/engine/registry.py: the BUILT_IN_ENGINES literal was not recognised"
        )
    return source[:close] + "\n" + spec + source[close + 1 :]


def _insert_registry_test_name(source: str, name: str) -> str:
    """Add the engine to the registry snapshot test's expected set."""
    marker = "def test_built_in_registry_covers_the_known_engines():"
    if marker not in source:
        raise ScaffoldError("dq/tests/test_registry.py has no snapshot test to update")
    head, tail = source.split(marker, 1)
    close = tail.find("\n    }")
    if close == -1:
        raise ScaffoldError(
            "dq/tests/test_registry.py: the snapshot's expected set was not recognised"
        )
    # Insert a line of our own before the set closes; every existing entry stays.
    return head + marker + tail[:close] + f'\n        "{name}",' + tail[close:]


def _extend_error_catalog(source: str, name: str, raise_sites_added: int) -> str:
    """Add the check builder's section and bump the stated raise-site total."""
    match = re.search(r"The framework raises (\d+) raise sites", source)
    if match is None:
        raise ScaffoldError(
            "docs/error-catalog.adoc does not state its raise-site total"
        )
    total = int(match.group(1)) + raise_sites_added
    source = source.replace(
        match.group(0), f"The framework raises {total} raise sites", 1
    )
    return source.rstrip("\n") + f"""

== dq/engine/{name}/{name}_check.py

The scaffolded {name} engine's checks payload.

* `ValueError` - the rule has no `checks` list, or two of its checks would
  report the same label. Give every check a distinct `name`, or checks that
  differ in kind or columns.
"""


def scaffold(root: Path, name: str, force: bool = False) -> List[Path]:
    """Scaffold the engine ``name`` under ``root``.

    Everything is computed and checked before anything is written, so a
    refused scaffold leaves the tree exactly as it was.

    Args:
        root: The repository root to write into.
        name: The engine name: a lowercase identifier.
        force: Overwrite generated files that already exist.

    Returns:
        Every file created or changed, in the order touched.

    Raises:
        ScaffoldError: For an invalid name, an engine that already exists, or
            a file the scaffold does not recognise.
    """
    if not _NAME.match(name or ""):
        raise ScaffoldError(
            f"Engine name {name!r} must be a lowercase identifier "
            "(letters, digits, underscores; not starting with a digit)"
        )
    cls = class_name(name)
    engine_dir = root / "dq" / "engine" / name

    new_files = {
        engine_dir / "__init__.py": HEADER.rstrip("\n") + "\n",
        engine_dir
        / f"{name}_engine.py": _format(
            ENGINE_TEMPLATE.format(
                header=HEADER, NAME=name.upper(), name=name, Class=cls
            )
        ),
        engine_dir
        / f"{name}_check.py": _format(
            CHECK_TEMPLATE.format(header=HEADER, name=name, Class=cls)
        ),
        root
        / "dq"
        / "tests"
        / f"test_{name}_unit.py": _format(
            TEST_TEMPLATE.format(header=HEADER, name=name, Class=cls)
        ),
        root
        / "examples"
        / f"{name}_validation.conf": EXAMPLE_TEMPLATE.format(
            NAME=name.upper(), name=name
        ),
    }
    existing = [path for path in new_files if path.exists()]
    if existing and not force:
        raise ScaffoldError(
            f"{existing[0].relative_to(root)} already exists; use --force to overwrite it"
        )

    def read(relative: str) -> str:
        path = root / relative
        if not path.is_file():
            raise ScaffoldError(f"{path} is missing; is --root the repository root?")
        return path.read_text(encoding="utf-8")

    spec = (
        "        EngineSpec(\n"
        f'            "{name}",\n'
        f'            "{engine_module_path(name)}",\n'
        f'            "{cls}",\n'
        '            "checks",\n'
        "        ),\n"
    )
    edited = {
        root
        / "dq"
        / "engine"
        / "registry.py": _format(
            _insert_registry_spec(read("dq/engine/registry.py"), name, spec)
        ),
        root
        / "dq"
        / "tests"
        / "test_registry.py": _format(
            _insert_registry_test_name(read("dq/tests/test_registry.py"), name)
        ),
        root
        / "docs"
        / "error-catalog.adoc": _extend_error_catalog(
            read("docs/error-catalog.adoc"), name, raise_sites_added=1
        ),
    }

    touched: List[Path] = []
    for path, text in new_files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        touched.append(path)
    for path, text in edited.items():
        path.write_text(text, encoding="utf-8")
        touched.append(path)
    return touched
