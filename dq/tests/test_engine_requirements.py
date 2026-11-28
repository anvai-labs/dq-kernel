# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""An engine that cannot run here says so, the same way, everywhere (SPEC-011).

Measured on 2026-11-28, before anything was built:

* With pydeequ installed and no ``SPARK_VERSION`` in the environment -- any
  shell, by default -- ``dq-engines`` died with a traceback. Asking whether
  ``pydeequ.dqdl`` is installed went through ``importlib.util.find_spec``,
  which imports the parent of a dotted name, and pydeequ refuses to be
  imported without the variable.
* Where no library was installed, the loader said ``Engine module
  'dq.engine.deequ.deequ_engine' not found: No module named 'pyspark'``:
  neither the library the engine lacks nor the extra that installs it. The
  business-rules engine, which needs none of its own, was listed with no
  hint at all.
* A hint named one extra where an engine needs two. pydeequ does not depend
  on pyspark, so a reader sent to ``pip install dq-kernel[deequ]`` came back
  for a second round.
* The refusal came after Spark had started, one rule at a time; and the
  PostgreSQL sink called itself an engine.

What is installed is judged without importing anything, so the question can
be asked of every engine before any work: by ``dq-engines``, by the loader
and by the run core, in one wording.
"""

import ast
import importlib.metadata as metadata
import json
import os
import subprocess
import sys
import tomllib
import types
from pathlib import Path

import pytest

from dq.config import RuleRef
from dq.engine import registry
from dq.engine.engine_loader import EngineLoader
from dq.engine.registry import (
    BUILT_IN_ENGINES,
    EngineSpec,
    importable,
    install_hint,
    module_itself_is_missing,
    refusal,
)
from dq.exceptions import EngineNotFoundError
from dq.notifier import InMemoryNotifier
from dq.observability import InMemoryObserver
from dq.run import run_rules

ROOT = Path(__file__).resolve().parents[2]

#: What pydeequ says, and the stand-in below says, where the variable is unset.
SAYS_SPARK_VERSION = "SPARK_VERSION environment variable is required"
DEEQU_UNMET = (
    "Engine 'deequ' cannot run here: 'pyspark' and 'pydeequ' are not installed. "
    "Install them with: pip install 'dq-kernel[spark,deequ]'  "
    "(or: poetry install -E spark -E deequ)."
)
PANDERA_UNMET = (
    "Engine 'pandera' cannot run here: 'pandera' is not installed. "
    "Install it with: pip install 'dq-kernel[pandera]'  "
    "(or: poetry install -E pandera)."
)

#: A vendor that records being imported and then refuses, as pydeequ does
#: without its variable. It comes before the real one on the path.
REFUSING_PACKAGE = """\
import pathlib

pathlib.Path({marker!r}).write_text("imported")


def _get_spark_version():
    raise RuntimeError("SPARK_VERSION environment variable is required")


SPARK_VERSION = _get_spark_version()
"""


def where_pydeequ_refuses(tmp_path):
    """Return a directory to put first on the path, and the file its pydeequ
    writes if ever run.

    It holds a ``pydeequ`` like the real one on a machine without its
    variable -- with the ``dqdl`` the dqdl engine imports -- and a stand-in
    for ``pyspark`` that answers what the deequ and the schema-validation
    engines import before they reach pydeequ. Both shadow the real packages,
    so the child process sees the same machine whatever this one holds.
    """
    site = tmp_path / "site"
    (site / "pydeequ" / "dqdl").mkdir(parents=True)
    (site / "pyspark" / "sql").mkdir(parents=True)
    marker = tmp_path / "pydeequ-was-imported"
    (site / "pydeequ" / "__init__.py").write_text(
        REFUSING_PACKAGE.format(marker=str(marker)), encoding="utf-8"
    )
    (site / "pyspark" / "__init__.py").write_text("", encoding="utf-8")
    (site / "pyspark" / "sql" / "__init__.py").write_text(
        "SparkSession = DataFrame = object\n", encoding="utf-8"
    )
    return str(site), marker


def in_child(code, first_on_path=""):
    """Run ``code`` in a fresh interpreter, the checkout importable and
    ``first_on_path`` before it."""
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("COV_CORE_", "COVERAGE_", "PYTHONPATH"))
    }
    environment["PYTHONPATH"] = os.pathsep.join(
        [part for part in (first_on_path, str(ROOT)) if part]
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(ROOT),
        env=environment,
    )


# ------------------------------------------------------- asking what is installed


def test_asking_whether_a_dotted_library_is_installed_imports_nothing(tmp_path):
    """The crash this guards against: ``importlib.util.find_spec`` imports the
    parent of a dotted name, and a parent may refuse."""
    site, marker = where_pydeequ_refuses(tmp_path)
    code = f"""
import importlib.util
from pathlib import Path
from dq.engine.registry import find_spec, importable, missing_vendor_imports

was_imported = Path({str(marker)!r})

assert importable("pydeequ.dqdl") is True
assert importable("pydeequ") is True
assert missing_vendor_imports(find_spec("dqdl")) == ()
assert not was_imported.exists()
print("judged without importing")

try:
    importlib.util.find_spec("pydeequ.dqdl")
except RuntimeError as error:
    assert "SPARK_VERSION" in str(error)
else:
    raise AssertionError("the stand-in did not refuse")
assert was_imported.exists(), "the stand-in was not run: it is not pydeequ's refusal"
print("and the way it was asked before imported the parent")
"""
    done = in_child(code, site)
    assert done.returncode == 0, done.stderr + done.stdout
    assert done.stdout.splitlines() == [
        "judged without importing",
        "and the way it was asked before imported the parent",
    ]


def test_the_listing_answers_where_an_installed_library_refuses_to_be_imported(
    tmp_path,
):
    """End to end, what a reader saw: a traceback, where nothing could be
    read. Now every engine is listed, and the three that need pydeequ say
    why they cannot be used."""
    site, _marker = where_pydeequ_refuses(tmp_path)
    code = """
import io
import json
from contextlib import redirect_stdout
from dq import engines

with redirect_stdout(io.StringIO()) as captured:
    text_code = engines.main([])
    json_code = engines.main(["--format", "json"])
print(json.dumps({"codes": [text_code, json_code], "text": captured.getvalue(),
                  "engines": engines.known_engines()}))
"""
    done = in_child(code, site)
    assert done.returncode == 0, done.stderr + done.stdout
    assert "Traceback" not in done.stderr
    shown = json.loads(done.stdout)
    assert shown["codes"] == [0, 0]
    listed = {engine["name"]: engine for engine in shown["engines"]}
    for name in ("deequ", "dqdl", "schemavalidation"):
        assert listed[name]["installed"] is False
        assert listed[name]["missing"] == []
        assert listed[name]["install_hint"] is None
        assert listed[name]["problem"] == "RuntimeError: " + SAYS_SPARK_VERSION
        assert f"  {name}: RuntimeError: {SAYS_SPARK_VERSION}" in shown["text"]
    assert listed["portable"]["installed"] is True
    assert "Cannot be used here:" in shown["text"]
    assert len(listed) == len(BUILT_IN_ENGINES)


@pytest.mark.parametrize(
    ("module", "installed"),
    [
        ("json", True),
        ("json.tool", True),
        ("no_such_package_at_all", False),
        ("no_such_package_at_all.sub", False),
        ("json.tool.is_a_module_not_a_package", False),
        ("importlib.metadata", True),
    ],
)
def test_what_counts_as_installed(module, installed):
    assert importable(module) is installed


def test_a_library_whose_import_was_refused_stays_refused(monkeypatch, tmp_path):
    """How a test makes a library absent: ``None`` in ``sys.modules``. The
    answer honours it, or such a test would measure nothing. A package
    already imported is asked for its submodules where it says they are."""
    monkeypatch.setitem(sys.modules, "some_vendor", None)
    assert importable("some_vendor") is False
    assert importable("some_vendor.engine") is False

    (tmp_path / "engine").mkdir()
    held = types.ModuleType("held_vendor")
    held.__path__ = [str(tmp_path)]
    monkeypatch.setitem(sys.modules, "held_vendor", held)
    assert importable("held_vendor.engine") is True
    assert importable("held_vendor.nothing") is False


def test_asking_never_raises_whatever_is_installed(monkeypatch):
    """The question is asked of every engine a pack names, so none of them
    can make asking fail."""
    monkeypatch.setattr(
        registry, "importable", lambda module: not module.startswith("pydeequ")
    )
    answers = [registry.cannot_run_here(spec) for spec in BUILT_IN_ENGINES.values()]
    assert [
        spec.name for spec, answer in zip(BUILT_IN_ENGINES.values(), answers) if answer
    ] == [
        "deequ",
        "dqdl",
        "schemavalidation",
    ]


# ------------------------------------------------------------ the declarations


def _imports(package):
    """Return every top-level module ``package``'s code imports, by statement
    or by a name handed to an import."""
    names = set()
    for path in sorted(ROOT.joinpath(*package.split(".")).rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names.update(alias.name.partition(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:
                    names.add(node.module.partition(".")[0])
            elif isinstance(node, ast.Call):
                called = node.func
                name = getattr(called, "attr", None) or getattr(called, "id", None)
                if name not in {"require_extra", "import_module", "__import__"}:
                    continue
                if node.args and isinstance(node.args[0], ast.Constant):
                    first = node.args[0].value
                    if isinstance(first, str):
                        names.add(first.partition(".")[0])
    return names


def _core_modules():
    """Return the modules the package's own dependencies provide: they need
    no extra, so no engine declares them."""
    declared = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "tool"
    ]["poetry"]["dependencies"]
    always = {
        name.lower()
        for name, need in declared.items()
        if name != "python" and not (isinstance(need, dict) and need.get("optional"))
    }
    return {
        module
        for module, distributions in metadata.packages_distributions().items()
        if any(name.lower() in always for name in distributions)
    }


def _extras():
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"][
        "poetry"
    ]["extras"]


ENGINES = list(BUILT_IN_ENGINES.values())


@pytest.mark.parametrize("spec", ENGINES, ids=lambda spec: spec.name)
def test_each_engine_declares_every_library_its_code_imports(spec):
    """The list the loader and the listing judge by. The business-rules and
    the schema-validation engines declared nothing, and were reported as an
    engine module that could not be found."""
    declared = {module.partition(".")[0] for module in spec.vendor_imports}
    allowed = declared | _core_modules() | set(sys.stdlib_module_names) | {"dq"}
    imported = _imports(spec.module.rpartition(".")[0])
    assert (
        imported <= allowed
    ), f"{spec.name} imports {sorted(imported - allowed)} and declares {sorted(declared)}"


#: How a distribution an extra names is imported, where the two differ.
IMPORTED_AS = {"databricks-labs-dqx": "databricks"}


@pytest.mark.parametrize("spec", ENGINES, ids=lambda spec: spec.name)
def test_each_declared_library_comes_from_a_declared_extra(spec):
    """A hint is worth its line only if what it names installs what is
    missing. Read from the packaging declaration, so the two cannot drift."""
    extras = _extras()
    assert set(spec.extras) <= set(extras), "an extra the package does not declare"
    provided = {
        IMPORTED_AS.get(name, name).partition(".")[0]
        for extra in spec.extras
        for name in extras.get(extra, ())
    }
    needed = {module.partition(".")[0] for module in spec.vendor_imports}
    assert needed <= provided, (
        f"{spec.name} needs {sorted(needed - provided)}; its extras "
        f"{list(spec.extras)} provide {sorted(provided)}"
    )


def test_an_engine_on_spark_names_spark_beside_its_own():
    """No extra of the package pulls in another: pydeequ, Great Expectations
    and DQX do not depend on pyspark, and each was hinted by its own extra."""
    extras = _extras()
    on_spark = [spec for spec in ENGINES if "pyspark" in spec.vendor_imports]
    assert len(on_spark) == 6
    for spec in on_spark:
        assert spec.extras[0] == "spark", spec.name
        assert "pyspark" not in {
            name for extra in spec.extras[1:] for name in extras[extra]
        }, f"{spec.name}: its own extra would have installed Spark after all"


def test_the_guide_teaches_the_declaration():
    """The page an engine author reads says what to declare, and why."""
    guide = (ROOT / "docs" / "writing-an-engine.adoc").read_text(encoding="utf-8")
    for word in ("vendor_imports", "extras", "Say what the engine needs"):
        assert word in guide, word
    assert 'extras=("spark", "soda")' in guide  # Spark beside the engine's own
    assert "cannot run here" in guide


# ---------------------------------------------------------------- the wording


@pytest.mark.parametrize(
    ("engine", "modules", "said"),
    [
        (
            "pandera",
            ("pandera",),
            "Engine 'pandera' cannot run here: 'pandera' is not installed.",
        ),
        (
            "deequ",
            ("pyspark", "pydeequ"),
            "Engine 'deequ' cannot run here: 'pyspark' and 'pydeequ' are not installed.",
        ),
        (
            "x",
            ("a", "b", "c"),
            "Engine 'x' cannot run here: 'a', 'b' and 'c' are not installed.",
        ),
    ],
)
def test_the_refusal_names_what_is_missing(engine, modules, said):
    assert refusal(engine, modules) == said


def test_the_refusal_names_how_to_install_it_when_an_extra_does():
    assert (
        refusal("pandera", ("pandera",), install_hint(BUILT_IN_ENGINES["pandera"]))
        == PANDERA_UNMET
    )
    assert (
        refusal(
            "deequ", ("pyspark", "pydeequ"), install_hint(BUILT_IN_ENGINES["deequ"])
        )
        == DEEQU_UNMET
    )


def test_an_engine_with_no_extra_is_told_to_install_nothing():
    """An engine from elsewhere, registered by entry point: its own
    documentation says how it is installed. No extra of this package does."""
    spec = EngineSpec("soda", "soda_dq.kernel", "SodaEngine", None)
    assert install_hint(spec) is None
    assert (
        refusal("soda", ("soda",), install_hint(spec))
        == "Engine 'soda' cannot run here: 'soda' is not installed."
    )


def test_the_engines_are_judged_once_each_in_the_order_named(monkeypatch):
    monkeypatch.setattr(
        registry,
        "importable",
        lambda module: not module.startswith("pydeequ") and module != "pandera",
    )
    unmet = registry.engines_that_cannot_run(
        ["pandera", "Deequ", "deequ", "portable", "no.such.engine", "dqdl"]
    )
    assert unmet == [
        PANDERA_UNMET,
        "Engine 'deequ' cannot run here: 'pydeequ' is not installed. "
        "Install it with: pip install 'dq-kernel[spark,deequ]'  "
        "(or: poetry install -E spark -E deequ).",
        "Engine 'dqdl' cannot run here: 'pydeequ.dqdl' is not installed. "
        "Install it with: pip install 'dq-kernel[spark,deequ]'  "
        "(or: poetry install -E spark -E deequ).",
    ]


# ------------------------------------------------------------------- the loader


class NoEngineImport:
    """An import finder that fails the test if an engine is imported under it."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith("dq.engine."):
            raise AssertionError(f"{fullname} was imported")
        return None


def test_the_loader_refuses_an_engine_before_the_engine_is_imported(monkeypatch):
    import dq.engine.checkspec  # noqa: F401 - the kernel's own, loaded before the hook
    import dq.engine.dq_engine  # noqa: F401
    import dq.engine.registry  # noqa: F401

    monkeypatch.setattr(sys, "meta_path", [NoEngineImport(), *sys.meta_path])
    monkeypatch.setattr(registry, "importable", lambda module: False)
    with pytest.raises(EngineNotFoundError) as refused:
        EngineLoader().load_engine("deequ", {}, 1)
    assert str(refused.value) == DEEQU_UNMET
    assert isinstance(refused.value, ImportError)  # the net the command line keeps


def test_the_loader_judges_only_what_the_engine_declares(monkeypatch):
    """The portable engine needs a backend, not a library of its own: the
    backend says what it needs when it is asked. It is not refused here."""
    monkeypatch.setattr(registry, "importable", lambda module: False)
    loaded = EngineLoader().load_engine("portable", {"plan": {}}, 1)
    assert type(loaded).__name__ == "PortableEngine"


def _registered(monkeypatch, name, **declared):
    """Register an engine whose module is ``dq.engine.<name>.<name>_engine``."""
    monkeypatch.setitem(
        BUILT_IN_ENGINES,
        name,
        EngineSpec(
            name,
            f"dq.engine.{name}.{name}_engine",
            f"{name.capitalize()}Engine",
            None,
            **declared,
        ),
    )
    return f"dq.engine.{name}.{name}_engine"


def _importing(module, outcome):
    """Return an ``import_module`` that raises ``outcome`` for ``module``."""

    def importing(name, *args, **kwargs):
        if name == module:
            raise outcome
        raise AssertionError(f"{name} was imported")

    return importing


def test_the_loader_names_a_library_the_declaration_missed(monkeypatch):
    module = _registered(monkeypatch, "undeclared", extras=("spark",))
    missing = ModuleNotFoundError("No module named 'some.library'", name="some.library")
    monkeypatch.setattr(
        "dq.engine.engine_loader.importlib.import_module", _importing(module, missing)
    )
    with pytest.raises(EngineNotFoundError) as refused:
        EngineLoader().load_engine("undeclared", {}, 1)
    assert str(refused.value) == (
        "Engine 'undeclared' cannot run here: 'some.library' is not installed. "
        "Install it with: pip install 'dq-kernel[spark]'  (or: poetry install -E spark)."
    )
    assert refused.value.__cause__ is missing


def test_an_import_error_that_names_no_module_is_not_the_engine_s():
    """One raised by hand names nothing, so it cannot be about the engine:
    it is said as a library the engine imports, in the words it carries."""
    nameless = ModuleNotFoundError("installed distribution is incomplete")
    assert module_itself_is_missing("dq.engine.x.x_engine", nameless) is False
    assert (
        refusal("x", (str(nameless),)) == "Engine 'x' cannot run here: "
        "'installed distribution is incomplete' is not installed."
    )


def test_an_engine_that_is_not_here_is_said_to_be_that():
    """The engine's own module: no extra installs it, and none is named."""
    with pytest.raises(EngineNotFoundError) as refused:
        EngineLoader().load_engine("nosuch", {}, 1)
    assert str(refused.value) == (
        "Engine module 'dq.engine.nosuch.nosuch_engine' not found: "
        "No module named 'dq.engine.nosuch'"
    )


def test_an_engine_that_refuses_to_be_imported_is_reported_with_the_reason(
    monkeypatch,
):
    module = _registered(monkeypatch, "refusing", extras=("spark", "deequ"))
    monkeypatch.setattr(
        "dq.engine.engine_loader.importlib.import_module",
        _importing(module, RuntimeError(SAYS_SPARK_VERSION)),
    )
    with pytest.raises(EngineNotFoundError) as refused:
        EngineLoader().load_engine("refusing", {}, 1)
    assert str(refused.value) == (
        f"Engine 'refusing' cannot be imported here: RuntimeError: {SAYS_SPARK_VERSION}"
    )


def test_an_engine_class_missing_from_its_module_is_said_so(monkeypatch):
    monkeypatch.setitem(
        BUILT_IN_ENGINES, "half", EngineSpec("half", __name__, "NoSuchEngine", None)
    )
    with pytest.raises(EngineNotFoundError) as refused:
        EngineLoader().load_engine("half", {}, 1)
    assert str(refused.value) == (
        f"Engine class 'NoSuchEngine' not found in module '{__name__}'"
    )


# ------------------------------------------------------------------ the run core


class Never:
    """Something the run core must not ask for under a given name."""

    def __init__(self, what, asked):
        self.what, self.asked = what, asked

    def __call__(self, *args, **kwargs):
        self.asked.append(self.what)
        raise AssertionError(f"{self.what} was asked for")


def test_the_run_core_refuses_before_the_first_rule_runs(monkeypatch):
    """Once, for the whole pack, as its shape is judged (ADR-001): nothing is
    loaded and no dataset is resolved. It used to be one rule's way in."""
    monkeypatch.setattr(registry, "importable", lambda module: module == "pandera")
    rules = (
        RuleRef(0, "portable", ("orders",), None, {}),
        RuleRef(1, "deequ", ("orders",), None, {}),
        RuleRef(2, "pandera", ("orders",), None, {}),
    )
    asked = []
    observer = InMemoryObserver()

    class NoScope:
        def run_scope(self, run_key):
            raise AssertionError("a refusal opens no scope")

    with pytest.raises(EngineNotFoundError) as refused:
        run_rules(
            rules,
            load_engine=Never("an engine", asked),
            dataset=Never("a dataset", asked),
            run_key=7,
            job_id="job",
            dataset_name="orders",
            policy="any",
            observer=observer,
            notifier_factory=Never("a notifier", asked),
            sink=NoScope(),
        )
    assert asked == []
    assert str(refused.value) == DEEQU_UNMET
    assert observer.notifications == ("run_started", "run_finished")
    finished = observer.events[1][1]
    assert finished.error == f"EngineNotFoundError: {DEEQU_UNMET}"
    assert (finished.rules, finished.outcomes) == (0, 0)


def test_a_pack_whose_engines_can_run_is_not_held_up(monkeypatch):
    """The control: with every declared library installed, the core goes on
    to the first rule."""
    monkeypatch.setattr(registry, "importable", lambda module: True)
    rules = (RuleRef(0, "deequ", ("orders",), None, {}),)
    applied = []

    class Engine:
        def apply(self, frame, repository=None):
            applied.append(frame)
            return [{"check": "complete", "success": True}]

    observer = InMemoryObserver()
    notifier = InMemoryNotifier()
    result = run_rules(
        rules,
        load_engine=lambda rule, run_key: Engine(),
        dataset=lambda name: name,
        run_key=1,
        job_id="job",
        dataset_name="orders",
        policy="any",
        observer=observer,
        notifier_factory=lambda: notifier,
    )
    assert applied == ["orders"]
    assert [outcome["check"] for outcome in result.outcomes] == ["complete"]
    assert observer.notifications == (
        "run_started",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    )


# ----------------------------------------------------------------------- a sink


def test_a_sink_that_needs_its_extra_is_called_a_sink(monkeypatch):
    from dq.sinks import sink_from_config

    for name in ("sqlalchemy", "sqlalchemy.sql", "sqlalchemy.engine"):
        monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(ImportError) as refused:
        sink_from_config({"type": "postgres"})
    said = str(refused.value)
    assert said.startswith(
        "The sqlalchemy sink requires the optional sqlalchemy package"
    )
    assert "-E sqlalchemy" in said
