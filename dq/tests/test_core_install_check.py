# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The check that the published wheel needs no extra.

CI installs the wheel alone and runs ``scripts/check_core_install.py``: the
development group carries three extras' packages, so no lane that installs
it can show the package manages without them, and every lane has Spark.
These tests hold the check itself honest -- what makes an environment unfit
for the proof, which modules may need an extra to import, and what a
command or an entry point must say when asked to work without one.
"""

import importlib.util
import json
import re
import tomllib
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_core_install.py"


def load():
    spec = importlib.util.spec_from_file_location("check_core_install", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = load()

#: What a rule-pack's metadata might declare: one dependency of each kind.
REQUIRES = [
    "pyhocon (<0.4.0,>=0.3.59)",
    "pyarrow (>=16,<27) ; extra == 'arrow'",
    'pywin32-ctypes (>=0.2.0,<0.3.0) ; sys_platform == "win32"',
]


@pytest.fixture
def wheel(tmp_path):
    """Return a function that writes a wheel holding only given requirements."""
    built = tmp_path / "dq_kernel-0.0.0-py3-none-any.whl"

    def write(requires):
        with zipfile.ZipFile(built, "w") as archive:
            archive.writestr(
                "dq_kernel-0.0.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: dq-kernel\nVersion: 0.0.0\n"
                + "".join(f"Requires-Dist: {line}\n" for line in requires),
            )
        return built

    return write


def missing(module):
    """Return the error Python raises for a module that is not installed."""
    return ModuleNotFoundError(f"No module named '{module}'", name=module)


def importer(outcomes):
    """Return an ``import_module`` that raises what ``outcomes`` says."""

    def import_module(name):
        if outcomes.get(name) is not None:
            raise outcomes[name]
        return SimpleNamespace(__file__="/nowhere/module.py")

    return import_module


# ------------------------------------------------------------ the requirements


def test_a_requirement_line_is_one_of_three_kinds():
    assert check.requirement_line(REQUIRES[0]) == ("pyhocon", check.ALWAYS)
    assert check.requirement_line(REQUIRES[1]) == ("pyarrow", check.EXTRA)
    assert check.requirement_line(REQUIRES[2]) == ("pywin32-ctypes", check.PLATFORM)
    assert check.requirement_line("DQ.Kernel_Dist (>=1)") == (
        "dq-kernel-dist",
        check.ALWAYS,
    )


def test_the_requirements_come_from_the_wheel_itself(wheel):
    assert check.requirements(wheel(REQUIRES)) == [
        ("pyhocon", check.ALWAYS),
        ("pyarrow", check.EXTRA),
        ("pywin32-ctypes", check.PLATFORM),
    ]


def extras_named(words: str) -> set:
    """Return every extra a refusal's expected words name."""
    return set(re.findall(r"-E ([a-z]+)", words))


def test_every_extra_a_refusal_must_name_is_declared():
    """Read from the packaging declaration, so the check and the package agree."""
    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())
    extras = pyproject["tool"]["poetry"]["extras"]
    named = set().union(*(extras_named(words) for _, _, words in check.ASKED_TO_WORK))
    assert named == {"arrow", "spark", "sqlalchemy", "otel", "deequ"}
    assert named <= set(extras), "a refusal names an extra the package does not declare"


def test_an_engine_on_spark_names_both_of_its_extras():
    """pydeequ does not depend on pyspark: ``dq-kernel[deequ]`` alone installs
    no Spark, and a reader sent to install it was sent back for a second."""
    asked = {name: words for name, _, words in check.ASKED_TO_WORK}
    assert extras_named(asked["dq.engine.engine_loader"]) == {"spark", "deequ"}
    assert check.LISTED_MISSING == ["pyspark", "pydeequ"]
    assert check.LISTED_HINT == (
        "pip install 'dq-kernel[spark,deequ]'  (or: poetry install -E spark -E deequ)"
    )
    # The command, before it starts Spark, says the same.
    (first,) = [words for _, _, words in check.ASKED_TO_RUN if "-E spark" in words]
    assert extras_named(first) == {"spark", "deequ"}


def test_both_profilers_are_asked_to_work_and_the_profiler_to_run():
    """Each profiler takes its library's own frame, and stops at the missing
    package before it looks at what it was given."""
    asked = {name for name, _, _ in check.ASKED_TO_WORK}
    assert {"dq.profile.arrow", "dq.profile.spark"} <= asked
    assert ("dq-profile", check.over_a_dataset, "-E arrow") in check.ASKED_TO_RUN


# ------------------------------------------------------- a fit environment


def test_an_environment_with_an_optional_dependency_proves_nothing():
    required = [
        ("pyyaml", check.ALWAYS),
        ("pyarrow", check.EXTRA),
        ("sqlalchemy", check.EXTRA),
    ]
    found = check.problems(required, {"pyyaml", "sqlalchemy"})
    assert len(found) == 1
    assert found[0].startswith("this environment holds optional dependencies")
    assert found[0].endswith("sqlalchemy")


def test_every_always_dependency_must_be_present():
    found = check.problems(
        [("pyyaml", check.ALWAYS), ("requests", check.ALWAYS)], {"requests"}
    )
    assert found == ["core dependencies are not installed: pyyaml"]


def test_a_platform_dependency_is_not_missed_when_the_platform_is_absent():
    required = [("pyyaml", check.ALWAYS), ("pywin32-ctypes", check.PLATFORM)]
    assert check.problems(required, {"pyyaml"}) == []


def test_the_development_environment_cannot_make_the_proof():
    """Why CI needs a bare install: with an extra installed, the check refuses."""
    pytest.importorskip(
        "pyarrow", reason="an extra must be installed for this to mean anything"
    )
    present = check.installed()
    assert "dq-kernel" in present
    assert check.problems(
        [("pyhocon", check.ALWAYS), ("pyarrow", check.EXTRA)], present
    )


def test_only_a_location_inside_the_interpreter_s_own_environment_counts(
    tmp_path, monkeypatch
):
    """Judged by directory, not by spelling: ``venv-copy`` starts like ``venv``."""
    root = tmp_path.resolve()
    monkeypatch.setattr(check.sys, "prefix", str(root / "venv"))
    inside = root / "venv" / "lib" / "site-packages" / "dq" / "__init__.py"
    assert check.from_installation(inside)
    assert not check.from_installation(root / "checkout" / "dq" / "__init__.py")
    assert not check.from_installation(root / "venv-copy" / "dq" / "__init__.py")


# --------------------------------------------------------------- the modules


def package_at(root):
    """Lay out a package the way the wheel ships it."""
    for relative in (
        "__init__.py",
        "plan.py",
        "engine/__init__.py",
        "engine/x.py",
        "sdk/fixtures/semantic-cases.json",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    return SimpleNamespace(__name__="dq", __file__=str(root / "__init__.py"))


def test_the_walk_covers_every_module_the_package_ships(tmp_path):
    package = package_at(tmp_path / "site-packages" / "dq")
    assert check.modules_of(package) == ["dq", "dq.engine", "dq.engine.x", "dq.plan"]


def test_nothing_a_package_might_ship_is_left_out_of_the_walk(tmp_path):
    """The wheel no longer ships its tests. Were they built into it again,
    the walk would import them and the proof would fail, not skip them."""
    root = tmp_path / "site-packages" / "dq"
    package = package_at(root)
    test = root / "tests" / "test_x.py"
    test.parent.mkdir()
    test.write_text("", encoding="utf-8")
    assert "dq.tests.test_x" in check.modules_of(package)


def test_the_build_leaves_out_what_needs_another_library():
    """The tests need pytest and the notebooks a Databricks workspace. The
    walk above skips nothing, so this is what keeps the bare install bare."""
    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())
    assert {"dq/tests", "dq/notebooks"} <= set(pyproject["tool"]["poetry"]["exclude"])


def test_a_module_that_needs_an_extra_to_import_is_a_problem(monkeypatch):
    """Any module, listed or not: this is what a hand-written list missed."""
    monkeypatch.setattr(
        check.importlib, "import_module", importer({"dq.history": missing("pyarrow")})
    )
    monkeypatch.setattr(check, "NEEDS_AN_EXTRA_TO_IMPORT", frozenset())
    assert check.import_problems(["dq.history", "dq.plan"]) == [
        "dq.history does not import without an extra: No module named 'pyarrow'"
    ]


def test_a_declared_module_must_stop_at_a_package_not_its_own(monkeypatch):
    modules = ("dq.engine.x", "dq.engine.y", "dq.engine.z")
    monkeypatch.setattr(
        check.importlib,
        "import_module",
        importer(
            {"dq.engine.x": missing("pyspark"), "dq.engine.y": missing("dq.plann")}
        ),
    )
    monkeypatch.setattr(check, "NEEDS_AN_EXTRA_TO_IMPORT", frozenset(modules))
    assert check.import_problems(list(modules)) == [
        "dq.engine.y, declared to need an extra, stops at a module of its own: "
        "No module named 'dq.plann'",
        "dq.engine.z is declared to need an extra and imports without one: "
        "remove it from NEEDS_AN_EXTRA_TO_IMPORT",
    ]


def test_a_module_that_fails_another_way_is_a_problem(monkeypatch):
    monkeypatch.setattr(
        check.importlib, "import_module", importer({"dq.plan": SyntaxError("bad")})
    )
    assert check.import_problems(["dq.plan"]) == [
        "dq.plan fails to import another way: SyntaxError: bad"
    ]


def test_the_log_says_which_package_each_declared_module_lacks(monkeypatch, capsys):
    monkeypatch.setattr(
        check.importlib, "import_module", importer({"dq.engine.x": missing("pyspark")})
    )
    monkeypatch.setattr(check, "NEEDS_AN_EXTRA_TO_IMPORT", frozenset({"dq.engine.x"}))
    assert check.import_problems(["dq.engine.x", "dq.plan"]) == []
    indented = [
        line for line in capsys.readouterr().out.splitlines() if line.startswith("  ")
    ]
    assert indented == ["  dq.engine.x (pyspark)"]


def test_every_declared_module_is_one_the_checkout_ships():
    """A name matching no file would hide nothing and catch nothing."""
    for name in sorted(check.NEEDS_AN_EXTRA_TO_IMPORT):
        assert (
            (REPOSITORY_ROOT / Path(*name.split("."))).with_suffix(".py").is_file()
        ), name


# -------------------------------------------------------------- the commands

COMMANDS = {
    "dq-run": ("dq.runner", "main"),
    "dq-validate": ("dq.validate", "main"),
}


def record_runs(monkeypatch, answers):
    """Make each command return what ``answers`` says for its first argument."""
    calls = []

    def run(target, arguments):
        calls.append((target, arguments))
        return answers.get(arguments[0], (0, "usage: something\n"))

    monkeypatch.setattr(check, "run", run)
    return calls


LISTING_PROBLEMS = check.listing_problems


def without_a_listing(monkeypatch):
    """Ask nothing of the engine listing: the test is about other commands."""
    monkeypatch.setattr(check, "listing_problems", lambda available: [])


def without_a_suggester(monkeypatch):
    """Ask nothing of the suggester: the test is about other commands."""
    without_a_listing(monkeypatch)
    monkeypatch.setattr(check, "suggests_a_draft", lambda available, scratch: [])


def only_commands(monkeypatch, commands, answers=()):
    """Leave the commands to be judged: nothing is asked to run or to suggest."""
    monkeypatch.setattr(check, "commands", lambda: dict(commands))
    monkeypatch.setattr(check, "ASKED_TO_RUN", ())
    without_a_suggester(monkeypatch)
    return record_runs(monkeypatch, dict(answers))


def test_the_wheel_s_own_commands_are_told_from_its_dependencies(monkeypatch):
    entry_points = [
        SimpleNamespace(name="dq-run", value="dq.runner:main"),
        SimpleNamespace(name="dq-engines", value="dq.engines:main"),
        SimpleNamespace(name="pyhocon", value="pyhocon.cli:main"),
    ]
    monkeypatch.setattr(check.metadata, "entry_points", lambda group: entry_points)
    assert check.commands() == {
        "dq-run": ("dq.runner", "main"),
        "dq-engines": ("dq.engines", "main"),
    }


def test_a_command_runs_isolated_so_it_cannot_import_the_checkout(monkeypatch):
    """A plain ``python -c`` puts the checkout, the working directory, first
    on the path: the command would run the checkout's code, not the wheel's."""
    seen = []

    def run(argv, **options):
        seen.append(argv)
        return SimpleNamespace(returncode=0, stdout="usage: x", stderr="")

    monkeypatch.setattr(check.subprocess, "run", run)
    assert check.run(("dq.runner", "main"), ["--help"]) == (0, "usage: x")
    assert seen == [
        [
            check.sys.executable,
            "-I",
            "-c",
            "import sys; from dq.runner import main; sys.exit(main() or 0)",
            "--help",
        ]
    ]


def test_every_command_is_asked_for_help(monkeypatch):
    calls = only_commands(monkeypatch, COMMANDS)
    assert check.command_problems(Path("scratch")) == []
    assert [arguments for _, arguments in calls if arguments == ["--help"]] == [
        ["--help"],
        ["--help"],
    ]


@pytest.mark.parametrize(
    ("answer", "problem"),
    [
        ((1, "Traceback: boom"), "--help exits 1: Traceback: boom"),
        ((1, "\n   \n"), "--help exits 1: (no output)"),
        ((2, "usage: dq-run\nerror: broken"), "--help exits 2: error: broken"),
        ((0, "silence"), "--help exits 0: silence"),
    ],
    ids=["crashed", "said-nothing", "printed-its-usage-then-failed", "silent"],
)
def test_a_command_that_does_not_answer_help_is_a_problem(monkeypatch, answer, problem):
    """The second is what argparse does for an error of its own: the usage is
    printed, and the exit code says it failed."""
    only_commands(monkeypatch, COMMANDS, {"--help": answer})
    assert check.command_problems(Path("scratch")) == [
        f"dq-run {problem}",
        f"dq-validate {problem}",
    ]


def test_the_validator_must_be_installed_and_accept_the_shipped_example(monkeypatch):
    refused = (2, "Configuration validation failed")
    calls = only_commands(monkeypatch, COMMANDS, {str(check.ON_SPARK): refused})
    assert check.command_problems(Path("scratch")) == [
        f"dq-validate {check.ON_SPARK.name} exits 2: Configuration validation failed"
    ]
    assert (("dq.validate", "main"), [str(check.ON_SPARK)]) in calls

    only_commands(monkeypatch, {"dq-run": ("dq.runner", "main")})
    assert check.command_problems(Path("scratch")) == [
        "the wheel installs no command named dq-validate"
    ]


def kernel_asked_to_run(monkeypatch):
    """Leave ``dq-kernel`` to be judged over one rule pack; return its path."""
    without_a_suggester(monkeypatch)
    rules = str(Path("scratch") / "rules.conf")
    monkeypatch.setattr(
        check,
        "commands",
        lambda: {
            "dq-kernel": ("dq.cli", "main"),
            "dq-validate": ("dq.validate", "main"),
        },
    )
    monkeypatch.setattr(
        check,
        "ASKED_TO_RUN",
        (("dq-kernel", lambda scratch: [rules], "-E spark -E deequ"),),
    )
    return rules


def test_a_command_that_exits_two_and_says_what_to_install_passes(monkeypatch):
    rules = kernel_asked_to_run(monkeypatch)
    record_runs(monkeypatch, {rules: (2, "Install: -E spark -E deequ")})
    assert check.command_problems(Path("scratch")) == []


@pytest.mark.parametrize(
    ("answer", "problem"),
    [
        (
            (1, "Traceback (most recent call last):\nUnboundLocalError: no value"),
            "dq-kernel, asked to run, exits 1 and does not say "
            "'-E spark -E deequ': UnboundLocalError: no value",
        ),
        (
            # Spark's extra alone, said before this change while the engine
            # needed a second: right code, and the reader comes back once more.
            (2, "Install Spark support with: pip install dq-kernel[spark]"),
            "dq-kernel, asked to run, exits 2 and does not say "
            "'-E spark -E deequ': "
            "Install Spark support with: pip install dq-kernel[spark]",
        ),
        (
            (2, "error"),
            "dq-kernel, asked to run, exits 2 and does not say "
            "'-E spark -E deequ': error",
        ),
    ],
    ids=["crashed", "sparks-extra-alone", "right-code-wrong-words"],
)
def test_a_command_is_judged_by_its_exit_code_and_by_its_words(
    monkeypatch, answer, problem
):
    """Exit 2 is what every command's help promises for a missing dependency.
    The first answer is the crash this guards against; the problem quotes a
    command's last line, where a traceback says what went wrong."""
    rules = kernel_asked_to_run(monkeypatch)
    record_runs(monkeypatch, {rules: answer})
    assert check.command_problems(Path("scratch")) == [problem]


def test_a_command_the_wheel_does_not_install_is_a_problem(monkeypatch):
    monkeypatch.setattr(check, "commands", lambda: dict(COMMANDS))
    without_a_suggester(monkeypatch)
    monkeypatch.setattr(
        check, "ASKED_TO_RUN", (("dq-kernel", lambda scratch: [], "spark"),)
    )
    assert check.command_problems(Path("scratch")) == [
        "the wheel installs no command named dq-kernel"
    ]


def test_the_run_over_files_hands_the_runner_a_dataset_that_is_there(tmp_path):
    """dq-run refuses a missing file before it needs pyarrow, which would
    let it name nothing and still exit 2."""
    dataset = tmp_path / "orders.parquet"
    arguments = check.over_files(tmp_path)
    assert dataset.is_file()
    assert arguments == [
        str(check.OVER_FILES),
        "--dataset",
        f"orders={dataset}",
        "--report",
        str(tmp_path / "report.json"),
    ]
    assert check.OVER_FILES.is_file() and check.ON_SPARK.is_file()


def test_the_profiling_is_asked_to_run_over_a_dataset_that_is_there(tmp_path):
    """dq-profile digests the bytes before it needs pyarrow: a file that is
    not there would be refused as missing, naming no extra."""
    dataset = tmp_path / "orders.parquet"
    arguments = check.over_a_dataset(tmp_path)
    assert dataset.is_file()
    assert arguments == [
        "--dataset",
        f"orders={dataset}",
        "--profile",
        str(tmp_path / "p.json"),
    ]


# ------------------------------------------------------------- the suggester

#: A draft a stand-in writes: one line a suggestion.
SUGGESTED = "".join(f"name = {word}\n" for word in check.DRAFT_WORDS)


def a_suggester(monkeypatch, scratch, *, code=0, text=SUGGESTED):
    """Leave ``dq-suggest`` to be judged: the other commands answer, unjudged.

    Asked to run, the stand-in exits with ``code`` and writes ``text`` where
    the draft goes -- nothing, where ``text`` is ``None``. Returns what it
    was given to read: the profile's text, or ``None`` where there was none.
    """
    read = []
    draft = scratch / "draft.conf"

    def run(target, arguments):
        if not any(value.endswith(".json") for value in arguments):
            return 0, "usage: the other commands answer, and are not judged"
        profile = next(Path(value) for value in arguments if value.endswith(".json"))
        read.append(profile.read_text() if profile.is_file() else None)
        if text is not None:
            draft.write_text(text)
        return code, "the draft is written"

    monkeypatch.setattr(
        check,
        "commands",
        lambda: COMMANDS | {"dq-suggest": ("dq.profile.cli", "suggest_main")},
    )
    monkeypatch.setattr(check, "ASKED_TO_RUN", ())
    monkeypatch.setattr(check, "run", run)
    without_a_listing(monkeypatch)
    return read


def test_the_suggester_is_asked_to_turn_a_profile_into_a_draft(
    monkeypatch, tmp_path, capsys
):
    read = a_suggester(monkeypatch, tmp_path)
    assert check.command_problems(tmp_path) == []
    assert read == [check.A_PROFILE]
    assert "dq-suggest turns a profile into a draft that validates" in (
        capsys.readouterr().out
    )


@pytest.mark.parametrize(
    ("code", "text"),
    [
        (1, SUGGESTED),
        (0, ""),
        (0, "name = isComplete\n"),
        (0, None),
    ],
    ids=[
        "its-draft-does-not-validate",
        "writes-nothing",
        "a-draft-that-is-not-one",
        "no-file-at-all",
    ],
)
def test_a_suggester_that_writes_no_draft_is_a_problem(
    monkeypatch, tmp_path, code, text
):
    a_suggester(monkeypatch, tmp_path, code=code, text=text)
    (found,) = check.command_problems(tmp_path)
    assert found == (
        f"dq-suggest over a profile exits {code} and does not write a draft: "
        "the draft is written"
    )


def test_a_wheel_that_installs_no_suggester_is_a_problem(monkeypatch, tmp_path):
    monkeypatch.setattr(check, "commands", lambda: dict(COMMANDS))
    monkeypatch.setattr(check, "ASKED_TO_RUN", ())
    record_runs(monkeypatch, {})
    without_a_listing(monkeypatch)
    assert check.command_problems(tmp_path) == [
        "the wheel installs no command named dq-suggest"
    ]


# ------------------------------------------------------ the engine listing

LISTING = {
    "engines": [
        {
            "name": check.LISTED,
            "installed": False,
            "missing": check.LISTED_MISSING,
            "install_hint": check.LISTED_HINT,
        }
    ]
}
ENGINES = {"dq-engines": ("dq.engines", "main")}


def test_the_listing_is_asked_what_an_engine_lacks(monkeypatch, capsys):
    """The crash this guards against: with pydeequ installed and no
    SPARK_VERSION in the environment, the command died with a traceback."""
    calls = only_commands(
        monkeypatch, COMMANDS | ENGINES, {"--format": (0, json.dumps(LISTING))}
    )
    monkeypatch.setattr(check, "listing_problems", LISTING_PROBLEMS)
    assert check.command_problems(Path("scratch")) == []
    assert (("dq.engines", "main"), ["--format", "json"]) in calls
    assert "dq-engines, asked to list, says deequ needs" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("code", "output"),
    [
        (1, "RuntimeError: SPARK_VERSION environment variable is required"),
        (0, json.dumps({"engines": [{"name": "deequ", "installed": True}]})),
        (
            0,
            json.dumps(
                {
                    "engines": [
                        {
                            "name": "deequ",
                            "installed": False,
                            "missing": ["pydeequ"],
                            "install_hint": "pip install 'dq-kernel[deequ]'",
                        }
                    ]
                }
            ),
        ),
        (0, "not a document"),
    ],
    ids=["crashed", "said-installed", "named-one-extra", "said-nothing-usable"],
)
def test_a_listing_that_does_not_say_what_an_engine_lacks_is_a_problem(
    monkeypatch, code, output
):
    only_commands(monkeypatch, COMMANDS | ENGINES, {"--format": (code, output)})
    monkeypatch.setattr(check, "listing_problems", LISTING_PROBLEMS)
    (found,) = check.command_problems(Path("scratch"))
    assert found.startswith(f"dq-engines, asked to list, exits {code} and does not ")
    assert found.endswith("say deequ lacks pyspark, pydeequ: " + output)


def test_a_wheel_that_installs_no_listing_is_a_problem(monkeypatch):
    monkeypatch.setattr(check, "commands", lambda: {})
    assert check.listing_problems(check.commands()) == [
        "the wheel installs no command named dq-engines"
    ]


def test_the_profile_the_suggester_is_given_is_one_the_package_reads(tmp_path):
    """The tests above stand on this constant: it is a profile the package
    accepts, whose column warrants every suggestion a Deequ draft can hold."""
    from dq.profile import suggest as suggesting
    from dq.profile.profile import Profile
    from dq.profile.suggest import warranted

    profile = Profile.from_json(check.A_PROFILE)
    assert [suggestion.kind for suggestion in warranted(profile)] == [
        "complete",
        "unique",
        "non_negative",
    ]
    draft = tmp_path / "draft.conf"
    draft.write_text(suggesting.render(suggesting.suggest(profile, "deequ"), "hocon"))
    assert all(word in draft.read_text() for word in check.DRAFT_WORDS)


# ------------------------------------------------------- asked to work


def test_an_entry_point_that_names_its_extra_passes():
    def call(module):
        raise ImportError(
            "The arrow adapter requires the optional pyarrow package; "
            "install it with `poetry install -E arrow`"
        )

    assert check.refuses_naming_the_extra(None, call, "-E arrow") == ""


@pytest.mark.parametrize(
    "failure, expected",
    [
        (ImportError("No module named 'pyarrow'"), "named no extra"),
        (ValueError("boom"), "failed another way: ValueError: boom"),
    ],
)
def test_an_entry_point_that_fails_any_other_way_is_a_problem(failure, expected):
    def call(module, failure=failure):
        raise failure

    assert expected in check.refuses_naming_the_extra(None, call, "-E arrow")


def test_an_entry_point_that_runs_means_an_extra_is_installed():
    assert check.refuses_naming_the_extra(
        None, lambda module: object(), "-E arrow"
    ) == ("it ran: an extra is installed")


def test_an_entry_point_that_does_not_name_its_extra_is_a_problem(monkeypatch, capsys):
    def asked_to(work):
        monkeypatch.setattr(
            check,
            "ASKED_TO_WORK",
            (("dq.adapter", lambda module: module.work(), "-E adapter"),),
        )
        monkeypatch.setattr(
            check.importlib, "import_module", lambda name: SimpleNamespace(work=work)
        )

    def refusing(message):
        def work():
            raise ImportError(message)

        return work

    asked_to(refusing("No module named 'vendor'"))
    assert check.asked_to_work_problems() == [
        "dq.adapter, asked to work, it named no extra: No module named 'vendor'"
    ]

    asked_to(refusing("install it with `poetry install -E adapter`"))
    assert check.asked_to_work_problems() == []
    printed = capsys.readouterr().out
    assert "dq.adapter, asked to work, names its extra: -E adapter" in printed


def test_with_the_extras_installed_no_sample_is_refused_for_its_extra():
    """A sample that named its extra here would name it in the empty
    environment for the wrong reason."""
    # One package for every extra a sample names, read from the samples.
    packages = {
        "arrow": "pyarrow",
        "spark": "pyspark",
        "deequ": "pydeequ",
        "sqlalchemy": "sqlalchemy",
        "otel": "opentelemetry",
    }
    named = set().union(*(extras_named(words) for _, _, words in check.ASKED_TO_WORK))
    assert named == set(packages), "a sample names an extra this test does not install"
    for package in packages.values():
        pytest.importorskip(
            package, reason="an extra must be installed for this to mean anything"
        )
    found = check.asked_to_work_problems()
    assert len(found) == len(check.ASKED_TO_WORK), found
    assert not any("named no extra" in problem for problem in found), found


# --------------------------------------------------------- the whole check


def every_stage_passes(monkeypatch):
    monkeypatch.setattr(check, "installed", lambda: {"pyhocon", "dq-kernel"})
    monkeypatch.setattr(check, "modules_of", lambda package: ["dq"])
    monkeypatch.setattr(check.importlib, "import_module", importer({}))
    monkeypatch.setattr(check, "from_installation", lambda location: True)
    monkeypatch.setattr(check, "import_problems", lambda names: [])
    monkeypatch.setattr(check, "asked_to_work_problems", lambda: [])
    monkeypatch.setattr(check, "command_problems", lambda scratch: [])


def test_the_check_passes_when_the_wheel_is_all_there_is(monkeypatch, wheel):
    every_stage_passes(monkeypatch)
    assert check.main([str(wheel(REQUIRES))]) == 0


def test_the_check_refuses_when_an_optional_dependency_is_installed(monkeypatch, wheel):
    every_stage_passes(monkeypatch)
    monkeypatch.setattr(check, "installed", lambda: {"pyhocon", "dq-kernel", "pyarrow"})
    assert check.main([str(wheel(REQUIRES))]) == 1


def test_the_check_refuses_when_the_package_is_not_installed(monkeypatch, wheel):
    every_stage_passes(monkeypatch)
    monkeypatch.setattr(check, "installed", lambda: {"pyhocon"})
    assert check.main([str(wheel(REQUIRES))]) == 1


def test_the_package_imported_from_outside_the_installation_is_a_problem(
    monkeypatch, wheel
):
    every_stage_passes(monkeypatch)
    monkeypatch.setattr(check, "from_installation", lambda location: False)
    assert check.main([str(wheel(REQUIRES))]) == 1


def test_an_unfit_environment_is_refused_before_any_module_is_imported(
    monkeypatch, wheel
):
    def never(names):
        raise AssertionError("the proof is void: nothing should be imported")

    every_stage_passes(monkeypatch)
    monkeypatch.setattr(check, "installed", lambda: {"pyhocon", "dq-kernel", "pyarrow"})
    monkeypatch.setattr(check, "import_problems", never)
    monkeypatch.setattr(check.importlib, "import_module", never)
    assert check.main([str(wheel(REQUIRES))]) == 1


def test_every_problem_is_reported_not_only_the_first(monkeypatch, wheel, capsys):
    every_stage_passes(monkeypatch)
    monkeypatch.setattr(check, "import_problems", lambda names: ["modules"])
    monkeypatch.setattr(check, "asked_to_work_problems", lambda: ["entry points"])
    monkeypatch.setattr(check, "command_problems", lambda scratch: ["commands"])
    assert check.main([str(wheel(REQUIRES))]) == 1
    reported = [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("problem:")
    ]
    assert reported == [
        "problem: modules",
        "problem: entry points",
        "problem: commands",
    ]


def test_it_takes_exactly_one_wheel():
    assert check.main([]) == 2
    assert check.main(["nowhere.whl"]) == 2
