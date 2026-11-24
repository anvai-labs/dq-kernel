# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract: pytest must always say *why* something failed.

The repository's ``addopts`` used to pass ``--no-summary``. Under pytest 9
that flag skips the entire terminal summary, so a failing test or a
collection error printed nothing but a count -- locally and in CI, where a
red lane then had to be diagnosed blind. These tests run real pytest
subprocesses against the repository configuration and assert the
traceback is in the output.
"""

import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPOSITORY_ROOT / "pyproject.toml"


def _run_pytest(tmp_path, source):
    """Run pytest on a throwaway module using the repository's own config."""
    module = tmp_path / "test_probe.py"
    module.write_text(source, encoding="utf-8")
    environment = os.environ.copy()
    environment.pop("PYTEST_ADDOPTS", None)  # only the repository config applies
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(module),
            "--rootdir",
            str(REPOSITORY_ROOT),
            "--config-file",
            str(PYPROJECT),
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_collection_error_prints_its_traceback(tmp_path):
    result = _run_pytest(
        tmp_path, "import a_module_that_does_not_exist_xyz\n\ndef test_x():\n    pass\n"
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "ERRORS" in output, f"no ERRORS section in pytest output:\n{output}"
    assert "a_module_that_does_not_exist_xyz" in output


def test_failing_test_prints_its_traceback(tmp_path):
    result = _run_pytest(
        tmp_path,
        "def test_fails():\n"
        "    actual = 1 + 1\n"
        "    assert actual == 3, 'sentinel failure message'\n",
    )
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "FAILURES" in output, f"no FAILURES section in pytest output:\n{output}"
    assert "sentinel failure message" in output
    assert "assert actual == 3" in output


def test_repository_addopts_never_silence_the_summary():
    import tomllib

    config = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    addopts = config["tool"]["pytest"]["ini_options"]["addopts"]
    assert "--no-summary" not in addopts
    assert not any(opt == "-rn" or opt.startswith("-rN") for opt in addopts.split())
