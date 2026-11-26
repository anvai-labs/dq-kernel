# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the shared optional-dependency import helper."""

import importlib.util
import sys
import types

import pytest

from dq.utils.imports import require_extra


def test_require_extra_returns_the_imported_module():
    module = require_extra("pyhocon", "pyhocon", "core")
    assert module is importlib.import_module("pyhocon")


def test_require_extra_message_names_extra_package_and_pin(monkeypatch):
    monkeypatch.setitem(sys.modules, "definitely_absent_vendor", None)
    with pytest.raises(ImportError) as excinfo:
        require_extra(
            "definitely_absent_vendor",
            "vendor-dist",
            "vendorengine",
            pin="vendor-dist>=1,<2",
        )
    message = str(excinfo.value)
    assert (
        "The vendorengine engine requires the optional vendor-dist package" in message
    )
    assert "`poetry install -E vendorengine`" in message
    assert "`pip install 'vendor-dist>=1,<2'`" in message


def test_require_extra_appends_note(monkeypatch):
    monkeypatch.setitem(sys.modules, "definitely_absent_vendor", None)
    with pytest.raises(ImportError, match="Note: python 3.12 only"):
        require_extra(
            "definitely_absent_vendor",
            "vendor-dist",
            "vendorengine",
            note="python 3.12 only",
        )


def test_require_extra_defaults_pip_hint_to_package(monkeypatch):
    monkeypatch.setitem(sys.modules, "definitely_absent_vendor", None)
    with pytest.raises(ImportError, match=r"`pip install 'vendor-dist'`"):
        require_extra("definitely_absent_vendor", "vendor-dist", "vendorengine")


def test_require_extra_chains_the_original_cause(monkeypatch):
    monkeypatch.setitem(sys.modules, "definitely_absent_vendor", None)
    with pytest.raises(ImportError) as excinfo:
        require_extra("definitely_absent_vendor", "vendor-dist", "vendorengine")
    assert excinfo.value.__cause__ is not None
