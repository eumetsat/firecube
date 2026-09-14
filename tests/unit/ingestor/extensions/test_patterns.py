# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Behavior and dependency isolation for the optional pattern parser."""

from __future__ import annotations

import builtins
import subprocess
import sys
from datetime import datetime

import pytest

from firecube.ingestor.extensions import parse_pattern

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("pattern", "text", "expected"),
    [
        (
            "invoice_{customer}_{number:04d}.txt",
            "invoice_acme_0042.txt",
            {"customer": "acme", "number": 42},
        ),
        (
            "measurement_{device}_{sequence:04d}_{recorded:%Y%m%dT%H%M%S}.nc",
            "measurement_probe_0042_20260912T103000.nc",
            {"device": "probe", "sequence": 42, "recorded": datetime(2026, 9, 12, 10, 30)},
        ),
        ("{folder}/{label}.txt", "invoices/acme.txt", {"folder": "invoices", "label": "acme"}),
    ],
)
def test_parse_concrete_fields(pattern, text, expected):
    assert parse_pattern(pattern, text) == expected


@pytest.mark.parametrize(
    ("pattern", "text"),
    [
        ("invoice_{number:04d}.txt", "prefix_invoice_0042.txt"),
        ("invoice_{number:04d}.txt", "invoice_0042.txt.suffix"),
        ("invoice_{number:04d}.txt", "/tmp/invoice_0042.txt"),
        ("measurement_{date:%Y%m%d}.nc", "measurement_20260230.nc"),
        ("invoice_{number", "invoice_42"),
    ],
)
def test_parse_errors_propagate(pattern, text):
    with pytest.raises(ValueError):
        parse_pattern(pattern, text)


def test_optional_dependency_is_loaded_only_on_invocation():
    code = """
import importlib.abc
import sys

class BlockTrollsift(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "trollsift" or fullname.startswith("trollsift."):
            raise ModuleNotFoundError("blocked optional dependency", name=fullname)

sys.meta_path.insert(0, BlockTrollsift())
import firecube
import firecube.ingestor.extensions
from firecube.ingestor.extensions import parse_pattern
assert "trollsift" not in sys.modules
try:
    parse_pattern("{value}", "hello")
except ImportError as exc:
    assert "firecube[patterns]" in str(exc), str(exc)
else:
    raise AssertionError("missing dependency was ignored")
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


@pytest.mark.parametrize(
    "error",
    [ModuleNotFoundError("broken dependency", name="another_package"), ImportError("broken API")],
)
def test_unrelated_import_failures_are_not_relabelled(monkeypatch, error):
    original_import = builtins.__import__

    def import_with_failure(name, *args, **kwargs):
        if name == "trollsift":
            raise error
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_with_failure)
    with pytest.raises(ImportError) as caught:
        parse_pattern("{value}", "hello")
    assert caught.value is error
