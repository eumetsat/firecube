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

"""Regression guard for package, runtime, docs, and CLI version consistency."""

from __future__ import annotations

import importlib.metadata
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest
from packaging.version import Version

import firecube
import firecube.ingestor
from firecube.ingestor.registry import version_compat

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _ROOT / "pyproject.toml"


def _pyproject_version() -> str:
    return tomllib.loads(_PYPROJECT.read_text())["project"]["version"]


def test_pyproject_version_is_valid_pep440() -> None:
    Version(_pyproject_version())


def test_metadata_version_matches_pyproject() -> None:
    assert importlib.metadata.version("firecube") == _pyproject_version()


def test_firecube_version_matches_pyproject() -> None:
    assert firecube.__version__ == _pyproject_version()


def test_ingestor_version_matches_pyproject() -> None:
    assert firecube.ingestor.__version__ == _pyproject_version()


def test_cli_version_output_matches_pyproject() -> None:
    if shutil.which("uv") is None:
        pytest.skip("uv is not available on PATH")

    completed = subprocess.run(
        ["uv", "run", "firecube", "--version"],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == f"Firecube {_pyproject_version()}"


def test_plugin_compat_core_version_matches_pyproject() -> None:
    assert _pyproject_version() == version_compat.CORE_VERSION
