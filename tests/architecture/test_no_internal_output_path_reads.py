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

"""Architectural lint: internal code must not read PipelineResult.output_path.

The compatibility property remains available in
``src/firecube/ingestor/types/context.py`` for external callers, but internal
code should read ``outputs.primary`` directly.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src"
_ALLOWED_FILE = (_SRC_ROOT / "firecube" / "ingestor" / "types" / "context.py").resolve()


def _offending_reads() -> list[tuple[str, int]]:
    return [
        (str(path.relative_to(_REPO_ROOT)), node.lineno)
        for path in _SRC_ROOT.rglob("*.py")
        if path.resolve() != _ALLOWED_FILE
        for tree in [ast.parse(path.read_text(encoding="utf-8"), filename=str(path))]
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "output_path"
    ]


def test_no_internal_output_path_reads() -> None:
    offenders = _offending_reads()
    assert not offenders, f"Found internal output_path reads: {offenders}"
