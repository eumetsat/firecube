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

# tests/unit/test_metrics_no_import_cycle.py
"""Regression guard: metrics.py must not create an import cycle with
firecube.core.filesystem.

Dual-probe: catches the regression from both entry directions — filesystem-first
fails when filesystem is loaded before metrics, and metrics-first fails when
metrics is loaded before filesystem.

History: commit beaef0a introduced a module-level controlplane import
(``from firecube.core.controlplane.types import IndexEnsuredEvent``) that
closed an 8-hop cycle through filesystem.instrumentation. The fix moved that
import inside the function body (lazy) and moved annotation-only symbols under
``TYPE_CHECKING``, removing all runtime module-level controlplane imports from
metrics.py. Importing any controlplane submodule at module-load time triggers
``controlplane/__init__.py``, which re-enters the cycle regardless of which
specific symbol was requested.
"""

from __future__ import annotations

import subprocess
import sys


def test_fresh_interpreter_can_import_filesystem() -> None:
    """Filesystem-first probe: from a fresh Python subprocess,
    `import firecube.core.filesystem` must succeed.

    Detects the regression when filesystem is loaded FIRST in the graph
    (mkdocs, some pytest collection orders, direct helper imports).
    """
    result = subprocess.run(
        [sys.executable, "-c", "import firecube.core.filesystem"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"Fresh-interpreter import of firecube.core.filesystem failed with "
        f"exit {result.returncode}.\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )


def test_fresh_interpreter_can_import_emit_helper() -> None:
    """Metrics-first probe: from a fresh Python subprocess,
    `from firecube.core.observability.metrics import emit_index_ensured_full`
    must succeed.

    Detects the regression when metrics.py is loaded FIRST in the graph
    (direct-helper callers, some import-analysis tools, CLI entry paths).
    This is complementary to the filesystem-first probe above — both must
    pass to prove the cycle is truly broken from both entry directions.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from firecube.core.observability.metrics import emit_index_ensured_full",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"Fresh-interpreter import of emit_index_ensured_full failed with "
        f"exit {result.returncode}.\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
