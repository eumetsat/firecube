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

import os
import subprocess
import sys
from pathlib import Path


def test_lazy_duckdb_import_subprocess():
    """Importing the duck extension does not import or require duckdb eagerly."""

    script = """
import sys
sys.modules['duckdb'] = None

import firecube.ingestor.extensions.duck

assert sys.modules['duckdb'] is None
print("SUCCESS")
"""
    repo_root = Path(__file__).resolve().parents[4]

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env={**os.environ, "PYTHONPATH": str(repo_root / "src")},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "SUCCESS"
