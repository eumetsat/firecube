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

"""Architecture tombstones: typed result objects must not be accessed via magic keys."""

from __future__ import annotations

import subprocess


def test_no_magic_key_metrics_access_in_core() -> None:
    """Core runtime must not access result.metrics via dict .get() or [] indexing."""
    result = subprocess.run(
        [
            "grep",
            "-En",
            r"result\.metrics\.get\(|result\.metrics\[",
            "src/firecube/ingestor/runtime/engine.py",
            "src/firecube/ingestor/runtime/recording.py",
            "src/firecube/ingestor/runtime/telemetry.py",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == "", f"Magic-key metrics access found:\n{result.stdout}"


def test_no_magic_key_outputs_access_in_core() -> None:
    """Core runtime must not keep outputs as dict-typed magic-key payloads."""
    result = subprocess.run(
        [
            "grep",
            "-En",
            r"outputs:\s*dict\[str, str\]\s*=\s*field\(default_factory=dict\)",
            "src/firecube/ingestor/types/context.py",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == "", f"Dict-typed outputs contract found:\n{result.stdout}"
