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

"""Tests that the post-scaffold hint walks the author through the next steps."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from firecube.cli.plugins.commands import plugins


def test_scaffold_hint_lists_next_steps_then_editable_install(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        plugins,
        [
            "create",
            "my-plugin",
            "--non-interactive",
            "--target-dir",
            str(tmp_path),
            "--author",
            "Test Author",
            "--license",
            "MIT",
            "--template",
            "zarr",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    output = result.output
    project = tmp_path / "firecube-my-plugin"
    steps = [
        f"cd {project}",
        "uv sync",
        "src/firecube_my_plugin/ingestor.py",
        "README.md",
        f"firecube plugins install --editable {project}",
    ]
    positions = [output.find(step) for step in steps]
    assert -1 not in positions, output
    assert positions == sorted(positions), output
