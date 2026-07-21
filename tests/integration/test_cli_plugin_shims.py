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

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner

from firecube.cli.main import cli


def test_missing_plugin_command_reports_install_hint() -> None:
    """Unknown plugin command fails gracefully with a package install hint."""
    runner = CliRunner()
    plugin_name = "definitely_missing_plugin"

    # Make the test deterministic: simulate "missing plugin" regardless of the local environment.
    # `_load_plugin_cli` first checks the CLI entry point, then falls back to plugin descriptor.
    with (
        patch("firecube.cli.plugins.commands.get_plugin_cli_command", return_value=None),
        patch(
            "firecube.cli.plugins.commands.get_plugin_descriptor",
            side_effect=KeyError(plugin_name),
        ),
    ):
        result = runner.invoke(cli, ["plugins", plugin_name], catch_exceptions=False)

    assert result.exit_code == 2
    assert f"Plugin '{plugin_name}' not found" in result.output
    assert "uv pip install firecube-definitely-missing-plugin" in result.output
