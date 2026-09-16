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

"""Tests that `plugins install` output names the install mode (copy/editable)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from firecube.cli.plugins.commands import plugins


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _invoke_install(runner: CliRunner, args: list[str]) -> str:
    """Invoke `firecube plugins install <args>` with subprocess calls mocked out."""
    from unittest.mock import patch

    with (
        patch("firecube.cli.plugins.mgmt._run_uv_pip"),
        patch("firecube.cli.plugins.mgmt.reset_plugin_discovery_cache"),
        patch(
            "firecube.cli.plugins.mgmt._verify_plugins_in_subprocess",
            return_value=["my_plugin"],
        ),
    ):
        result = runner.invoke(plugins, ["install", *args], catch_exceptions=False)
    return result.output


def test_install_copy_mode_output_says_copy(runner: CliRunner) -> None:
    """Non-editable install prints '(copy)' in the output."""
    output = _invoke_install(runner, ["my-plugin-package"])
    assert "(copy)" in output


def test_install_editable_mode_output_says_editable(runner: CliRunner) -> None:
    """Editable install prints '(editable)' in the output."""
    output = _invoke_install(runner, ["--editable", "my-plugin-package"])
    assert "(editable)" in output
