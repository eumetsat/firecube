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

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

# Fixture path
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "demo_plugin"


@pytest.mark.integration
def test_plugin_lifecycle():
    """Verify install, introspection, execution, and uninstall of a plugin."""
    plugin_name = "demo_plugin"

    # Ensure subprocesses see the correct virtualenv
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix

    cmd_base = [sys.executable, "-m", "firecube.cli.main"]

    # 1. Install
    print(f"Installing {FIXTURE_DIR}...")
    install_res = subprocess.run(
        [*cmd_base, "plugins", "install", str(FIXTURE_DIR)], capture_output=True, text=True, env=env
    )
    assert install_res.returncode == 0, f"Install failed: {install_res.stderr}"
    assert "Installing into environment" in install_res.stdout

    # 2. Verify List
    result = subprocess.run([*cmd_base, "plugins", "list"], capture_output=True, text=True, env=env)
    assert result.returncode == 0
    assert plugin_name in result.stdout, f"Plugin not found in list. Output:\n{result.stdout}"

    # 3. Verify Describe
    result = subprocess.run(
        [*cmd_base, "plugins", "describe", plugin_name, "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Describe failed: {result.stderr}"
    data = json.loads(result.stdout)
    assert data["name"] == plugin_name
    assert "greeting" in data["tiers"]["plugin"]

    # 4. Verify Explain
    result = subprocess.run(
        [*cmd_base, "plugins", "explain", f"{plugin_name}.plugin.greeting", "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Explain failed: {result.stderr}"
    data = json.loads(result.stdout)
    assert data["type"] == "string"
    assert data["default"] == "Hello"

    # 5. Verify Execution
    result = subprocess.run(
        [*cmd_base, "plugins", "demo_plugin", "greet", "--name", "Integration"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"Execution failed: {result.stderr}"
    assert "Hello, Integration!" in result.stdout

    # 6. Uninstall
    print(f"Uninstalling {plugin_name}...")
    result = subprocess.run(
        [*cmd_base, "plugins", "uninstall", plugin_name], capture_output=True, text=True, env=env
    )
    assert result.returncode == 0, f"Uninstall failed: {result.stderr}"

    # 7. Verify Gone
    result = subprocess.run([*cmd_base, "plugins", "list"], capture_output=True, text=True, env=env)
    assert result.returncode == 0
    assert plugin_name not in result.stdout


@pytest.mark.integration
def test_plugin_editable_install_verifies_in_fresh_interpreter():
    """Editable install verification must detect the plugin it just installed.

    An editable install writes a ``.pth`` file the installing interpreter has
    not processed, so the in-process verification used to spuriously report the
    plugin as missing. Verification now runs in a fresh subprocess, so the
    install command's own output must list the freshly installed plugin.
    """
    plugin_name = "demo_plugin"

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix
    cmd_base = [sys.executable, "-m", "firecube.cli.main"]

    try:
        install_res = subprocess.run(
            [*cmd_base, "plugins", "install", "--editable", str(FIXTURE_DIR)],
            capture_output=True,
            text=True,
            env=env,
        )
        assert install_res.returncode == 0, f"Install failed: {install_res.stderr}"
        # The verification output of the install command itself (not a separate
        # `plugins list`) must show the plugin -- this is the regression guard.
        assert "Detected plugins:" in install_res.stdout
        assert plugin_name in install_res.stdout, (
            f"Editable install verification did not detect plugin. Output:\n{install_res.stdout}"
        )
    finally:
        subprocess.run(
            [*cmd_base, "plugins", "uninstall", plugin_name],
            capture_output=True,
            text=True,
            env=env,
        )


if __name__ == "__main__":
    test_plugin_lifecycle()
