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

"""
Plugin management commands (install/uninstall).
"""

from __future__ import annotations

import json
import subprocess
import sys

import click

from firecube.ingestor.registry.loader import reset_plugin_discovery_cache

from .registry import get_plugin_distributions


def _verify_plugins_in_subprocess() -> list[str]:
    """List installed plugins in a fresh interpreter.

    An editable install adds a ``.pth`` file to site-packages that the current
    interpreter has not processed. The entry-point *metadata* is already visible
    via ``importlib.metadata``, but importing the freshly installed module
    (``ep.load()``) fails in-process until a new interpreter reprocesses the
    site directories. Running discovery in a fresh ``sys.executable`` subprocess
    matches exactly what the user's next CLI command will see.
    """

    snippet = (
        "import json;"
        "from firecube.ingestor.registry.metadata import list_plugins;"
        "print(json.dumps(list_plugins()))"
    )
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip())


def _run_uv_pip(args: list[str]) -> None:
    """Run uv pip command in the current environment."""

    # We use `uv` directly as it might not be installed as a module.
    # We target the current python executable explicitly.
    cmd = ["uv", "pip", *args, "--python", sys.executable]

    click.echo(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(f"Command failed with exit code {exc.returncode}") from exc


@click.command(
    "install",
    epilog="""\b
Examples:
  # install from index
  firecube plugins install firecube-example-plugin

\b
  # install from a local checkout
  firecube plugins install /path/to/firecube-example-plugin

\b
  # install from git
  firecube plugins install git+ssh://git@example.com/org/firecube-example-plugin.git

\b
  # editable install
  firecube plugins install --editable /path/to/firecube-example-plugin
""",
)
@click.argument("packages", nargs=-1, required=True)
@click.option(
    "-e",
    "--editable",
    is_flag=True,
    help="install in editable mode (equivalent to 'uv pip install -e ...')",
)
def install_plugin(packages: tuple[str, ...], editable: bool) -> None:
    """install plugin packages

    This is a wrapper around 'uv pip install'.

    \b
    PACKAGE SPECIFIERS (passed through to `uv pip install`)
      - Distribution name:
          firecube-example-plugin
      - Local path (directory, wheel, or sdist):
          /path/to/firecube-example-plugin
          ./dist/firecube_example_plugin-0.1.0-py3-none-any.whl
      - Git URL (https or ssh):
          git+https://example.com/org/firecube-example-plugin.git
          git+ssh://git@example.com/org/firecube-example-plugin.git

    \b
    EXAMPLES
      - Install from index:
          firecube plugins install firecube-example-plugin
      - Install from a local checkout:
          firecube plugins install /path/to/firecube-example-plugin
      - Install from git:
          firecube plugins install git+ssh://git@example.com/org/firecube-example-plugin.git
      - Editable install (local or VCS):
          firecube plugins install --editable /path/to/firecube-example-plugin
    """

    click.echo(f"Installing into environment: {sys.executable}")

    args: list[str] = ["install"]
    if editable:
        for pkg in packages:
            args.extend(["-e", pkg])
    else:
        args.extend(packages)

    _run_uv_pip(args)
    reset_plugin_discovery_cache()

    # Verify in a fresh interpreter: an editable install writes a .pth file that
    # this process has not loaded, so in-process discovery can spuriously report
    # the plugin as missing even though the install succeeded.
    click.echo("\nVerifying installed plugins...")
    try:
        plugins = _verify_plugins_in_subprocess()
        click.echo(f"Detected plugins: {', '.join(plugins)}")
    except Exception as exc:
        click.secho(f"Warning: Failed to refresh plugin list: {exc}", fg="yellow")


@click.command(
    "uninstall",
    epilog="""\b
Examples:
  # uninstall a plugin by name
  firecube plugins uninstall example_plugin

\b
  # uninstall by distribution package name
  firecube plugins uninstall example_plugin --dist firecube-example-plugin
""",
)
@click.argument("plugin")
@click.option("--dist", help="bypass resolution and uninstall this distribution package directly")
def uninstall_plugin(plugin: str, dist: str | None) -> None:
    """uninstall a plugin

    Resolves the plugin name to its distribution package and uninstalls it.
    """

    pkg_to_remove = dist

    if not pkg_to_remove:
        # Resolve plugin -> distribution
        dists = get_plugin_distributions(plugin)

        if not dists:
            raise click.ClickException(
                f"Plugin '{plugin}' does not appear to be installed (no matching entry points found). "
                f"If you know the package name, use --dist."
            )

        if len(dists) > 1:
            # Ambiguous
            dist_names = [d[0] for d in dists]
            raise click.ClickException(
                f"Ambiguous plugin name '{plugin}'. It is provided by multiple distributions: {', '.join(dist_names)}. "
                f"Please verify which one to uninstall and use --dist <package_name>."
            )

        # Resolved to exactly one
        pkg_to_remove = dists[0][0]
        version = dists[0][1]
        click.echo(
            f"Resolved plugin '{plugin}' to distribution '{pkg_to_remove}' (version {version})."
        )

    click.echo(f"Uninstalling '{pkg_to_remove}' from {sys.executable}...")
    _run_uv_pip(["uninstall", pkg_to_remove])
    reset_plugin_discovery_cache()
