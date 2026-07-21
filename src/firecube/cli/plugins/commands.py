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

"""Plugin inspection commands for the Firecube CLI."""

from __future__ import annotations

import contextlib
import importlib
import json
from pathlib import Path

import click

from firecube.core import observability
from firecube.ingestor.registry.metadata import (
    get_plugin_descriptor,
    list_plugins,
)

from .introspect import describe_plugin, explain_plugin
from .mgmt import install_plugin, uninstall_plugin
from .registry import get_plugin_cli_command, list_plugin_cli_names


def _load_plugin_cli(plugin: str) -> click.Command:
    """Load a plugin-provided click command exposed under `firecube plugins <plugin> ...`."""

    # Priority 1: Entry Point (v1 Contract)
    cmd = get_plugin_cli_command(plugin)
    if cmd:
        if not isinstance(cmd, click.Command):
            raise click.ClickException(
                f"Plugin '{plugin}' CLI entry point is not a valid Click Command or Group."
            )
        cmd.name = plugin
        return cmd

    # Priority 2: Legacy Convention & Graceful Failure
    try:
        descriptor = get_plugin_descriptor(plugin)
    except KeyError as exc:
        # Graceful failure for missing plugins
        hint = (
            f"Plugin '{plugin}' not found. To install it, try:\n  uv pip install firecube-"
            f"{plugin.replace('_', '-')}"
        )
        raise click.UsageError(hint) from exc

    module_root = descriptor.module.split(".", 1)[0]
    # Convention: plugin can provide `<root>.plugin_cli:cli` (click.Command / click.Group).
    try:
        mod = importlib.import_module(f"{module_root}.plugin_cli")
    except ModuleNotFoundError as exc:
        # If the module exists but plugin_cli doesn't, it might just be an ingestor-only plugin.
        # But if we are here, the user explicitly tried `firecube plugins <plugin> ...`.
        raise click.ClickException(
            f"Plugin '{plugin}' does not expose CLI commands via 'firecube.plugin_cli' entry point "
            f"or '{module_root}.plugin_cli:cli' convention."
        ) from exc

    cmd = getattr(mod, "cli", None)
    if not isinstance(cmd, click.Command):
        raise click.ClickException(
            f"Plugin '{plugin}' has '{module_root}.plugin_cli', but it does not define a click command named 'cli'."
        )

    # Ensure usage/help shows the plugin identifier (not the underlying function name).
    cmd.name = plugin
    return cmd


class PluginsGroup(click.Group):
    """`firecube plugins ...` with dynamic subcommands for installed plugins."""

    def list_commands(self, ctx: click.Context) -> list[str]:
        commands = set(super().list_commands(ctx))
        with contextlib.suppress(Exception):
            commands.update(list_plugin_cli_names())
        return sorted(commands)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        cmd = super().get_command(ctx, cmd_name)
        if cmd is not None:
            return cmd

        # Fall back to plugin-specific subcommands.
        # Propagate exceptions (like UsageError from missing plugin) to let Click handle them.
        return _load_plugin_cli(cmd_name)


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, cls=PluginsGroup)
def plugins() -> None:
    """inspect installed ingestion plugins

    Plugins are discovered via Python entry points (firecube.plugins); use
    plugins list to see what is installed and plugins describe to explore
    a plugin's options.
    """

    observability.init_observability("firecube-plugins")


def _echo_json(data: object) -> None:
    click.echo(json.dumps(data, indent=2))


@plugins.command(
    "list",
    epilog="""\b
Examples:
  # list all available plugins in table format
  firecube plugins list

\b
  # list as JSON for scripting
  firecube plugins list -f json

See also: firecube plugins describe, firecube ingest
""",
)
@click.option(
    "-f", "--format", "output_format", type=click.Choice(["table", "json", "csv"]), default="table"
)
def list_plugins_cmd(output_format: str) -> None:
    """list all registered plugins

    Lists all registered ingestion plugins discovered via entry points. Shows
    plugin name, module path, and a short description for each installed
    plugin. Use -f to control output format.
    """

    names = list_plugins()
    if output_format == "json":
        _echo_json(names)
        return
    if not names:
        click.echo("No plugins registered.")
        return
    for name in names:
        click.echo(name)


# describe and explain are now in plugin_introspect


@plugins.command(
    "create",
    epilog="""\b
Examples:
  # create a new plugin with interactive prompts
  firecube plugins create my-plugin

\b
  # create non-interactively with explicit options
  firecube plugins create my-plugin --target-dir /src --author "Jane Doe" --template zarr --non-interactive

See also: firecube plugins install, firecube plugins describe
""",
)
@click.argument("name")
@click.option(
    "--target-dir",
    type=click.Path(path_type=Path),
    default=Path.cwd(),
    help="target directory (default: current directory)",
)
@click.option("--author", help="author name")
@click.option("--email", help="author email")
@click.option("--license", help="license type (e.g. MIT, Apache-2.0)")
@click.option(
    "--template",
    type=click.Choice(["base", "zarr", "parquet"]),
    help="ingestor template to use",
)
@click.option(
    "--write-strategy",
    type=click.Choice(["xarray", "zarr-python"]),
    help="Zarr write strategy: 'xarray' (default, append-based) or 'zarr-python' (direct region writes)",
)
@click.option("--non-interactive", is_flag=True, help="do not ask interactive questions")
def create_plugin(
    name: str,
    target_dir: Path,
    author: str | None,
    email: str | None,
    license: str | None,
    template: str | None,
    write_strategy: str | None,
    non_interactive: bool,
) -> None:
    """create a new plugin project structure"""

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    # Interactive Wizard
    if not non_interactive:
        click.echo("Creating a new Firecube plugin.")
        name = click.prompt("Plugin Name", default=name)

        if not author:
            author = click.prompt("Author Name", default="Firecube Developer")
        if not email:
            email = click.prompt("Author Email", default="dev@example.com")
        if not license:
            license = click.prompt("License", default="MIT")
        if not template:
            template = click.prompt(
                "Template",
                default="base",
                type=click.Choice(["base", "zarr", "parquet"]),
            )
        if template == "zarr" and not write_strategy:
            write_strategy = click.prompt(
                "Zarr write strategy",
                default="xarray",
                type=click.Choice(["xarray", "zarr-python"]),
            )

    # Apply defaults for non-interactive mode if flags weren't provided
    author = author or "Firecube Developer"
    email = email or "dev@example.com"
    license = license or "MIT"
    template = template or "base"

    # Resolve zarr template variant based on write strategy
    resolved_template = template
    if template == "zarr" and write_strategy == "zarr-python":
        resolved_template = "direct_zarr"

    try:
        project_path = create_plugin_structure(
            name,
            target_dir,
            author_name=author,
            author_email=email,
            license=license,
            template_type=resolved_template,
        )
        click.echo(f"✨ Created plugin project: {project_path}")
        click.echo("\nTo install for development:")
        click.echo(f"  cd {project_path}")
        click.echo("  uv sync")
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


plugins.add_command(install_plugin)
plugins.add_command(uninstall_plugin)
plugins.add_command(describe_plugin)
plugins.add_command(explain_plugin)
