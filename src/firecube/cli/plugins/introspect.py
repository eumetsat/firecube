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
Plugin introspection commands (describe/explain).

This module implements the logic for:
1. describe: High-level summary of a plugin (tiers, version, compatibility).
2. explain: Deep dive into specific configuration options.
"""

from __future__ import annotations

import dataclasses
import json
import typing
from typing import Any

import click

from firecube.ingestor.registry.metadata import get_plugin_descriptor

from .registry import get_plugin_distributions, resolve_plugin_configs


def _type_to_str(py_type: Any) -> str:
    """Convert a Python type to a friendly string."""

    if py_type is None:
        return "null"
    if py_type is str:
        return "string"
    if py_type is int:
        return "integer"
    if py_type is float:
        return "number"
    if py_type is bool:
        return "boolean"
    origin = typing.get_origin(py_type)
    if origin is list:
        return "list"
    if origin is dict:
        return "dict"
    if origin is typing.Union:
        args = typing.get_args(py_type)
        return " | ".join(_type_to_str(a) for a in args if a is not type(None))

    if hasattr(py_type, "__name__"):
        return py_type.__name__
    return str(py_type)


def _dataclass_to_schema(dc: type[Any]) -> dict[str, Any]:
    """Generate a simplified schema from a dataclass."""

    if not dataclasses.is_dataclass(dc):
        return {}

    try:
        type_hints = typing.get_type_hints(dc, include_extras=True)
    except Exception:
        # Fallback if resolution fails (e.g. missing imports in runtime)
        type_hints = {f.name: f.type for f in dataclasses.fields(dc)}

    schema: dict[str, Any] = {}
    for field in dataclasses.fields(dc):
        # Resolve real type from hints
        resolved_type = type_hints.get(field.name, field.type)

        # Determine if required
        # Start with assumption: Required if no default/factory
        required = True
        default: Any = None

        if field.default is not dataclasses.MISSING:
            required = False
            default = field.default
        elif field.default_factory is not dataclasses.MISSING:
            required = False
            default = "<factory>"

        # Check for Optional (Union including NoneType)
        origin = typing.get_origin(resolved_type)
        if origin is typing.Union:
            args = typing.get_args(resolved_type)
            if type(None) in args:
                required = False

        schema[field.name] = {
            "type": _type_to_str(resolved_type),
            "required": required,
            "default": default,
        }
    return schema


@click.command(
    "describe",
    epilog="""\b
Examples:
  # show all options for a plugin
  firecube plugins describe <plugin>

\b
  # show options as JSON
  firecube plugins describe <plugin> -f json

See also: firecube plugins explain, firecube plugins list, firecube ingest
""",
)
@click.argument("plugin")
@click.option(
    "-f", "--format", "output_format", type=click.Choice(["table", "json", "csv"]), default="table"
)
def describe_plugin(plugin: str, output_format: str) -> None:
    """show metadata and config for a plugin

    Displays all available ingestion options with their types, defaults, and
    descriptions. Useful for discovering what --option keys are accepted by
    firecube ingest.
    """

    try:
        descriptor = get_plugin_descriptor(plugin)
        configs = resolve_plugin_configs(plugin)
        dists = get_plugin_distributions(plugin)
    except KeyError as exc:
        raise click.ClickException(f"Plugin '{plugin}' not found.") from exc

    dist_info = dists[0] if dists else ("unknown", "unknown")

    info: dict[str, Any] = {
        "name": descriptor.name,
        "module": descriptor.module,
        "distribution": dist_info[0],
        "version": dist_info[1],
        "description": descriptor.description,
        "product_name": descriptor.product_name,
        "tiers": {},
        "products": descriptor.products,  # Legacy products key
    }

    # Introspect tiers
    if configs.engine:
        info["tiers"]["engine"] = _dataclass_to_schema(configs.engine)
    if configs.template:
        info["tiers"]["template"] = _dataclass_to_schema(configs.template)
    if configs.plugin:
        info["tiers"]["plugin"] = _dataclass_to_schema(configs.plugin)

    if output_format == "json":
        click.echo(json.dumps(info, indent=2))
        return

    # Text Output
    click.echo(f"Name:        {info['name']}")
    click.echo(f"Version:     {info['version']} ({info['distribution']})")
    click.echo(f"Module:      {info['module']}")
    if info["product_name"]:
        click.echo(f"Product:     {info['product_name']}")
    if info["description"]:
        click.echo(f"Description: {info['description']}")

    click.echo("\nOptions Sections:")
    for tier, schema in info["tiers"].items():
        if not schema:
            continue
        click.echo(f"  [{tier.upper()}]")
        for key, field_info in schema.items():
            req_marker = "*" if field_info["required"] else " "
            default_str = (
                f" (default: {field_info['default']})" if not field_info["required"] else ""
            )
            click.echo(f"    {req_marker} {key} [{field_info['type']}]{default_str}")

    if info.get("products"):
        click.echo("\nProducts:")
        for key, value in info["products"].items():
            click.echo(f"  - {key}: {value}")


@click.command(
    "explain",
    epilog="""\b
Examples:
  # explain a specific option
  firecube plugins explain <plugin>.<tier>.<field>

\b
  # show explanation as JSON
  firecube plugins explain <plugin>.<tier>.<field> -f json

See also: firecube plugins describe, firecube plugins list
""",
)
@click.argument("path")
@click.option(
    "-f", "--format", "output_format", type=click.Choice(["table", "json", "csv"]), default="table"
)
def explain_plugin(path: str, output_format: str) -> None:
    """explain a specific config option

    Explains a config option for a plugin by its dotted path. Shows type,
    default, description, and allowed values for a single option field. The
    path format is <plugin>.<tier>.<field> (e.g. example_plugin.ingest.option).
    """

    parts = path.split(".")
    if len(parts) < 1:
        raise click.UsageError("Path must be at least <plugin>.")

    plugin_name = parts[0]
    tier_name = parts[1] if len(parts) > 1 else None
    field_name = parts[2] if len(parts) > 2 else None

    try:
        configs = resolve_plugin_configs(plugin_name)
    except KeyError as exc:
        raise click.ClickException(f"Plugin '{plugin_name}' not found.") from exc

    if not tier_name:
        click.echo(f"Plugin '{plugin_name}' has tiers: engine, template, plugin.")
        return

    tier_cls = getattr(configs, tier_name, None)
    if not tier_cls:
        raise click.ClickException(
            f"Tier '{tier_name}' not found in plugin '{plugin_name}'. Available: engine, template, plugin."
        )

    if not field_name:
        # Explain the whole tier
        schema = _dataclass_to_schema(tier_cls)
        if output_format == "json":
            click.echo(json.dumps(schema, indent=2))
        else:
            click.echo(f"Configuration for '{plugin_name}.{tier_name}':")
            for key, field_info in schema.items():
                click.echo(f"  {key}: {field_info['type']}")
        return

    # Explain specific field
    try:
        type_hints = typing.get_type_hints(tier_cls, include_extras=True)
    except Exception:
        type_hints = {f.name: f.type for f in dataclasses.fields(tier_cls)}

    target_field = None
    for field in dataclasses.fields(tier_cls):
        if field.name == field_name:
            target_field = field
            break

    if not target_field:
        raise click.ClickException(
            f"Field '{field_name}' not found in '{plugin_name}.{tier_name}'."
        )

    # Resolve real type
    resolved_type = type_hints.get(target_field.name, target_field.type)

    # Determine required
    required = True
    if (
        target_field.default is not dataclasses.MISSING
        or target_field.default_factory is not dataclasses.MISSING
    ):
        required = False

    origin = typing.get_origin(resolved_type)
    if origin is typing.Union:
        args = typing.get_args(resolved_type)
        if type(None) in args:
            required = False

    # Generate details
    details = {
        "path": path,
        "type": _type_to_str(resolved_type),
        "default": (
            str(target_field.default) if target_field.default is not dataclasses.MISSING else None
        ),
        "required": required,
    }

    if output_format == "json":
        click.echo(json.dumps(details, indent=2))
    else:
        click.echo(f"Field:    {details['path']}")
        click.echo(f"Type:     {details['type']}")
        click.echo(f"Required: {details['required']}")
        if not details["required"]:
            click.echo(f"Default:  {details['default']}")
