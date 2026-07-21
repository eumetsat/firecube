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

from typing import Any, get_type_hints

import click

from firecube.cli._errors import UnknownOptionError
from firecube.cli._shared_options import format_option as display_format_option
from firecube.cli.plugins.registry import PluginConfigSchemas, resolve_plugin_configs
from firecube.ingestor.config.coercion import coerce_cli_value
from firecube.ingestor.config.engine import is_experimental_option_key

__all__ = [
    "TypedOptionsParam",
    "coerce_options_for_plugin",
    "display_format_option",
    "output_path_option",
]


def output_path_option(
    required: bool = True,
    help_text: str = "destination file path",
) -> Any:
    """Shared ``-o/--output`` decorator for file destination paths.

    The decorated callback receives the value as ``output_path``. The type is
    ``click.Path()`` (string path), distinct from ``workspace_option`` which uses
    ``click.Path(path_type=Path)``.

    Args:
        required: Whether ``--output`` must be provided.
        help_text: Help text displayed in ``--help`` output.
    """

    def decorator(f: Any) -> Any:
        return click.option(
            "-o",
            "--output",
            "output_path",
            required=required,
            type=click.Path(),
            help=help_text,
        )(f)

    return decorator


# Keys owned by dedicated `firecube ingest` flags. They are valid EngineConfig
# fields, so they would pass unknown-key validation — but the free-form merge
# happens after typed-flag resolution and would silently override the explicit
# flag. Hard-reject at parse time.
_TYPED_FLAG_OWNED_KEYS: dict[str, str] = {
    "write_mode": "--write-mode",
    "slot_start": "--slot-start",
    "slot_end": "--slot-end",
    "slot_size": "--slot-size",
    "slot_group": "--slot-group",
}


class TypedOptionsParam(click.ParamType):
    """Parse-time validation of ``--option key=value`` pairs against plugin schema."""

    name = "TYPED_OPTION"

    def __init__(self, plugin_name: str | None = None) -> None:
        """Create a typed option parser.

        Args:
            plugin_name: Optional plugin name for parse-time schema validation. If omitted,
                the parser attempts to read ``plugin`` from the Click context.
        """
        self._plugin_name = plugin_name

    def convert(
        self,
        value: str,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> tuple[str, Any]:
        if not isinstance(value, str):
            self.fail("Option must be a key=value string.", param, ctx)

        if "=" not in value:
            self.fail("Use key=value syntax (e.g., --option pipeline_workers=4).", param, ctx)

        key, raw_value = value.split("=", 1)
        key = key.strip()

        if not key:
            self.fail("Option key cannot be empty.", param, ctx)
        if raw_value == "":
            self.fail(f"Option value for '{key}' cannot be empty.", param, ctx)

        owning_flag = _TYPED_FLAG_OWNED_KEYS.get(key)
        if owning_flag is not None:
            self.fail(
                f"'{key}' is owned by the typed flag {owning_flag}; "
                f"pass {owning_flag} instead of --option {key}=...",
                param,
                ctx,
            )

        if is_experimental_option_key(key):
            return key, raw_value

        plugin_name = self._plugin_name
        if plugin_name is None and ctx is not None:
            plugin_name = ctx.params.get("plugin")

        if plugin_name:
            try:
                schemas = resolve_plugin_configs(str(plugin_name))
                valid_keys = _enumerate_all_valid_keys(schemas)
                if valid_keys and key not in valid_keys:
                    raise UnknownOptionError(key, str(plugin_name), sorted(valid_keys))

                target_type = _get_field_type(schemas, key)
                if target_type is not None:
                    try:
                        return key, coerce_cli_value(raw_value, target_type, key)
                    except (TypeError, ValueError) as exc:
                        self.fail(str(exc), param, ctx)
            except (ImportError, AttributeError, KeyError):
                pass

        return key, raw_value


def coerce_options_for_plugin(
    plugin: str,
    extra_options: tuple[tuple[str, object], ...],
) -> tuple[tuple[str, object], ...]:
    """Validate/coerce parsed ``--option`` pairs against a resolved plugin name."""
    typed_param = TypedOptionsParam(plugin)
    coerced: list[tuple[str, object]] = []
    for key, value in extra_options:
        if isinstance(value, str):
            coerced.append(typed_param.convert(f"{key}={value}", None, None))
        else:
            coerced.append((key, value))
    return tuple(coerced)


def _enumerate_all_valid_keys(schemas: PluginConfigSchemas) -> set[str]:
    """Gather all valid ``--option`` keys across engine, template, and plugin tiers."""
    from firecube.ingestor.config.engine import config_keys

    keys: set[str] = set()
    keys.update(config_keys(schemas.engine))
    if schemas.template is not None:
        keys.update(config_keys(schemas.template))
    if schemas.plugin is not None:
        keys.update(config_keys(schemas.plugin))
    return keys


def _get_field_type(schemas: PluginConfigSchemas, key: str) -> Any | None:
    """Return the expected Python annotation for an option key from any tier."""
    for cls in (schemas.engine, schemas.template, schemas.plugin):
        if cls is None:
            continue
        hints = get_type_hints(cls)
        if key in hints:
            return hints[key]
    return None
