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

"""Custom Click formatter for clean, grouped help output."""

from __future__ import annotations

import shutil
from collections.abc import Iterable
from inspect import cleandoc
from typing import Any

import click

MAX_WIDTH = 100

# Populated in main.py
COMMAND_GROUPS: dict[str, list[dict[str, Any]]] = {}
OPTION_GROUPS: dict[str, list[dict[str, Any]]] = {}


class FirecubeFormatter(click.HelpFormatter):
    """Help formatter: section headers at col 0, tight alignment."""

    def __init__(
        self,
        width: int | None = None,
        max_width: int | None = None,
        **kwargs: Any,
    ) -> None:
        terminal_width = min(shutil.get_terminal_size((80, 24)).columns, MAX_WIDTH)
        resolved_width = width or terminal_width
        resolved_max_width = max_width or MAX_WIDTH
        super().__init__(width=resolved_width, max_width=resolved_max_width, **kwargs)

    def write_heading(self, heading: str) -> None:
        """Write 'Heading:' at column 0."""
        self.write(f"\n{heading}:\n")

    def write_dl(
        self,
        rows: Iterable[tuple[str, str]],
        col_max: int = 80,
        col_spacing: int = 2,
    ) -> None:
        """Write definition list with tight alignment."""
        rows_list = list(rows)
        if not rows_list:
            return

        max_term = max((len(term) for term, _ in rows_list if term), default=0)
        col_width = min(max_term + col_spacing, col_max)

        for term, definition in rows_list:
            if not term:
                if definition:
                    self.write(f"\n{definition}:\n")
                continue

            if definition:
                self.write(f"  {term:<{col_width}}{definition}\n")
            else:
                self.write(f"  {term}\n")


class FirecubeGroup(click.Group):
    """Click Group with COMMAND_GROUPS section rendering."""

    def format_help_text(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        if self.help is None:
            return

        text = cleandoc(self.help).partition("\f")[0]
        if not text:
            return

        formatter.write_paragraph()
        with formatter.indentation():
            formatter.write_text(text.split("\n\n", 1)[0] if ctx.parent is None else text)

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        self.format_usage(ctx, formatter)
        self.format_help_text(ctx, formatter)
        self._format_commands_grouped(ctx, formatter)
        self.format_options(ctx, formatter)
        self.format_epilog(ctx, formatter)

    def format_options(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        _patched_format_options(self, ctx, formatter)

    def _format_commands_grouped(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        command_key = (ctx.command_path or ctx.info_name or self.name or "").split()[-1]
        groups_config = COMMAND_GROUPS.get(command_key, [])
        if not groups_config:
            self.format_commands(ctx, formatter)
            return

        all_names = [name for group in groups_config for name in group.get("commands", [])]
        if not all_names:
            return

        col_width = max(len(name) for name in all_names) + 2

        for group in groups_config:
            name = group.get("name", "Commands")
            commands = group.get("commands", [])
            if not commands:
                continue

            formatter.write_heading(name)
            for command_name in commands:
                command = self.get_command(ctx, command_name)
                if command is None or command.hidden:
                    continue
                help_text = command.get_short_help_str(limit=formatter.width - col_width - 4)
                formatter.write(f"  {command_name:<{col_width}}{help_text}\n")


def _patched_format_options(
    self: click.Command, ctx: click.Context, formatter: click.HelpFormatter
) -> None:
    """Render grouped options when configured for a command path."""
    groups_config = OPTION_GROUPS.get(ctx.command_path, [])

    if not groups_config:
        options = []
        for param in self.get_params(ctx):
            record = param.get_help_record(ctx)
            if record is not None:
                options.append(record)
        if options:
            with formatter.section("Options"):
                formatter.write_dl(options)
        return

    param_by_name: dict[str, click.Parameter] = {}
    for param in self.get_params(ctx):
        for opt in getattr(param, "opts", []):
            param_by_name[opt] = param

    used: set[int] = set()

    for group in groups_config:
        rows: list[tuple[str, str]] = []
        for option_name in group.get("options", []):
            param = param_by_name.get(option_name)
            if param is None:
                continue

            param_id = id(param)
            if param_id in used:
                continue

            used.add(param_id)
            record = param.get_help_record(ctx)
            if record is not None:
                rows.append(record)

        if rows:
            with formatter.section(group.get("name", "Options")):
                formatter.write_dl(rows)

    remaining = []
    for param in self.get_params(ctx):
        if id(param) in used:
            continue
        record = param.get_help_record(ctx)
        if record is not None:
            remaining.append(record)

    def _is_help_only(record: tuple[str, str]) -> bool:
        opts = record[0] if isinstance(record, tuple) else str(record)
        return "-h" in opts and "--help" in opts

    non_help_remaining = [r for r in remaining if not _is_help_only(r)]
    if non_help_remaining:
        with formatter.section("Options"):
            formatter.write_dl(non_help_remaining)


def install_option_groups_patch() -> None:
    """Monkey-patch click.Command.format_options to support OPTION_GROUPS."""
    click.Command.format_options = _patched_format_options  # type: ignore[method-assign]
    click.Context.formatter_class = FirecubeFormatter
