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

"""Render a library `Message` with the flags and commands of the live Click definitions.

Import it through `firecube.cli._errors.render`, which loads this module on first use,
so `firecube` start-up loads nothing new.
"""

from __future__ import annotations

import shlex
from collections.abc import Iterator, Mapping
from types import MappingProxyType

import click

from firecube.core.errors import (
    CommandName,
    Message,
    Operation,
    Param,
    Placeholder,
    PluginOption,
)

OPERATIONS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "adopt_product": ("migrate", "adopt"),
        "abandon_run": ("chunks", "runs", "abandon"),
        "clear_claim": ("chunks", "claims", "clear"),
        "rebuild_compact": ("chunks", "compact", "rebuild"),
        "delete_span": ("chunks", "delete-span"),
        "preallocate": ("zarr", "preallocate"),
        "rebuild_index": ("zarr", "index", "rebuild"),
        "validate": ("zarr", "validate"),
    }
)
"""The command path after `firecube` of each operation in `OPERATION_FUNCTIONS`."""

# Confirmation options a rendered command never carries: the operator adds one knowingly.
_CONFIRMATIONS = frozenset({"yes_i_really_mean_it"})


def render(value: BaseException | str, ctx: click.Context | None = None) -> str:
    """Return the operator text of an exception, a `Message` or a string.

    An exception whose first argument is a `Message` renders that message; any other
    exception gives `str(exc)`. A plain string comes back unchanged.

    Args:
        value: The exception, message or line to print.
        ctx: The running command's context; defaults to the current Click context.

    Raises:
        ValueError: An `Operation` names an argument its command does not declare.
        LookupError: An operation's command path is missing from the CLI.
    """
    if ctx is None:
        ctx = click.get_current_context(silent=True)
    if isinstance(value, BaseException):
        message = _exception_message(value)
        return str(value) if message is None else _render_message(message, ctx)
    if isinstance(value, Message):
        return _render_message(value, ctx)
    return value


def _exception_message(exc: BaseException) -> Message | None:
    """Return the exception's first argument when it is a `Message`, else `None`."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else None


def _render_message(message: Message, ctx: click.Context | None) -> str:
    """Fill the template with each part's CLI text; plain values format as in the library."""
    values = {name: _render_part(part, ctx) for name, part in message.parts.items()}
    return message.template.format_map(values)


def _render_part(part: object, ctx: click.Context | None) -> object:
    if isinstance(part, Message):
        return _render_message(part, ctx)
    if isinstance(part, Param):
        return _render_param(part, ctx)
    if isinstance(part, PluginOption):
        return f"--option {shlex.quote(str(part))}"
    if isinstance(part, Operation):
        return _render_operation(part)
    if isinstance(part, CommandName):
        return _render_command_name(part, ctx)
    if isinstance(part, Placeholder):
        return part.text
    return part


def _contexts(ctx: click.Context | None) -> Iterator[click.Context]:
    """Yield the running context, then each parent up to the root."""
    while ctx is not None:
        yield ctx
        ctx = ctx.parent


def _long_option(option: click.Option, *, secondary: bool = False) -> str | None:
    """Return the first `--` spelling of an option (its `--no-` side when `secondary`)."""
    spellings = option.secondary_opts if secondary else option.opts
    return next((s for s in spellings if s.startswith("--")), spellings[0] if spellings else None)


def _flag_text(option: click.Option, value: object) -> str | None:
    """Spell a flag set to `value`; `None` when a false flag has no `--no-` side."""
    return _long_option(option, secondary=not value)


def _value_text(value: object) -> str:
    return value.text if isinstance(value, Placeholder) else shlex.quote(str(value))


def _render_param(param: Param, ctx: click.Context | None) -> str:
    """Spell `param` as the running command's option, or as `str(param)` when it has none.

    A flag prints its long option alone, whatever the value.
    """
    for context in _contexts(ctx):
        for option in context.command.params:
            if not isinstance(option, click.Option) or option.name != param.name:
                continue
            spelling = _long_option(option)
            if spelling is None:
                return str(param)
            if option.is_flag or param.value is None:
                return spelling
            return f"{spelling} {_value_text(param.value)}"
    return str(param)


def _render_command_name(name: CommandName, ctx: click.Context | None) -> str:
    """Print `firecube <path>`, or the running command's path built from the context chain."""
    if name.path:
        return " ".join(("firecube", *name.path))
    if ctx is None:
        return str(name)
    words = [context.info_name or "" for context in _contexts(ctx) if context.parent is not None]
    return " ".join(("firecube", *reversed(words)))


def _command(path: tuple[str, ...]) -> click.Command:
    """Look up a command of the `firecube` root group by its path."""
    from firecube.cli.main import cli

    lookup_ctx = click.Context(cli, info_name="firecube")
    command: click.Command = cli
    for word in path:
        found = command.get_command(lookup_ctx, word) if isinstance(command, click.Group) else None
        if found is None:
            raise LookupError(f"firecube has no command {' '.join(path)!r}")
        command = found
    return command


def _render_operation(operation: Operation) -> str:
    """Print the operation's command with its arguments in the command's declaration order.

    A missing or `None` required argument prints as `<name>`; confirmation options never print.
    """
    path = OPERATIONS[operation.name]
    command = _command(path)
    declared = {param.name for param in command.params}
    unknown = sorted(set(operation.arguments) - declared)
    if unknown:
        raise ValueError(
            f"Operation {operation.name!r} names arguments {unknown} that "
            f"`firecube {' '.join(path)}` does not declare"
        )
    words = ["firecube", *path]
    for param in command.params:
        if param.name is None or param.name in _CONFIRMATIONS:
            continue
        value = operation.arguments.get(param.name)
        if isinstance(param, click.Option):
            words.extend(_option_words(param, value))
        elif value is not None:
            words.extend(_value_text(item) for item in _values(param, value))
        elif param.required:
            words.append(f"<{param.name}>")
    return " ".join(words)


def _option_words(option: click.Option, value: object) -> list[str]:
    """Spell one option of a rendered command; a missing required one prints `<name>`."""
    spelling = _long_option(option)
    if spelling is None:
        return []
    if value is None:
        return [spelling, f"<{option.name}>"] if option.required else []
    if option.is_flag:
        text = _flag_text(option, value)
        return [] if text is None else [text]
    words: list[str] = []
    for item in _values(option, value):
        words.extend((spelling, _value_text(item)))
    return words


def _values(param: click.Parameter, value: object) -> list[object]:
    """Split a repeated or variadic parameter's list into its values."""
    if (param.multiple or param.nargs == -1) and isinstance(value, list | tuple):
        return list(value)
    return [value]
