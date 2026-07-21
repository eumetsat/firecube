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

"""Click introspection helper for firecube CLI docs-example tests.

Provides three services:
1. Enumerate all static leaf commands in the Click tree.
2. Return the effective required options for a command path (Click
   ``required=True`` flags UNION semantic-required overrides).
3. Strip root-group options from token sequences to find the subcommand path.
"""

from __future__ import annotations

import dataclasses
import functools

import click

from firecube.cli.main import cli as _root_cli


@dataclasses.dataclass(frozen=True)
class CommandSpec:
    path: tuple[str, ...]
    required_options: frozenset[str]
    optional_options: frozenset[str]
    required_args: tuple[str, ...]
    hidden: bool


# Sourced from src/firecube/cli/_command_schemas.py:IngestCommandConfig.__post_init__
# These four flags are NOT marked required=True at the Click decorator level
# (src/firecube/cli/main.py:451-496) but ARE enforced post-parse via
# IngestCommandConfig.__post_init__ which raises click.UsageError when missing.
# If that __post_init__ changes, update this map AND run the drift-guard test.
_SEMANTIC_REQUIRED_OVERRIDES: dict[tuple[str, ...], frozenset[str]] = {
    ("ingest",): frozenset(
        {
            "--target",
            "--write-mode",
        }
    ),
}


def _preferred_option_name(param: click.Option) -> str:
    long_opts = [opt for opt in param.opts if opt.startswith("--")]
    if long_opts:
        return max(long_opts, key=len)
    return param.opts[0]


def _walk(cmd: click.Command, path: tuple[str, ...], specs: list[CommandSpec]) -> None:
    if isinstance(cmd, click.Group):
        for name, child in cmd.commands.items():
            _walk(child, (*path, name), specs)
        return

    required_options = frozenset(
        _preferred_option_name(param)
        for param in cmd.params
        if isinstance(param, click.Option) and param.required and not param.is_eager
    )
    optional_options = frozenset(
        _preferred_option_name(param)
        for param in cmd.params
        if isinstance(param, click.Option) and not param.required and not param.is_eager
    )
    required_args = tuple(
        param.name.upper()
        for param in cmd.params
        if isinstance(param, click.Argument) and param.name is not None
    )
    specs.append(
        CommandSpec(
            path=path,
            required_options=required_options | _SEMANTIC_REQUIRED_OVERRIDES.get(path, frozenset()),
            optional_options=optional_options,
            required_args=required_args,
            hidden=getattr(cmd, "hidden", False),
        )
    )


@functools.lru_cache(maxsize=1)
def enumerate_commands(root: click.Group | None = None) -> tuple[CommandSpec, ...]:
    """Walk the Click command tree and return one CommandSpec per leaf command."""

    if root is None:
        root = _root_cli
    specs: list[CommandSpec] = []
    _walk(root, path=(), specs=specs)
    return tuple(specs)


@functools.lru_cache(maxsize=1)
def command_paths() -> frozenset[tuple[str, ...]]:
    return frozenset(s.path for s in enumerate_commands())


def required_options_for(path: tuple[str, ...]) -> frozenset[str]:
    for spec in enumerate_commands():
        if spec.path == path:
            return spec.required_options
    return frozenset()


@functools.lru_cache(maxsize=1)
def root_group_options() -> dict[str, bool]:
    """Return mapping of root-group option names → whether they take a value."""

    result: dict[str, bool] = {}
    for param in _root_cli.params:
        if isinstance(param, click.Option):
            takes_value = param.nargs != 0 and not param.is_flag
            for opt_name in param.opts:
                result[opt_name] = takes_value
    return result


def extract_command_path(
    tokens: list[str] | tuple[str, ...],
    known_paths: frozenset[tuple[str, ...]],
    root_opts: dict[str, bool],
) -> tuple[tuple[str, ...] | None, list[str]]:
    """Strip root-group options, then find the longest-prefix command path.

    Returns (command_path, positional_args) where command_path is None if no
    known path matches (phantom command or unknown).
    """

    tokens = list(tokens)
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t.startswith("--") and "=" in t:
            opt_name = t.split("=", 1)[0]
            if opt_name in root_opts:
                i += 1
                continue
        if t in root_opts:
            i += 2 if root_opts[t] else 1
            continue
        break
    remaining = tokens[i:]

    if not remaining:
        return (), []

    candidate: list[str] = []
    for t in remaining:
        if t.startswith("-"):
            break
        candidate.append(t)

    for n in range(len(candidate), 0, -1):
        path = tuple(candidate[:n])
        if path in known_paths:
            positional: list[str] = []
            seen_positional = False
            for t in remaining[n:]:
                if t.startswith("-"):
                    if seen_positional:
                        break
                    continue
                positional.append(t)
                seen_positional = True
            return path, positional
    positional: list[str] = []
    seen_positional = False
    for t in remaining:
        if t.startswith("-"):
            if seen_positional:
                break
            continue
        positional.append(t)
        seen_positional = True
    return None, positional


__all__ = [
    "_SEMANTIC_REQUIRED_OVERRIDES",
    "CommandSpec",
    "command_paths",
    "enumerate_commands",
    "extract_command_path",
    "required_options_for",
    "root_group_options",
]
