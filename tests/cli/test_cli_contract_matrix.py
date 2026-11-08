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

from importlib import import_module
from typing import Any, cast

import click
import pytest

from firecube.cli.main import cli as _root_cli
from tests.cli._cli_introspection import enumerate_commands

_command_contracts = import_module("tests.cli._command_contracts")
CLI_COMMAND_CONTRACTS = cast(Any, _command_contracts).CLI_COMMAND_CONTRACTS
pytestmark = pytest.mark.contract


CANONICAL_SHORT_FLAGS: dict[str, str] = {
    "-t": "--target",
    "-a": "--archive",
    "-o": "--output",
    "-i": "--input-data",
    "-g": "--group",
    "-w": "--write-mode",
    "-f": "--format",
    "-r": "--resolutions",
}
SHORT_FLAG_ALLOWLIST: dict[tuple[str, ...], dict[str, str]] = {
    ("plugins", "install"): {"-e": "--editable"},
}


def _walk_commands(
    cmd: click.Command, path: tuple[str, ...], groups: bool = False
) -> list[tuple[tuple[str, ...], click.Command]]:
    """Return the leaves under ``cmd``; with ``groups``, also ``cmd`` and every group."""
    if isinstance(cmd, click.Group):
        result: list[tuple[tuple[str, ...], click.Command]] = [(path, cmd)] if groups else []
        for name, child in cmd.commands.items():
            result.extend(_walk_commands(child, (*path, name), groups))
        return result
    return [(path, cmd)]


def test_no_new_leaf_command_without_contract() -> None:
    specs = enumerate_commands()

    for spec in specs:
        if spec.hidden:
            continue
        assert spec.path in CLI_COMMAND_CONTRACTS, (
            f"Command {spec.path!r} is not in CLI_COMMAND_CONTRACTS. Add it."
        )


def test_manifest_entries_match_actual_command_options() -> None:
    specs = {spec.path: spec for spec in enumerate_commands()}

    for path, entry in CLI_COMMAND_CONTRACTS.items():
        if entry.tier != "write":
            continue
        spec = specs.get(path)
        if spec is None:
            continue
        for flag in entry.required_storage_flags:
            assert flag in spec.required_options or flag in spec.optional_options, (
                f"Manifest says {path} write-tier requires {flag} but it's not in Click options"
            )


_ROWS = list(CLI_COMMAND_CONTRACTS.items())
_LABELLED_ROWS = [(path, entry) for path, entry in _ROWS if entry.label_source != "none"]


def _ids(rows: list[tuple[tuple[str, ...], Any]]) -> list[str]:
    return [".".join(path) for path, _entry in rows]


def _options(path: tuple[str, ...]) -> frozenset[str]:
    spec = next(s for s in enumerate_commands() if s.path == path)
    return spec.required_options | spec.optional_options


@pytest.mark.parametrize("short", ["-n", "-p", "-s"])
def test_dropped_short_flags_are_not_reused(short: str) -> None:
    bound = [
        ".".join(path) or "<root>"
        for path, cmd in _walk_commands(_root_cli, (), groups=True)
        for param in cmd.params
        if isinstance(param, click.Option) and short in (*param.opts, *param.secondary_opts)
    ]
    assert bound == []


@pytest.mark.parametrize(("path", "entry"), _LABELLED_ROWS, ids=_ids(_LABELLED_ROWS))
def test_product_commands_bind_target(path: tuple[str, ...], entry: Any) -> None:
    options = _options(path)
    assert "--target" in options
    assert not options & {"--product", "--source"}
    command = dict(_walk_commands(_root_cli, ()))[path]
    assert any({"-t", "--target"} <= set(param.opts) for param in command.params)


@pytest.mark.parametrize(("path", "entry"), _ROWS, ids=_ids(_ROWS))
def test_product_name_exists_iff_label_is_cli_owned(path: tuple[str, ...], entry: Any) -> None:
    assert ("--product-name" in _options(path)) == (entry.label_source == "cli")


def test_short_flags_match_canonical_registry() -> None:
    for path, cmd in _walk_commands(_root_cli, ()):
        allow = SHORT_FLAG_ALLOWLIST.get(path, {})
        for param in cmd.params:
            if not isinstance(param, click.Option):
                continue
            all_opts = list(param.opts) + list(getattr(param, "secondary_opts", []))
            shorts = [
                o for o in all_opts if len(o) == 2 and o.startswith("-") and not o.startswith("--")
            ]
            longs = [o for o in all_opts if o.startswith("--")]
            for short in shorts:
                if short in allow:
                    assert allow[short] in longs, (
                        f"{'.'.join(path)}: allowlisted {short} but missing {allow[short]}"
                    )
                    continue
                if short in CANONICAL_SHORT_FLAGS:
                    assert CANONICAL_SHORT_FLAGS[short] in longs, (
                        f"{'.'.join(path)}: {short} bound to {longs}, "
                        f"registry says {CANONICAL_SHORT_FLAGS[short]}"
                    )
                else:
                    assert short in CANONICAL_SHORT_FLAGS or short in allow, (
                        f"{'.'.join(path)}: short flag {short} (bound to {longs}) "
                        f"is not in the canonical registry and not allow-listed"
                    )
