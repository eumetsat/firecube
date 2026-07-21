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

from collections.abc import Iterator

import click
import pytest
from click.testing import CliRunner

from firecube.cli._audience import (
    INTERNAL_TOKENS_USER_FORBIDDEN,
    TIER_1_PATHS,
    TIER_3_PATHS,
    USER_FACING_PATHS,
)
from firecube.cli.main import cli

pytestmark = pytest.mark.docs_static


def _iter_commands(
    group: click.Command,
    path: tuple[str, ...] = (),
) -> Iterator[tuple[tuple[str, ...], click.Command]]:
    """Recursively yield (path_tuple, command) for all commands."""

    if isinstance(group, click.Group):
        for name, cmd in group.commands.items():
            yield from _iter_commands(cmd, (*path, name))
    yield path, group


ALL_COMMANDS = list(_iter_commands(cli))
LEAF_COMMANDS = [(path, cmd) for path, cmd in ALL_COMMANDS if path]
USER_FACING_COMMANDS = [(p, c) for p, c in LEAF_COMMANDS if p in USER_FACING_PATHS]
TIER_1_COMMANDS = [(p, c) for p, c in LEAF_COMMANDS if p in TIER_1_PATHS]
TIER_3_COMMANDS = [(p, c) for p, c in LEAF_COMMANDS if p in TIER_3_PATHS]


def _ids(commands: list[tuple[tuple[str, ...], click.Command]]) -> list[str]:
    return [".".join(path) for path, _ in commands]


@pytest.mark.parametrize(
    "path,cmd",
    USER_FACING_COMMANDS,
    ids=_ids(USER_FACING_COMMANDS),
)
def test_user_facing_hides_internals(path: tuple[str, ...], cmd: click.Command) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, [*path, "--help"], prog_name="firecube")
    for token in INTERNAL_TOKENS_USER_FORBIDDEN:
        assert token not in result.output, (
            f"Forbidden internal token '{token}' found in user-facing help for {path}"
        )
    assert cmd is not None


@pytest.mark.parametrize("path,cmd", TIER_1_COMMANDS, ids=_ids(TIER_1_COMMANDS))
def test_tier1_has_safety_flags(path: tuple[str, ...], cmd: click.Command) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, [*path, "--help"], prog_name="firecube")
    assert "--dry-run" in result.output, f"Tier 1 command {path} missing --dry-run"
    assert "--yes-i-really-mean-it" in result.output, (
        f"Tier 1 command {path} missing --yes-i-really-mean-it"
    )
    assert cmd is not None


@pytest.mark.parametrize("path,cmd", TIER_3_COMMANDS, ids=_ids(TIER_3_COMMANDS))
def test_tier3_no_confirmation_required(
    path: tuple[str, ...],
    cmd: click.Command,
) -> None:
    runner = CliRunner()
    result = runner.invoke(cli, [*path, "--help"], prog_name="firecube")
    assert "--yes-i-really-mean-it" not in result.output, (
        f"Tier 3 idempotent command {path} should NOT advertise --yes-i-really-mean-it"
    )
    assert cmd is not None
