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

import re
import shlex
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


EPILOG_COMMANDS = [(p, c) for p, c in LEAF_COMMANDS if c.epilog]
REMOVED_SPELLING = re.compile(
    r"(?<!\S)(-[nps]|--product|--source|--output-format|--quiet|--json)(?=[\s=]|$)"
)


def _examples(epilog: str) -> list[str]:
    """Return each example line joined, reduced to its ``firecube`` invocation.

    The invocation is cut at a pipe, ``&&`` or the closing ``)`` of ``$(...)``. A line that
    holds no invocation is returned whole, so the parse test fails on it.
    """

    text = re.sub(r"\\\n\s*|\x08", " ", epilog.partition("Examples:")[2])
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    lines = [ln for ln in lines if not ln.startswith(("#", "See also")) and ln[-1] != ":"]
    found = [re.search(r"\bfirecube\b[^|&)]*", ln) for ln in lines]
    return [m.group().strip() if m else ln for m, ln in zip(found, lines, strict=True)]


def _parse_example(example: str) -> click.Command:
    tokens = shlex.split(re.sub(r"<[\w-]+>", "x", example))
    if tokens[0] != "firecube":
        raise click.UsageError("no firecube invocation found")
    args = tokens[1:]
    ctx = cli.make_context("firecube", args)
    group: click.Command | None = cli
    rest = [*ctx._protected_args, *ctx.args]
    while isinstance(group, click.Group):
        name, group, rest = group.resolve_command(ctx, rest)
        assert group is not None
        ctx = group.make_context(name, rest, parent=ctx)
        rest = [*ctx._protected_args, *ctx.args] if isinstance(group, click.Group) else []
    return ctx.command


@pytest.mark.parametrize("path,cmd", LEAF_COMMANDS, ids=_ids(LEAF_COMMANDS))
def test_help_prints_each_section_heading_once(path: tuple[str, ...], cmd: click.Command) -> None:
    result = CliRunner().invoke(cli, [*path, "--help"], prog_name="firecube")
    headings = [ln for ln in result.output.splitlines() if ln.endswith(":") and ln[0] != " "]
    repeated = sorted({h for h in headings if headings.count(h) > 1})
    assert not repeated, f"{path} help prints a section heading twice: {repeated}"


@pytest.mark.parametrize("path,cmd", EPILOG_COMMANDS, ids=_ids(EPILOG_COMMANDS))
def test_epilog_has_examples_block(path: tuple[str, ...], cmd: click.Command) -> None:
    assert "Examples:" in (cmd.epilog or ""), f"{path} epilog has no Examples block"
    assert _examples(cmd.epilog or ""), f"{path} Examples block holds no firecube command"


@pytest.mark.parametrize("path,cmd", EPILOG_COMMANDS, ids=_ids(EPILOG_COMMANDS))
def test_epilog_examples_parse_against_their_command(
    path: tuple[str, ...], cmd: click.Command
) -> None:
    failures = []
    for example in _examples(cmd.epilog or ""):
        try:
            resolved = _parse_example(example)
        except click.ClickException as exc:
            failures.append(f"{example!r}: {exc.format_message()}")
        else:
            if resolved is not cmd:
                failures.append(f"{example!r} resolves to {resolved.name!r}, not this command")
    assert not failures, f"{path} examples do not parse: {failures}"


@pytest.mark.parametrize("path,cmd", EPILOG_COMMANDS, ids=_ids(EPILOG_COMMANDS))
def test_epilog_uses_no_removed_spelling(path: tuple[str, ...], cmd: click.Command) -> None:
    lines = (cmd.epilog or "").splitlines()
    bad = [ln.strip() for ln in lines if "/tmp" in ln or REMOVED_SPELLING.search(ln)]
    bad += [ex for ex in _examples(cmd.epilog or "") if "file://" in ex and "--storage-" in ex]
    assert not bad, (
        f"{path} epilog lines use a removed spelling, /tmp or a storage flag beside file://: {bad}"
    )
