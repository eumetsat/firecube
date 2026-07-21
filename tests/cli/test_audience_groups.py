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

import click
import pytest

from firecube.cli._audience import (
    GROUP_AUDIENCE,
    OPERATOR_FACING_PATHS,
    USER_FACING_PATHS,
)
from firecube.cli.main import cli as _root_cli


def _top_level_groups() -> list[tuple[str, click.Group]]:
    return [(name, cmd) for name, cmd in _root_cli.commands.items() if isinstance(cmd, click.Group)]


_TOP_LEVEL_GROUPS = _top_level_groups()
_TOP_LEVEL_GROUP_IDS = [name for name, _ in _TOP_LEVEL_GROUPS]


def test_group_audience_mapping_exists() -> None:
    assert isinstance(GROUP_AUDIENCE, dict)
    assert GROUP_AUDIENCE, "GROUP_AUDIENCE must classify at least one group"


def test_group_audience_values_are_valid() -> None:
    for name, audience in GROUP_AUDIENCE.items():
        assert audience in {"user", "operator"}, (
            f"GROUP_AUDIENCE[{name!r}] = {audience!r}; expected 'user' or 'operator'"
        )


@pytest.mark.parametrize(
    "name,group",
    _TOP_LEVEL_GROUPS,
    ids=_TOP_LEVEL_GROUP_IDS,
)
def test_every_top_level_group_has_audience(name: str, group: click.Group) -> None:
    assert name in GROUP_AUDIENCE, (
        f"Top-level Click group {name!r} has no audience classification. "
        f"Add it to GROUP_AUDIENCE in src/firecube/cli/_audience.py."
    )
    assert group is not None


def test_user_facing_groups_have_no_operator_only_leaves() -> None:
    for name, audience in GROUP_AUDIENCE.items():
        if audience != "user":
            continue
        operator_leaves = sorted(path for path in OPERATOR_FACING_PATHS if path and path[0] == name)
        assert not operator_leaves, (
            f"User-facing group {name!r} contains operator-facing leaves: "
            f"{operator_leaves}. Either reclassify the group or move the leaves."
        )


def test_operator_facing_groups_have_no_user_only_leaves() -> None:
    for name, audience in GROUP_AUDIENCE.items():
        if audience != "operator":
            continue
        user_leaves = sorted(path for path in USER_FACING_PATHS if path and path[0] == name)
        assert not user_leaves, (
            f"Operator-facing group {name!r} contains user-facing leaves: "
            f"{user_leaves}. Either reclassify the group or move the leaves."
        )
