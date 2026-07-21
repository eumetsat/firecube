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
    OPERATOR_FACING_PATHS,
    USER_FACING_PATHS,
    classify,
)


def test_classify_user_ingest() -> None:
    assert classify(("ingest",)) == "user"


def test_classify_operator_delete() -> None:
    assert classify(("chunks", "delete")) == "operator"


def test_classify_user_validate() -> None:
    assert classify(("zarr", "validate")) == "user"


def test_classify_operator_archive_create() -> None:
    assert classify(("archive", "create")) == "operator"


def test_classify_unknown_path_raises_click_exception() -> None:
    with pytest.raises(click.ClickException, match="Unknown command path"):
        classify(("nonexistent", "cmd"))


@pytest.mark.parametrize("path", sorted(USER_FACING_PATHS))
def test_all_user_facing_paths_classify_as_user(path: tuple[str, ...]) -> None:
    assert classify(path) == "user"


@pytest.mark.parametrize("path", sorted(OPERATOR_FACING_PATHS))
def test_all_operator_facing_paths_classify_as_operator(path: tuple[str, ...]) -> None:
    assert classify(path) == "operator"


def test_user_and_operator_paths_do_not_overlap() -> None:
    assert USER_FACING_PATHS.isdisjoint(OPERATOR_FACING_PATHS)
