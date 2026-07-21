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

"""Click error boundary policy tests.

CLI boundary contract:
- Known user-input errors must surface as ``click.UsageError`` / ``click.ClickException``
  so Click renders them as clean ``Error: ...`` messages (no Python traceback).
- The ``wrap_user_facing_errors`` decorator must convert a whitelist of known
  downstream errors into ``click.ClickException`` while letting unrelated
  exceptions propagate normally.
"""

from __future__ import annotations

import click
import pytest

from firecube.cli._audience import classify
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._slot_env import _parse_int


def test_parse_int_invalid_value_raises_click_usage_error() -> None:
    with pytest.raises(click.UsageError, match="must be an integer"):
        _parse_int("notanint", name="FIRECUBE_SLOT_START")


def test_classify_unknown_path_raises_click_exception() -> None:
    with pytest.raises(click.ClickException, match="Unknown command path"):
        classify(("nonexistent", "cmd"))


def test_wrap_user_facing_errors_converts_known_zarr_errors() -> None:
    class GroupNotFoundError(Exception):
        pass

    @wrap_user_facing_errors
    def boom() -> None:
        raise GroupNotFoundError("missing group 'g'")

    with pytest.raises(click.ClickException, match="missing group 'g'"):
        boom()


def test_wrap_user_facing_errors_does_not_swallow_unexpected_exceptions() -> None:
    @wrap_user_facing_errors
    def boom() -> None:
        raise RuntimeError("internal bug")

    with pytest.raises(RuntimeError, match="internal bug"):
        boom()


def test_wrap_user_facing_errors_passes_through_click_exceptions() -> None:
    @wrap_user_facing_errors
    def boom() -> None:
        raise click.UsageError("invalid flag")

    with pytest.raises(click.UsageError, match="invalid flag"):
        boom()


def test_wrap_user_facing_errors_preserves_return_value() -> None:
    @wrap_user_facing_errors
    def ok(x: int, y: int) -> int:
        return x + y

    assert ok(2, 3) == 5
