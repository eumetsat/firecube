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

import logging

import click
import pytest

from firecube.cli._slot_env import resolve_slot_range_from_env


def test_cli_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_START", "100")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "200")

    assert resolve_slot_range_from_env(0, 50, None) == (0, 50, None)


def test_env_vars_used_when_cli_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_START", "100")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "200")

    assert resolve_slot_range_from_env(None, None, None) == (100, 200, None)


def test_job_completion_index_with_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("FIRECUBE_SLOT_SIZE", "100")

    assert resolve_slot_range_from_env(None, None, None) == (200, 300, None)


def test_job_completion_index_without_size_warns(
    monkeypatch: pytest.MonkeyPatch,
    caplog,
) -> None:
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    caplog.set_level(logging.WARNING)

    assert resolve_slot_range_from_env(None, None, None) == (None, None, None)
    assert "parallel mode NOT activated" in caplog.text


def test_no_env_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    assert resolve_slot_range_from_env(None, None, None) == (None, None, None)


def test_invalid_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_START", "abc")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "200")

    with pytest.raises(click.UsageError, match="must be an integer"):
        resolve_slot_range_from_env(None, None, None)
