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
import os

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.errors import SlotWindowError
from firecube.core.runtime import resolve_slot_window


def test_cli_overrides_env() -> None:
    env = {"FIRECUBE_SLOT_START": "100", "FIRECUBE_SLOT_END": "200"}

    assert resolve_slot_window(0, 50, None, None, env=env) == (0, 50, None)


def test_env_vars_used_when_cli_absent() -> None:
    env = {"FIRECUBE_SLOT_START": "100", "FIRECUBE_SLOT_END": "200"}
    env |= {"JOB_COMPLETION_INDEX": "2", "FIRECUBE_SLOT_SIZE": "100"}

    assert resolve_slot_window(None, None, None, None, env=env) == (100, 200, None)


def test_explicit_env_wins_and_os_environ_is_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_START", "900")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "950")
    before = dict(os.environ)

    env = {"FIRECUBE_SLOT_START": "1", "FIRECUBE_SLOT_END": "2"}

    assert resolve_slot_window(None, None, None, None, env=env) == (1, 2, None)
    assert resolve_slot_window(None, None, None, None, env={}) == (None, None, None)
    assert dict(os.environ) == before


def test_job_completion_index_with_size() -> None:
    env = {"JOB_COMPLETION_INDEX": "2", "FIRECUBE_SLOT_SIZE": "100"}

    assert resolve_slot_window(None, None, None, None, env=env) == (200, 300, None)


def test_job_completion_index_without_size_warns(caplog) -> None:
    caplog.set_level(logging.WARNING)

    env = {"JOB_COMPLETION_INDEX": "2"}

    assert resolve_slot_window(None, None, None, None, env=env) == (None, None, None)
    assert "parallel mode NOT activated" in caplog.text


def test_no_env_returns_none() -> None:
    assert resolve_slot_window(None, None, None, None, env={}) == (None, None, None)


def test_library_half_specified_window_raises_slot_window_error() -> None:
    with pytest.raises(SlotWindowError, match="slot_start and slot_end must be provided together"):
        resolve_slot_window(0, None, None, None, env={})


@pytest.mark.parametrize(
    "command",
    [["ingest", "cli_test_plugin"], ["zarr", "preallocate", "direct_zarr_capable_test_plugin"]],
    ids=["ingest", "preallocate"],
)
def test_slot_start_without_slot_end_exits_2(tmp_path, command: list[str]) -> None:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    argv = ["--config-file", str(config), *command]
    argv += ["--target", (tmp_path / "s.zarr").as_uri(), "--write-mode", "direct"]
    argv += ["--product-name", "qa", "--storage-type", "local", "--storage-driver", "fsspec"]

    result = CliRunner().invoke(cli, [*argv, "--slot-start", "0"])

    assert result.exit_code == 2, result.output
    assert "--slot-start" in result.output and "--slot-end" in result.output
    assert not (tmp_path / "s.zarr").exists()


def test_invalid_env_raises() -> None:
    env = {"FIRECUBE_SLOT_START": "abc", "FIRECUBE_SLOT_END": "200"}

    with pytest.raises(SlotWindowError, match="must be an integer"):
        resolve_slot_window(None, None, None, None, env=env)
