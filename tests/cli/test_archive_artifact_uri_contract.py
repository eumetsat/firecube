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

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli


class _EmptyTensogramFile:
    def __enter__(self) -> _EmptyTensogramFile:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def message_count(self) -> int:
        return 0


def _install_fake_tensogram(monkeypatch: pytest.MonkeyPatch, seen: dict[str, str]) -> None:
    class FakeTensogramFile:
        @staticmethod
        def open(path: str) -> _EmptyTensogramFile:
            seen["path"] = path
            return _EmptyTensogramFile()

    fake_module = SimpleNamespace(
        TensogramFile=FakeTensogramFile,
        validate_file=lambda path, level="default": (
            seen.setdefault("path", path) and {"file_issues": [], "messages": []}
        ),
    )

    monkeypatch.setattr(
        "firecube.core.tensogram._compat.require_tensogram",
        lambda _caller: None,
    )
    monkeypatch.setitem(sys.modules, "tensogram", fake_module)


@pytest.mark.parametrize("subcommand", ["info", "validate", "list"])
def test_archive_artifact_inspect_accepts_file_uri(
    subcommand: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / "archive.tgm"
    archive_path.write_bytes(b"not a real archive")
    seen: dict[str, str] = {}
    _install_fake_tensogram(monkeypatch, seen)

    result = CliRunner().invoke(cli, ["archive", subcommand, "--archive", archive_path.as_uri()])

    assert result.exit_code != 2
    assert seen["path"] == str(archive_path)
