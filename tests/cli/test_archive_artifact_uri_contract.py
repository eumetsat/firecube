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

import json
import shlex
import sys
import zipfile
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
        "firecube.core.archive.tensogram._compat.require_tensogram",
        lambda _caller: None,
    )
    monkeypatch.setitem(sys.modules, "tensogram", fake_module)


@pytest.mark.parametrize("folder", ["", "sp ace"])
@pytest.mark.parametrize("subcommand", ["info", "validate", "list"])
def test_archive_artifact_inspect_accepts_file_uri(
    subcommand: str,
    folder: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_path = tmp_path / folder / "archive.tgm"
    archive_path.parent.mkdir(exist_ok=True)
    archive_path.write_bytes(b"not a real archive")
    seen: dict[str, str] = {}
    _install_fake_tensogram(monkeypatch, seen)

    result = CliRunner().invoke(cli, ["archive", subcommand, "--archive", f"file://{archive_path}"])

    assert result.exit_code != 2
    assert seen["path"] == str(archive_path)


@pytest.mark.contract
@pytest.mark.parametrize(
    ("line", "sentence"),
    [
        ("info --archive {p}/a.zip", "Did you mean file://{p}/a.zip?"),
        ("validate --archive {p}/a.zip", "Did you mean file://{p}/a.zip?"),
        ("list --archive {p}/a.zip", "Did you mean file://{p}/a.zip?"),
        ("create --target file://{p}/s --archive FILE://{p}/a.tgm --dry-run", "must be lowercase"),
        ("restore --target file://{p}/t --archive FILE://{p}/a.tgm --dry-run", "must be lowercase"),
        ("info --archive FILE://{p}/a.tgm", "must be lowercase"),
        ("validate --archive File://{p}/a.zip", "must be lowercase"),
        (
            "create --target file://{p}/s --archive file://localhost{p}/a.tgm --dry-run",
            "(got 'localhost')",
        ),
        ("list --archive gs://bucket/x.zip", "'gs' not supported"),
        ("info --archive ''", "must not be empty"),
        ("info --archive ~firecube_no_such_user_17/a.zip", "URI scheme required"),
    ],
)
def test_archive_commands_admit_archive_uri_through_the_uri_rule(
    line, sentence, tmp_path, monkeypatch
):
    root = {"zarr_format": 3, "node_type": "group", "attributes": {}}
    with zipfile.ZipFile(tmp_path / "a.zip", "w") as zf:
        zf.writestr("zarr.json", json.dumps(root))
    (tmp_path / "a.tgm").write_bytes(b"not a real archive")
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    result = CliRunner().invoke(cli, ["archive", *shlex.split(line.format(p=tmp_path))])
    assert not list(cwd.iterdir())
    assert result.exit_code == 2, result.output
    assert sentence.format(p=tmp_path) in result.output
    assert "Invalid value for '--archive'" in result.output
