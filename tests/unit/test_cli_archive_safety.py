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
import sys
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def test_create_help_advertises_yes_flag():
    r = CliRunner().invoke(cli, ["archive", "create", "--help"])
    assert r.exit_code == 0
    assert "--overwrite" in r.output
    assert "--yes-i-really-mean-it" in r.output


def test_restore_help_advertises_yes_flag():
    r = CliRunner().invoke(cli, ["archive", "restore", "--help"])
    assert r.exit_code == 0
    assert "--overwrite" in r.output
    assert "--yes-i-really-mean-it" in r.output
    assert "--group" not in r.output


def test_info_help_uses_format_option_not_json_flag():
    r = CliRunner().invoke(cli, ["archive", "info", "--help"])
    assert r.exit_code == 0
    assert "-f, --format" in r.output or "--format" in r.output
    assert "--json" not in r.output


def test_info_rejects_legacy_json_flag(tmp_path):
    target = tmp_path / "missing.tgm"
    target.write_text("")
    r = CliRunner().invoke(cli, ["archive", "info", "--json", str(target)])
    assert r.exit_code != 0
    assert "no such option" in r.output.lower() or "--json" in r.output


class _FakeTensogramMetadata:
    def __init__(self) -> None:
        self.extra = {
            "firecube": {
                "archive_version": "v1",
                "role": "data",
                "group": "F024",
                "compression": "zstd",
                "source_uri": "file:///tmp/product.zarr",
                "archived_at": "2026-06-09T00:00:00+00:00",
            }
        }
        self.base: list[object] = []


class _FakeTensogramArchive:
    def __enter__(self) -> _FakeTensogramArchive:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def message_count(self) -> int:
        return 1

    def file_decode_metadata(self, index: int) -> _FakeTensogramMetadata:
        assert index == 0
        return _FakeTensogramMetadata()

    def file_decode_descriptors(self, index: int) -> dict[str, list[object]]:
        assert index == 0
        return {"descriptors": []}


def _install_fake_tensogram(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeTensogramFile:
        @staticmethod
        def open(path: str) -> _FakeTensogramArchive:
            assert path
            return _FakeTensogramArchive()

    monkeypatch.setitem(
        sys.modules,
        "tensogram",
        SimpleNamespace(TensogramFile=_FakeTensogramFile),
    )

    import firecube.core.tensogram._compat as compat

    monkeypatch.setattr(compat, "HAS_TENSOGRAM", True)


def test_info_reports_archive_version_in_table(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _install_fake_tensogram(monkeypatch)
    archive = tmp_path / "archive.tgm"
    archive.write_bytes(b"fake")

    result = CliRunner().invoke(cli, ["archive", "info", "--archive", archive.as_uri()])

    assert result.exit_code == 0, result.output
    assert "Format:         v1 (multi-group)" in result.output


def test_info_reports_archive_version_in_json(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _install_fake_tensogram(monkeypatch)
    archive = tmp_path / "archive.tgm"
    archive.write_bytes(b"fake")

    result = CliRunner().invoke(
        cli, ["archive", "info", "--archive", archive.as_uri(), "-f", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["format"] == "v1"


def test_create_overwrite_non_tty_without_yes_exits_nonzero(tmp_path):
    src = tmp_path / "src.zarr"
    src.mkdir()
    tgt = tmp_path / "out.tgm"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            f"file://{src}",
            "--archive",
            tgt.as_uri(),
            "--overwrite",
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert r.exit_code != 0
    assert (
        "yes-i-really-mean-it" in r.output.lower()
        or "overwritewithoutconfirmation" in r.output.lower()
    )


def test_restore_overwrite_non_tty_without_yes_exits_nonzero(tmp_path):
    src = tmp_path / "archive.tgm"
    src.write_text("")
    tgt = tmp_path / "out.zarr"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            src.as_uri(),
            "--target",
            f"file://{tgt}",
            "--overwrite",
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert r.exit_code != 0
    assert (
        "yes-i-really-mean-it" in r.output.lower()
        or "overwritewithoutconfirmation" in r.output.lower()
    )


def test_create_without_overwrite_requires_no_confirmation(tmp_path, monkeypatch):
    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_TARGET_PATH", raising=False)
    src = tmp_path / "missing.zarr"
    tgt = tmp_path / "out.tgm"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            f"file://{src}",
            "--archive",
            tgt.as_uri(),
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert "OverwriteWithoutConfirmation" not in r.output
    assert "yes-i-really-mean-it" not in r.output.lower()
