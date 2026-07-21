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

from pathlib import Path

from click.testing import CliRunner

from firecube.cli.main import cli


def _invoke_create(args: list[str]):
    return CliRunner().invoke(cli, ["archive", "create", *args])


def test_create_rejects_storage_type_source_uri_mismatch() -> None:
    result = _invoke_create(
        [
            "--source",
            "file:///tmp/x.zarr",
            "--archive",
            "file:///tmp/x.tgm",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


def test_create_rejects_remote_tgm_archive() -> None:
    result = _invoke_create(
        [
            "--source",
            "file:///tmp/x.zarr",
            "--archive",
            "s3://bucket/x.tgm",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code != 0
    assert "Remote .tgm artifacts not yet supported" in result.output
    assert "Traceback" not in result.output


def test_create_resolves_file_uri_archive(tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_zarr_to_tgm(source: str, target: str, **kwargs):
        seen["source"] = source
        seen["target"] = target
        return {
            "target": target,
            "groups": [],
            "variables": [],
            "file_size_bytes": 0,
            "compression": kwargs["compression"],
        }

    import firecube.core.tensogram.converter as converter

    monkeypatch.setattr(converter, "zarr_to_tgm", fake_zarr_to_tgm)

    archive_path = tmp_path / "archive.tgm"
    result = _invoke_create(
        [
            "--source",
            "file:///tmp/x.zarr",
            "--archive",
            f"file://{archive_path}",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 0, result.output
    assert seen["target"] == str(archive_path)
