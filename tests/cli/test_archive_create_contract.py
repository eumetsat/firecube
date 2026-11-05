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

import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding


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


@pytest.mark.contract
def test_create_writes_nothing_for_a_refused_archive_uri(
    tmp_path: Path, tmp_local_zarr: Path, monkeypatch: pytest.MonkeyPatch
):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    args = ["--source", f"file://{tmp_local_zarr}", "--archive", f"FILE://{tmp_path}/out.tgm"]

    def snapshot() -> dict[str, bytes | None]:
        return {
            str(p): p.read_bytes() if p.is_file() else None for p in sorted(tmp_path.rglob("*"))
        }

    before = snapshot()
    result = CliRunner().invoke(cli, ["archive", "create", *args])
    assert snapshot() == before
    assert os.listdir(cwd) == []
    assert result.exit_code == 2
    assert "URI scheme must be lowercase (file:// or s3://)" in result.output
    assert not (tmp_path / "out.tgm").exists()


@pytest.mark.contract
def test_create_rejects_bare_archive_path() -> None:
    result = _invoke_create(
        [
            "--source",
            "file:///tmp/x.zarr",
            "--archive",
            "/tmp/foo.tgm",
            "--dry-run",
        ]
    )

    assert result.exit_code == 2
    assert "URI scheme required (file:// or s3://). Did you mean file:///tmp/foo.tgm?" in (
        result.output
    )
    assert "Traceback" not in result.output
    assert "[dry-run] Would archive" not in result.output


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
    assert "Remote artifacts not yet supported" in result.output
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

    import firecube.core.archive.tensogram.converter as converter

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


def test_create_refuses_a_360_day_time_coordinate_and_records_the_run_failed(
    tmp_path: Path,
) -> None:
    src = tmp_path / "product.zarr"
    times = xr.date_range("2020-01-01", periods=5, freq="D", calendar="360_day", use_cftime=True)
    xr.Dataset({"FWI": (["time"], np.arange(5, dtype="float32"))}, coords={"time": times}).to_zarr(
        src
    )
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    try:
        manager.record_run_started(
            product="product.zarr", run_id="seed", output_path=str(src),
            output_format="zarr", size=0, meta={},
        )  # fmt: skip
    finally:
        manager.close()
    args = ["--source", src.as_uri(), "--archive", (tmp_path / "out.tgm").as_uri()]
    args += ["--storage-type", "local", "--storage-driver", "fsspec"]
    env = {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}

    result = CliRunner().invoke(cli, ["archive", "create", *args], env=env)

    assert result.exit_code != 0, result.output
    assert "Cannot archive time coordinate 'time': calendar='360_day' is not Gregorian-like" in (
        result.output
    )
    assert "Traceback" not in result.output
    events = "".join(p.read_text() for p in (src / ".firecube" / "runs").glob("archive-create-*/*"))
    assert "maintenance_failed" in events
