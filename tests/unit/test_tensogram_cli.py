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

"""Unit tests for firecube CLI archive commands."""

from __future__ import annotations

from click.testing import CliRunner

from firecube.cli.main import cli

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


class TestArchiveHelp:
    """Archive subcommand help texts expose expected options."""

    def test_archive_create_help(self):
        result = CliRunner().invoke(cli, ["archive", "create", "--help"])
        assert result.exit_code == 0
        assert "--source" in result.output
        assert "--archive" in result.output
        assert "--storage-type" in result.output
        assert "--storage-driver" in result.output

    def test_archive_restore_help(self):
        result = CliRunner().invoke(cli, ["archive", "restore", "--help"])
        assert result.exit_code == 0
        assert "--archive" in result.output
        assert "--storage-type" in result.output
        assert "--storage-driver" in result.output

    def test_archive_info_help(self):
        result = CliRunner().invoke(cli, ["archive", "info", "--help"])
        assert result.exit_code == 0
        assert "--archive" in result.output
        assert "--format [table|json|csv]" in result.output
        assert "firecube archive info --archive file:///tmp/archive.tgm" in result.output

    def test_archive_validate_help(self):
        result = CliRunner().invoke(cli, ["archive", "validate", "--help"])
        assert result.exit_code == 0
        assert "--archive" in result.output
        assert "--quick" in result.output
        assert "firecube archive validate --archive file:///tmp/archive.tgm" in result.output


class TestArchiveListCommand:
    """archive list replaces ls."""

    def test_archive_list_help(self):
        result = CliRunner().invoke(cli, ["archive", "list", "--help"])
        assert result.exit_code == 0
        assert "list contents" in result.output.lower()

    def test_archive_ls_not_found(self):
        result = CliRunner().invoke(cli, ["archive", "ls"])
        assert result.exit_code != 0
        assert "No such command" in result.output


class TestLocalArchiveWithoutStorageConfig:
    """archive create works without FIRECUBE_STORAGE_TYPE for local paths."""

    def test_local_create_no_storage_env(self, tmp_path, monkeypatch):
        import numpy as np
        import xarray as xr

        monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
        monkeypatch.delenv("FIRECUBE_TARGET_PATH", raising=False)
        runner = CliRunner()
        src = str(tmp_path / "test.zarr")
        src_uri = f"file://{src}"
        tgt_uri = f"file://{tmp_path / 'test.tgm'}"
        ds = xr.Dataset(
            {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1, 2, 3, 4]},
        )
        ds.to_zarr(src)
        result = runner.invoke(
            cli,
            ["archive", "create", "--source", src_uri, "--archive", tgt_uri, *_LOCAL_STORAGE_FLAGS],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "test.tgm").exists()


class TestArchiveCreateIntegration:
    """archive create with a real Zarr source produces a .tgm file."""

    def test_produces_tgm_file(self, tmp_path, monkeypatch):
        import numpy as np
        import xarray as xr

        monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "local")
        monkeypatch.setenv("FIRECUBE_TARGET_PATH", str(tmp_path))
        runner = CliRunner()
        src = str(tmp_path / "test.zarr")
        src_uri = f"file://{src}"
        tgt_uri = f"file://{tmp_path / 'test.tgm'}"
        ds = xr.Dataset(
            {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1, 2, 3, 4]},
        )
        ds.to_zarr(src)
        result = runner.invoke(
            cli,
            ["archive", "create", "--source", src_uri, "--archive", tgt_uri, *_LOCAL_STORAGE_FLAGS],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "test.tgm").exists()
