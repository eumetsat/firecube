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
from tests.helpers.rows import table_row

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


class TestArchiveHelp:
    """Archive subcommand help texts expose expected options."""

    def test_archive_create_help(self):
        result = CliRunner().invoke(cli, ["archive", "create", "--help"])
        assert result.exit_code == 0
        assert "--target" in result.output
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


class TestArchiveCreateLocal:
    """archive create with a real Zarr source produces a .tgm file."""

    def test_produces_tgm_file(self, tmp_path, monkeypatch, subtests):
        import numpy as np
        import xarray as xr

        for row_id, storage_env in (
            ("test_local_create_no_storage_env", False),
            ("test_produces_tgm_file", True),
        ):
            with table_row(subtests, row_id, tmp_path, monkeypatch) as (workdir, mp):
                assert mp is not None
                if storage_env:
                    mp.setenv("FIRECUBE_STORAGE_TYPE", "local")
                    mp.setenv("FIRECUBE_TARGET_PATH", str(workdir))
                else:
                    mp.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
                    mp.delenv("FIRECUBE_TARGET_PATH", raising=False)
                src = str(workdir / "test.zarr")
                ds = xr.Dataset(
                    {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
                    coords={"t": [0, 1, 2], "y": [1, 2, 3, 4]},
                )
                ds.to_zarr(src)
                args = ["archive", "create", "--target", f"file://{src}"]
                args += ["--archive", f"file://{workdir / 'test.tgm'}", *_LOCAL_STORAGE_FLAGS]
                result = CliRunner().invoke(cli, args)
                assert result.exit_code == 0, result.output
                assert (workdir / "test.tgm").exists()
