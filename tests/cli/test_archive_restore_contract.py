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

from click.testing import CliRunner

from firecube.cli.main import cli


def _invoke_restore(args: list[str]):
    return CliRunner().invoke(cli, ["archive", "restore", *args])


def test_restore_rejects_storage_type_target_uri_mismatch() -> None:
    result = _invoke_restore(
        [
            "--archive",
            "file:///tmp/x.tgm",
            "--target",
            "file:///tmp/x.zarr",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


def test_restore_rejects_remote_tgm_archive() -> None:
    result = _invoke_restore(
        [
            "--archive",
            "s3://bucket/a.tgm",
            "--target",
            "file:///tmp/x.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code != 0
    assert "Remote .tgm artifacts not yet supported" in result.output
    assert "Traceback" not in result.output
