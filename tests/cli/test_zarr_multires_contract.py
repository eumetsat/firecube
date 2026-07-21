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


def _invoke_multires(args: list[str]):
    return CliRunner().invoke(cli, ["zarr", "multires", *args])


def test_coherence_mismatch_rejected() -> None:
    result = _invoke_multires(
        [
            "--target",
            "file:///tmp/x.zarr",
            "--product-name",
            "pn",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
            "--resolutions",
            "2",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


def test_case_insensitive_flags_accepted(tmp_path: Path) -> None:
    result = _invoke_multires(
        [
            "--target",
            (tmp_path / "x.zarr").as_uri(),
            "--product-name",
            "pn",
            "--storage-type",
            "LOCAL",
            "--storage-driver",
            "FSSPEC",
            "--resolutions",
            "2",
        ]
    )

    assert "Invalid value" not in result.output
