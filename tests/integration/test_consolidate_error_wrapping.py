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

"""Error-wrapping coverage for ``firecube zarr consolidate-time-coord``."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.cli.zarr import _consolidate as zarr_cli
from firecube.ingestor.errors import ConfigurationError

pytestmark = pytest.mark.integration


def _create_legacy_cube(cube: Path, total: int = 16) -> None:
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    values = np.datetime64("2024-01-01T00:00:00", "ns") + np.arange(
        total, dtype=np.int64
    ) * np.timedelta64(600, "s")
    root.create_group("data").create_array(
        "time",
        data=values,
        chunks=(1,),
        overwrite=True,
        dimension_names=("time",),
    )


def _args(cube: Path) -> list[str]:
    return [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        cube.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def test_configuration_error_is_user_facing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)

    def boom(*args, **kwargs):
        raise ConfigurationError("test-induced consolidate failure")

    monkeypatch.setattr(zarr_cli, "_read_consolidate_reference_index", boom)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code != 0, result.output
    assert "Traceback" not in result.output
    assert "Error:" in result.output
    assert "test-induced consolidate failure" in result.output
