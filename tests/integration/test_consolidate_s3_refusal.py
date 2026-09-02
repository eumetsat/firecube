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

"""Regression coverage for remote refusal in ``firecube zarr consolidate-time-coord``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


def _make_legacy_cube(cube: Path, size: int = 4) -> None:
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    values = np.datetime64("2024-01-01T00:00:00", "ns") + np.arange(
        size, dtype=np.int64
    ) * np.timedelta64(
        600_000_000_000,
        "ns",
    )
    root.create_array("time", data=values, chunks=(1,), overwrite=True, dimension_names=("time",))


def test_consolidate_time_coord_refuses_remote_s3_before_store_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = "s3://example-bucket/cube.zarr"
    called = False

    def _boom(*args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("remote store open must not be attempted")

    monkeypatch.setattr("firecube.core.filesystem.store_factory.create_zarr_store", _boom)

    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "consolidate-time-coord",
            "--target",
            target,
            "--product-name",
            "cube.zarr",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ],
    )

    assert result.exit_code != 0, result.output
    assert (
        "remote (s3) consolidate-time-coord is not currently supported due to non-atomic crash-recovery; see follow-up plan"
        in result.output
    )
    assert not called, "remote store open must be refused before IO"


def test_consolidate_time_coord_local_still_works(tmp_path: Path) -> None:
    cube = tmp_path / "cube.zarr"
    _make_legacy_cube(cube)

    result = CliRunner().invoke(
        cli,
        [
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
        ],
    )

    assert result.exit_code == 0, result.output
    root = zarr.open_group(store=str(cube), mode="r", zarr_format=3)
    time = cast(Any, root["time"])
    assert tuple(time.chunks) == (4,)
