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
from typing import Any

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.unit


def _make_store(
    path: Path,
    *,
    shape: tuple[int, ...] = (2, 2),
    chunks: tuple[int, ...] = (1, 2),
    values: Any | None = None,
) -> Path:
    root = zarr.open_group(store=str(path), mode="w", zarr_format=3)
    arr = root.require_group("data").create_array(
        "values",
        shape=shape,
        dtype="float32",
        chunks=chunks,
        dimension_names=("y", "x"),
    )
    data = np.arange(np.prod(shape), dtype=np.float32).reshape(shape) if values is None else values
    arr[...] = data
    return path


def _invoke_compare(args: list[str]):
    return CliRunner().invoke(cli, ["zarr", "compare", *args])


def _required_args(a: Path, b: Path) -> list[str]:
    return [
        a.as_uri(),
        b.as_uri(),
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def test_compare_identical_stores_exit_zero(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr")

    result = _invoke_compare(_required_args(a, b))

    assert result.exit_code == 0, result.output
    assert result.stderr == ""


def test_compare_mismatched_shape_exit_nonzero_with_stderr(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", shape=(2, 2), chunks=(1, 2))
    b = _make_store(tmp_path / "b.zarr", shape=(3, 2), chunks=(1, 2))

    result = _invoke_compare(_required_args(a, b))

    assert result.exit_code == 3
    assert "shape" in result.stderr
    assert "Traceback" not in result.output


def test_compare_nan_positions_equal_exit_zero(tmp_path: Path) -> None:
    values = np.array([[1.0, np.nan], [3.0, 4.0]], dtype=np.float32)
    a = _make_store(tmp_path / "a.zarr", values=values)
    b = _make_store(tmp_path / "b.zarr", values=values.copy())

    result = _invoke_compare(_required_args(a, b))

    assert result.exit_code == 0, result.output


def test_compare_missing_store_uri_exit_nonzero(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    missing = tmp_path / "missing.zarr"

    result = _invoke_compare(_required_args(a, missing))

    assert result.exit_code != 0
    assert "Missing" in result.output or "missing" in result.output
    assert "Traceback" not in result.output


def test_compare_help_lists_required_flags() -> None:
    result = _invoke_compare(["--help"])

    assert result.exit_code == 0
    assert "--storage-type" in result.output
    assert "--storage-driver" in result.output
    assert "A_URI" in result.output
    assert "B_URI" in result.output
