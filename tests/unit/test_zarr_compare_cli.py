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
    shape: tuple[int, ...] = (4, 4),
    chunks: tuple[int, ...] = (2, 2),
    values: Any | None = None,
) -> Path:
    root = zarr.open_group(store=str(path), mode="w", zarr_format=3)
    arr = root.create_array(
        "data",
        shape=shape,
        dtype="float32",
        chunks=chunks,
    )
    arr[...] = (
        np.arange(np.prod(shape), dtype=np.float32).reshape(shape) if values is None else values
    )
    return path


def _invoke(args: list[str]):
    return CliRunner(capture="fd").invoke(cli, ["zarr", "compare", *args])


def test_equivalent_exits_0(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr")

    result = _invoke([a.as_uri(), b.as_uri()])

    assert result.exit_code == 0, result.output
    assert result.stderr == ""


def test_equivalent_exits_0_with_explicit_flags(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr")

    result = _invoke(
        [a.as_uri(), b.as_uri(), "--storage-type", "local", "--storage-driver", "fsspec"]
    )

    assert result.exit_code == 0, result.output


def test_content_diff_exits_1(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr", values=np.zeros((4, 4), dtype=np.float32))

    result = _invoke([a.as_uri(), b.as_uri()])

    assert result.exit_code == 1
    assert "values differ" in result.stderr
    assert "Traceback" not in result.output


def test_layout_only_exits_0_with_warning(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", chunks=(2, 2))
    b = _make_store(tmp_path / "b.zarr", chunks=(4, 4))

    result = _invoke([a.as_uri(), b.as_uri()])

    assert result.exit_code == 0, result.output
    assert "WARNING: layout differences only" in result.stderr
    assert "chunks" in result.stderr


def test_layout_only_warning_mentions_realign(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", chunks=(2, 2))
    b = _make_store(tmp_path / "b.zarr", chunks=(4, 4))

    result = _invoke([a.as_uri(), b.as_uri()])

    assert "realign" in result.stderr


def test_smart_default_driver_defaults_to_fsspec(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr")

    result = _invoke([a.as_uri(), b.as_uri(), "--storage-type", "local"])

    assert result.exit_code == 0, result.output
