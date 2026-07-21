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

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    """Reset plugin discovery state so fixture plugins re-discover per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    importlib.reload(importlib.import_module("direct_zarr_non_capable_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _args(
    target: str,
    plugin: str = "direct_zarr_capable_test_plugin",
    *,
    storage_type: str = "local",
) -> list[str]:
    return [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        target,
        "--product-name",
        f"{plugin}_product",
        "--storage-type",
        storage_type,
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_preallocate_help() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "--help"])

    assert result.exit_code == 0, result.output
    assert "Examples:" in result.output
    assert (
        "idempotent" in result.output.lower()
        or "no-op" in result.output.lower()
        or "safe to re-run" in result.output.lower()
    )


def test_setup_schema_removed() -> None:
    result = CliRunner().invoke(cli, ["zarr", "setup-schema", "--help"])

    assert result.exit_code != 0
    assert "No such command" in result.output


def test_preallocate_missing_required_options() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "test_product"])

    assert result.exit_code != 0
    assert "target" in result.output.lower() or "missing" in result.output.lower()


def test_preallocate_rejects_file_uri_with_s3_storage_type() -> None:
    result = CliRunner().invoke(
        cli,
        _args("file:///tmp/x.zarr", storage_type="s3"),
        prog_name="firecube",
    )

    assert result.exit_code != 0, result.output
    assert "incompatible" in result.output


def test_preallocate_rejects_s3_uri_with_local_storage_type() -> None:
    result = CliRunner().invoke(
        cli,
        _args("s3://bucket/x.zarr", storage_type="local"),
        prog_name="firecube",
    )

    assert result.exit_code != 0, result.output
    assert "incompatible" in result.output


def test_preallocate_not_top_level() -> None:
    assert cli.commands.get("preallocate") is None

    result = CliRunner().invoke(cli, ["preallocate", "--help"])
    assert result.exit_code != 0
    assert "No such command" in result.output


def test_non_capable_plugin_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _args(
            f"file://{tmp_path / 'out.zarr'}",
            plugin="direct_zarr_non_capable_test_plugin",
        ),
    )

    assert result.exit_code != 0, result.output
    assert (
        "does not support slot-range parallelism" in result.output
        or "SUPPORTS_SLOT_RANGE_PARALLELISM" in result.output
    )


def test_preallocate_fresh_target(tmp_path: Path) -> None:
    """Fresh target: creates arrays, exits 0."""
    target_path = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code == 0, result.output
    assert "created" in result.output
    arr = cast(Any, zarr.open_group(store=str(target_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (1000, 10)


def test_preallocate_matching_arrays_noop(tmp_path: Path) -> None:
    """Re-run on matching arrays: exits 0, logs no-op."""
    target_path = tmp_path / "matching.zarr"
    args = _args(f"file://{target_path}")

    first = CliRunner().invoke(cli, args)
    assert first.exit_code == 0, first.output

    second = CliRunner().invoke(cli, args)
    assert second.exit_code == 0, second.output
    assert "no-op" in second.output or "match the plan" in second.output
    arr = cast(Any, zarr.open_group(store=str(target_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (1000, 10)


def test_preallocate_accepts_matching_existing_array(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_path = tmp_path / "existing.zarr"
    zarr.open_group(store=str(target_path), mode="w", zarr_format=3).require_group(
        "data"
    ).create_array("data", shape=(1000, 10), dtype=np.float32, chunks=(100, 10), fill_value=0.0)

    import direct_zarr_capable_test_plugin as plugin_module

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "zarr_schema",
        lambda self, ctx: [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        shape=(1000, 10),
                        dtype=np.float32,
                        chunks=(100, 10),
                        fill_value=0.0,
                    )
                ],
            )
        ],
    )

    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code == 0, result.output
    assert "no-op" in result.output


def test_preallocate_mismatched_shape_errors(tmp_path: Path) -> None:
    """Mismatched shape: exits non-zero with diff."""
    target_path = tmp_path / "drift.zarr"
    zarr.open_group(store=str(target_path), mode="w", zarr_format=3).require_group(
        "data"
    ).create_array("data", shape=(5, 10), dtype=np.float32, chunks=(100, 10), fill_value=0.0)

    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code != 0, result.output
    assert "Existing arrays mismatch the plan" in result.output
    assert "shape:" in result.output
    assert "expected" in result.output
    assert "found" in result.output


def test_preallocate_mismatched_chunks_errors(tmp_path: Path) -> None:
    target_path = tmp_path / "chunk-drift.zarr"
    zarr.open_group(store=str(target_path), mode="w", zarr_format=3).require_group(
        "data"
    ).create_array("data", shape=(1000, 10), dtype=np.float32, chunks=(10, 10), fill_value=0.0)

    result = CliRunner().invoke(cli, _args(f"file://{target_path}"))

    assert result.exit_code != 0, result.output
    assert "chunks:" in result.output
    assert "expected" in result.output
    assert "found" in result.output
