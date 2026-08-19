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
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import IndexSpec, RegularTimeAxis, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.errors import SchemaSizeMismatchError
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema

pytestmark = pytest.mark.integration

GROUP = "data"
ARRAY = "data"
SMALL_HORIZON = 10
GROWN_HORIZON = 20


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    """Reset plugin discovery state so monkeypatched fixture plugins are re-discovered."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group=GROUP,
            arrays=[
                ZarrArraySpec(
                    name=ARRAY,
                    shape=(1, 10),
                    dtype=np.float32,
                    chunks=(5, 10),
                    fill_value=0.0,
                    dimension_names=("timestamp", "x"),
                )
            ],
        )
    ]


def _preallocate_small_store_and_write_slots(store_path: Path) -> None:
    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema(),
        global_expected={GROUP: SMALL_HORIZON},
        product="horizon-growth-product",
        run_id="initial-small-horizon",
        chunk_manager=None,
    )
    writer = RegionZarrWriter(str(store_path))
    writer.write_1d(GROUP, ARRAY, ts_index=0, data=np.arange(10, dtype=np.float32))
    writer.write_1d(GROUP, ARRAY, ts_index=9, data=np.full((10,), 9, dtype=np.float32))


def _cli_args(target: str) -> list[str]:
    return [
        "zarr",
        "preallocate",
        "direct_zarr_capable_test_plugin",
        "--target",
        target,
        "--product-name",
        "horizon-growth-product",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_setup_global_zarr_schema_rejects_existing_smaller_horizon(tmp_path: Path) -> None:
    """Observed behavior: direct schema setup rejects, and does not resize, small arrays."""
    store_path = tmp_path / "setup-growth.zarr"
    _preallocate_small_store_and_write_slots(store_path)

    with pytest.raises(SchemaSizeMismatchError) as exc_info:
        _setup_global_zarr_schema(
            strategy=_strategy(str(store_path)),
            schema=_schema(),
            global_expected={GROUP: GROWN_HORIZON},
            product="horizon-growth-product",
            run_id="grown-horizon",
            chunk_manager=None,
        )

    message = str(exc_info.value)
    assert (
        message == "Schema drift for group data: existing array shape[0]=10 < global_expected=20. "
        "Run `firecube zarr preallocate` first."
    )
    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (SMALL_HORIZON, 10)
    assert np.asarray(arr[0]).tolist() == np.arange(10, dtype=np.float32).tolist()
    assert np.asarray(arr[9]).tolist() == np.full((10,), 9, dtype=np.float32).tolist()


def test_cli_preallocate_rejects_existing_smaller_horizon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Observed behavior: CLI preallocate rejects the growth plan instead of resizing."""
    store_path = tmp_path / "cli-growth.zarr"
    _preallocate_small_store_and_write_slots(store_path)

    import direct_zarr_capable_test_plugin as plugin_module

    def index_spec_small_to_grown(self, ctx):
        return IndexSpec(
            name="horizon_growth_v1",
            groups={
                GROUP: RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=GROWN_HORIZON,
                )
            },
        )

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "index_spec",
        index_spec_small_to_grown,
    )
    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "zarr_schema",
        lambda self, ctx: _schema(),
    )

    result = CliRunner().invoke(cli, _cli_args(f"file://{store_path}"), prog_name="firecube")

    assert result.exit_code != 0, result.output
    assert "Existing arrays mismatch the plan: array 'data/data' has mismatches." in result.output
    assert "Mismatch: shape: expected (20, 10), found (10, 10)" in result.output
    assert "Either delete them or update the plan to match." in result.output
    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (SMALL_HORIZON, 10)
    assert np.asarray(arr[0]).tolist() == np.arange(10, dtype=np.float32).tolist()
    assert np.asarray(arr[9]).tolist() == np.full((10,), 9, dtype=np.float32).tolist()
