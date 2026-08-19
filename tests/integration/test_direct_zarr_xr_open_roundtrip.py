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

"""Round-trip integration test: DirectZarr store opens cleanly with xr.open_zarr.

End-to-end coverage for the Zarr v3 native ``dimension_names`` write path:
drives ``multi_group_capable_test_plugin`` through ``IndexedRegionStrategy``
into a temp directory, then re-opens the resulting store with both
``zarr.open_group`` and ``xr.open_zarr`` to assert the on-disk schema honors
the DirectZarr parity contract:

- per-array ``dimension_names`` set via Zarr v3 native metadata
  (no legacy ``_ARRAY_DIMENSIONS`` attribute)
- per-array ``attrs`` preserved verbatim
- static (``time_indexed=False``) arrays round-trip values byte-for-byte
- ``xr.open_zarr`` resolves dims without emitting ``_ARRAY_DIMENSIONS``
  fallback warnings
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr
import zarr
from multi_group_capable_test_plugin import (  # type: ignore[reportMissingImports]
    MultiGroupCapableTestIngestor,
)

from firecube.core.index_spec import RegularTimeAxis
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import PluginContext
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrGroupSpec
from firecube.ingestor.types.context import PipelineBatch

pytestmark = pytest.mark.integration


def _preallocate_schema(
    target_path: Path,
    schema: Sequence[ZarrGroupSpec],
    global_expected: dict[str, int],
) -> None:
    """Pre-allocate Zarr arrays per the plugin schema, honoring ``time_indexed``.

    Mirrors the parallel-mode pod-startup pre-allocation path
    (``_setup_global_zarr_schema``): time-indexed arrays expand the leading
    axis to the global expected count, static arrays keep the declared shape.
    """
    writer = RegionZarrWriter(f"file://{target_path}")
    for group_spec in schema:
        expected = global_expected.get(group_spec.group, 0)
        for arr_spec in group_spec.arrays:
            if arr_spec.time_indexed:
                effective_shape: tuple[int, ...] = (expected, *arr_spec.shape[1:])
            else:
                effective_shape = arr_spec.shape
            writer.ensure_group(
                f"{group_spec.group}/{arr_spec.name}",
                shape=effective_shape,
                dtype=arr_spec.dtype,
                fill_value=arr_spec.fill_value,
                chunks=arr_spec.chunks,
                shards=arr_spec.shards,
                attrs=arr_spec.attrs,
                dimension_names=arr_spec.dimension_names,
            )


def _run_direct_zarr_ingest(target_path: Path) -> None:
    """Drive ``multi_group_capable_test_plugin`` through ``IndexedRegionStrategy``.

    Uses ``slot_range=(0, 100)`` so the strategy treats this as parallel-mode
    and skips its own (sequential-only) schema-setup path; we pre-allocate the
    arrays first with the correct ``time_indexed``-aware shapes.  A small batch
    of 10 items per group keeps the run trivial while still exercising both
    time-indexed and static write intents across two groups.
    """
    ingestor = MultiGroupCapableTestIngestor()
    ctx = cast(PluginContext, MagicMock(spec=PluginContext))
    ctx._ctx = MagicMock()
    ingestor._bind_index_at_startup(ctx)
    schema = ingestor.zarr_schema(ctx)
    global_expected = {
        group: cast(int, cast(RegularTimeAxis, axis).size)
        for group, axis in ingestor.index_spec(ctx).groups.items()
    }

    _preallocate_schema(target_path, schema, global_expected)

    items: list[Any] = [("group_a", i) for i in range(10)] + [("group_b", i) for i in range(10)]
    batch = PipelineBatch(batch_id="t16", data_path=target_path, items=items)
    intents = ingestor.build_write_intents(batch, ctx)

    group_to_intents: dict[str, list[WriteIntent]] = {}
    for intent in intents:
        group_to_intents.setdefault(intent.group, []).append(intent)

    strategy = IndexedRegionStrategy(
        store_uri=f"file://{target_path}",
        schema=schema,
        coord_names_by_group={spec.group: spec.coord_names for spec in schema},
    )
    strategy.write_groups(
        group_to_intents=group_to_intents,
        schema=schema,
        slot_range=(0, 100),
    )


def test_xr_open_zarr_emits_no_array_dimensions_warning(tmp_path: Path) -> None:
    """``xr.open_zarr`` must resolve dims from native ``dimension_names``.

    A warning containing ``_ARRAY_DIMENSIONS`` would indicate xarray fell back
    to the Zarr v2 legacy convention — which means the writer failed to set
    Zarr v3 ``dimension_names`` correctly.  The DirectZarr path must rely on
    v3 native metadata only.
    """
    target_path = tmp_path / "out.zarr"
    _run_direct_zarr_ingest(target_path)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # group_a's calibration array shares dim name "x" with primary/lat/lon
        # but has size 4 vs 10 (fixture quirk); drop it so xarray can merge the
        # remaining variables into a single Dataset.  This test still exercises
        # calibration's metadata via the zarr.open_group round-trip below.
        ds_a = xr.open_zarr(
            str(target_path),
            group="group_a",
            consolidated=False,
            decode_times=False,
            drop_variables=["calibration"],
        )
        ds_b = xr.open_zarr(
            str(target_path),
            group="group_b",
            consolidated=False,
            decode_times=False,
        )

    array_dim_warnings = [w for w in caught if "_ARRAY_DIMENSIONS" in str(w.message)]
    assert array_dim_warnings == [], (
        f"xr.open_zarr emitted {len(array_dim_warnings)} _ARRAY_DIMENSIONS "
        f"warning(s); native Zarr v3 dimension_names not honored. Messages: "
        f"{[str(w.message) for w in array_dim_warnings]}"
    )

    try:
        assert "timestamp" in ds_a.dims, f"missing 'timestamp' dim in group_a: {ds_a.dims}"
        assert "x" in ds_a.dims, f"missing 'x' dim in group_a: {ds_a.dims}"
        assert ds_a.sizes["x"] == 10
        assert "primary" in ds_a.variables
        # lat/lon are static — xarray may surface them as data_vars or coords
        # depending on classification; both paths put them in .variables.
        assert "lat" in ds_a.variables, f"missing 'lat' variable in group_a: {list(ds_a.variables)}"
        assert "lon" in ds_a.variables, f"missing 'lon' variable in group_a: {list(ds_a.variables)}"

        assert "timestamp" in ds_b.dims
        assert "x" in ds_b.dims
        assert ds_b.sizes["x"] == 5
        assert "primary" in ds_b.variables

        assert ds_a["primary"].attrs.get("role") == "primary"
        assert ds_a["lat"].attrs.get("units") == "degrees_north"
        assert ds_a["lon"].attrs.get("units") == "degrees_east"
        assert ds_b["primary"].attrs.get("role") == "primary"
    finally:
        ds_a.close()
        ds_b.close()


def test_static_arrays_round_trip_via_zarr_open_group(tmp_path: Path) -> None:
    """Static ``lat``/``lon`` arrays survive a write→read cycle byte-for-byte.

    Also verifies Zarr v3 native ``dimension_names`` and ``attrs`` are stored
    in the array metadata (not as the v2 ``_ARRAY_DIMENSIONS`` attribute) for
    both static and time-indexed arrays.
    """
    target_path = tmp_path / "out.zarr"
    _run_direct_zarr_ingest(target_path)

    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    group_a = cast(Any, root["group_a"])

    lat = cast(Any, group_a["lat"])
    lon = cast(Any, group_a["lon"])
    primary = cast(Any, group_a["primary"])
    calibration = cast(Any, group_a["calibration"])

    # Expected payload mirrors the plugin's static intent: np.arange(10, float64).
    expected_static = np.arange(10, dtype=np.float64)
    np.testing.assert_array_equal(np.asarray(lat[:]), expected_static)
    np.testing.assert_array_equal(np.asarray(lon[:]), expected_static)

    assert lat.metadata.dimension_names == ("lat",)
    assert lon.metadata.dimension_names == ("lon",)
    assert primary.metadata.dimension_names == ("timestamp", "x")
    assert calibration.metadata.dimension_names == ("timestamp", "x")

    assert dict(lat.attrs).get("units") == "degrees_north"
    assert dict(lon.attrs).get("units") == "degrees_east"
    assert dict(primary.attrs).get("role") == "primary"
    assert dict(primary.attrs).get("units") == "1"
    assert dict(calibration.attrs).get("role") == "calibration"

    for arr in (lat, lon, primary, calibration):
        assert "_ARRAY_DIMENSIONS" not in dict(arr.attrs), (
            f"array {arr.name!r} carries legacy _ARRAY_DIMENSIONS; "
            "DirectZarr must use Zarr v3 native dimension_names only"
        )

    group_b = cast(Any, root["group_b"])
    primary_b = cast(Any, group_b["primary"])
    assert primary_b.metadata.dimension_names == ("timestamp", "x")
    assert "_ARRAY_DIMENSIONS" not in dict(primary_b.attrs)
