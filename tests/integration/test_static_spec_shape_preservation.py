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

"""Static-array shape preservation through preallocate and IndexedRegion.

Preallocate must create static (time_indexed=False) arrays at their declared
shape and must not stretch them along the time axis.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner
from zarr.storage import LocalStore

from firecube.cli.main import cli
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import (
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.integration

_CLI_GROUP = "data"
_MIXED_GROUP = "g"
_TIME_LEN = 5
_Y = 4


def _invoke_preallocate(target: str) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            "direct_zarr_capable_test_plugin",
            "--product-name",
            "direct_zarr_capable_test_product",
            "--target",
            target,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "staged",
        ],
    )


def _make_strategy_mixed(store_uri: str) -> IndexedRegionStrategy:
    schema = [
        ZarrGroupSpec(
            group=_MIXED_GROUP,
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(_TIME_LEN, _Y),
                    dtype="float32",
                    chunks=(1, _Y),
                    fill_value=np.float32("nan"),
                ),
                ZarrArraySpec(
                    name="lat",
                    shape=(_Y,),
                    dtype="float64",
                    chunks=(_Y,),
                    fill_value=np.float64("nan"),
                    time_indexed=False,
                ),
            ],
        )
    ]
    return IndexedRegionStrategy(store_uri=store_uri, schema=schema)


def _preallocate_mixed(tmp_path: Path) -> None:
    writer = RegionZarrWriter(f"file://{tmp_path}")
    writer.ensure_group(
        f"{_MIXED_GROUP}/data",
        shape=(_TIME_LEN, _Y),
        dtype="float32",
        fill_value=np.float32("nan"),
        chunks=(1, _Y),
    )
    writer.ensure_group(
        f"{_MIXED_GROUP}/lat",
        shape=(_Y,),
        dtype="float64",
        fill_value=np.float64("nan"),
        chunks=(_Y,),
    )


def _static_intent(lat_values: np.ndarray) -> WriteIntent:
    return WriteIntent(group=_MIXED_GROUP, array="lat", ts_index=0, data=lat_values, kind="static")


def _read_lat(tmp_path: Path) -> np.ndarray:
    root = zarr.open_group(store=LocalStore(str(tmp_path)), mode="r", zarr_format=3)
    return np.asarray(cast(Any, root[f"{_MIXED_GROUP}/lat"])[:])


def test_static_spec_preserved_via_cli_preallocate(tmp_path: Path) -> None:
    """CLI preallocate keeps a static array at its declared (non-time) shape."""
    result = _invoke_preallocate(f"file://{tmp_path}")

    assert result.exit_code == 0
    assert "Traceback" not in result.output

    root = zarr.open_group(str(tmp_path), mode="r", zarr_format=3)
    assert cast(Any, root[f"{_CLI_GROUP}/data"]).shape == (1000, 10)
    assert cast(Any, root[f"{_CLI_GROUP}/lat"]).shape == (10,)


def test_static_spec_preserved_via_indexed_region_strategy(tmp_path: Path) -> None:
    """GREEN-only smoke — IndexedRegion allocator is implicitly safe in the legitimate-construction case (expected_time_count=None); T7 makes it explicit."""
    _preallocate_mixed(tmp_path)
    strategy = _make_strategy_mixed(f"file://{tmp_path}")
    lat_values = np.arange(_Y, dtype=np.float64)

    strategy.write_groups(group_to_intents={_MIXED_GROUP: [_static_intent(lat_values)]})

    np.testing.assert_array_equal(_read_lat(tmp_path), lat_values)
    assert cast(
        Any,
        zarr.open_group(store=LocalStore(str(tmp_path)), mode="r", zarr_format=3)[
            f"{_MIXED_GROUP}/lat"
        ],
    ).shape == (_Y,)


def test_static_spec_resume_idempotent_via_indexed_region(tmp_path: Path) -> None:
    """GREEN-only smoke — NORDLIS-style resume regression."""
    _preallocate_mixed(tmp_path)
    strategy = _make_strategy_mixed(f"file://{tmp_path}")
    lat_values = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float64)

    strategy.write_groups(group_to_intents={_MIXED_GROUP: [_static_intent(lat_values)]})
    strategy.write_groups(group_to_intents={_MIXED_GROUP: [_static_intent(lat_values)]})

    np.testing.assert_array_equal(_read_lat(tmp_path), lat_values)
    assert cast(
        Any,
        zarr.open_group(store=LocalStore(str(tmp_path)), mode="r", zarr_format=3)[
            f"{_MIXED_GROUP}/lat"
        ],
    ).shape == (_Y,)
