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

"""Window-scoped prefill regression for regular coord arrays."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import numpy as np
import pytest
import regular_axis_test_plugin as _regular_plugin_module
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, RegularTimeAxis
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.ingestor.api import DirectZarrIngestor, PluginContext, ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD_NAME = "time"
_SLOT_COUNT = 12
_CADENCE_S = 600
_EPOCH = np.datetime64("2024-01-01T00:00:00", "ns")
_EXPECTED_VALUES = _EPOCH + np.arange(_SLOT_COUNT, dtype=np.int64) * np.timedelta64(
    _CADENCE_S, "s"
).astype("timedelta64[ns]")


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_regular_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _register_regular_plugin(name: str) -> None:
    _plugin_name = name

    class _DynamicRegularAxisIngestor(DirectZarrIngestor):
        PRODUCT_NAME: ClassVar[str] = _plugin_name
        time_dim_name: ClassVar[str] = _COORD_NAME

        def discover_source_files(self, ctx: PluginContext) -> list[str]:
            return []

        def index_spec(self, ctx: PluginContext) -> IndexSpec:
            return IndexSpec(
                name=f"{_plugin_name}_v1",
                groups={
                    _GROUP: RegularTimeAxis(
                        coordinate=_COORD_NAME,
                        epoch="2024-01-01T00:00:00Z",
                        cadence_s=_CADENCE_S,
                        mode="exact",
                        slot_count=_SLOT_COUNT,
                    ),
                },
            )

        def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
            return [
                ZarrGroupSpec(
                    group=_GROUP,
                    arrays=[
                        ZarrArraySpec(
                            name=_COORD_NAME,
                            shape=(_SLOT_COUNT,),
                            dtype="datetime64[ns]",
                            chunks=None,
                            fill_value=np.datetime64("NaT", "ns"),
                            expected_time_count=_SLOT_COUNT,
                            time_indexed=True,
                            dimension_names=(_COORD_NAME,),
                        ),
                    ],
                )
            ]

        def build_write_intents(self, batch: Any, ctx: PluginContext) -> list[Any]:
            return []

    _DynamicRegularAxisIngestor.__name__ = f"DynamicRegularAxisIngestor_{name}"
    _DynamicRegularAxisIngestor.name = _plugin_name  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_plugin_name] = _DynamicRegularAxisIngestor


def _preallocate_args(
    plugin: str,
    product: str,
    target_path: Path,
    *,
    slot_start: int | None = None,
    slot_end: int | None = None,
) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if slot_start is not None:
        args.extend(["--slot-start", str(slot_start)])
    if slot_end is not None:
        args.extend(["--slot-end", str(slot_end)])
    return args


def _root(target_path: Path, mode: Literal["r", "r+", "a", "w", "w-"] = "r") -> Any:
    return zarr.open_group(store=str(target_path), mode=mode, zarr_format=3)


def test_window_scoped_prefill_writes_only_the_requested_slice(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    plugin_name = "prefill_window_scoped"
    _register_regular_plugin(plugin_name)
    runner = CliRunner()

    first = runner.invoke(
        cli,
        _preallocate_args(
            plugin_name,
            plugin_name,
            target_path,
            slot_start=3,
            slot_end=7,
        ),
    )

    assert first.exit_code == 0, first.output
    coord = cast(Any, _root(target_path, mode="r+")[f"{_GROUP}/{_COORD_NAME}"])
    values = np.asarray(coord[:])
    np.testing.assert_array_equal(values[3:7], _EXPECTED_VALUES[3:7])
    assert np.all(np.isnat(values[:3]))
    assert np.all(np.isnat(values[7:]))
    assert coord.attrs[ATTR_PREALLOCATED] is True

    coord[1] = _EXPECTED_VALUES[1] + np.timedelta64(1, "s")
    second = runner.invoke(
        cli,
        _preallocate_args(
            plugin_name,
            plugin_name,
            target_path,
            slot_start=3,
            slot_end=7,
        ),
    )

    assert second.exit_code == 0, second.output
    assert "no-op (matches nominal grid) in window [3, 7)" in second.output
    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    values = np.asarray(coord[:])
    assert values[1] == _EXPECTED_VALUES[1] + np.timedelta64(1, "s")
    np.testing.assert_array_equal(values[3:7], _EXPECTED_VALUES[3:7])
    assert np.all(np.isnat(values[:1]))
    assert np.all(np.isnat(values[2:3]))
    assert np.all(np.isnat(values[7:]))
