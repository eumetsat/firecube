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
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner
from zarr.storage import LocalStore

from firecube.cli.main import cli
from firecube.core.api import FIRECUBE_STATIC_WRITTEN_ATTR
from firecube.ingestor.api import EngineConfig
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.integration


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="g",
            arrays=[
                ZarrArraySpec(name="data", shape=(200, 4, 1), dtype="float32"),
                ZarrArraySpec(name="lat", shape=(4,), dtype="float64", time_indexed=False),
            ],
        )
    ]


def _preallocate(path: Path) -> None:
    root = zarr.open_group(store=LocalStore(str(path)), mode="a", zarr_format=3)
    group = root.require_group("g")
    group.create_array("data", shape=(200, 4, 1), dtype="float32", chunks=(1, 4, 1))
    group.create_array("lat", shape=(4,), dtype="float64", chunks=(4,))


def _strategy(path: Path) -> IndexedRegionStrategy:
    return IndexedRegionStrategy(store_uri=path.as_uri(), schema=_schema())


def _static_intent() -> WriteIntent:
    return WriteIntent(
        group="g",
        array="lat",
        ts_index=0,
        data=np.array([1.0, 2.0, 3.0, 4.0]),
        kind="static",
    )


def _dynamic_intent(slot: int) -> WriteIntent:
    return WriteIntent(
        group="g",
        array="data",
        ts_index=slot,
        y_slice=slice(0, 4),
        data=np.array([[9.0], [8.0], [7.0], [6.0]], dtype=np.float32),
        kind="region",
    )


def _lat_array(path: Path) -> Any:
    root = zarr.open_group(store=LocalStore(str(path)), mode="r", zarr_format=3)
    return cast(Any, root["g/lat"])


def test_non_owner_pod_skips_static_writes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = tmp_path / "store.zarr"
    _preallocate(store)

    _strategy(store).write_groups(
        group_to_intents={"g": [_static_intent()]},
        slot_range=(100, 200),
        suppress_static_emission_for_non_owner=True,
        static_owner_slot_start=0,
    )

    lat = _lat_array(store)
    np.testing.assert_array_equal(lat[:], np.zeros((4,), dtype=np.float64))
    assert lat.attrs.get(FIRECUBE_STATIC_WRITTEN_ATTR) is None
    assert (
        "static write suppressed as non-owner: group=g array=lat slot_start=100 owner=0"
        in caplog.text
    )


def test_owner_pod_writes_static_as_normal(tmp_path: Path) -> None:
    store = tmp_path / "store.zarr"
    _preallocate(store)

    _strategy(store).write_groups(
        group_to_intents={"g": [_static_intent()]},
        slot_range=(0, 100),
        suppress_static_emission_for_non_owner=True,
        static_owner_slot_start=0,
    )

    lat = _lat_array(store)
    np.testing.assert_array_equal(lat[:], np.array([1.0, 2.0, 3.0, 4.0]))
    assert lat.attrs.get(FIRECUBE_STATIC_WRITTEN_ATTR) is True


def test_suppression_flag_false_default_writes_static(tmp_path: Path) -> None:
    store = tmp_path / "store.zarr"
    _preallocate(store)

    _strategy(store).write_groups(
        group_to_intents={"g": [_static_intent()]},
        slot_range=(100, 200),
    )

    np.testing.assert_array_equal(_lat_array(store)[:], np.array([1.0, 2.0, 3.0, 4.0]))


def test_suppression_does_not_affect_dynamic_writes(tmp_path: Path) -> None:
    store = tmp_path / "store.zarr"
    _preallocate(store)

    _strategy(store).write_groups(
        group_to_intents={"g": [_static_intent(), _dynamic_intent(100)]},
        slot_range=(100, 101),
        suppress_static_emission_for_non_owner=True,
        static_owner_slot_start=0,
    )

    root = zarr.open_group(store=LocalStore(str(store)), mode="r", zarr_format=3)
    data = cast(Any, root["g/data"])
    np.testing.assert_array_equal(data[100, :, :], np.array([[9.0], [8.0], [7.0], [6.0]]))
    assert _lat_array(store).attrs.get(FIRECUBE_STATIC_WRITTEN_ATTR) is None


def test_misconfig_suppression_true_without_owner_slot_raises() -> None:
    with pytest.raises(ConfigurationError):
        EngineConfig(suppress_static_emission_for_non_owner=True)


def test_show_options_lists_both_new_fields() -> None:
    result = CliRunner().invoke(
        cli, ["ingest", "direct_zarr_capable_test_plugin", "--show-options"]
    )

    assert result.exit_code == 0, result.output
    assert "--option suppress_static_emission_for_non_owner" in result.output
    assert "--option static_owner_slot_start" in result.output


def test_misconfig_suppression_true_without_slot_start_raises() -> None:
    """suppression=True + slot_start=None → ConfigurationError at construction time.

    Regression for serial-run silent-drop bug: without slot_start, every static
    write would be suppressed on the assumption that a co-worker owns them, but
    there is no co-worker in serial mode.
    """
    with pytest.raises(ConfigurationError, match=r"slot_start"):
        EngineConfig(
            suppress_static_emission_for_non_owner=True,
            static_owner_slot_start=0,
            slot_start=None,
        )


def test_serial_run_never_suppresses_static_intents() -> None:
    """Defense-in-depth: `_should_suppress_static_intent` returns False when
    `slot_range is None` (serial mode), regardless of the suppression flag.
    """
    from types import SimpleNamespace

    intent = SimpleNamespace(group="G", array="A")

    result = IndexedRegionStrategy._should_suppress_static_intent(
        intent,
        slot_range=None,
        suppress_static_emission_for_non_owner=True,
        static_owner_slot_start=0,
    )

    assert result is False
