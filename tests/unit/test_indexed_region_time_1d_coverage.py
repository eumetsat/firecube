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

import tempfile
from typing import Any

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)
from firecube.ingestor.templates.direct_zarr import WriteIntent

pytestmark = pytest.mark.unit


def _preallocate(tmp: str) -> None:
    store = LocalStore(tmp)
    root = zarr.open_group(store=store, mode="a", zarr_format=3)
    grp = root.require_group("g")
    grp.create_array(
        "timestamp",
        shape=(10,),
        dtype="datetime64[s]",
        fill_value=np.datetime64("NaT", "s"),
    )
    grp.create_array("temperature", shape=(10,), dtype="float64")


def _1d_intent(
    array: str,
    ts_index: int,
    timestamp_val: object | None,
    data: Any,
) -> WriteIntent:
    return WriteIntent(
        group="g",
        array=array,
        ts_index=ts_index,
        data=data,
        kind="1d",
        timestamp_val=timestamp_val,
    )


def _timestamp_intent(ts_index: int, timestamp_val: np.datetime64) -> WriteIntent:
    return WriteIntent(
        group="g",
        array="timestamp",
        ts_index=ts_index,
        data=None,
        kind="timestamp",
        timestamp_val=timestamp_val,
    )


def test_1d_on_time_coord_advances_bounds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}")
        ts_val = np.datetime64("2023-12-01", "s")
        intent = _1d_intent(
            array="timestamp",
            ts_index=0,
            timestamp_val=ts_val,
            data=ts_val,
        )

        result = strategy.write_groups(group_to_intents={"g": [intent]})

        assert len(result["coverage"]) == 1
        entry = result["coverage"][0]
        assert entry["time_min"] == "2023-12-01T00:00:00Z"
        assert entry["time_max"] == "2023-12-01T00:00:00Z"
        assert "timestamp" in entry["arrays"]


def test_1d_on_non_time_array_no_time_bounds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}")
        ts_val = np.datetime64("2023-12-01", "s")
        intent = _1d_intent(
            array="temperature",
            ts_index=0,
            timestamp_val=ts_val,
            data=np.float64(42.0),
        )

        result = strategy.write_groups(group_to_intents={"g": [intent]})

        assert len(result["coverage"]) == 1
        entry = result["coverage"][0]
        assert entry["time_min"] is None
        assert entry["time_max"] is None
        assert "temperature" in entry["arrays"]


def test_1d_on_time_coord_no_val_no_bounds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}")
        intent = _1d_intent(
            array="timestamp",
            ts_index=0,
            timestamp_val=None,
            data=np.datetime64("2023-12-01", "s"),
        )

        result = strategy.write_groups(group_to_intents={"g": [intent]})

        assert len(result["coverage"]) == 1
        entry = result["coverage"][0]
        assert entry["time_min"] is None
        assert entry["time_max"] is None
        assert "timestamp" in entry["arrays"]


def test_timestamp_intent_still_sets_bounds() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}")
        ts_val = np.datetime64("2024-06-15T12:00:00", "s")
        intent = _timestamp_intent(ts_index=0, timestamp_val=ts_val)

        result = strategy.write_groups(group_to_intents={"g": [intent]})

        assert len(result["coverage"]) == 1
        entry = result["coverage"][0]
        assert entry["time_min"] == "2024-06-15T12:00:00Z"
        assert entry["time_max"] == "2024-06-15T12:00:00Z"
        assert entry["arrays"] == ["timestamp"]


def test_mixed_1d_intents() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp)
        strategy = IndexedRegionStrategy(store_uri=f"file://{tmp}")
        ts_val_a = np.datetime64("2023-01-01", "s")
        ts_val_b = np.datetime64("2023-01-02", "s")
        bogus_ts = np.datetime64("1999-01-01", "s")

        intents = [
            _1d_intent(
                array="timestamp",
                ts_index=0,
                timestamp_val=ts_val_a,
                data=ts_val_a,
            ),
            _1d_intent(
                array="timestamp",
                ts_index=1,
                timestamp_val=ts_val_b,
                data=ts_val_b,
            ),
            _1d_intent(
                array="temperature",
                ts_index=0,
                timestamp_val=bogus_ts,
                data=np.float64(1.0),
            ),
            _1d_intent(
                array="temperature",
                ts_index=1,
                timestamp_val=bogus_ts,
                data=np.float64(2.0),
            ),
        ]

        result = strategy.write_groups(group_to_intents={"g": intents})

        assert len(result["coverage"]) == 1
        entry = result["coverage"][0]
        assert entry["time_min"] == "2023-01-01T00:00:00Z"
        assert entry["time_max"] == "2023-01-02T00:00:00Z"
        assert set(entry["arrays"]) == {"timestamp", "temperature"}
