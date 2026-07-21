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

from typing import ClassVar

import pytest

from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent


class _RecordingWriter:
    instances: ClassVar[list["_RecordingWriter"]] = []

    def __init__(
        self, store_uri, *, store=None, coord_names=frozenset(), time_coord_name="timestamp"
    ):
        self.store_uri = store_uri
        self.store = store
        self.coord_names = coord_names
        self.time_coord_name = time_coord_name
        self.timestamp_arrays: list[str] = []
        self.__class__.instances.append(self)

    def write_timestamp(self, *, group, ts_index, timestamp_val):
        self.timestamp_arrays.append(self.time_coord_name)


def _timestamp_intent() -> WriteIntent:
    return WriteIntent(
        group="data",
        array="timestamp",
        ts_index=0,
        data=None,
        kind="timestamp",
        timestamp_val="2025-01-01T00:00:00",
    )


@pytest.mark.unit
def test_timestamp_intent_writes_to_configured_array(monkeypatch):
    """A kind='timestamp' intent writes to the configured time array, not the kind token."""
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        _RecordingWriter,
    )
    _RecordingWriter.instances.clear()

    strategy = IndexedRegionStrategy(store_uri="memory://test.zarr", time_coord_name="time")
    result = strategy.write_groups(group_to_intents={"data": [_timestamp_intent()]})

    writer = _RecordingWriter.instances[0]
    assert writer.time_coord_name == "time"
    assert writer.timestamp_arrays == ["time"]
    assert result["coverage"][0]["arrays"] == ["time"]


@pytest.mark.unit
def test_timestamp_intent_back_compat_default(monkeypatch):
    """Default time_coord_name='timestamp' produces back-compat behavior."""
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        _RecordingWriter,
    )
    _RecordingWriter.instances.clear()

    strategy = IndexedRegionStrategy(store_uri="memory://test.zarr")
    result = strategy.write_groups(group_to_intents={"data": [_timestamp_intent()]})

    writer = _RecordingWriter.instances[0]
    assert writer.time_coord_name == "timestamp"
    assert writer.timestamp_arrays == ["timestamp"]
    assert result["coverage"][0]["arrays"] == ["timestamp"]
