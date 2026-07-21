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

import numpy as np
import pytest
import zarr

from firecube.core.zarr.region_writer import RegionZarrWriter


@pytest.mark.unit
def test_write_timestamp_uses_configured_coord_path_and_value(tmp_path):
    store_uri = str(tmp_path / "custom_coord.zarr")
    timestamp = np.datetime64("2025-01-01T00:00:00", "s")

    writer = RegionZarrWriter(store_uri, time_coord_name="time")
    writer.write_timestamp("G", 0, timestamp)

    root = zarr.open_group(store=store_uri, mode="r")
    assert "G/time" in root
    assert "G/timestamp" not in root
    assert np.asarray(root["G/time"][0]) == timestamp  # type: ignore[index]


@pytest.mark.unit
def test_resolve_timestamp_index_reads_configured_coord_path(tmp_path):
    store_uri = str(tmp_path / "resolve_custom_coord.zarr")
    timestamp = np.datetime64("2025-01-01T00:00:00", "s")

    writer = RegionZarrWriter(store_uri, time_coord_name="time")
    writer.write_timestamp("G", 0, timestamp)

    assert writer.resolve_timestamp_index("G", timestamp) == 0
    assert writer.resolve_timestamp_index("G", np.datetime64("2025-01-02T00:00:00", "s")) == 1

    root = zarr.open_group(store=store_uri, mode="r")
    assert "G/time" in root
    assert "G/timestamp" not in root


@pytest.mark.unit
@pytest.mark.parametrize("time_coord_name", ["time", "timestamp"])
def test_write_timestamp_sets_native_dimension_names(tmp_path, time_coord_name):
    """The on-demand time coordinate carries Zarr v3 native ``dimension_names``.

    ``write_timestamp`` creates the time coordinate lazily (no plugin schema spec
    declares it), so its self-naming must come from the writer. xarray resolves
    a 1-D coordinate's dimension from this native metadata; without it the store
    falls back to the legacy v2 ``_ARRAY_DIMENSIONS`` convention. The dim name
    must track the configured coord name, not a hardcoded literal.
    """
    store_uri = str(tmp_path / "dimnames.zarr")

    writer = RegionZarrWriter(store_uri, time_coord_name=time_coord_name)
    writer.write_timestamp("G", 0, "2025-01-01T00:00:00")

    arr = zarr.open_group(store=store_uri, mode="r")[f"G/{time_coord_name}"]
    assert arr.metadata.dimension_names == (time_coord_name,)  # type: ignore[union-attr]
    assert "_ARRAY_DIMENSIONS" not in dict(arr.attrs)
