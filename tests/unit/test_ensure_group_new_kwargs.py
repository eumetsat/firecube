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

import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter


def _make_writer(path: str) -> RegionZarrWriter:
    return RegionZarrWriter(f"file://{path}")


def test_create_with_attrs() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        w = _make_writer(tmp)
        w.ensure_group(
            "g/temperature",
            shape=(10, 4, 5),
            dtype="float32",
            attrs={"units": "K", "long_name": "temperature"},
        )
        arr = zarr.open_array(LocalStore(tmp), path="g/temperature", mode="r")
        stored = dict(arr.attrs)
        assert stored["units"] == "K"
        assert stored["long_name"] == "temperature"


def test_create_with_dimension_names() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        w = _make_writer(tmp)
        w.ensure_group(
            "g/temperature",
            shape=(10, 4, 5),
            dtype="float32",
            dimension_names=("time", "y", "x"),
        )
        arr = zarr.open_array(LocalStore(tmp), path="g/temperature", mode="r")
        assert getattr(arr.metadata, "dimension_names", None) == ("time", "y", "x")


def test_create_with_shards() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        w = _make_writer(tmp)
        w.ensure_group(
            "g/temperature",
            shape=(10, 4, 5),
            dtype="float32",
            chunks=(1, 2, 5),
            shards=(2, 4, 5),
        )
        arr = zarr.open_array(LocalStore(tmp), path="g/temperature", mode="r")
        assert arr.shape == (10, 4, 5)
        assert arr.shards == (2, 4, 5)


def test_reserved_attrs_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        w = _make_writer(tmp)
        with pytest.raises(ValueError, match="Reserved attr"):
            w.ensure_group(
                "g/temperature",
                shape=(10, 4, 5),
                dtype="float32",
                attrs={"_ARRAY_DIMENSIONS": ["time", "y", "x"]},
            )


def test_resume_conflicting_dimension_names_raises() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        w = _make_writer(tmp)
        w.ensure_group(
            "g/temperature",
            shape=(10, 4, 5),
            dtype="float32",
            dimension_names=("time", "y", "x"),
        )
        with pytest.raises(SchemaDriftError, match="dimension_names"):
            w.ensure_group(
                "g/temperature",
                shape=(10, 4, 5),
                dtype="float32",
                dimension_names=("t", "lat", "lon"),
            )
