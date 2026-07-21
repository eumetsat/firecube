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

"""Regression tests for staged seeding of static array metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec
from tests.helpers.storage import assert_no_fsspec_bypass, make_local_session, make_test_session

pytestmark = pytest.mark.unit


def _write_array_json(
    base: Path,
    *,
    group: str,
    array: str,
    shape: list[int],
    attrs: dict[str, object],
) -> None:
    arr_path = base / group / array
    arr_path.mkdir(parents=True, exist_ok=True)
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": shape,
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": [min(shape[0], 40), *shape[1:]]},
        },
        "dimension_names": [array] + [f"d{i}" for i in range(len(shape) - 1)],
        "data_type": "float64",
        "fill_value": None,
        "chunk_key_encoding": {"name": "default", "separator": "/"},
        "codecs": [],
        "attributes": attrs,
    }
    (arr_path / "zarr.json").write_text(json.dumps(meta))


def _static_schema(store_uri: str) -> IndexedRegionStrategy:
    schema = [
        ZarrGroupSpec(
            group="G",
            arrays=[
                ZarrArraySpec(
                    name="lat",
                    shape=(4,),
                    dtype="float64",
                    fill_value=np.float64("nan"),
                    time_indexed=False,
                )
            ],
        )
    ]
    return IndexedRegionStrategy(store_uri=store_uri, schema=schema)


def _static_intent(data: np.ndarray) -> WriteIntent:
    return WriteIntent(group="G", array="lat", ts_index=0, data=data, kind="static")


def test_strip_marker_preserve_shape_attrs(tmp_path: Path) -> None:
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _write_array_json(
        final,
        group="G",
        array="lat",
        shape=[4],
        attrs={
            "_ARRAY_DIMENSIONS": ["lat"],
            "_FillValue": None,
            "firecube_static_written": True,
            "custom_attr": "kept",
        },
    )

    raw = json.loads((final / "G" / "lat" / "zarr.json").read_text())
    assert raw["attributes"]["firecube_static_written"] is True
    assert raw["attributes"]["_ARRAY_DIMENSIONS"] == ["lat"]
    assert raw["attributes"]["_FillValue"] is None

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    seeded = json.loads((temp / "G" / "lat" / "zarr.json").read_text())
    assert "attributes" in seeded
    assert seeded["attributes"].get("firecube_static_written") is None
    assert seeded["attributes"]["_ARRAY_DIMENSIONS"] == ["lat"]
    assert seeded["attributes"]["_FillValue"] is None
    assert seeded["attributes"]["custom_attr"] == "kept"


def test_staged_resume_static_array_round_trip(tmp_path: Path) -> None:
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    data = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float64)

    first_run = _static_schema(f"file://{final}")
    first_run.write_groups(group_to_intents={"G": [_static_intent(data)]})

    root = zarr.open_group(store=LocalStore(final), mode="r", zarr_format=3)
    assert root["G/lat"].attrs.get("firecube_static_written") is True

    session = make_local_session(str(final))
    with assert_no_fsspec_bypass():
        seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    seeded = json.loads((temp / "G" / "lat" / "zarr.json").read_text())
    assert seeded["attributes"].get("firecube_static_written") is None

    second_run = _static_schema(f"file://{temp}")
    second_run.write_groups(group_to_intents={"G": [_static_intent(data)]})

    make_test_session(tmp_path).upload_tree(
        StorageUri.from_local_path(temp),
        StorageUri.from_local_path(final),
    )

    root = zarr.open_group(store=LocalStore(final), mode="r", zarr_format=3)
    assert root["G/lat"].attrs.get("firecube_static_written") is True
    np.testing.assert_array_equal(np.asarray(cast(Any, root["G/lat"])[:]), data)
