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

"""``firecube_group_identity_hash`` survives staged-metadata seeding.

Staged-write mode copies ``zarr.json`` files from the final target into a
temp workspace so append cursors read the right shape. The seed helper
strips ``firecube_static_written`` (RUN-STATE) but preserves all SHAPE
attrs, including ``firecube_group_identity_hash``. This guarantees that
per-group ingest-startup verification still works on the temp store.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import numpy as np
import pytest
import zarr

from firecube.core.api import FIRECUBE_GROUP_IDENTITY_HASH_ATTR, FIRECUBE_STATIC_WRITTEN_ATTR
from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import make_local_session

pytestmark = pytest.mark.integration

_STAMPED_HASH = "f" * 64
_GROUP = "grp"
_COORD_NAME = "timestamp"
_SLOT_COUNT = 16


def _build_final_target_with_stamped_coord(target_path: Path) -> None:
    target_path.mkdir()
    root = zarr.open_group(store=str(target_path), mode="a", zarr_format=3)
    group = root.create_group(_GROUP)
    coord = group.create_array(
        _COORD_NAME,
        shape=(_SLOT_COUNT,),
        dtype="datetime64[ns]",
        chunks=(_SLOT_COUNT,),
        fill_value=np.array(np.datetime64("NaT", "ns"), dtype="datetime64[ns]")[()],
        dimension_names=(_COORD_NAME,),
        attributes={
            FIRECUBE_GROUP_IDENTITY_HASH_ATTR: _STAMPED_HASH,
            FIRECUBE_STATIC_WRITTEN_ATTR: True,
        },
    )
    values = np.arange(_SLOT_COUNT, dtype="int64").astype("datetime64[s]").astype("datetime64[ns]")
    coord[...] = values


def _coord_zarr_json_attrs(store_path: Path) -> dict:
    zj = store_path / _GROUP / _COORD_NAME / "zarr.json"
    payload = json.loads(zj.read_text())
    return cast(dict, payload.get("attributes") or {})


def test_group_identity_hash_survives_staged_seed(tmp_path: Path) -> None:
    final_target = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"
    temp_store.parent.mkdir(parents=True, exist_ok=True)
    _build_final_target_with_stamped_coord(final_target)

    result = seed_staged_store_metadata(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_target),
        groups=[_GROUP],
        session=make_local_session(str(temp_store)),
    )
    assert result[_GROUP]["seeded"] is True

    seeded_attrs = _coord_zarr_json_attrs(temp_store)
    assert seeded_attrs.get(FIRECUBE_GROUP_IDENTITY_HASH_ATTR) == _STAMPED_HASH
    assert FIRECUBE_STATIC_WRITTEN_ATTR not in seeded_attrs
