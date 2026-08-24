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

"""Contract tests for the ``IndexedWrite`` dataclass.

Locks the public API surface for coordinate-keyed write intents:

- ``.region()`` and ``.slot()`` are the only supported builders.
- ``coordinate`` is a raw field (not a classmethod), rejected when ``None``.
- ``y_slice`` must be a real ``slice`` object when set.
- No ``.static()`` or ``.coordinate()`` factory — those live on ``WriteIntent``
  or are engine-owned respectively.
- The dataclass is frozen; instances cannot be mutated post-construction.
- Both SDK façades re-export the type.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import numpy as np
import pytest

from firecube.ingestor.templates.direct_zarr import IndexedWrite


def test_region_builder_populates_fields() -> None:
    coord = datetime(2024, 1, 1, tzinfo=UTC)
    data = np.zeros((100, 2048))
    iw = IndexedWrite.region(
        group="data",
        array="counts",
        coordinate=coord,
        data=data,
        y_slice=slice(0, 100),
        channel_index=3,
    )

    assert iw.group == "data"
    assert iw.array == "counts"
    assert iw.coordinate is coord
    assert iw.data is data
    assert iw.y_slice == slice(0, 100)
    assert iw.channel_index == 3
    assert iw._kind == "region"


def test_slot_builder_populates_fields() -> None:
    coord = datetime(2024, 1, 1, tzinfo=UTC)
    data = np.zeros((4,))
    iw = IndexedWrite.slot(
        group="data",
        array="qa",
        coordinate=coord,
        data=data,
    )

    assert iw.group == "data"
    assert iw.array == "qa"
    assert iw.coordinate is coord
    assert iw.data is data
    assert iw.y_slice is None
    assert iw.channel_index is None
    assert iw._kind == "slot"


def test_coordinate_none_rejected() -> None:
    with pytest.raises(ValueError, match="coordinate must not be None"):
        IndexedWrite.slot(
            group="data",
            array="qa",
            coordinate=None,
            data=np.zeros((4,)),
        )


def test_y_slice_non_slice_rejected() -> None:
    with pytest.raises(ValueError, match="y_slice must be a slice"):
        IndexedWrite(
            group="data",
            array="counts",
            coordinate=datetime(2024, 1, 1, tzinfo=UTC),
            data=np.zeros((100, 2048)),
            y_slice=42,  # type: ignore[arg-type]
        )


def test_no_static_builder() -> None:
    assert not callable(getattr(IndexedWrite, "static", None))


def test_coordinate_is_field_not_classmethod() -> None:
    assert not callable(getattr(IndexedWrite, "coordinate", None))


def test_frozen_instance_rejects_mutation() -> None:
    iw = IndexedWrite.slot(
        group="data",
        array="qa",
        coordinate=datetime(2024, 1, 1, tzinfo=UTC),
        data=np.zeros((4,)),
    )
    with pytest.raises(FrozenInstanceError):
        iw.group = "other"  # type: ignore[misc]


def test_reexport_from_core_api() -> None:
    from firecube.core.api import IndexedWrite as CoreIndexedWrite

    assert CoreIndexedWrite is IndexedWrite


def test_reexport_from_ingestor_api() -> None:
    from firecube.ingestor.api import IndexedWrite as IngestorIndexedWrite

    assert IngestorIndexedWrite is IndexedWrite
