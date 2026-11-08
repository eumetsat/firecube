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
- ``firecube.sdk`` re-exports the type.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from firecube.ingestor.templates.direct_zarr import IndexedWrite
from tests.helpers.rows import table_row


def test_write_intent_builders_and_exports(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "region_builder_populates_fields", tmp_path):
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

    with table_row(subtests, "slot_builder_populates_fields", tmp_path):
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

    with (
        table_row(subtests, "coordinate_none_rejected", tmp_path),
        pytest.raises(ValueError, match="coordinate must not be None"),
    ):
        IndexedWrite.slot(
            group="data",
            array="qa",
            coordinate=None,
            data=np.zeros((4,)),
        )

    with (
        table_row(subtests, "y_slice_non_slice_rejected", tmp_path),
        pytest.raises(ValueError, match="y_slice must be a slice"),
    ):
        IndexedWrite(
            group="data",
            array="counts",
            coordinate=datetime(2024, 1, 1, tzinfo=UTC),
            data=np.zeros((100, 2048)),
            y_slice=42,  # type: ignore[arg-type]
        )

    with table_row(subtests, "no_static_builder", tmp_path):
        assert not callable(getattr(IndexedWrite, "static", None))

    with table_row(subtests, "coordinate_is_field_not_classmethod", tmp_path):
        assert not callable(getattr(IndexedWrite, "coordinate", None))

    with table_row(subtests, "frozen_instance_rejects_mutation", tmp_path):
        iw = IndexedWrite.slot(
            group="data",
            array="qa",
            coordinate=datetime(2024, 1, 1, tzinfo=UTC),
            data=np.zeros((4,)),
        )
        with pytest.raises(FrozenInstanceError):
            iw.group = "other"  # type: ignore[misc]

    with table_row(subtests, "exported_from_sdk", tmp_path):
        from firecube.sdk import IndexedWrite as SdkIndexedWrite

        assert SdkIndexedWrite is IndexedWrite
