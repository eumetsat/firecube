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

"""Contract tests for the ``_compile_indexed_write`` pure compiler (B3).

Behavior under test:

- Purity: same ``(iw, resolved_index)`` inputs produce equal outputs.
- Failure boundary: missing coordinate / missing group raises
  ``IndexedWriteCompilationError`` with structured fields.
- Dispatch: ``.region()``-built ``IndexedWrite`` compiles to ``kind="region"``;
  ``.slot()``-built ``IndexedWrite`` compiles to ``kind="1d"``.
- Payload passthrough: ``iw.data`` identity preserved for both callables and
  ``np.ndarray`` payloads (never invoked, never copied).
- Return shape: always ``list[WriteIntent]``.
- Coverage: ``RegularTimeAxis``, ``IntegerAxis``, and ``IrregularTimeAxis``.
"""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core.errors import IndexedWriteCompilationError
from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import (
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    RegularTimeAxis,
)
from firecube.ingestor.templates.direct_zarr import (
    IndexedWrite,
    _compile_indexed_write,
)


def _regular_index() -> ResolvedIndex:
    spec = IndexSpec(
        name="reg_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="exact",
                slot_count=10,
            ),
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _integer_index() -> ResolvedIndex:
    spec = IndexSpec(name="int_v1", groups={"data": IntegerAxis(slot_count=5)})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _irregular_index() -> ResolvedIndex:
    spec = IndexSpec(
        name="irr_v1",
        groups={
            "data": IrregularTimeAxis(
                coordinate="timestamp",
                values=(
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:10:00Z",
                    "2024-01-01T00:20:00Z",
                ),
            )
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


def test_compile_is_pure_function() -> None:
    idx = _regular_index()

    def payload() -> np.ndarray:
        return np.zeros((4, 4))

    iw = IndexedWrite.region(
        group="data",
        array="counts",
        coordinate="2024-01-01T00:10:00Z",
        data=payload,
        y_slice=slice(0, 4),
    )

    out1 = _compile_indexed_write(iw, idx)
    out2 = _compile_indexed_write(iw, idx)

    assert out1 == out2


def test_missing_coordinate_raises_compilation_error() -> None:
    idx = _irregular_index()
    iw = IndexedWrite.slot(
        group="data",
        array="counts",
        coordinate="2099-12-31T00:00:00Z",
        data=np.zeros((4,)),
    )

    with pytest.raises(IndexedWriteCompilationError) as exc_info:
        _compile_indexed_write(iw, idx)

    err = exc_info.value
    assert err.coordinate == "2099-12-31T00:00:00Z"
    assert "not in resolved index" in err.reason
    assert "data" in err.reason
    assert "IndexedWrite" in err.iw_repr
    assert err.__cause__ is not None


def test_missing_group_raises_compilation_error() -> None:
    idx = _regular_index()
    iw = IndexedWrite.slot(
        group="does_not_exist",
        array="counts",
        coordinate="2024-01-01T00:00:00Z",
        data=np.zeros((4,)),
    )

    with pytest.raises(IndexedWriteCompilationError) as exc_info:
        _compile_indexed_write(iw, idx)

    err = exc_info.value
    assert "does_not_exist" in err.reason
    assert isinstance(err.__cause__, KeyError)


def test_region_iw_compiles_to_region_writeintent() -> None:
    idx = _regular_index()
    arr = np.arange(16, dtype=np.float32).reshape(4, 4)
    iw = IndexedWrite.region(
        group="data",
        array="counts",
        coordinate="2024-01-01T00:20:00Z",
        data=arr,
        y_slice=slice(0, 4),
        channel_index=1,
    )

    out = _compile_indexed_write(iw, idx)

    assert len(out) == 1
    wi = out[0]
    assert wi.kind == "region"
    assert wi.group == "data"
    assert wi.array == "counts"
    assert wi.ts_index == 2
    assert wi.y_slice == slice(0, 4)
    assert wi.channel_index == 1
    assert wi.data is arr


def test_slot_iw_compiles_to_slot_writeintent() -> None:
    idx = _integer_index()
    arr = np.zeros((3,), dtype=np.float32)
    iw = IndexedWrite.slot(
        group="data",
        array="tally",
        coordinate=3,
        data=arr,
    )

    out = _compile_indexed_write(iw, idx)

    assert len(out) == 1
    wi = out[0]
    assert wi.kind == "1d"
    assert wi.group == "data"
    assert wi.array == "tally"
    assert wi.ts_index == 3
    assert wi.y_slice is None
    assert wi.channel_index is None
    assert wi.data is arr


def test_callable_data_passes_through_unchanged() -> None:
    idx = _regular_index()
    invoked = False

    def payload() -> np.ndarray:
        nonlocal invoked
        invoked = True
        return np.zeros((4, 4))

    iw = IndexedWrite.region(
        group="data",
        array="counts",
        coordinate="2024-01-01T00:00:00Z",
        data=payload,
        y_slice=slice(0, 4),
    )

    out = _compile_indexed_write(iw, idx)

    assert out[0].data is payload
    assert not invoked


def test_ndarray_data_passes_through_unchanged() -> None:
    idx = _integer_index()
    arr = np.zeros((3,), dtype=np.float32)
    iw = IndexedWrite.slot(
        group="data",
        array="tally",
        coordinate=0,
        data=arr,
    )

    out = _compile_indexed_write(iw, idx)

    assert out[0].data is arr


def test_returns_list() -> None:
    idx = _regular_index()
    iw = IndexedWrite.slot(
        group="data",
        array="counts",
        coordinate="2024-01-01T00:00:00Z",
        data=np.zeros((4,)),
    )

    out = _compile_indexed_write(iw, idx)

    assert isinstance(out, list)
    assert len(out) == 1


def test_compile_supports_irregular_axis() -> None:
    idx = _irregular_index()
    arr = np.zeros((2, 2), dtype=np.float32)
    iw = IndexedWrite.region(
        group="data",
        array="counts",
        coordinate="2024-01-01T00:10:00Z",
        data=arr,
        y_slice=slice(0, 2),
    )

    out = _compile_indexed_write(iw, idx)

    assert out[0].ts_index == 1
    assert out[0].data is arr
