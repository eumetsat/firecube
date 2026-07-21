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

from typing import Any

import numpy as np
import pytest

from firecube.core.controlplane.manager import ChunkManager as ChunkManagerClass
from firecube.ingestor.templates.direct_zarr import (
    ZarrArraySpec,
    ZarrGroupSpec,
    _compute_schema_hash,
)

pytestmark = pytest.mark.unit


class _Writer:
    def __init__(self) -> None:
        self.appended: list[dict[str, Any]] = []

    def append(
        self, event_type: str, record: dict[str, Any], *, meta: dict[str, Any], flush: bool
    ) -> None:
        self.appended.append(
            {"event_type": event_type, "record": record, "meta": meta, "flush": flush}
        )


def _schema(*, dtype: Any = np.float32) -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(1, 4, 5),
                    dtype=dtype,
                    chunks=(1, 4, 5),
                    fill_value=0.0,
                )
            ],
        )
    ]


def _record(manager: ChunkManagerClass, writer: _Writer, *, schema_hash: str) -> dict[str, Any]:
    manager.repo._wal_writer._writer = lambda product, run_id, resume_existing: writer  # type: ignore[method-assign]
    manager.record_schema_verification(
        product="product",
        run_id="run-1",
        group="data",
        plugin="plugin",
        schema_hash=schema_hash,
        verified_at="2026-05-29T00:00:00+00:00",
        expected_time_count=10,
        meta={"source": "test"},
    )
    return writer.appended[0]


def test_record_includes_schema_hash(chunk_manager: ChunkManagerClass) -> None:
    writer = _Writer()
    event = _record(chunk_manager, writer, schema_hash="abcdef1234567890")

    assert event["event_type"] == "schema_verification"
    assert event["record"]["schema_hash"] == "abcdef1234567890"


def test_record_includes_group_and_plugin_and_run_id(chunk_manager: ChunkManagerClass) -> None:
    event = _record(chunk_manager, _Writer(), schema_hash="abcdef1234567890")

    assert event["record"]["group"] == "data"
    assert event["record"]["plugin"] == "plugin"
    assert event["record"]["run_id"] == "run-1"


def test_two_pods_same_schema_produce_same_hash() -> None:
    first = _compute_schema_hash(_schema(), {"data": 10})
    second = _compute_schema_hash(_schema(), {"data": 10})

    assert first == second


def test_schema_hash_changes_on_dtype_change() -> None:
    assert _compute_schema_hash(_schema(dtype=np.float32), {"data": 10}) != _compute_schema_hash(
        _schema(dtype=np.float64),
        {"data": 10},
    )
