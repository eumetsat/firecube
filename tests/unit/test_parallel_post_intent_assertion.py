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

from pathlib import Path

import pytest

from firecube.ingestor.errors import WriteIntentRangeError
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent

pytestmark = pytest.mark.unit


class _Writer:
    def ensure_group(self, group: str, **kwargs) -> None:
        _ = (group, kwargs)


def _intent(ts_index: int) -> WriteIntent:
    return WriteIntent(
        group="data",
        array="values",
        ts_index=ts_index,
        data=None,
        y_slice=slice(0, 1),
    )


def _patch_writer(monkeypatch: pytest.MonkeyPatch, dispatched: list[int]) -> None:
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        lambda *args, **kwargs: _Writer(),
    )
    monkeypatch.setattr(
        IndexedRegionStrategy,
        "_dispatch_intent",
        staticmethod(lambda writer, intent: dispatched.append(intent.ts_index)),
    )


def test_intent_within_range_accepted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dispatched: list[int] = []
    _patch_writer(monkeypatch, dispatched)

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={"data": [_intent(99)]},
        slot_range=(0, 100),
    )

    assert dispatched == [99]


def test_intent_at_upper_boundary_rejected(tmp_path: Path) -> None:
    with pytest.raises(WriteIntentRangeError, match=r"slot_range=\[0, 100\)"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={"data": [_intent(100)]},
            slot_range=(0, 100),
        )


def test_intent_negative_rejected(tmp_path: Path) -> None:
    with pytest.raises(WriteIntentRangeError, match="ts_index=-1"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={"data": [_intent(-1)]},
            slot_range=(0, 100),
        )


def test_no_slot_range_skips_assertion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dispatched: list[int] = []
    _patch_writer(monkeypatch, dispatched)

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={"data": [_intent(1000)]},
        slot_range=None,
    )

    assert dispatched == [1000]


def test_out_of_range_intent_fails_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_writer(*args, **kwargs):
        raise AssertionError("writer must not be constructed")

    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        fail_writer,
    )
    monkeypatch.setattr(
        IndexedRegionStrategy,
        "_dispatch_intent",
        staticmethod(lambda writer, intent: (_ for _ in ()).throw(AssertionError("no dispatch"))),
    )

    with pytest.raises(WriteIntentRangeError):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={"data": [_intent(4), _intent(10)]},
            slot_range=(0, 5),
        )
