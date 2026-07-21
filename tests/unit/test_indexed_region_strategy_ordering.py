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

from dataclasses import dataclass
from types import SimpleNamespace

from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)


@dataclass(frozen=True)
class _ArraySpec:
    name: str
    shape: tuple[int, ...]
    dtype: object
    fill_value: object | None = None
    chunks: tuple[int, ...] | None = None
    shards: tuple[int, ...] | None = None
    attrs: object | None = None
    dimension_names: tuple[str, ...] | None = None
    time_indexed: bool = True


@dataclass(frozen=True)
class _GroupSpec:
    group: str
    arrays: list[_ArraySpec]


def test_write_groups_enters_claim_before_schema_initialization(monkeypatch) -> None:
    calls: list[str] = []

    class _Writer:
        def ensure_group(self, group: str, **kwargs) -> None:
            calls.append("ensure_group")

        def set_group_attrs(self, group: str, attrs) -> None:
            calls.append("set_group_attrs")

    class _Claim:
        def __enter__(self):
            calls.append("claim_enter")
            return self

        def __exit__(self, exc_type, exc, tb):
            calls.append("claim_exit")
            return False

    def _claim_for_group(group_name: str):
        return _Claim()

    def _dispatch_intent(writer, intent) -> None:
        calls.append("dispatch")

    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        lambda *args, **kwargs: _Writer(),
    )
    monkeypatch.setattr(
        IndexedRegionStrategy,
        "_dispatch_intent",
        staticmethod(_dispatch_intent),
    )

    strategy = IndexedRegionStrategy(store_uri="/tmp/test.zarr", schema=[])
    schema = [_GroupSpec(group="data", arrays=[_ArraySpec("values", (1, 2), object())])]
    intents = [
        SimpleNamespace(
            kind="region",
            group="data",
            array="values",
            ts_index=0,
            data=None,
            y_slice=None,
            channel_index=None,
            timestamp_val=None,
        )
    ]

    strategy.write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        claim_for_group=_claim_for_group,
    )

    assert calls == [
        "claim_enter",
        "ensure_group",
        "set_group_attrs",
        "claim_exit",
        "claim_enter",
        "dispatch",
        "claim_exit",
    ]
