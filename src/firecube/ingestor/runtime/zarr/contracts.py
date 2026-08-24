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

"""Protocol contracts for Zarr write strategies."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

import xarray as xr


@runtime_checkable
class AppendWriteStrategy(Protocol):
    """Protocol implemented by xarray-append Zarr write strategies."""

    def write_groups(
        self,
        *,
        group_to_timestamps: Mapping[str, Sequence[Any]],
        dataset_for_batch: Callable[[str, Sequence[Any]], xr.Dataset | None],
        batch_size: int,
        claim_for_group: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]: ...


@runtime_checkable
class RegionWriteStrategy(Protocol):
    """Protocol implemented by direct region-based Zarr write strategies.

    When ``claim_for_slot`` is provided, ``claim_for_group`` protects schema
    setup only and ``claim_for_slot`` protects per-``ts_index`` intent dispatch.
    If ``claim_for_slot`` is ``None``, implementations may fall back to
    ``claim_for_group`` for dispatch coordination.
    """

    def write_groups(
        self,
        *,
        group_to_intents: dict[str, list[Any]],
        schema: Sequence[Any] | None = None,
        claim_for_group: Callable[[str], Any] | None = None,
        claim_for_slot: Callable[[str, int], Any] | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
        codec_pipelines_by_array: Mapping[tuple[str, str], tuple[Any, Any, Any]] | None = None,
        region_write_concurrency: int = 1,
    ) -> dict[str, Any]: ...
