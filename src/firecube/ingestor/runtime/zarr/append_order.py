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

"""Run-scoped validation of the committed boundary for plain appends."""

from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import xarray as xr
import zarr
from zarr.errors import GroupNotFoundError

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.zarr.time_decode import decode_or_passthrough
from firecube.ingestor.errors import AppendOverwriteRefused, InsertRefusedError


def _values(values: np.ndarray, attrs: dict[str, Any]) -> np.ndarray:
    decoded = decode_or_passthrough(values, attrs)
    if decoded.dtype.kind == "M":
        return decoded.astype("datetime64[ns]")
    return decoded


@dataclass
class _Boundary:
    length: int
    maximum: Any


class AppendOrder:
    """Validate each stored coordinate prefix once while the write gate is held.

    Newly committed tails are read on the next append. A shorter array after
    rollback invalidates the cached prefix. No state is shared across runs.
    """

    def __init__(self) -> None:
        self._boundaries: dict[tuple[str, str, str], _Boundary] = {}

    def _maximum(
        self,
        handle: ZarrStoreHandle,
        group: str,
        time_dim: str,
        *,
        baseline: _Boundary | None = None,
    ) -> _Boundary | None:
        key = (handle.target_uri, group, time_dim)
        try:
            root = zarr.open_group(**handle.zarr_kwargs(), mode="r", use_consolidated=False)
        except (FileNotFoundError, GroupNotFoundError):
            self._boundaries.pop(key, None)
            return None
        if group not in root or time_dim not in root[group]:
            self._boundaries.pop(key, None)
            return None
        array = cast(Any, root[group])[time_dim]
        length = array.shape[0]
        previous = self._boundaries.get(key)
        if previous is not None and previous.length > length:
            previous = None
        if baseline is not None:
            if length < baseline.length:
                raise ValueError(
                    "Staged time coordinate is shorter than the resume coordinate; seed staged metadata before append."
                )
            if previous is None:
                # Metadata-only staging contains fill values for the prefix.
                # Its authoritative values are in the final target, already
                # validated above; only the workspace tail was written here.
                previous = baseline
        start = previous.length if previous is not None else 0
        values = _values(np.asarray(array[start:]), dict(array.attrs))
        if values.size:
            invalid = np.isnat(values) if values.dtype.kind == "M" else np.isnan(values)
            if bool(np.any(invalid)):
                raise AppendOverwriteRefused(
                    refused_timestamps=["<missing existing value>"], reason="nat_existing"
                )
            if not bool(np.all(values[:-1] < values[1:])) or (
                previous is not None
                and previous.maximum is not None
                and values[0] <= previous.maximum
            ):
                raise AppendOverwriteRefused(
                    refused_timestamps=[str(v) for v in values[:3]],
                    reason="unsorted_existing_coord",
                )
            maximum = values[-1]
        else:
            maximum = previous.maximum if previous is not None else None
        boundary = _Boundary(length, maximum)
        self._boundaries[key] = boundary
        return boundary

    def assert_append(
        self,
        ds: xr.Dataset,
        *,
        group: str,
        time_dim: str,
        write_store: ZarrStoreHandle,
        resume_store: ZarrStoreHandle | None,
    ) -> None:
        """Require an append tail to follow both the workspace and final target."""
        incoming = _values(np.asarray(ds[time_dim].values), dict(ds[time_dim].attrs))
        if not incoming.size:
            return
        baseline = None
        if resume_store is not None and resume_store.target_uri != write_store.target_uri:
            baseline = self._maximum(resume_store, group, time_dim)
        current = self._maximum(write_store, group, time_dim, baseline=baseline)
        for boundary in [baseline, current]:
            if (
                boundary is not None
                and boundary.maximum is not None
                and incoming[0] <= boundary.maximum
            ):
                raise InsertRefusedError(
                    refused_timestamps=[str(incoming[0])],
                    reason="insert",
                    existing_max=str(boundary.maximum),
                )
