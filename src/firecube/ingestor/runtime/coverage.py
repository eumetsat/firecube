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

"""Per-group write coverage tracking for span recording.

Extracted from the MTG FCI plugin as a generic runtime utility.
Accumulates per-group writes, tracks time bounds, merges contiguous
index ranges, and produces `SpanCoverage` objects compatible
with `SpanRecorder.record_batch_success`.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from firecube.core.controlplane.types import SpanCoverage


class CoverageTracker:
    """Collect per-group write coverage entries for span recording.

    Coverage entries emitted by `build_coverage` follow the schema
    expected by ingestion runtime span extraction.
    """

    def __init__(self, time_dim_name: str = "timestamp") -> None:
        """Initialize empty per-group coverage state."""
        self._time_dim_name = time_dim_name
        self._groups: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _time_to_iso8601(time_val: Any) -> str | None:
        """Convert supported timestamp-like values to ISO-8601.

        Args:
            time_val: Timestamp-like value such as ``datetime``,
                ``np.datetime64``, or an ISO string.

        Returns:
            ISO-8601 string when conversion is possible, otherwise ``None``.
        """
        if time_val is None:
            return None

        if isinstance(time_val, str):
            return time_val

        if hasattr(time_val, "isoformat"):
            try:
                return str(time_val.isoformat())
            except Exception:
                pass

        if isinstance(time_val, np.datetime64):
            text = np.datetime_as_string(time_val, timezone="UTC")
            return text if text != "NaT" else None

        return None

    @staticmethod
    def _merge_index_ranges(indices: set[int]) -> list[list[int]]:
        """Merge timestamp indices into inclusive contiguous ranges.

        Args:
            indices: Set of written timestamp indices.

        Returns:
            Inclusive ``[start, end]`` ranges sorted by start.
        """
        if not indices:
            return []

        sorted_indices = sorted(indices)
        ranges: list[list[int]] = []
        start = sorted_indices[0]
        end = sorted_indices[0]

        for idx in sorted_indices[1:]:
            if idx == end + 1:
                end = idx
                continue
            ranges.append([start, end])
            start = idx
            end = idx

        ranges.append([start, end])
        return ranges

    def record_write(
        self,
        group: str,
        arrays: list[str],
        ts_index: int,
        time_val: Any,
        aligned: bool,
    ) -> None:
        """Record one write operation for coverage accumulation.

        Args:
            group: Zarr group written by the operation.
            arrays: Array paths touched by the write.
            ts_index: Timestamp index written in this operation.
            time_val: Timestamp value corresponding to ``ts_index``.
            aligned: Whether this write was chunk-aligned.
        """
        entry = self._groups.setdefault(
            group,
            {
                "arrays": set(),
                "indices": set(),
                "aligned": True,
                "time_min": None,
                "time_max": None,
            },
        )

        entry["arrays"].update(arrays)
        entry["indices"].add(int(ts_index))
        entry["aligned"] = bool(entry["aligned"]) and bool(aligned)

        iso_time = self._time_to_iso8601(time_val)
        if iso_time is None:
            return

        current_min = entry["time_min"]
        current_max = entry["time_max"]
        if current_min is None or iso_time < current_min:
            entry["time_min"] = iso_time
        if current_max is None or iso_time > current_max:
            entry["time_max"] = iso_time

    def build_coverage(
        self,
        state_array_name: str = "firecube_timestamp_state",
        time_dim_name: str | None = None,
    ) -> list[SpanCoverage]:
        """Build span-coverage entries for all recorded groups.

        Args:
            state_array_name: Timestamp-state variable name under each group.
            time_dim_name: Time dimension name to record on each coverage
                entry; falls back to the tracker's constructor value.

        Returns:
            List of `SpanCoverage` objects, one per recorded group.
        """
        effective_dim = time_dim_name or self._time_dim_name
        coverage: list[SpanCoverage] = []
        for group in sorted(self._groups):
            entry = self._groups[group]
            coverage.append(
                SpanCoverage(
                    group=group,
                    arrays=sorted(entry["arrays"]),
                    time_index_ranges=self._merge_index_ranges(entry["indices"]),
                    aligned=bool(entry["aligned"]),
                    state_array=f"{group}/{state_array_name}",
                    state_deleted_value=2,
                    time_min=entry["time_min"],
                    time_max=entry["time_max"],
                    time_dim_name=effective_dim,
                )
            )
        return coverage
