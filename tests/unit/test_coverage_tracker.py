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

from datetime import UTC, datetime

import numpy as np

from firecube.core.controlplane.types import SpanCoverage
from firecube.ingestor.runtime.coverage import CoverageTracker


class TestSingleGroupAccumulation:
    def test_single_write_produces_one_coverage(self):
        tracker = CoverageTracker()
        tracker.record_write(
            group="vis_06",
            arrays=["vis_06/data", "vis_06/quality"],
            ts_index=0,
            time_val="2024-01-01T00:00:00Z",
            aligned=True,
        )
        result = tracker.build_coverage()
        assert len(result) == 1
        cov = result[0]
        assert isinstance(cov, SpanCoverage)
        assert cov.group == "vis_06"
        assert cov.arrays == ["vis_06/data", "vis_06/quality"]
        assert cov.time_index_ranges == [[0, 0]]
        assert cov.aligned is True
        assert cov.time_min == "2024-01-01T00:00:00Z"
        assert cov.time_max == "2024-01-01T00:00:00Z"
        assert cov.state_array == "vis_06/firecube_timestamp_state"

    def test_multiple_writes_same_group_accumulate(self):
        tracker = CoverageTracker()
        tracker.record_write(
            "g1", ["g1/a"], ts_index=0, time_val="2024-01-01T00:00:00Z", aligned=True
        )
        tracker.record_write(
            "g1", ["g1/b"], ts_index=1, time_val="2024-01-01T01:00:00Z", aligned=True
        )
        tracker.record_write(
            "g1", ["g1/a"], ts_index=2, time_val="2024-01-01T02:00:00Z", aligned=False
        )

        result = tracker.build_coverage()
        assert len(result) == 1
        cov = result[0]
        assert sorted(cov.arrays) == ["g1/a", "g1/b"]
        assert cov.time_index_ranges == [[0, 2]]
        assert cov.aligned is False
        assert cov.time_min == "2024-01-01T00:00:00Z"
        assert cov.time_max == "2024-01-01T02:00:00Z"


class TestMultiGroup:
    def test_separate_groups_produce_separate_coverage(self):
        tracker = CoverageTracker()
        tracker.record_write(
            "alpha", ["alpha/x"], ts_index=0, time_val="2024-01-01T00:00:00Z", aligned=True
        )
        tracker.record_write(
            "beta", ["beta/y"], ts_index=5, time_val="2024-06-01T00:00:00Z", aligned=True
        )

        result = tracker.build_coverage()
        assert len(result) == 2
        assert result[0].group == "alpha"
        assert result[1].group == "beta"
        assert result[0].time_index_ranges == [[0, 0]]
        assert result[1].time_index_ranges == [[5, 5]]


class TestContiguousRangeMerging:
    def test_contiguous_indices_merge_into_single_range(self):
        tracker = CoverageTracker()
        for i in [3, 4, 5, 6]:
            tracker.record_write(
                "g", ["g/a"], ts_index=i, time_val=f"2024-01-0{i}T00:00:00Z", aligned=True
            )

        cov = tracker.build_coverage()[0]
        assert cov.time_index_ranges == [[3, 6]]

    def test_non_contiguous_indices_produce_multiple_ranges(self):
        tracker = CoverageTracker()
        for i in [0, 1, 5, 6, 10]:
            tracker.record_write("g", ["g/a"], ts_index=i, time_val=None, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.time_index_ranges == [[0, 1], [5, 6], [10, 10]]

    def test_empty_tracker_produces_empty_list(self):
        tracker = CoverageTracker()
        assert tracker.build_coverage() == []


class TestTimeBounds:
    def test_datetime_objects_converted(self):
        tracker = CoverageTracker()
        dt = datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC)
        tracker.record_write("g", ["g/a"], ts_index=0, time_val=dt, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.time_min == "2024-03-15T12:00:00+00:00"
        assert cov.time_max == "2024-03-15T12:00:00+00:00"

    def test_np_datetime64_converted(self):
        tracker = CoverageTracker()
        ts = np.datetime64("2024-07-20T10:30:00", "ns")
        tracker.record_write("g", ["g/a"], ts_index=0, time_val=ts, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.time_min == "2024-07-20T10:30:00.000000000Z"
        assert cov.time_max == "2024-07-20T10:30:00.000000000Z"

    def test_none_time_val_skips_bounds(self):
        tracker = CoverageTracker()
        tracker.record_write("g", ["g/a"], ts_index=0, time_val=None, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.time_min is None
        assert cov.time_max is None


class TestBuildCoverageSchema:
    def test_state_array_uses_custom_name(self):
        tracker = CoverageTracker()
        tracker.record_write("g", ["g/a"], ts_index=0, time_val=None, aligned=True)

        cov = tracker.build_coverage(state_array_name="custom_state")[0]
        assert cov.state_array == "g/custom_state"

    def test_state_deleted_value_default(self):
        tracker = CoverageTracker()
        tracker.record_write("g", ["g/a"], ts_index=0, time_val=None, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.state_deleted_value == 2

    def test_timestamps_written_property(self):
        tracker = CoverageTracker()
        for i in range(5):
            tracker.record_write("g", ["g/a"], ts_index=i, time_val=None, aligned=True)

        cov = tracker.build_coverage()[0]
        assert cov.timestamps_written == 5

    def test_api_not_reexported(self):
        import firecube.ingestor.api as api_module

        assert not hasattr(api_module, "CoverageTracker")
        assert "CoverageTracker" not in getattr(api_module, "__all__", ())
