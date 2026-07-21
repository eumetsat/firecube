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

import dataclasses
import json

from firecube.core.controlplane.types import SpanCoverage, build_span_entry


def test_span_coverage_no_time_index_ranges():
    cov = SpanCoverage(group="g", arrays=["a"])
    assert cov.time_index_ranges is None
    assert cov.timestamps_written == 0
    assert cov.region_spec is None
    assert cov.write_strategy is None


def test_span_coverage_append_style():
    cov = SpanCoverage(
        group="data_1km",
        arrays=["counts", "flags"],
        time_index_ranges=[[0, 4], [10, 12]],
        time_min="2024-01-01T00:00:00",
        time_max="2024-01-02T00:00:00",
    )
    assert cov.time_index_ranges == [[0, 4], [10, 12]]
    assert cov.timestamps_written == 8  # (4-0+1) + (12-10+1)
    assert cov.region_spec is None
    assert cov.write_strategy is None


def test_span_coverage_region_style():
    cov = SpanCoverage(
        group="data_1km",
        arrays=["counts"],
        region_spec={"slots": [100, 101]},
        write_strategy="indexed_region",
    )
    assert cov.time_index_ranges is None
    assert cov.timestamps_written == 0
    assert cov.region_spec == {"slots": [100, 101]}
    assert cov.write_strategy == "indexed_region"


def test_span_coverage_serializes_safely():
    cov = SpanCoverage(
        group="data_1km",
        arrays=["counts"],
        region_spec={"slots": [100, 101], "dimension": "time"},
        write_strategy="indexed_region",
    )
    d = dataclasses.asdict(cov)
    serialized = json.dumps(d)
    roundtrip = json.loads(serialized)
    assert roundtrip["group"] == "data_1km"
    assert roundtrip["region_spec"] == {"slots": [100, 101], "dimension": "time"}
    assert roundtrip["write_strategy"] == "indexed_region"
    assert roundtrip["time_index_ranges"] is None


def test_build_span_entry_no_time_index_ranges():
    entry = build_span_entry(
        run_id="run-001",
        batch_id="b001",
        group="data_1km",
        meta={},
        arrays=["counts"],
        time_index_ranges=None,
    )
    assert entry["span"]["time_index_ranges"] == []
    assert entry["span"]["timestamps_written"] == 0
    assert entry["schema_version"] == "v2"


def test_build_span_entry_with_region_spec():
    entry = build_span_entry(
        run_id="run-001",
        batch_id="b001",
        group="data_1km",
        meta={},
        arrays=["counts"],
        region_spec={"slots": [5, 6]},
        write_strategy="indexed_region",
    )
    assert entry["span"]["region_spec"] == {"slots": [5, 6]}
    assert entry["span"]["write_strategy"] == "indexed_region"
    assert entry["span"]["time_index_ranges"] == []


def test_build_span_entry_append_unchanged():
    entry = build_span_entry(
        run_id="run-001",
        batch_id="b001",
        group="F024",
        meta={"plugin": "test"},
        arrays=["F024/FWI"],
        time_index_ranges=[[0, 1]],
    )
    assert entry["span"]["time_index_ranges"] == [[0, 1]]
    assert entry["span"]["timestamps_written"] == 2
    assert "region_spec" not in entry["span"]
    assert "write_strategy" not in entry["span"]
