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

from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id


def test_single_pod_returns_base_unchanged():
    assert derive_pod_run_id("run-abc", None, None) == "run-abc"


def test_single_pod_partial_none_returns_base_unchanged():
    assert derive_pod_run_id("run-abc", 0, None) == "run-abc"
    assert derive_pod_run_id("run-abc", None, 100) == "run-abc"


def test_parallel_adds_slot_signature():
    assert derive_pod_run_id("run-abc", 100, 200) == "run-abc__slot=100-200"


def test_deterministic_for_same_inputs():
    a = derive_pod_run_id("run-abc", 0, 100)
    b = derive_pod_run_id("run-abc", 0, 100)
    assert a == b


def test_different_ranges_produce_different_ids():
    assert derive_pod_run_id("base", 0, 100) != derive_pod_run_id("base", 100, 200)


def test_zero_start_is_distinct_from_none():
    assert derive_pod_run_id("base", 0, 100) == "base__slot=0-100"
    assert derive_pod_run_id("base", None, None) == "base"


def test_slot_group_none_preserves_phase3_format():
    assert derive_pod_run_id("run", 0, 100, None) == "run__slot=0-100"


def test_slot_group_set_includes_group():
    assert derive_pod_run_id("run", 0, 100, "group_a") == "run__group=group_a__slot=0-100"


def test_slot_group_set_no_range_returns_base():
    assert derive_pod_run_id("run", None, None, "group_a") == "run"


def test_different_groups_same_range_distinct_run_ids():
    assert derive_pod_run_id("run", 0, 100, "A") != derive_pod_run_id("run", 0, 100, "B")
