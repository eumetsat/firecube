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

"""Tests for engine-internal parallel execution state."""

from firecube.ingestor.runtime.parallel_execution_state import _ParallelExecutionState


def test_dataclass_defaults() -> None:
    state = _ParallelExecutionState()

    assert state.global_expected == {}
    assert state.schema_verified == {}


def test_dataclass_construction_with_global_expected() -> None:
    state = _ParallelExecutionState(global_expected={"a": 100})

    assert state.global_expected == {"a": 100}
    assert state.schema_verified == {}


def test_dataclass_is_not_in_api() -> None:
    import firecube.ingestor.api as api

    assert not hasattr(api, "_ParallelExecutionState")


def test_schema_verified_field_independent_from_global_expected() -> None:
    state = _ParallelExecutionState()

    state.global_expected["a"] = 100
    state.schema_verified["a"] = True

    assert state.global_expected == {"a": 100}
    assert state.schema_verified == {"a": True}

    state.schema_verified["b"] = False
    state.global_expected["c"] = 300

    assert state.global_expected == {"a": 100, "c": 300}
    assert state.schema_verified == {"a": True, "b": False}
