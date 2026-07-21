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

"""Engine-internal parallel execution state. Plugins MUST NOT import from this module."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _ParallelExecutionState:
    """State shared across parallel execution phases within one ingestor instance."""

    global_expected: dict[str, int] = field(default_factory=dict)
    schema_verified: dict[str, bool] = field(default_factory=dict)
    slot_index_model_resolved: bool = False
