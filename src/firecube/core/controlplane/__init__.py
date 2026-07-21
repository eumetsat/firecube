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

"""ChunkManager facade.

`firecube.core.controlplane` is the stable public import path for control-plane
operations over product-local `.firecube/` state.
"""

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.metrics import WalMetrics, active_wal_metrics, collect_wal_metrics
from firecube.core.controlplane.repo import describe_control_plane
from firecube.core.controlplane.types import (
    ChunkInfo,
    ClaimInfo,
    DeletionPlan,
    RunInfo,
    SpanCoverage,
    WriteDomain,
    build_span_entry,
)

__all__ = [
    "ChunkInfo",
    "ChunkManager",
    "ClaimInfo",
    "DeletionPlan",
    "RunInfo",
    "SpanCoverage",
    "WalMetrics",
    "WriteDomain",
    "active_wal_metrics",
    "build_span_entry",
    "collect_wal_metrics",
    "describe_control_plane",
]
