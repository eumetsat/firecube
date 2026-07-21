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

from firecube.core.observability.telemetry import IngestionTelemetry


def test_protocol_surface_has_no_legacy_methods():
    assert hasattr(IngestionTelemetry, "emit")
    assert hasattr(IngestionTelemetry, "flush")
    assert hasattr(IngestionTelemetry, "span")
    assert hasattr(IngestionTelemetry, "collect_memory_stats")

    assert not hasattr(IngestionTelemetry, "record_file_processed")
    assert not hasattr(IngestionTelemetry, "record_batch_processed")
    assert not hasattr(IngestionTelemetry, "record_error")
    assert not hasattr(IngestionTelemetry, "update_progress")
