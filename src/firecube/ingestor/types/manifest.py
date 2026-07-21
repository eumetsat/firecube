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

"""Strict schema for ingestion result manifests.

This module defines the contract for the final JSON output of the CLI,
ensuring downstream consumers (e.g. Airflow) have a stable API.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass
class IngestManifest:
    """The public contract for ingestion results."""

    # Core Identity
    plugin: str
    output_format: str

    # Storage Results
    stored_at: str
    files: int
    bytes: int
    duration_s: float

    # Schema Version (Must be after non-default fields)
    schema_version: Literal["v1"] = "v1"

    # Detailed Metrics (Pipeline Stats)
    metrics: dict[str, Any] = field(default_factory=dict)

    # Extended Metadata (Optional but recommended)
    run_id: str | None = None
    product: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}
