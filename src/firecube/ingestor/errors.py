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

"""Exceptions for Firecube Ingestor."""

from firecube.core.errors import (
    ConfigurationError,
    FirecubeError,
    ManifestError,
    SchemaDriftError,
    StorageError,
)


class IngestorError(FirecubeError):
    """Base exception for all ingestor-layer errors."""


class UnboundedAxisError(ConfigurationError):
    """Raised when a regular axis has no fixed extent but one is required.

    Set ``RegularTimeAxis(end_date=...)`` or ``slot_count=...`` in the
    declared ``IndexSpec`` to give the axis a fixed extent.

    Args:
        group: Name of the index group whose axis lacks a fixed extent.
    """

    def __init__(self, group: str) -> None:
        super().__init__(
            f"group {group!r}: axis has no fixed extent — set "
            "RegularTimeAxis(end_date=...) or slot_count=... "
            "to enable parallel ingestion"
        )
        self.group = group


class SchemaSizeMismatchError(IngestorError):
    """Raised when an existing Zarr array's shape is smaller than the global expected size.

    Existing arrays mismatch the plan. Either delete them or update the plan to match.
    """


class ResumeConflictError(IngestorError):
    """Existing data conflicts with this run's resume/overwrite settings.

    Raised when previously ingested entries for the product are detected but
    the run was started without ``resume_existing`` or ``force_reingest``,
    or lacks the slice options needed to match existing entries safely.
    """


class RangeOverlapError(ResumeConflictError):
    """Raised when a new slot-range invocation overlaps with an active non-terminal run.

    Overlapping ranges risk Zarr chunk-boundary corruption.
    Abandon the conflicting run first: firecube chunks runs abandon ...
    """


class StagedMetadataError(IngestorError):
    """Staged metadata seeding failed for an existing staged-write target.

    Raised by ``seed_staged_store_metadata`` (strict mode) when zarr.json
    metadata cannot be copied from the final target into the temp store.
    """


class WriteIntentRangeError(IngestorError):
    """Raised when a WriteIntent's ts_index falls outside the assigned slot range.

    This is a correctness violation — the plugin filter is advisory; this error
    is the mandatory backstop. NEVER silently drop out-of-range intents.
    """


__all__ = [
    "ConfigurationError",
    "IngestorError",
    "ManifestError",
    "RangeOverlapError",
    "ResumeConflictError",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "StagedMetadataError",
    "StorageError",
    "UnboundedAxisError",
    "WriteIntentRangeError",
]
