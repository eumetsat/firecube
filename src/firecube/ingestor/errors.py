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

from typing import Literal

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


class SeedingFailedError(FirecubeError):
    """Raised when data-chunk seeding fails; workspace has been deleted."""


class WriteIntentRangeError(IngestorError):
    """Raised when a WriteIntent's ts_index falls outside the assigned slot range.

    This is a correctness violation — the plugin filter is advisory; this error
    is the mandatory backstop. NEVER silently drop out-of-range intents.
    """


class AppendOverwriteRefused(ResumeConflictError):
    """Raised when incoming timestamps cannot be reconciled for region overwrite.

    Args:
        refused_timestamps: Timestamp values that triggered the refusal.
        reason: Classification of why the overwrite was refused.
        array_path: ``<group>/<state_var_name>`` of the timestamp-state array
            the classifier could not read. Required for
            ``reason="state_array_missing"``; ignored otherwise.
        existing_max: Stored append boundary, when known, for an insertion refusal.
    """

    def __init__(
        self,
        refused_timestamps: list[str],
        reason: Literal[
            "insert",
            "duplicates_incoming",
            "duplicates_existing",
            "nat_incoming",
            "nat_existing",
            "non_contiguous",
            "time_coord_mismatch",
            "unsorted_existing_coord",
            "unsorted_incoming",
            "state_array_missing",
        ],
        *,
        array_path: str | None = None,
        existing_max: str | None = None,
    ) -> None:
        n = len(refused_timestamps)
        if reason == "insert":
            boundary = f" ({existing_max})" if existing_max is not None else ""
            hint = (
                f"Append values must follow the stored maximum{boundary}. Resume and "
                "force-reingest cannot insert absent earlier timestamps. Correct the input "
                "order, or rebuild a new target from all required inputs in chronological order."
            )
        elif reason == "unsorted_existing_coord":
            hint = (
                "Existing append coordinate must be unique and strictly increasing. "
                "Run `firecube zarr validate <target>` on the target product URI "
                "to diagnose the coordinate order before retrying."
            )
        elif reason == "unsorted_incoming":
            hint = (
                "The incoming batch's append coordinate is not monotonically "
                "non-decreasing; the append path never sorts silently. Sort the "
                "batch in build_dataset (or set pipeline_batch_size so batches do "
                "not straddle unordered inputs) before retrying."
            )
        elif reason == "state_array_missing":
            if array_path is None:
                raise ValueError("array_path is required for reason='state_array_missing'")
            hint = (
                f"Timestamp-state array {array_path!r} does not exist in the target "
                "store, so incoming timestamps cannot be classified against it. "
                "Rerun with `--option resume_existing=true` so the existing group is "
                "upgraded with the state array before classification."
            )
        else:
            hint = (
                "Use force_reingest=True only on stores with no duplicate/NaT "
                "timestamps and contiguous overlap."
            )
        super().__init__(
            f"Cannot overwrite {n} timestamp(s) [{', '.join(refused_timestamps[:3])}{'...' if n > 3 else ''}]: {reason}. "
            f"{hint}"
        )
        self.refused_timestamps = refused_timestamps
        self.reason = reason
        self.array_path = array_path
        self.existing_max = existing_max


class InsertRefusedError(AppendOverwriteRefused):
    """Raised when an append would place new timestamps before its permitted boundary."""


class DuplicateExistingTimestampsError(AppendOverwriteRefused):
    """Raised when the existing store contains duplicate timestamps that must be repaired before overwrite."""


class IntegrityGuardError(SchemaDriftError):
    """Raised when the post-write integrity guard detects state corruption in seeded chunks.

    The staged-write integrity guard verifies, after a batch write and before
    promotion of the workspace to the final target, that every slot which was
    ``firecube_timestamp_state == 1`` in the final target inside the batch's
    ``touched_chunks`` set remains ``state == 1`` in the workspace. If any
    such slot has changed, the workspace is deleted and this error is raised
    so the run fails loudly instead of promoting a corrupted state array.
    """


class SchemaDriftReingestError(IngestorError):
    """Raised when re-ingesting into a store whose schema has changed incompatibly."""

    def __init__(
        self,
        *,
        store_uri: str,
        dataset_variable: str,
        reason: Literal["extra_incoming_variable", "batch_missing_store_variable"],
    ) -> None:
        super().__init__(
            f"Cannot write variable {dataset_variable!r} into {store_uri!r}: {reason}. "
            "The incoming time-aligned arrays must match the existing store schema."
        )
        self.store_uri = store_uri
        self.dataset_variable = dataset_variable
        self.reason = reason


STATIC_VAR_DRIFT_MSG_TMPL = (
    "Static (non-time-indexed) variable {name!r} value drift: stored differs from incoming. "
    "Static variables must not change across appends. "
    "If this variable legitimately varies per file, add {time_dim!r} to its dims in read_dataset. "
    "Store: {store_uri!s}"
)

STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL = (
    "force_reingest with different static value for {name!r} would overwrite non-time-indexed data. "
    "Create a new store instead. "
    "Store: {store_uri!s}"
)

STATIC_VAR_NEW_ON_APPEND_TMPL = (
    "Static (non-time-indexed) variable {name!r} appeared on append but is not present in the existing store. "
    "Adding a new static variable to an existing store is a schema change and is refused. "
    "Either remove {name!r} from build_dataset(), or create a new store. "
    "Store: {store_uri!s}"
)


__all__ = [
    "STATIC_VAR_DRIFT_FORCE_REINGEST_TMPL",
    "STATIC_VAR_DRIFT_MSG_TMPL",
    "STATIC_VAR_NEW_ON_APPEND_TMPL",
    "AppendOverwriteRefused",
    "ConfigurationError",
    "DuplicateExistingTimestampsError",
    "IngestorError",
    "InsertRefusedError",
    "IntegrityGuardError",
    "ManifestError",
    "RangeOverlapError",
    "ResumeConflictError",
    "SchemaDriftError",
    "SchemaDriftReingestError",
    "SchemaSizeMismatchError",
    "SeedingFailedError",
    "StagedMetadataError",
    "StorageError",
    "UnboundedAxisError",
    "WriteIntentRangeError",
]
