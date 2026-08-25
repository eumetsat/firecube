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

"""Engine configuration for Firecube ingestion.

This module defines the configuration for the execution runtime, workspace,
and global write policies. It strictly validates options extracted from
the user input.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, fields
from typing import Any, TypeVar, get_type_hints

from firecube.ingestor.errors import ConfigurationError

T = TypeVar("T", bound="EngineConfig")

SYSTEM_KEYS = {
    "run_id",
    "storage",
    "manifest_run_id",
}

EXPERIMENTAL_OPTION_PREFIX_RE = re.compile(r"^x_[a-z0-9_]+$")


def is_experimental_option_key(key: str) -> bool:
    """Return True if ``key`` is in the reserved ``x_*`` experimental namespace."""
    return bool(EXPERIMENTAL_OPTION_PREFIX_RE.match(key))


_SLOT_GROUP_SAFE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def config_keys(cls: type) -> set[str]:
    """Return the set of known field names for a dataclass."""
    return {f.name for f in fields(cls)}


@dataclass
class EngineConfig:
    """Configuration for the Firecube Engine (Runtime & Workspace).

    These options control HOW the ingestion runs, not WHAT product logic is applied.

    Attributes:
        pipeline_parallel: Enable parallel batch preprocessing.
        pipeline_workers: Number of pipeline worker threads.
        pipeline_batch_size: Number of source items per batch.
        cleanup_workspace: Delete temporary workspace files after the run.
        workspace: Optional workspace directory override.
        include_patterns: Optional file patterns for source discovery.
        write_mode: Write strategy, either ``"staged"`` or ``"direct"``.
        resume_existing: Continue a compatible incomplete or overlapping run.
        force_reingest: Re-process existing spans intentionally.
        incremental: Reserved incremental-mode switch.
        dry_run: Build and validate the run without committing writes.
        duckdb_persist_batches: Persist intermediate DuckDB batch data.
        upload_workers: Number of staged upload workers.
        no_progress: Disable progress logging.
        validate_zarr: Validate Zarr state as part of resume checks.
        validate_zarr_group: Zarr group to validate when validation is enabled.
        validate_zarr_timeout_s: Optional Zarr validation timeout in seconds.
        validate_zarr_max_chunks: Optional validation chunk-scan limit.
        validate_zarr_on_timeout: Timeout behavior, usually ``"warn"``.
        skip_preflight: Skip storage preflight checks.
        slot_start: First slot index for orchestrated parallel ingestion.
        slot_end: One-past-last slot index for orchestrated parallel ingestion.
        slot_size: Slot width used when deriving slot ranges from the environment.
        slot_group: Zarr group owned by this worker in multi-group slot runs.
        suppress_static_emission_for_non_owner: Skip static writes in slot-range
            workers whose ``slot_start`` does not match ``static_owner_slot_start``.
        static_owner_slot_start: V1 scalar static-owner slot start for one group
            per run; required when static suppression is enabled.
    """

    # Execution (Pipeline)
    pipeline_parallel: bool = False
    pipeline_workers: int = 1
    pipeline_batch_size: int = 10

    # Workspace & Resources
    cleanup_workspace: bool = False
    workspace: str | None = None

    # Discovery (common for file-based ingestors/templates)
    include_patterns: list[str] | None = None

    # Write Strategy
    write_mode: str = "staged"  # staged | direct
    resume_existing: bool = False
    force_reingest: bool = False
    incremental: bool = False
    dry_run: bool = False

    # Extension Strategy (Temporary Stabilization)
    duckdb_persist_batches: bool = False

    # Storage upload
    upload_workers: int = 4

    # Progress control
    no_progress: bool = False

    # Resume validation
    validate_zarr: bool = False
    validate_zarr_group: str = ""
    validate_zarr_timeout_s: int | None = None
    validate_zarr_max_chunks: int | None = None
    validate_zarr_on_timeout: str = "warn"

    # Storage preflight
    skip_preflight: bool = False

    # Slot-range parallelism (Phase 3)
    slot_start: int | None = None
    slot_end: int | None = None
    slot_size: int | None = None
    slot_group: str | None = None
    suppress_static_emission_for_non_owner: bool = False
    static_owner_slot_start: int | None = None

    def __post_init__(self) -> None:
        if self.suppress_static_emission_for_non_owner and self.static_owner_slot_start is None:
            raise ConfigurationError(
                "suppress_static_emission_for_non_owner=True requires static_owner_slot_start"
            )
        if self.suppress_static_emission_for_non_owner and self.slot_start is None:
            raise ConfigurationError(
                "suppress_static_emission_for_non_owner=True requires slot_start to be set "
                "(serial runs would silently skip all static writes without a slot boundary)"
            )

        if self.slot_group is not None and not self.slot_group.strip():
            raise ValueError(
                "slot_group must be a non-empty string when set; got empty or whitespace string."
            )
        if self.slot_group is not None and not _SLOT_GROUP_SAFE.match(self.slot_group):
            warnings.warn(
                f"slot_group={self.slot_group!r} contains characters outside [A-Za-z0-9_.-]; "
                f"will be URL-encoded in run_id and WAL paths. This works correctly but may "
                f"make run IDs less human-readable. Consider sanitizing your zarr_schema() "
                f"group names if you control them.",
                stacklevel=2,
            )

        if self.slot_start is None and self.slot_end is None:
            return

        if self.slot_start is None or self.slot_end is None:
            raise ValueError("--slot-start and --slot-end must be provided together")

        if self.slot_start < 0:
            raise ValueError(f"slot_start must be non-negative, got {self.slot_start}")
        if self.slot_end < 0:
            raise ValueError(f"slot_end must be non-negative, got {self.slot_end}")
        if self.slot_start >= self.slot_end:
            raise ValueError(
                f"slot_start must be < slot_end, got [{self.slot_start}, {self.slot_end})"
            )

    @classmethod
    def from_options(cls: type[T], options: dict[str, Any]) -> T:
        """Create config from options, strictly rejecting unknown keys."""
        from firecube.ingestor.config.coercion import coerce_cli_value

        known = config_keys(cls)
        unknown = set(options.keys()) - known

        if unknown:
            raise ValueError(
                f"Unknown Engine options: {', '.join(sorted(unknown))}. "
                f"Valid keys: {', '.join(sorted(known))}"
            )

        type_hints = get_type_hints(cls)
        init_kwargs = {}
        for key, value in options.items():
            target_type = type_hints.get(key)
            if key == "include_patterns":
                if value is None:
                    init_kwargs[key] = None
                elif isinstance(value, str):
                    init_kwargs[key] = [value]
                elif isinstance(value, (list, tuple, set)):
                    init_kwargs[key] = [str(v) for v in value]
                else:
                    raise ValueError(f"Invalid include_patterns value: {value!r}")
            else:
                init_kwargs[key] = coerce_cli_value(value, target_type, key)
        return cls(**init_kwargs)
