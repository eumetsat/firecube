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

import functools
import json
import re
from dataclasses import dataclass, fields
from typing import Any, TypeVar, get_type_hints

from firecube.core.discovery import (
    StacSearchError,
    reject_legacy_input_patterns,
    validate_input_filters,
    validate_stac_search,
)
from firecube.core.errors import ConfigurationError

T = TypeVar("T", bound="EngineConfig")

_UNSET: object = object()

SYSTEM_KEYS = {
    "run_id",
    "storage",
    "manifest_run_id",
}

EXPERIMENTAL_OPTION_PREFIX_RE = re.compile(r"^x_[a-z0-9_]+$")


def is_experimental_option_key(key: str) -> bool:
    """Return True if ``key`` is in the reserved ``x_*`` experimental namespace."""
    return bool(EXPERIMENTAL_OPTION_PREFIX_RE.match(key))


def config_keys(cls: type) -> set[str]:
    """Return the set of known field names for a dataclass."""
    return {f.name for f in fields(cls)}


def _parse_stac_search(value: Any) -> dict[str, Any] | None:
    """Accept a mapping, or a JSON object as text (a free-form override)."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StacSearchError() from exc
    return validate_stac_search(value)


def _refuse_write_mode[C](cls: type[C]) -> type[C]:
    """Tombstone: make the constructor refuse `write_mode`, which `ExecutionParams` now holds.

    A wrapper, not an `InitVar`: an `InitVar` default stays readable as `EngineConfig().write_mode`.
    """
    init = cls.__init__

    @functools.wraps(init)
    def __init__(self: C, *args: Any, write_mode: object = _UNSET, **kwargs: Any) -> None:
        if write_mode is not _UNSET:
            raise ConfigurationError(
                "EngineConfig takes no write_mode: write mode is now a field of "
                "ExecutionParams, so pass ExecutionParams(write_mode=...) instead."
            )
        init(self, *args, **kwargs)

    cls.__init__ = __init__  # type: ignore[method-assign]
    return cls


@_refuse_write_mode
@dataclass
class EngineConfig:
    """Configuration for the Firecube Engine (Runtime & Workspace).

    These options control HOW the ingestion runs, not WHAT product logic is applied.

    Attributes:
        pipeline_workers: Number of pipeline worker threads. A value of 2 or
            more selects parallel pipeline execution; 1 (the default) runs
            batches sequentially. Must be at least 1.
        pipeline_batch_size: Number of source items per batch. Must be at
            least 1.
        extract_workers: Number of parallel archive-extraction workers used
            by plugins that extract a batch of source archives (for example
            ZIPs) to disk before decoding. Extraction is disk-bound and
            claims no slots, so this is independent of ``pipeline_workers``;
            when both are above 1 the two multiply. Must be at least 1.
        cleanup_workspace: Delete temporary workspace files after the run.
        workspace: Optional workspace directory override.
        input_filters: Optional list of input-file filters. Positive entries
            add matches to built-in discovery; ``!`` entries exclude matches
            regardless of order. Matching is case-sensitive; default suffix
            recognition remains case-insensitive. Supply one JSON list through
            ``--input-filters`` or a list in plugin configuration. An explicit
            CLI list replaces configured filters; ``[]`` restores built-in
            selection. Custom discovery hooks must apply this field themselves.
            See ``discover_input_files`` for matching and escaping rules. On a
            ``stac://`` source, filters narrow the selected assets on the basename
            of their href instead.
        stac_search: Optional STAC item search filter applied to every
            ``stac://`` source of the run: a mapping with any of the keys
            ``datetime``, ``bbox``, ``intersects`` and ``ids``. The collection
            always comes from the source URI. Supply one JSON object through
            ``--stac-search`` or a table in plugin configuration.
        resume_existing: Continue a compatible incomplete or overlapping run.
        force_reingest: Re-process existing spans intentionally. Overlapping
            timestamps are overwritten in-place (region overwrite); new tail
            timestamps are appended. No separate delete step is required.
            Refuses inserts, duplicates, and NaT values. Use
            ``chunks delete`` first when physical chunk removal is needed.
        incremental: Reserved incremental-mode switch.
        duckdb_persist_batches: Persist intermediate DuckDB batch data.
        upload_workers: Number of staged upload workers.
        no_progress: Disable progress logging.
        allow_empty_source: When ``True``, treat an empty source list as a
            successful no-op rather than raising an error. Defaults to
            ``False`` (empty source is an error). Opt-in via
            ``--option allow_empty_source=true``.
        validate_zarr: Validate Zarr state as part of resume checks.
        validate_zarr_group: Zarr group to validate when validation is enabled.
        validate_zarr_timeout_s: Optional Zarr validation timeout in seconds.
        validate_zarr_max_chunks: Optional validation chunk-scan limit.
        validate_zarr_on_timeout: Timeout behavior, usually ``"warn"``.
        skip_preflight: Skip storage preflight checks.

    The write mode, the slot window and the dry run are not options: `ExecutionParams` holds them.
    """

    # Execution (Pipeline)
    pipeline_workers: int = 1
    pipeline_batch_size: int = 10
    extract_workers: int = 4

    # Workspace & Resources
    cleanup_workspace: bool = False
    workspace: str | None = None

    # Discovery (common for file-based ingestors/templates)
    input_filters: list[str] | None = None
    stac_search: dict[str, Any] | None = None

    # Write Strategy
    resume_existing: bool = False
    force_reingest: bool = False
    incremental: bool = False

    # Extension Strategy (Temporary Stabilization)
    duckdb_persist_batches: bool = False

    # Storage upload
    upload_workers: int = 4

    # Progress control
    no_progress: bool = False

    # Source validation
    allow_empty_source: bool = False

    # Resume validation
    validate_zarr: bool = False
    validate_zarr_group: str = ""
    validate_zarr_timeout_s: int | None = None
    validate_zarr_max_chunks: int | None = None
    validate_zarr_on_timeout: str = "warn"

    # Storage preflight
    skip_preflight: bool = False

    def __post_init__(self) -> None:
        self.input_filters = validate_input_filters(self.input_filters)
        if self.stac_search is not None:
            self.stac_search = validate_stac_search(self.stac_search)
        if self.pipeline_workers < 1:
            raise ValueError(f"pipeline_workers must be >= 1, got {self.pipeline_workers}")
        if self.pipeline_batch_size < 1:
            raise ValueError(f"pipeline_batch_size must be >= 1, got {self.pipeline_batch_size}")
        if self.extract_workers < 1:
            raise ValueError(f"extract_workers must be >= 1, got {self.extract_workers}")

    @classmethod
    def from_options(cls: type[T], options: dict[str, Any]) -> T:
        """Create config from options, strictly rejecting unknown keys."""
        from firecube.ingestor.config.coercion import coerce_cli_value

        reject_legacy_input_patterns(options)
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
            if key == "input_filters":
                init_kwargs[key] = validate_input_filters(value)
            elif key == "stac_search":
                init_kwargs[key] = _parse_stac_search(value)
            else:
                init_kwargs[key] = coerce_cli_value(value, target_type, key)
        return cls(**init_kwargs)
