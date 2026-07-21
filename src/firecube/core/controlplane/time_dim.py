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

"""Time-dimension resolution for control-plane span deletion.

The time axis of a cube is identified by name, never by positional
fallback: a silent index-0 guess deletes chunks along the wrong axis when
the cube was written with a custom ``time_dim_name`` (see
``plans/DESIGN.md``, Risks To Avoid).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from firecube.core.controlplane.types import ChunkInfo

log = logging.getLogger(__name__)


def resolve_time_dim_index(dim_names: Sequence[str], expected: str) -> int:
    """Return the index of expected in dim_names, or raise ValueError.

    Replaces the former hard-coded lookup with silent index-0 fallback, which could
    corrupt deletion of chunks if the cube used a different time dimension name.
    """
    try:
        return dim_names.index(expected)
    except ValueError:
        raise ValueError(
            f"Cube dim_names={dim_names!r} does not contain expected time dimension "
            f"'{expected}'. The cube was probably written with a different time_dim_name. "
            "Pass the cube's time dimension explicitly (CLI: --time-dim) to proceed."
        ) from None


def resolve_span_time_dims(
    spans: Sequence[ChunkInfo],
    *,
    store_uri: str,
    storage_config: Any,
    explicit: str | None,
    default: str,
) -> dict[str, str]:
    """Resolve the time dimension name for every span before any deletion.

    Resolution order per span: the ``time_dim_name`` recorded in the span
    spec at write time, else discovery from the span's 1-D timestamp-state
    array ``dimension_names``, else the explicit caller-supplied name, else
    ``default``. An explicit name that contradicts a recorded or discovered
    name raises instead of being trusted — deleting along an
    operator-asserted axis that disagrees with the cube's own metadata is
    how chunks get destroyed on the wrong dimension. Runs as a pre-flight
    pass so a misconfiguration aborts with zero chunks deleted.
    """
    from firecube.core.zarr.validation import read_chunk_grid

    discovery_cache: dict[str, str | None] = {}
    resolved: dict[str, str] = {}
    for span in spans:
        payload = span.record if isinstance(span.record, dict) else {}
        raw_spec = payload.get("span")
        spec: dict[str, Any] = raw_spec if isinstance(raw_spec, dict) else {}

        raw_recorded = spec.get("time_dim_name")
        recorded = str(raw_recorded) if raw_recorded else None

        discovered: str | None = None
        state_path = spec.get("state_array")
        if recorded is None and state_path:
            if state_path in discovery_cache:
                discovered = discovery_cache[state_path]
            else:
                try:
                    dim_names, _, _ = read_chunk_grid(
                        store_uri, state_path, storage_config=storage_config
                    )
                    discovered = str(dim_names[0]) if len(dim_names) == 1 else None
                except Exception as exc:
                    log.debug("Time-dim discovery from state array %s failed: %s", state_path, exc)
                    discovered = None
                discovery_cache[state_path] = discovered

        authoritative = recorded or discovered
        if explicit and authoritative and explicit != authoritative:
            source = (
                "recorded in the span record"
                if recorded
                else f"discovered from the timestamp-state array {state_path!r}"
            )
            raise ValueError(
                f"Span {span.key}: explicit time dimension {explicit!r} contradicts "
                f"{authoritative!r} ({source}). Refusing to delete along a contradicted "
                "axis; drop the explicit --time-dim or fix the span records."
            )
        resolved[span.key] = authoritative or explicit or default
    return resolved
