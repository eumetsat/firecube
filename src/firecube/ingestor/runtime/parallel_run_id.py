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

"""Per-pod run ID derivation for slot-range parallel ingestion (Phase 3)."""

from __future__ import annotations

from urllib.parse import quote


def derive_pod_run_id(
    base_run_id: str,
    slot_start: int | None,
    slot_end: int | None,
    slot_group: str | None = None,
) -> str:
    """Derive a pod-specific run ID from a base run ID and slot range.

    Returns ``base_run_id`` unchanged when the slot range is absent.
    Returns ``{base_run_id}__slot={slot_start}-{slot_end}`` when a slot range is
    present without a slot group.
    Returns ``{base_run_id}__group={slot_group}__slot={slot_start}-{slot_end}``
    when both a slot group and slot range are present.

    The signature is deterministic: same base + same range always produces the
    same ID, so operators can use ``firecube chunks runs list`` to filter by
    slot signature without re-generating UUIDs.
    """
    if slot_start is None or slot_end is None:
        return base_run_id
    if slot_group is None:
        return f"{base_run_id}__slot={slot_start}-{slot_end}"
    # URL-encode slot_group to make path-safe. safe="" encodes ALL reserved chars
    # including "/" and "=" which would corrupt WAL path structure.
    # run_id stays OPAQUE — no caller parses back.
    encoded = quote(slot_group, safe="")
    return f"{base_run_id}__group={encoded}__slot={slot_start}-{slot_end}"
