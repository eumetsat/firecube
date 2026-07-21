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

"""Small repository projection helpers."""

from __future__ import annotations

import re

_POD_SLOT_RE = re.compile(r"__slot=(?P<start>\d+)-(?P<end>\d+)$")
_POD_GROUP_RE = re.compile(r"__group=(?P<group>.+?)__slot=\d+-\d+$")


def deserialize_slot_range(value: object) -> tuple[int, int] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return (int(value[0]), int(value[1]))
    return None


def deserialize_slot_group(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return None


def parse_pod_run_id_slot(
    run_id: str,
) -> tuple[tuple[int, int] | None, str | None]:
    """Recover ``(slot_range, slot_group)`` from a deterministic pod run id.

    Inverse of ``derive_pod_run_id``. Exists ONLY for read-side recovery
    when structured ``run.json`` metadata is missing or torn-read. Writers
    MUST NOT use this as the source of truth — always propagate
    ``slot_range`` / ``slot_group`` explicitly through the control-plane
    write APIs.

    Returns ``(None, None)`` for any run id that does not carry the
    canonical ``__group=NAME__slot=START-END`` (or ``__slot=START-END``)
    suffix, or whose suffix is malformed. Never raises.
    """
    try:
        if "__group=" in run_id:
            group_match = _POD_GROUP_RE.search(run_id)
            if group_match is None:
                return (None, None)
            group: str | None = group_match.group("group")
        else:
            group = None

        slot_match = _POD_SLOT_RE.search(run_id)
        if slot_match is None:
            return (None, None)

        start = int(slot_match.group("start"))
        end = int(slot_match.group("end"))
        return ((start, end), group)
    except Exception:
        return (None, None)
