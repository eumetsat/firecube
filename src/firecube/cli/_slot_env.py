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

"""K8s/env slot-range discovery with explicit precedence rules.

Precedence (highest to lowest):
1. CLI --slot-start/--slot-end (already parsed; passed as cli_slot_start/cli_slot_end)
2. FIRECUBE_SLOT_START + FIRECUBE_SLOT_END env vars (both required)
3. JOB_COMPLETION_INDEX + (--slot-size OR FIRECUBE_SLOT_SIZE) -> slot_start = index * size
4. None (single-pod mode)

Slot group precedence is independent:
1. CLI --slot-group
2. FIRECUBE_SLOT_GROUP
3. None
"""

from __future__ import annotations

import logging
import os

import click

log = logging.getLogger(__name__)


def _parse_int(value: str, *, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise click.UsageError(f"{name} must be an integer, got {value!r}") from exc


def resolve_slot_range_from_env(
    cli_slot_start: int | None,
    cli_slot_end: int | None,
    cli_slot_size: int | None,
    cli_slot_group: str | None = None,
) -> tuple[int | None, int | None, str | None]:
    """Resolve final (slot_start, slot_end, slot_group) from CLI args + environment.

    An empty or whitespace-only ``FIRECUBE_SLOT_GROUP`` is treated as unset,
    matching the standard shell convention where ``VAR=`` is indistinguishable
    from an omitted variable at the operator's intent level. Without this,
    a K8s manifest that conditionally injects ``FIRECUBE_SLOT_GROUP: ""`` for
    a single-group deployment would look like a request to scope the window
    to a group named ``""`` and fail loudly at preallocate time.
    """
    slot_group = cli_slot_group
    env_slot_group = os.getenv("FIRECUBE_SLOT_GROUP")
    if slot_group is None and env_slot_group is not None and env_slot_group.strip():
        slot_group = env_slot_group

    if cli_slot_start is not None and cli_slot_end is not None:
        return cli_slot_start, cli_slot_end, slot_group

    env_slot_start = os.getenv("FIRECUBE_SLOT_START")
    env_slot_end = os.getenv("FIRECUBE_SLOT_END")
    if env_slot_start is not None and env_slot_end is not None:
        start = _parse_int(env_slot_start, name="FIRECUBE_SLOT_START")
        end = _parse_int(env_slot_end, name="FIRECUBE_SLOT_END")
        log.info(
            "slot_range resolved from env vars FIRECUBE_SLOT_START/END: [%s, %s)",
            start,
            end,
        )
        return start, end, slot_group

    job_completion_index = os.getenv("JOB_COMPLETION_INDEX")
    if job_completion_index is not None:
        slot_size = cli_slot_size
        if slot_size is None:
            env_slot_size = os.getenv("FIRECUBE_SLOT_SIZE")
            slot_size = (
                _parse_int(env_slot_size, name="FIRECUBE_SLOT_SIZE")
                if env_slot_size is not None
                else None
            )

        if slot_size is None:
            log.warning(
                "JOB_COMPLETION_INDEX detected but no slot size; parallel mode NOT activated"
            )
            return None, None, slot_group

        index = _parse_int(job_completion_index, name="JOB_COMPLETION_INDEX")
        start = index * slot_size
        end = (index + 1) * slot_size
        log.info(
            "slot_range resolved from JOB_COMPLETION_INDEX=%s * slot_size=%s: [%s, %s)",
            index,
            slot_size,
            start,
            end,
        )
        return start, end, slot_group

    return None, None, slot_group
