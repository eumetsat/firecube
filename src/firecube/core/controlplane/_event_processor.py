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

"""Event-to-record conversion and state projection for the control plane.

This module contains pure transformation functions that convert WAL events
into projected records (the "current state" view of chunks).

Key concepts:
  - Events are raw WAL entries with event_type, record, timestamp, meta
  - Records are projected state: the authoritative view of a chunk's existence
  - Upsert-by-key: each record has a unique key; later events overwrite earlier ones
  - Deterministic ordering: runs are sorted by (completed_at, run_id) ascending,
    ensuring the last-written record for a key always comes from the latest run

Why sequential application is required after parallel reads:
  - WAL segments can be read in parallel (IO-bound, thread-safe)
  - But projection (applying events to state dict) MUST be sequential because
    the upsert semantics depend on application order. If run B completed after
    run A, run B's records must overwrite run A's. Sorting + sequential apply
    guarantees this invariant.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from firecube.core.controlplane.metrics import record_wal_corruption
from firecube.core.controlplane.types import (
    EVENT_REPLACEMENT_COMMITTED,
    EVENT_RUN_STARTED_WITH_REPLACEMENT,
    SCHEMA_VERSION,
    ChunkInfo,
)
from firecube.core.errors import ControlPlaneCorruptionError
from firecube.core.storage.uri import StorageUri


def record_from_event(event: dict[str, Any], log: logging.Logger) -> dict[str, Any]:
    """Convert a raw WAL event into a projected record dict.

    Validates schema version and fills defaults for timestamp.
    Raises ControlPlaneCorruptionError if schema version is unsupported.
    """
    record = dict(event.get("record") or {})
    record.setdefault("schema_version", SCHEMA_VERSION)
    record.setdefault("timestamp", float(event.get("timestamp", time.time()) or time.time()))
    if record.get("schema_version") != SCHEMA_VERSION:
        record_wal_corruption()
        msg = f"Unsupported record schema version in event {event.get('event_id')}: {record.get('schema_version')}"
        log.error(msg)
        raise ControlPlaneCorruptionError(msg)
    return record


def record_to_chunk_info(
    product: str, record: dict[str, Any], control_uri: StorageUri
) -> ChunkInfo:
    """Convert a projected record dict into a ChunkInfo dataclass instance."""
    return ChunkInfo(
        key=str(record.get("key", "")),
        product=product,
        chunk_type=str(record.get("type", "unknown")),
        size=int(record.get("size", 0) or 0),
        timestamp=float(record.get("timestamp", 0.0) or 0.0),
        manifest_path=control_uri.to_str(),
        status=record.get("status"),
        replaces=record.get("replaces"),
        replaced_at=record.get("replaced_at"),
        meta=record.get("meta"),
        record=record,
    )


def apply_events(
    current: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    log: logging.Logger,
) -> None:
    """Apply a list of events to the current state dict via upsert-by-key.

    Each event is converted to a record, and the record's key determines
    placement in the state dict. Later events overwrite earlier ones for
    the same key — this is the core projection invariant.
    """
    for event in events:
        event_type = str(event.get("event_type", "") or "")
        record = record_from_event(event, log)
        if event_type == EVENT_RUN_STARTED_WITH_REPLACEMENT:
            continue
        if event_type == EVENT_REPLACEMENT_COMMITTED:
            replacing_run_id = str(record.get("replacing_run_id", "") or "")
            replaced_at = float(record.get("timestamp", time.time()) or time.time())
            for raw_key in record.get("replaced_span_keys") or []:
                key = str(raw_key or "")
                if not key or key not in current:
                    continue
                updated = dict(current[key])
                updated["status"] = "replaced"
                updated["replaced_by"] = replacing_run_id
                updated["replaced_at"] = replaced_at
                current[key] = updated
            continue
        key = str(record.get("key", "") or "")
        if key:
            current[key] = record


def sorted_complete_runs(run_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort completed runs by (completed_at, run_id) for deterministic projection.

    Only runs with status='complete' are included. The sort guarantees:
    - Earlier completions are processed first
    - run_id breaks ties (alphabetical) for determinism
    - When applied sequentially, the LAST run's records win on key conflict
    """
    return sorted(
        [entry for entry in run_entries if str(entry.get("status", "")) == "complete"],
        key=lambda item: (
            float(item.get("completed_at", 0.0) or 0.0),
            str(item.get("run_id", "")),
        ),
    )
