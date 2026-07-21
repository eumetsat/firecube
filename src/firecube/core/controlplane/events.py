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

"""Immutable run event log writer for ChunkManager v2."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from firecube.core.controlplane.types import (
    DEFAULT_RUN_STALE_THRESHOLD_S,
    SCHEMA_FILENAME,
    SCHEMA_VERSION,
    ChunkEvent,
)
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.storage.uri import StorageUri

_log = logging.getLogger(__name__)

DEFAULT_EVENT_SEGMENT_SIZE = 25
DEFAULT_RUN_META_HEARTBEAT_SECONDS = 30.0
_SEGMENT_PATTERN = re.compile(r"events-(\d+)\.jsonl$")


class RunEventWriter:
    """Append immutable event segments for a single run."""

    def __init__(
        self,
        *,
        fs: StorageFilesystemFull,
        control_uri: StorageUri,
        product: str,
        run_id: str,
        segment_size: int = DEFAULT_EVENT_SEGMENT_SIZE,
        resume_meta: dict[str, Any] | None = None,
        heartbeat_threshold_s: float = DEFAULT_RUN_META_HEARTBEAT_SECONDS,
        run_stale_threshold_s: int = DEFAULT_RUN_STALE_THRESHOLD_S,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        self._fs = fs
        self._control_uri = control_uri
        self._product = product
        self._run_id = run_id
        self._segment_size = max(1, int(segment_size))
        self._run_uri = self._control_uri.join("runs", self._run_id)
        self._run_meta_uri = self._run_uri.join("run.json")
        self._run_stale_threshold_s = int(run_stale_threshold_s)
        self._buffer: list[dict[str, Any]] = []
        self._part_index = 0
        self._event_index = 0
        self._heartbeat_threshold_s = max(0.0, float(heartbeat_threshold_s))
        self._events_written = int((resume_meta or {}).get("events", 0) or 0)
        self._status = str((resume_meta or {}).get("status", "started"))
        self._started_at = float((resume_meta or {}).get("started_at", time.time()) or time.time())
        self._updated_at = float(
            (resume_meta or {}).get("updated_at", self._started_at) or self._started_at
        )
        self._last_meta_write = time.time()
        resumed_slot_range = (resume_meta or {}).get("slot_range") if resume_meta else None
        resumed_slot_group = (resume_meta or {}).get("slot_group") if resume_meta else None
        self._slot_range: tuple[int, int] | None = (
            slot_range
            if slot_range is not None
            else (
                (int(resumed_slot_range[0]), int(resumed_slot_range[1]))
                if isinstance(resumed_slot_range, (list, tuple)) and len(resumed_slot_range) == 2
                else None
            )
        )
        self._slot_group = (
            slot_group
            if slot_group is not None
            else (resumed_slot_group if isinstance(resumed_slot_group, str) else None)
        )

        try:
            self._fs.makedirs(control_uri, exist_ok=True)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception:
            _log.debug("makedirs failed for %s (non-fatal)", control_uri.to_str())
        try:
            self._fs.makedirs(self._run_uri, exist_ok=True)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception:
            _log.debug("makedirs failed for %s (non-fatal)", self._run_uri.to_str())

        self._part_index = self._discover_next_part_index()
        self._write_schema_file()
        if resume_meta is None:
            self._write_run_meta()

    @property
    def run_uri(self) -> str:
        """Return the URI of the run directory in the control plane."""
        return self._run_uri.to_str()

    def next_event_id(self) -> str:
        """Generate the next sequential event ID for this run (run_id:part:index)."""
        return f"{self._run_id}:{self._part_index:05d}:{self._event_index:06d}"

    def append(
        self,
        event_type: str,
        record: dict[str, Any],
        *,
        meta: dict[str, Any] | None = None,
        flush: bool = False,
    ) -> ChunkEvent:
        """Append an event to the current segment buffer, auto-flushing at segment_size."""
        event = ChunkEvent(
            event_id=self.next_event_id(),
            event_type=event_type,
            product=self._product,
            run_id=self._run_id,
            timestamp=float(time.time()),
            record=dict(record),
            meta=dict(meta or {}),
        )
        self._buffer.append(event.to_dict())
        self._event_index += 1
        self._events_written += 1
        self._updated_at = float(event.timestamp)
        if flush or len(self._buffer) >= self._segment_size:
            self.flush()
        elif time.time() - self._last_meta_write >= self._heartbeat_threshold_s:
            self._write_run_meta()
        return event

    def flush(self) -> None:
        """Write buffered events to a new segment file and update run metadata."""
        if not self._buffer:
            return
        part_uri = self._run_uri.join(f"events-{self._part_index:05d}.jsonl")
        payload = "\n".join(json.dumps(item, separators=(",", ":")) for item in self._buffer)
        payload = f"{payload}\n"
        with self._fs.open(part_uri, "w") as handle:
            handle.write(payload)
        self._buffer.clear()
        self._part_index += 1
        self._event_index = 0
        self._write_run_meta()

    def finalize(self, *, status: str, error: str | None = None) -> None:
        """Flush remaining events, mark the run as terminal, and write final metadata."""
        self._status = status
        self._updated_at = time.time()
        self.flush()
        self._write_run_meta(error=error, completed=True)

    def _discover_next_part_index(self) -> int:
        try:
            entries = self._fs.ls(self._run_uri, detail=False)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception:
            return 0

        highest = -1
        for entry in entries:
            path = entry if isinstance(entry, str) else str(entry)
            match = _SEGMENT_PATTERN.search(str(path))
            if match:
                highest = max(highest, int(match.group(1)))
        return highest + 1

    def _write_schema_file(self) -> None:
        schema_uri = self._control_uri.join(SCHEMA_FILENAME)
        if self._fs.exists(schema_uri):
            return
        payload = {
            "schema_version": SCHEMA_VERSION,
            "layout": "chunkmanager-v2",
            "created_at": self._started_at,
        }
        try:
            schema_bytes = json.dumps(payload).encode("utf-8")
            self._fs.atomic_writer.write_atomic(schema_uri, schema_bytes)
        except FileExistsError:
            return

    def _write_run_meta(self, *, error: str | None = None, completed: bool = False) -> None:
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "product": self._product,
            "run_id": self._run_id,
            "status": self._status,
            "parts": self._part_index,
            "events": self._events_written,
            "started_at": self._started_at,
            "updated_at": self._updated_at,
            "run_uri": self._run_uri.to_str(),
            "run_stale_threshold_s": self._run_stale_threshold_s,
        }
        if error:
            payload["error"] = error
        if completed:
            payload["completed_at"] = self._updated_at
        if self._slot_range is not None:
            payload["slot_range"] = [int(self._slot_range[0]), int(self._slot_range[1])]
        if self._slot_group is not None:
            payload["slot_group"] = self._slot_group

        with self._fs.open(self._run_meta_uri, "w") as handle:
            json.dump(payload, handle, separators=(",", ":"))
        self._last_meta_write = time.time()
