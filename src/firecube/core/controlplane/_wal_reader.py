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

"""WAL (Write-Ahead Log) reader for the control plane.

Reads and parses WAL event segments stored under .firecube/runs/<run_id>/.
Each run directory contains:
  - run.json: run metadata (status, timestamps, configuration)
  - events-<timestamp>.jsonl: append-only event segments

Segment structure:
  - One JSON object per line (newline-delimited JSON)
  - Each object has schema_version, event_type, record, timestamp, meta

Torn tail recovery:
  - If the last line of a segment is incomplete (no trailing newline),
    it's treated as a torn write from a crash. The partial line is discarded.
  - Multiple torn tails in a single run are treated as corruption (manual repair).
  - A torn tail in a non-terminal segment with valid later segments is acceptable
    only if all later events are terminal (run_completed/failed/abandoned).

Thread safety:
  - All read operations are safe for concurrent use (no mutable state modified).
  - Multiple WalReader instances can read the same product simultaneously.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from firecube.core.controlplane.metrics import record_wal_corruption, record_wal_torn_tail_recovery
from firecube.core.controlplane.types import RUNS_DIRNAME, SCHEMA_VERSION
from firecube.core.errors import ControlPlaneCorruptionError
from firecube.core.storage.uri import StorageUri


class WalReader:
    """Reads WAL segments and run metadata from the control-plane filesystem.

    Constructed with already-bound filesystem and resolver from ManifestRepository.
    All methods are read-only and thread-safe.
    """

    def __init__(
        self,
        *,
        fs: Any,
        resolver: Any,
        log: logging.Logger,
        run_stale_threshold_s: int,
    ) -> None:
        self._fs = fs
        self._resolver = resolver
        self.log = log
        self.run_stale_threshold_s = run_stale_threshold_s

    @staticmethod
    def _entry_to_uri(entry: Any, base_uri: StorageUri) -> StorageUri:
        name = entry.get("name") if isinstance(entry, dict) else entry
        if isinstance(name, StorageUri):
            return name
        raw = str(name or "")
        if "://" in raw:
            return StorageUri.parse(raw)
        if base_uri.protocol == "file" and raw.startswith("/"):
            return StorageUri.from_local_path(raw)
        path = raw
        if base_uri.authority and path.startswith(f"{base_uri.authority}/"):
            path = path[len(base_uri.authority) + 1 :]
        return StorageUri(protocol=base_uri.protocol, authority=base_uri.authority, path=path)

    def list_run_segment_paths(self, run_dir: StorageUri) -> list[StorageUri]:
        """List all .jsonl event segment paths in a run directory, sorted."""
        try:
            entries = self._fs.ls(run_dir, detail=False)
        except Exception:
            return []
        paths = [self._entry_to_uri(item, run_dir) for item in entries]
        return sorted(
            (path for path in paths if path.path.endswith(".jsonl")),
            key=lambda p: p.to_str(),
        )

    @staticmethod
    def _run_id_from_dir(run_dir: StorageUri, runs_dir: StorageUri) -> str:
        """Return the full run-id subpath beneath the runs directory."""
        runs_path = runs_dir.path.rstrip("/")
        run_path = run_dir.path.rstrip("/")
        prefix = f"{runs_path}/"
        if run_path.startswith(prefix):
            return run_path[len(prefix) :]
        return run_path.lstrip("/")

    @staticmethod
    def _run_id_from_parent(run_dir: StorageUri) -> str:
        """Return a run-id relative to the directory's immediate parent."""
        parent_path = run_dir.parent().path.rstrip("/")
        run_path = run_dir.path.rstrip("/")
        return run_path.removeprefix(f"{parent_path}/").lstrip("/")

    def path_timestamp(self, path: StorageUri) -> float:
        """Extract filesystem modification timestamp for a path."""
        try:
            info = self._fs.info(path)
        except Exception:
            return 0.0
        for key in ("mtime", "LastModified", "updated", "created"):
            value = info.get(key)
            timestamp = self._coerce_timestamp(value)
            if timestamp is not None:
                return timestamp
        return 0.0

    def _coerce_timestamp(self, value: Any) -> float | None:
        """Convert various timestamp representations to float epoch seconds."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                return float(raw)
            except ValueError:
                pass
            try:
                if raw.endswith("Z"):
                    raw = raw[:-1] + "+00:00"
                return datetime.fromisoformat(raw).timestamp()
            except ValueError:
                return None
        return None

    def build_orphan_run_entry(
        self,
        *,
        product: str,
        run_id: str,
        run_dir: StorageUri,
        run_uri: str,
        segment_paths: list[StorageUri],
        error: str,
    ) -> dict[str, Any]:
        """Construct a synthetic run entry for runs missing valid run.json metadata."""
        timestamps = [
            ts for ts in [self.path_timestamp(path) for path in segment_paths] if ts > 0.0
        ]
        if not timestamps:
            run_ts = self.path_timestamp(run_dir)
            if run_ts > 0.0:
                timestamps = [run_ts]
        started_at = min(timestamps) if timestamps else 0.0
        updated_at = max(timestamps) if timestamps else 0.0
        return {
            "schema_version": SCHEMA_VERSION,
            "product": product,
            "run_id": run_id,
            "status": "orphaned",
            "parts": len(segment_paths),
            "events": 0,
            "started_at": started_at,
            "updated_at": updated_at,
            "run_uri": run_uri,
            "run_stale_threshold_s": self.run_stale_threshold_s,
            "error": error,
            "run_dir": run_dir.to_str(),
        }

    def read_run_entry(
        self,
        *,
        product: str,
        run_dir: StorageUri,
        run_uri: str,
        run_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Read and parse a single run's metadata from run.json.

        Returns None if the run directory has no metadata and no segments.
        Returns an orphan entry if metadata is missing but segments exist.
        """
        run_id = run_id or self._run_id_from_parent(run_dir)
        meta_uri = run_dir.join("run.json")
        segment_paths = self.list_run_segment_paths(run_dir)

        if self._fs.exists(meta_uri):
            raw: str | None = None
            try:
                with self._fs.open(meta_uri, "r") as handle:
                    raw = str(handle.read())
                payload = json.loads(raw)
            except Exception:
                if not segment_paths:
                    if raw is not None and not raw.strip():
                        # A zero-byte run.json with no WAL segments is a peer's
                        # first meta write still in flight (stores written
                        # before meta writes became atomic), not corruption: an
                        # instant earlier the file did not exist and this
                        # reader would have returned None.
                        self.log.debug("Skipping pending run meta at %s", meta_uri.to_str())
                        return None
                    raise
                return self.build_orphan_run_entry(
                    product=product,
                    run_id=run_id,
                    run_dir=run_dir,
                    run_uri=run_uri,
                    segment_paths=segment_paths,
                    error="unreadable_run_meta",
                )
            payload["run_dir"] = run_dir.to_str()
            payload["run_uri"] = str(payload.get("run_uri") or run_uri)
            payload["product"] = product
            return payload

        if segment_paths:
            return self.build_orphan_run_entry(
                product=product,
                run_id=run_id,
                run_dir=run_dir,
                run_uri=run_uri,
                segment_paths=segment_paths,
                error="missing_run_meta",
            )
        return None

    def list_run_entries(self, product: str) -> list[dict[str, Any]]:
        """Discover all run entries for a product by scanning the runs/ directory."""
        control_path, control_uri = self._resolver(product)
        runs_dir = control_path.join(RUNS_DIRNAME)
        if not self._fs.exists(runs_dir):
            return []
        try:
            entries = self._fs.ls(runs_dir, detail=False)
        except Exception as exc:
            self.log.warning("Failed to list run entries at %s: %s", runs_dir.to_str(), exc)
            return []
        run_entries: list[dict[str, Any]] = []
        for entry in entries:
            run_dir = self._entry_to_uri(entry, runs_dir)
            run_id = self._run_id_from_dir(run_dir, runs_dir)
            run_uri = control_uri.join(
                RUNS_DIRNAME,
                run_id,
            ).to_str()
            try:
                payload = self.read_run_entry(
                    product=product,
                    run_dir=run_dir,
                    run_uri=run_uri,
                    run_id=run_id,
                )
            except Exception as exc:
                record_wal_corruption()
                msg = f"Failed to read run metadata for {run_dir.to_str()}: {exc}"
                self.log.error(msg)
                raise ControlPlaneCorruptionError(msg) from exc
            if payload is not None:
                run_entries.append(payload)
        return run_entries

    def read_run_segment(
        self,
        *,
        path: StorageUri,
        product: str,
        run_id: str,
        allow_torn_tail: bool,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Parse a single WAL segment file, returning (events, recovered_tail).

        If allow_torn_tail is True and the last line is incomplete (no trailing
        newline), it's silently discarded as a torn write. recovered_tail=True
        indicates this happened.
        """
        try:
            with self._fs.open(path, "r") as handle:
                payload = handle.read()
        except Exception as exc:
            record_wal_corruption()
            msg = f"Failed to read WAL segment {path.to_str()}: {exc}"
            self.log.error(msg)
            raise ControlPlaneCorruptionError(msg) from exc

        text = str(payload)
        lines = text.splitlines(keepends=True)
        events: list[dict[str, Any]] = []
        recovered_tail = False

        for line_index, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                is_last_line = line_index == len(lines) - 1
                if allow_torn_tail and is_last_line and not text.endswith("\n"):
                    self.log.warning(
                        "Recovered torn WAL tail for product=%s run=%s segment=%s",
                        product,
                        run_id,
                        path.to_str(),
                    )
                    recovered_tail = True
                    record_wal_torn_tail_recovery()
                    break
                record_wal_corruption()
                msg = f"Corrupt WAL event in {path.to_str()} at line {line_index + 1}: {exc}"
                self.log.error(msg)
                raise ControlPlaneCorruptionError(msg) from exc
            if event.get("schema_version") != SCHEMA_VERSION:
                record_wal_corruption()
                msg = (
                    f"Unsupported event schema version in {path.to_str()}: "
                    f"{event.get('schema_version')}"
                )
                self.log.error(msg)
                raise ControlPlaneCorruptionError(msg)
            events.append(event)
        return events, recovered_tail

    def read_run_events(self, product: str, run_entry: dict[str, Any]) -> list[dict[str, Any]]:
        """Read all events for a run across all segments, handling torn tails.

        Segments are read in sorted order. Torn tail recovery rules:
        - At most one torn tail is allowed per run
        - A torn tail in the last segment of a non-terminal run is OK (active write)
        - A torn tail in a non-last segment is OK only if all later events are terminal
        - Otherwise, corruption is raised (manual repair needed)
        """
        run_dir = StorageUri.parse(str(run_entry["run_dir"]))
        run_id = str(run_entry.get("run_id", "unknown"))
        try:
            entries = self._fs.ls(run_dir, detail=False)
        except Exception as exc:
            record_wal_corruption()
            msg = f"Failed to list WAL segments for run {run_id}: {exc}"
            self.log.error(msg)
            raise ControlPlaneCorruptionError(msg) from exc

        segment_paths = [self._entry_to_uri(item, run_dir) for item in entries]
        event_paths = sorted(
            (path for path in segment_paths if path.path.endswith(".jsonl")),
            key=lambda p: p.to_str(),
        )
        events: list[dict[str, Any]] = []
        status = str(run_entry.get("status", ""))
        is_terminal = status in {"complete", "failed", "abandoned"}
        recovered_tail_segment_index: int | None = None
        later_terminal_events: list[dict[str, Any]] = []

        for path_index, path in enumerate(event_paths):
            segment_events, recovered_tail = self.read_run_segment(
                path=path,
                product=product,
                run_id=run_id,
                allow_torn_tail=True,
            )
            if recovered_tail:
                if recovered_tail_segment_index is not None:
                    record_wal_corruption()
                    msg = f"Multiple torn WAL tails detected for run {run_id}; manual repair required."
                    self.log.error(msg)
                    raise ControlPlaneCorruptionError(msg)
                recovered_tail_segment_index = path_index
            events.extend(segment_events)
            if (
                recovered_tail_segment_index is not None
                and path_index > recovered_tail_segment_index
            ):
                later_terminal_events.extend(segment_events)

        if recovered_tail_segment_index is None:
            return events

        last_segment_index = len(event_paths) - 1
        if recovered_tail_segment_index == last_segment_index and not is_terminal:
            return events

        if (
            recovered_tail_segment_index < last_segment_index
            and later_terminal_events
            and all(
                event.get("event_type") in {"run_completed", "run_failed", "run_abandoned"}
                for event in later_terminal_events
            )
        ):
            return events

        record_wal_corruption()
        msg = f"Run {run_id} has a torn WAL tail in a non-recoverable position."
        self.log.error(msg)
        raise ControlPlaneCorruptionError(msg)
