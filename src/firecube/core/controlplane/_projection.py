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

"""Projection/read collaborator for the control-plane repository."""

from __future__ import annotations

from collections.abc import Callable, Generator
from datetime import datetime
from typing import Any

from firecube.core.controlplane._event_processor import (
    apply_events,
    record_from_event,
    record_to_chunk_info,
    sorted_complete_runs,
)
from firecube.core.controlplane._snapshot import load_current_state, read_snapshot_records
from firecube.core.controlplane.ranges import (
    normalize_index_ranges as _normalize_index_ranges,
)
from firecube.core.controlplane.ranges import (
    subtract_index_ranges as _subtract_index_ranges,
)
from firecube.core.controlplane.repo_utils import (
    deserialize_slot_group,
    deserialize_slot_range,
    parse_pod_run_id_slot,
)
from firecube.core.controlplane.types import CONTROL_DIRNAME, ChunkInfo, RunInfo
from firecube.core.errors import ManifestError
from firecube.core.storage.uri import StorageUri


def _apply_overwrite_ranges_to_active_coverage(
    records: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Subtract replacing-span overwrite ranges from prior active span coverage."""
    active_spans = [
        record
        for record in records.values()
        if record.get("type") == "span" and record.get("status") == "active"
    ]
    active_spans.sort(
        key=lambda record: (
            float(record.get("timestamp", 0.0) or 0.0),
            str(record.get("key", "") or ""),
        )
    )

    prior_by_group: dict[str, list[dict[str, Any]]] = {}
    for record in active_spans:
        raw_meta = record.get("meta")
        meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
        group = str(meta.get("group", "") or "")
        run_id = str(meta.get("run_id", "") or "")
        overwrites = _normalize_index_ranges(meta.get("overwrites_index_ranges"))
        if overwrites:
            for prior in prior_by_group.get(group, []):
                raw_prior_meta = prior.get("meta")
                prior_meta: dict[str, Any] = (
                    raw_prior_meta if isinstance(raw_prior_meta, dict) else {}
                )
                if str(prior_meta.get("run_id", "") or "") == run_id:
                    continue
                _subtract_from_span_payload(prior, overwrites)
        prior_by_group.setdefault(group, []).append(record)
    return records


def _subtract_from_span_payload(record: dict[str, Any], overwrites: list[list[int]]) -> None:
    payload = record.get("span")
    if not isinstance(payload, dict):
        return
    ranges = _normalize_index_ranges(payload.get("time_index_ranges"))
    if not ranges:
        return
    remaining = _subtract_index_ranges(ranges, overwrites)
    if remaining == ranges:
        return
    payload["time_index_ranges"] = remaining
    payload["timestamps_written"] = sum(end - start + 1 for start, end in remaining)


def _run_entry_wal_order(entry: dict[str, Any]) -> tuple[float, str]:
    """Sort key placing run entries in the order their WAL segments were opened."""
    return (float(entry.get("started_at", 0.0) or 0.0), str(entry.get("run_id", "")))


class ManifestProjection:
    """Read WAL and snapshot records through ManifestRepository's compatibility surface."""

    def __init__(self, repo: Any) -> None:
        self._repo = repo

    def discover_manifests(self) -> list[str]:
        self._repo._ensure_bound()
        if self._repo._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        if self._repo._fs is None:
            raise ManifestError("Repository not bound — call bind() first")
        manifests: list[str] = []
        try:
            entries = self._repo._fs.ls(self._repo._resolver.base_uri, detail=False)
        except Exception as exc:
            self._repo.log.warning(
                "Failed to list products at %s: %s",
                self._repo._resolver.base_uri.to_str(),
                exc,
            )
            return manifests
        for entry in entries:
            product_uri = self._repo._entry_to_uri(entry, self._repo._resolver.base_uri)
            if not self._repo._fs.isdir(product_uri):
                continue
            control_path = product_uri.join(CONTROL_DIRNAME)
            if self._repo._fs.exists(control_path):
                manifests.append(control_path.to_str())
        return sorted(set(manifests))

    def parse_manifest(self, manifest_uri: str) -> Generator[ChunkInfo, None, None]:
        product = self._repo._product_from_manifest_uri(manifest_uri)
        yield from self.list_chunks(product=product, include_replaced=True)

    def list_runs(
        self,
        *,
        product: str,
        status: str | None = None,
        non_terminal: bool = False,
    ) -> list[RunInfo]:
        runs = [
            self._run_info_from_entry(product, entry)
            for entry in self._repo._list_run_entries(product)
        ]
        if status is not None:
            runs = [run for run in runs if run.status == status]
        if non_terminal:
            runs = [run for run in runs if not run.is_terminal]
        return runs

    def list_chunks(
        self,
        pattern: str | None = None,
        product: str | None = None,
        before: datetime | None = None,
        after: datetime | None = None,
        chunk_type: str | None = None,
        status: str | None = None,
        include_replaced: bool = False,
        meta: dict[str, Any] | None = None,
        time_min_after: str | None = None,
        time_max_before: str | None = None,
        time_overlaps: tuple[str, str] | None = None,
        filter_fn: Callable[[ChunkInfo], bool] | None = None,
    ) -> list[ChunkInfo]:
        chunks: list[ChunkInfo] = []
        products = (
            [product]
            if product
            else [self._repo._product_from_manifest_uri(uri) for uri in self.discover_manifests()]
        )

        for item in products:
            if not item:
                continue
            current_only = not include_replaced and status is None
            if current_only:
                records = list(self._load_current_state(item).values())
            else:
                # ``include_replaced`` asks for the latest state of every key,
                # not every WAL record: a replaced span otherwise appears twice
                # (its active record and the "replaced" copy written later).
                records = self._load_history_records(item, latest_per_key=include_replaced)
            control_uri = StorageUri.parse(self._repo._manifest_uri_for_product(item))
            for record in records:
                chunk = record_to_chunk_info(item, record, control_uri)
                if chunk_type is None and chunk.chunk_type == "run":
                    continue
                if pattern and not self._repo._matches_pattern(chunk.key, pattern):
                    continue
                if chunk_type and chunk.chunk_type != chunk_type:
                    continue
                if meta and (
                    not isinstance(chunk.meta, dict)
                    or not all(chunk.meta.get(k) == v for k, v in meta.items())
                ):
                    continue
                if time_min_after:
                    chunk_time_min = self._repo._parse_iso_dt((chunk.meta or {}).get("time_min"))
                    requested_time_min = self._repo._parse_iso_dt(time_min_after)
                    if chunk_time_min is None or requested_time_min is None:
                        continue
                    if chunk_time_min < requested_time_min:
                        continue
                if time_max_before:
                    chunk_time_max = self._repo._parse_iso_dt((chunk.meta or {}).get("time_max"))
                    requested_time_max = self._repo._parse_iso_dt(time_max_before)
                    if chunk_time_max is None or requested_time_max is None:
                        continue
                    if chunk_time_max > requested_time_max:
                        continue
                if time_overlaps and not self._repo._meta_time_overlaps(chunk.meta, *time_overlaps):
                    continue
                if filter_fn and not filter_fn(chunk):
                    continue
                if status is not None and chunk.status != status:
                    continue
                if status is None and not include_replaced and not chunk.is_active:
                    continue
                if before and chunk.datetime >= before:
                    continue
                if after and chunk.datetime <= after:
                    continue
                chunks.append(chunk)

        chunks.sort(key=lambda c: c.timestamp, reverse=True)
        return chunks

    def _load_current_state(self, product: str) -> dict[str, dict[str, Any]]:
        self._repo._ensure_bound()
        assert self._repo._fs is not None
        current = load_current_state(
            product,
            product_control_exists_fn=self._repo._product_control_exists,
            list_run_entries_fn=self._repo._list_run_entries,
            read_run_events_fn=self._repo._read_run_events,
            fs=self._repo._fs,
            resolver=self._repo._resolver,
            log=self._repo.log,
        )
        return _apply_overwrite_ranges_to_active_coverage(current)

    def _load_history_records(
        self, product: str, *, latest_per_key: bool = False
    ) -> list[dict[str, Any]]:
        """Return keyed WAL records for ``product`` sorted by record timestamp.

        With ``latest_per_key`` only the last record per key survives, in WAL
        order (run entries by ``started_at`` then event order), mirroring the
        upsert-by-key invariant of `apply_events`. WAL order, not the record
        timestamp, decides: a replacement record carries the caller-supplied
        replacement time, which need not be later than the active record's.
        """
        if not self._repo._product_control_exists(product):
            return []
        run_entries = self._repo._list_run_entries(product)
        if latest_per_key:
            run_entries = sorted(run_entries, key=_run_entry_wal_order)
        events: list[dict[str, Any]] = []
        latest: dict[str, dict[str, Any]] = {}
        for run_entry in run_entries:
            for event in self._repo._read_run_events(product, run_entry):
                record = record_from_event(event, self._repo.log)
                key = record.get("key")
                if not key:
                    continue
                if latest_per_key:
                    latest[str(key)] = record
                else:
                    events.append(record)
        if latest_per_key:
            events = list(latest.values())
        events.sort(key=lambda item: float(item.get("timestamp", 0.0) or 0.0))
        return events

    def _project_records_from_runs(
        self,
        run_entries: list[dict[str, Any]],
        *,
        completed_before: float | None = None,
    ) -> dict[str, dict[str, Any]]:
        current: dict[str, dict[str, Any]] = {}
        eligible = [entry for entry in run_entries if str(entry.get("status", "")) == "complete"]
        if completed_before is not None:
            eligible = [
                entry
                for entry in eligible
                if float(entry.get("completed_at", 0.0) or 0.0) <= completed_before
            ]
        for run_entry in sorted_complete_runs(eligible):
            product = str(run_entry.get("product", "")) or ""
            apply_events(current, self._repo._read_run_events(product, run_entry), self._repo.log)
        return _apply_overwrite_ranges_to_active_coverage(current)

    def _read_snapshot_records(self, latest: dict[str, Any]) -> list[dict[str, Any]]:
        self._repo._ensure_bound()
        assert self._repo._fs is not None
        return read_snapshot_records(latest, fs=self._repo._fs)

    def _run_info_from_entry(self, product: str, payload: dict[str, Any]) -> RunInfo:
        run_id = str(payload.get("run_id", ""))
        slot_range = deserialize_slot_range(payload.get("slot_range"))
        slot_group = deserialize_slot_group(payload.get("slot_group"))
        if slot_range is None or slot_group is None:
            parsed_range, parsed_group = parse_pod_run_id_slot(run_id)
            if slot_range is None and parsed_range is not None:
                self._repo.log.info(
                    "Healed missing slot_range for run_id=%s via run-id suffix recovery",
                    run_id,
                )
                slot_range = parsed_range
            if slot_group is None and parsed_group is not None:
                slot_group = parsed_group
        return RunInfo(
            product=product,
            run_id=run_id,
            status=str(payload.get("status", "unknown")),
            run_dir=str(payload.get("run_dir", "")),
            run_uri=str(payload.get("run_uri", "")),
            started_at=float(payload.get("started_at", 0.0) or 0.0),
            updated_at=float(payload.get("updated_at", 0.0) or 0.0),
            completed_at=(
                float(payload.get("completed_at", 0.0) or 0.0)
                if payload.get("completed_at") is not None
                else None
            ),
            events=int(payload.get("events", 0) or 0),
            parts=int(payload.get("parts", 0) or 0),
            error=payload.get("error"),
            stale_threshold_s=int(
                payload.get("run_stale_threshold_s", self._repo.run_stale_threshold_s)
                or self._repo.run_stale_threshold_s
            ),
            slot_range=slot_range,
            slot_group=slot_group,
            timestamps_skipped=int(payload.get("timestamps_skipped", 0) or 0),
        )
