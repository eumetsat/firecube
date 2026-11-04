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

"""Compact-cache reading for the control plane.

The compact cache is a point-in-time materialized view of all completed-run records.
It lives under .firecube/compact/ with a LATEST.json pointer in the same directory.
Readers never read the v0.1.8 layout (.firecube/LATEST.json, .firecube/snapshots/);
a store that holds only that layout replays the full WAL.

Strategy:
  - On read (list_chunks): try the compact cache first, fall back to WAL replay
  - On write (rebuild_compact): project all completed runs, write atomically
  - The cache is advisory: WAL is always authoritative; the cache accelerates reads

Parallel WAL replay (in _load_current_state):
  - Segment reads are IO-bound → parallelized via ThreadPoolExecutor (max 8 workers)
  - Projection (apply_events) is sequential — order determines which record wins
  - Single-run case stays sequential (no thread pool overhead for trivial case)

Thread safety:
  - Compact-cache reads are safe for concurrent use
  - Compact-cache writes use a local lock file (non-remote) or assume orchestration (remote)
  - Multiple readers + single writer is the expected concurrency model
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from firecube.core.controlplane._event_processor import apply_events, sorted_complete_runs
from firecube.core.controlplane.metrics import record_wal_corruption
from firecube.core.controlplane.types import COMPACT_DIRNAME, LATEST_POINTER, SCHEMA_VERSION
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError, Message, Operation
from firecube.core.filesystem import StorageFilesystem
from firecube.core.storage.uri import StorageUri


def read_latest_pointer(
    product: str, *, fs: StorageFilesystem, resolver: Any, log: logging.Logger
) -> dict[str, Any] | None:
    """Read a product's compact/LATEST.json pointer; return None when it is absent."""
    control_path, _control_uri = resolver(product)
    latest_path = control_path.join(COMPACT_DIRNAME, LATEST_POINTER)
    if not fs.exists(latest_path):
        return None
    with fs.open(latest_path, "r") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != SCHEMA_VERSION:
        record_wal_corruption()
        msg = (
            f"Unsupported control-plane schema version in {latest_path.to_str()}: "
            f"{payload.get('schema_version')}"
        )
        log.error(msg)
        raise ControlPlaneCorruptionError(msg)
    return payload


def _stored_path_to_uri(raw: str) -> StorageUri:
    """Parse a LATEST.json path field that may be a bare local path or a URI."""
    if "://" in raw:
        return StorageUri.parse(raw)
    return StorageUri.from_local_path(raw)


def read_compact_records(latest: dict[str, Any], *, fs: StorageFilesystem) -> list[dict[str, Any]]:
    """Read all records from the compact-cache file that LATEST.json names."""
    compact_path_str = str(latest.get("snapshot_path") or "")
    compact_meta_path_str = str(latest.get("snapshot_meta_path") or "")
    if not compact_path_str or not compact_meta_path_str:
        raise ManifestError("LATEST.json is missing compact cache path metadata")
    compact_path = _stored_path_to_uri(compact_path_str)
    compact_meta_path = _stored_path_to_uri(compact_meta_path_str)
    if not fs.exists(compact_path):
        raise FileNotFoundError(compact_path_str)
    if not fs.exists(compact_meta_path):
        raise FileNotFoundError(compact_meta_path_str)
    with fs.open(compact_meta_path, "r") as handle:
        meta = json.load(handle)
    if meta.get("schema_version") != SCHEMA_VERSION:
        record_wal_corruption()
        msg = (
            f"Unsupported compact cache schema version in {compact_meta_path_str}: "
            f"{meta.get('schema_version')}"
        )
        raise ControlPlaneCorruptionError(msg)
    with fs.open(compact_path, "r") as handle:
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(handle, start=1):
            text = str(line).strip()
            if not text:
                continue
            try:
                records.append(json.loads(text))
            except json.JSONDecodeError as exc:
                raise ManifestError(
                    f"Compact cache {compact_path_str} contains invalid JSON at line "
                    f"{line_number}: {exc}"
                ) from exc
        return records


def load_current_state(
    product: str,
    *,
    product_control_exists_fn: Any,
    list_run_entries_fn: Any,
    read_run_events_fn: Any,
    fs: StorageFilesystem,
    resolver: Any,
    log: logging.Logger,
) -> dict[str, dict[str, Any]]:
    """Load the current projected state for a product.

    Tries the compact cache + incremental WAL replay first.
    Falls back to full WAL replay if the compact cache is unavailable.
    Uses parallel reads for 2+ runs (ThreadPoolExecutor, max 8 workers).
    """
    _t0 = time.time()
    if not product_control_exists_fn(product):
        return {}

    run_entries = list_run_entries_fn(product)
    complete_runs = [entry for entry in run_entries if str(entry.get("status", "")) == "complete"]
    latest: dict[str, Any] | None = None
    try:
        latest = read_latest_pointer(product, fs=fs, resolver=resolver, log=log)
    except ControlPlaneCorruptionError:
        raise
    except Exception as exc:
        log.error("Ignoring invalid latest pointer for %s: %s", product, exc)
        latest = None

    if latest is not None:
        cutoff_ts = float(latest.get("completed_before", 0.0) or 0.0)
        compact_age_s = time.time() - cutoff_ts if cutoff_ts > 0 else 0.0
        if compact_age_s > 86400:
            log.warning(
                Message(
                    "Compact cache for {product} is {hours:.1f} hours old (cutoff: {cutoff}). "
                    "Consider rebuilding it: {rebuild}",
                    product=product,
                    hours=compact_age_s / 3600,
                    cutoff=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(cutoff_ts)),
                    rebuild=Operation("rebuild_compact"),
                )
            )

    current: dict[str, dict[str, Any]] = {}
    if latest is not None:
        try:
            compact_records = read_compact_records(latest, fs=fs)
            current = {
                record.get("key", ""): record for record in compact_records if record.get("key")
            }
            cutoff = float(latest.get("completed_before", 0.0) or 0.0)
            # Use >= to avoid missing runs that complete at the same timestamp as the
            # cache cutoff. Redundant replay of the last compacted run is harmless
            # (upsert semantics: same key -> same record -> no change).
            replay_runs = [
                entry
                for entry in complete_runs
                if float(entry.get("completed_at", 0.0) or 0.0) >= cutoff
            ]
        except (FileNotFoundError, ManifestError) as exc:
            log.error(
                "Compact cache unavailable for %s; falling back to WAL replay: %s", product, exc
            )
            replay_runs = complete_runs
            current = {}
    else:
        replay_runs = complete_runs

    sorted_replay_runs = sorted_complete_runs(replay_runs)
    replay_mode = "sequential"
    # Why <= 1 stays sequential: no benefit from thread pool overhead for one run
    if len(sorted_replay_runs) <= 1:
        for run_entry in sorted_replay_runs:
            events = read_run_events_fn(product, run_entry)
            apply_events(current, events, log)
    else:
        replay_mode = "parallel"
        # Why max_workers=8: diminishing returns beyond 8 concurrent IO ops;
        # most products have fewer than 8 runs needing replay
        max_workers = min(len(sorted_replay_runs), 8)
        future_to_index: dict[Any, int] = {}
        replay_events: dict[int, list[dict[str, Any]]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for index, run_entry in enumerate(sorted_replay_runs):
                future = executor.submit(read_run_events_fn, product, run_entry)
                future_to_index[future] = index
            # Why future.result() propagates errors: corrupt WAL in any run should
            # abort the entire state load, not silently skip records
            for future, index in future_to_index.items():
                replay_events[index] = future.result()
        # Sequential application AFTER parallel reads — order matters for upsert semantics
        for index, _run_entry in enumerate(sorted_replay_runs):
            apply_events(current, replay_events[index], log)
    log.debug(
        "_load_current_state for %s: %.2fs (%d runs replayed, %s)",
        product,
        time.time() - _t0,
        len(replay_runs),
        replay_mode,
    )
    return current
