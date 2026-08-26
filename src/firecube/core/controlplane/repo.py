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

"""WAL-backed repository for the `.firecube/` control-plane root."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from firecube.core.controlplane._helpers import (
    describe_control_plane,
)
from firecube.core.controlplane._helpers import (
    open_controlplane_fs_cached as _open_controlplane_fs_cached,
)
from firecube.core.controlplane._paths import run_dir_for
from firecube.core.controlplane._projection import ManifestProjection
from firecube.core.controlplane._snapshot import (
    read_latest_pointer,
)
from firecube.core.controlplane._wal_reader import WalReader
from firecube.core.controlplane._wal_writer import ManifestWalWriter
from firecube.core.controlplane.claims import FilesystemClaimService
from firecube.core.controlplane.events import RunEventWriter
from firecube.core.controlplane.metrics import (
    record_wal_snapshot_rebuild,
)
from firecube.core.controlplane.repo_utils import (
    deserialize_slot_group as _deserialize_slot_group,  # noqa: F401
)
from firecube.core.controlplane.repo_utils import (
    deserialize_slot_range as _deserialize_slot_range,  # noqa: F401
)
from firecube.core.controlplane.types import (
    CLAIMS_DIRNAME,
    CONTROL_DIRNAME,
    DEFAULT_RUN_STALE_THRESHOLD_S,
    LATEST_POINTER,
    RUNS_DIRNAME,
    SCHEMA_VERSION,
    SNAPSHOT_DIRNAME,
    ChunkInfo,
    ClaimInfo,
    IndexEnsuredEvent,
    RunInfo,
    SpanCoverage,
    WriteDomain,
)
from firecube.core.errors import (
    ManifestError,
)
from firecube.core.filesystem import StorageFilesystem, StorageFilesystemFull
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target

log = logging.getLogger(__name__)


@dataclass(slots=True)
class _RunEntriesCache:
    entries_by_product: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


class _ControlRootResolver:
    """Derive product and `.firecube/` control-plane paths from the configured output base."""

    def __init__(
        self,
        repo: ManifestRepository,
        base_uri: StorageUri,
        fs: StorageFilesystem,
    ) -> None:
        self._repo = repo
        self.base_uri = base_uri
        self._fs = fs

    def product_root(self, product: str) -> tuple[StorageUri, StorageUri]:
        """Return (filesystem_uri, canonical_uri) for a product root directory."""
        product = str(product or "").strip("/")
        if not product:
            raise ManifestError("product must be non-empty")
        from firecube.core.product import ensure_product_uri

        if product == self._repo.binding.identity.product_name:
            product_uri = self._repo.binding.identity.product_uri
        else:
            product_uri = StorageUri.parse(ensure_product_uri(self.base_uri.to_str(), product))
        _fs, product_fs_uri = self._repo._get_fs(product_uri)
        return product_fs_uri, product_uri

    def __call__(self, product: str) -> tuple[StorageUri, StorageUri]:
        product_fs_uri, product_uri = self.product_root(product)
        return product_fs_uri.join(CONTROL_DIRNAME), product_uri.join(CONTROL_DIRNAME)


class ManifestRepository:
    def __init__(
        self,
        binding: StorageBinding,
        workspace: Path,
        *,
        filesystem: StorageFilesystem | None = None,
        run_stale_threshold_s: int = DEFAULT_RUN_STALE_THRESHOLD_S,
    ):
        self.binding = binding
        self.workspace = workspace
        self._base_uri: StorageUri = binding.identity.product_uri.parent()
        self.storage_config = self._storage_config_from_binding(binding)
        self.workspace = workspace
        self.run_stale_threshold_s = int(run_stale_threshold_s)
        self._filesystem = filesystem
        self._fs_cache: dict[tuple, StorageFilesystem] = {}
        self._run_entries_cache: _RunEntriesCache | None = None
        self.log = logging.getLogger(f"{__name__}.ManifestRepository")
        self._writers: dict[tuple[str, str], RunEventWriter] = {}
        self._resolver: _ControlRootResolver | None = None
        self._fs: Any | None = None
        self.claims: FilesystemClaimService | None = None
        self._wal_reader: WalReader | None = None
        self._binding_key: str | None = self._binding_key_for(binding)
        self._wal_writer = ManifestWalWriter(self)
        self._projection = ManifestProjection(self)

    @property
    def base_uri(self) -> str:
        return self._base_uri.to_str()

    def record_entry(self, product: str, entry: dict[str, Any]) -> None:
        raise ManifestError(
            "ChunkManager v2 no longer accepts arbitrary chunk-entry writes. Use record_run_started(), "
            "record_span(), and record_run_terminal() instead."
        )

    @contextmanager
    def run_entries_cache_scope(self) -> Generator[None, None, None]:
        prev = self._run_entries_cache
        self._run_entries_cache = _RunEntriesCache()
        try:
            yield
        finally:
            self._run_entries_cache = prev

    def record_run_started(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        self._wal_writer.record_run_started(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=size,
            meta=meta,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def record_run_terminal(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        status: str,
        error: str | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        self._wal_writer.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=size,
            meta=meta,
            status=status,
            error=error,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def record_run_started_with_replacement(
        self, *, product: str, run_id: str, replaces: list[str]
    ) -> None:
        self._wal_writer.record_run_started_with_replacement(
            product=product, run_id=run_id, replaces=replaces
        )

    def record_maintenance_started(
        self, *, product: str, run_id: str, op: str, scope_meta: dict[str, Any] | None = None
    ) -> None:
        self._wal_writer.record_maintenance_started(
            product=product, run_id=run_id, op=op, scope_meta=scope_meta
        )

    def record_maintenance_completed(
        self, *, product: str, run_id: str, op: str, scope_meta: dict[str, Any] | None = None
    ) -> None:
        self._wal_writer.record_maintenance_completed(
            product=product, run_id=run_id, op=op, scope_meta=scope_meta
        )

    def record_maintenance_failed(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
        error: str,
    ) -> None:
        self._wal_writer.record_maintenance_failed(
            product=product, run_id=run_id, op=op, scope_meta=scope_meta, error=error
        )

    def record_replacement_committed(
        self, *, product: str, run_id: str, replacing_run_id: str, replaced_span_keys: list[str]
    ) -> None:
        self._wal_writer.record_replacement_committed(
            product=product,
            run_id=run_id,
            replacing_run_id=replacing_run_id,
            replaced_span_keys=replaced_span_keys,
        )

    def record_span_event(
        self,
        *,
        product: str,
        run_id: str,
        batch_id: str,
        group: str,
        status: str,
        reason: str | None = None,
        coverage: SpanCoverage | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self._wal_writer.record_span_event(
            product=product,
            run_id=run_id,
            batch_id=batch_id,
            group=group,
            status=status,
            reason=reason,
            coverage=coverage,
            meta=meta,
        )

    def record_schema_verification_event(
        self,
        *,
        product: str,
        run_id: str,
        group: str,
        plugin: str,
        schema_hash: str,
        verified_at: str,
        expected_time_count: int,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self._wal_writer.record_schema_verification_event(
            product=product,
            run_id=run_id,
            group=group,
            plugin=plugin,
            schema_hash=schema_hash,
            verified_at=verified_at,
            expected_time_count=expected_time_count,
            meta=meta,
        )

    def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
        self._wal_writer.record_index_ensured_event(event)

    def record_slot_index_model_event(
        self,
        *,
        product: str,
        run_id: str,
        event_type: str,
        identity_hash: str,
        model_name: str,
        group: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self._wal_writer.record_slot_index_model_event(
            product=product,
            run_id=run_id,
            event_type=event_type,
            identity_hash=identity_hash,
            model_name=model_name,
            group=group,
            meta=meta,
        )

    def discover_manifests(self) -> list[str]:
        return self._projection.discover_manifests()

    def parse_manifest(self, manifest_uri: str) -> Generator[ChunkInfo, None, None]:
        yield from self._projection.parse_manifest(manifest_uri)

    def list_runs(
        self, *, product: str, status: str | None = None, non_terminal: bool = False
    ) -> list[RunInfo]:
        return self._projection.list_runs(product=product, status=status, non_terminal=non_terminal)

    def list_stale_runs(self, *, product: str) -> list[RunInfo]:
        return [run for run in self.list_runs(product=product, non_terminal=True) if run.stale]

    def abandon_run(
        self, *, product: str, run_id: str, reason: str, meta: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self._wal_writer.abandon_run(
            product=product, run_id=run_id, reason=reason, meta=meta
        )

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
        return self._projection.list_chunks(
            pattern=pattern,
            product=product,
            before=before,
            after=after,
            chunk_type=chunk_type,
            status=status,
            include_replaced=include_replaced,
            meta=meta,
            time_min_after=time_min_after,
            time_max_before=time_max_before,
            time_overlaps=time_overlaps,
            filter_fn=filter_fn,
        )

    def mark_chunks_replaced(
        self, chunk_keys: list[str], product: str, replacement_timestamp: float
    ) -> dict[str, Any]:
        return self._wal_writer.mark_chunks_replaced(chunk_keys, product, replacement_timestamp)

    def remove_from_manifest(
        self, manifest_uri: str, chunks_to_remove: list[ChunkInfo]
    ) -> tuple[int, int]:
        return self._wal_writer.remove_from_manifest(chunks_to_remove)

    def acquire_claim(self, *, product: str, domain: WriteDomain, owner_id: str):
        if self.claims is None:
            self._ensure_bound()
        assert self.claims is not None
        return self.claims.acquire(product=product, domain=domain, owner_id=owner_id)

    def list_claims(self, *, product: str | None = None) -> list[ClaimInfo]:
        if self.claims is None:
            self._ensure_bound()
        assert self.claims is not None
        return self.claims.list_claims(product=product)

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        if self.claims is None:
            self._ensure_bound()
        assert self.claims is not None
        return self.claims.list_stale_claims(product=product)

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        if self.claims is None:
            self._ensure_bound()
        assert self.claims is not None
        return self.claims.clear_claim(product=product, domain_id=domain_id, force=force)

    def rebuild_snapshot(self, product: str) -> dict[str, Any]:
        _rebuild_t0 = time.time()
        self._ensure_product_control_root(product)
        self._assert_compaction_allowed(product)
        if self._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        if self._fs is None:
            raise ManifestError("Repository not bound — call bind() first")
        control_path, control_uri = self._resolver(product)
        try:
            self._fs.makedirs(control_path.join(SNAPSHOT_DIRNAME), exist_ok=True)
        except Exception:
            self.log.debug(
                "makedirs failed for %s (non-fatal)", control_path.join(SNAPSHOT_DIRNAME)
            )

        is_remote = is_remote_target(control_uri.to_str())
        lock_path = control_path.join(SNAPSHOT_DIRNAME, "compact.lock")
        lock_token = uuid.uuid4().hex
        lock_acquired = False
        if is_remote:
            self.log.debug(
                "rebuild_snapshot for %s: skipping local lock (remote target — orchestration handles exclusion)",
                product,
            )
        if not is_remote:
            try:
                lock_bytes = json.dumps({"token": lock_token, "created_at": time.time()}).encode(
                    "utf-8"
                )
                self._fs.atomic_writer.write_atomic(lock_path, lock_bytes)
                lock_acquired = True
            except FileExistsError:
                return {"product": product, "skipped": True, "locked": True}

        completed_runs = [
            run
            for run in self._list_run_entries(product)
            if str(run.get("status", "")) == "complete"
        ]
        records = self._project_records_from_runs(completed_runs)
        completed_before = max(
            (float(run.get("completed_at", 0.0) or 0.0) for run in completed_runs),
            default=0.0,
        )
        generation = str(time.time_ns())
        snapshot_path = control_path.join(SNAPSHOT_DIRNAME, f"snapshot-{generation}.jsonl")
        snapshot_meta_path = control_path.join(SNAPSHOT_DIRNAME, f"snapshot-{generation}.meta.json")
        latest_path = control_path.join(LATEST_POINTER)

        try:
            payload = "\n".join(
                json.dumps(record, separators=(",", ":"))
                for record in sorted(records.values(), key=lambda item: item.get("key", ""))
            )
            if payload:
                payload += "\n"
            with self._fs.open(snapshot_path, "w") as handle:
                handle.write(payload)
            meta = {
                "schema_version": SCHEMA_VERSION,
                "generation": generation,
                "completed_before": completed_before,
                "created_at": time.time(),
                "records": len(records),
                "product": product,
            }
            with self._fs.open(snapshot_meta_path, "w") as handle:
                json.dump(meta, handle, separators=(",", ":"))
            with self._fs.open(latest_path, "w") as handle:
                json.dump(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "generation": generation,
                        "snapshot_path": (
                            snapshot_path.path
                            if snapshot_path.protocol == "file"
                            else snapshot_path.to_str()
                        ),
                        "snapshot_meta_path": (
                            snapshot_meta_path.path
                            if snapshot_meta_path.protocol == "file"
                            else snapshot_meta_path.to_str()
                        ),
                        "completed_before": completed_before,
                        "product": product,
                    },
                    handle,
                    separators=(",", ":"),
                )
            record_wal_snapshot_rebuild(time.time() - _rebuild_t0)
            self.log.debug("rebuild_snapshot for %s: %.2fs", product, time.time() - _rebuild_t0)
            snapshot_uri = control_uri.join(
                SNAPSHOT_DIRNAME, f"snapshot-{generation}.jsonl"
            ).to_str()
            return {
                "product": product,
                "generation": generation,
                "records": len(records),
                "snapshot_path": snapshot_uri,
                "locked": False,
                "remote": is_remote,
            }
        finally:
            if lock_acquired:
                try:
                    with self._fs.open(lock_path, "r") as handle:
                        payload = json.load(handle)
                    if payload.get("token") == lock_token:
                        self._fs.rm(lock_path, recursive=False)
                except Exception:
                    self.log.debug("lock cleanup failed for %s (non-fatal)", lock_path)

    def get_product_root_uri(self, product: str) -> str:
        self._ensure_bound()
        if self._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        _path, uri = self._resolver.product_root(product)
        return uri.to_str()

    def get_control_root_uri(self, product: str) -> str:
        self._ensure_bound()
        if self._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        _path, uri = self._resolver(product)
        return uri.to_str()

    def get_latest_pointer_uri(self, product: str) -> str:
        return describe_control_plane(product_uri=self.get_product_root_uri(product))[
            "latest_pointer"
        ]

    def _matches_pattern(self, key: str, pattern: str) -> bool:
        regex_pattern = f"^{pattern.replace('*', '.*').replace('?', '.')}$"
        try:
            return bool(re.match(regex_pattern, key))
        except re.error:
            return False

    def _get_fs(self, uri: str | StorageUri) -> tuple[StorageFilesystem, StorageUri]:
        if not uri:
            raise ManifestError("Invalid manifest URI")
        root_uri = uri if isinstance(uri, StorageUri) else StorageUri.parse(str(uri))
        if self._filesystem is not None:
            return self._filesystem, root_uri
        return _open_controlplane_fs_cached(
            root_uri,
            binding=self.binding,
            cache=self._fs_cache,
        )

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

    def _manifest_uri_for_product(self, product: str) -> str:
        return self.get_control_root_uri(product)

    def _product_from_manifest_uri(self, manifest_uri: str) -> str:
        parts = StorageUri.parse(str(manifest_uri)).path.rstrip("/").split("/")
        if parts and parts[-1] == CONTROL_DIRNAME:
            return parts[-2]
        return parts[-1]

    def _ensure_bound(self) -> None:
        if self._resolver is not None and self._fs is not None:
            return
        base_uri = self._base_uri or StorageUri.from_local_path(self.workspace.resolve())
        if not base_uri.to_str():
            raise ManifestError("ChunkManager requires an output base URI or workspace.")
        fs, base_path = self._get_fs(base_uri)
        self._resolver = _ControlRootResolver(self, base_path, fs)
        self._fs = fs
        self.claims = FilesystemClaimService(
            fs=cast(StorageFilesystemFull, fs), control_root_resolver=self._resolver
        )
        self._wal_reader = WalReader(
            fs=fs,
            resolver=self._resolver,
            log=self.log,
            run_stale_threshold_s=self.run_stale_threshold_s,
        )

    def _writer(
        self,
        product: str,
        run_id: str,
        *,
        resume_existing: bool = False,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> RunEventWriter:
        return self._wal_writer._writer(
            product,
            run_id,
            resume_existing=resume_existing,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def _build_run_record(
        self,
        *,
        run_id: str,
        output_path: str,
        output_format: str,
        status: str,
        size: int,
        meta: dict[str, Any],
        error: str | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> dict[str, Any]:
        return ManifestWalWriter._build_run_record(
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            status=status,
            size=size,
            meta=meta,
            error=error,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def _ensure_product_control_root(self, product: str) -> None:
        self._ensure_bound()
        if self._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        if self._fs is None:
            raise ManifestError("Repository not bound — call bind() first")
        control_path, _control_uri = self._resolver(product)
        if self._fs.exists(control_path):
            return
        try:
            self._fs.makedirs(control_path, exist_ok=True)
        except Exception:
            self.log.debug("makedirs failed for %s (non-fatal)", control_path)
        try:
            self._fs.makedirs(control_path.join(RUNS_DIRNAME), exist_ok=True)
            self._fs.makedirs(control_path.join(SNAPSHOT_DIRNAME), exist_ok=True)
            self._fs.makedirs(control_path.join(CLAIMS_DIRNAME), exist_ok=True)
        except Exception:
            self.log.debug("makedirs failed for %s (non-fatal)", control_path)

    def _product_control_exists(self, product: str) -> bool:
        self._ensure_bound()
        if self._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        if self._fs is None:
            raise ManifestError("Repository not bound — call bind() first")
        control_path, _control_uri = self._resolver(product)
        return bool(self._fs.exists(control_path))

    def _load_current_state(self, product: str) -> dict[str, dict[str, Any]]:
        return self._projection._load_current_state(product)

    def _list_run_entries(self, product: str) -> list[dict[str, Any]]:
        self._ensure_bound()
        assert self._wal_reader is not None
        if self._run_entries_cache is not None:
            cached = self._run_entries_cache.entries_by_product.get(product)
            if cached is not None:
                return cached
            result = self._wal_reader.list_run_entries(product)
            self._run_entries_cache.entries_by_product[product] = result
            return result
        return self._wal_reader.list_run_entries(product)

    def _read_run_events(self, product: str, run_entry: dict[str, Any]) -> list[dict[str, Any]]:
        self._ensure_bound()
        assert self._wal_reader is not None
        return self._wal_reader.read_run_events(product, run_entry)

    def _parse_iso_dt(self, value: Any) -> datetime | None:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(UTC).replace(tzinfo=None)

    def _meta_time_overlaps(
        self,
        meta: dict[str, Any] | None,
        start: str,
        end: str,
    ) -> bool:
        if not isinstance(meta, dict):
            return False
        chunk_start = self._parse_iso_dt(meta.get("time_min"))
        chunk_end = self._parse_iso_dt(meta.get("time_max"))
        window_start = self._parse_iso_dt(start)
        window_end = self._parse_iso_dt(end)
        if chunk_start is None or chunk_end is None or window_start is None or window_end is None:
            return False
        return chunk_start <= window_end and chunk_end >= window_start

    def _load_history_records(self, product: str) -> list[dict[str, Any]]:
        return self._projection._load_history_records(product)

    def _project_records_from_runs(
        self,
        run_entries: list[dict[str, Any]],
        *,
        completed_before: float | None = None,
    ) -> dict[str, dict[str, Any]]:
        return self._projection._project_records_from_runs(
            run_entries,
            completed_before=completed_before,
        )

    def _read_latest_pointer(self, product: str) -> dict[str, Any] | None:
        self._ensure_bound()
        assert self._fs is not None
        return read_latest_pointer(product, fs=self._fs, resolver=self._resolver, log=self.log)

    def _read_snapshot_records(self, latest: dict[str, Any]) -> list[dict[str, Any]]:
        return self._projection._read_snapshot_records(latest)

    def _run_info_from_entry(self, product: str, payload: dict[str, Any]) -> RunInfo:
        return self._projection._run_info_from_entry(product, payload)

    def _get_run_entry(self, *, product: str, run_id: str) -> dict[str, Any]:
        self._ensure_bound()
        assert self._wal_reader is not None and self._resolver is not None
        run_dir, run_uri = run_dir_for(self._resolver, product, run_id)
        entry = self._wal_reader.read_run_entry(
            product=product, run_dir=run_dir, run_uri=run_uri, run_id=run_id
        )
        if entry is None:
            raise ManifestError(f"Run '{run_id}' not found for product '{product}'.")
        return entry

    def _assert_compaction_allowed(self, product: str) -> None:
        claims = self.list_claims(product=product)
        if claims:
            rendered = ", ".join(
                f"{claim.domain} (owner={claim.owner_id}, stale={claim.stale})" for claim in claims
            )
            raise ManifestError(
                f"Cannot rebuild snapshot for {product}: blocking claims exist: {rendered}. "
                "Clear claims explicitly first."
            )

        blocking_runs = [run for run in self.list_runs(product=product) if not run.is_terminal]
        if blocking_runs:
            rendered = ", ".join(
                f"{run.run_id} (status={run.status}, updated_at={run.updated_at:.0f}, stale={run.stale})"
                for run in blocking_runs
            )
            raise ManifestError(
                f"Cannot rebuild snapshot for {product}: non-terminal runs exist: {rendered}. "
                "Abandon stale runs explicitly first."
            )

    def close(self) -> None:
        """Flush all WAL writers and release bound filesystem resources."""
        for writer in list(self._writers.values()):
            try:
                writer.flush()
            except Exception:
                self.log.warning("Failed to flush WAL writer on close (events may be lost)")
        self._writers.clear()
        self._fs_cache.clear()
        self._resolver = None
        self._fs = None
        self._wal_reader = None
        self.claims = None

    def _binding_key_for(
        self,
        binding: StorageBinding,
    ) -> str:
        """Stable cache key for filesystem binding."""
        ck = binding.cache_key()
        return f"{ck.driver}:{ck.protocol}:{ck.authority or ''}:{ck.endpoint_url or ''}:{ck.region or ''}:{ck.credential_fingerprint or ''}"

    def _storage_config_from_binding(self, binding: StorageBinding) -> Any:
        from firecube.core.storage.session import storage_config_from_binding

        return storage_config_from_binding(binding)
