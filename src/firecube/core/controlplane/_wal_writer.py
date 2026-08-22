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

"""WAL write collaborator for :mod:`firecube.core.controlplane.repo`."""

from __future__ import annotations

import time
import uuid
from typing import Any

from firecube.core.controlplane import types
from firecube.core.controlplane.events import RunEventWriter
from firecube.core.controlplane.repo_utils import parse_pod_run_id_slot
from firecube.core.controlplane.types import (
    EVENT_INDEX_ENSURED,
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    MAINTENANCE_KIND,
    MAINTENANCE_OPS,
    SCHEMA_VERSION,
    IndexEnsuredEvent,
    SpanCoverage,
    build_span_entry,
)
from firecube.core.errors import ManifestError
from firecube.core.storage.uri import StorageUri


class ManifestWalWriter:
    """Append run, span, maintenance, and replacement events to the WAL."""

    def __init__(self, repo: Any) -> None:
        self._repo = repo

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
        record = self._build_run_record(
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            status="started",
            size=size,
            meta=meta,
            slot_range=slot_range,
            slot_group=slot_group,
        )
        self._writer(product, run_id, slot_range=slot_range, slot_group=slot_group).append(
            types.EVENT_RUN_STARTED,
            record,
            meta=record.get("meta") or {},
            flush=True,
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
        if status not in {"complete", "failed", "abandoned"}:
            raise ManifestError(f"Unsupported terminal run status: {status}")
        record = self._build_run_record(
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
        event_type = {
            "complete": types.EVENT_RUN_COMPLETED,
            "failed": types.EVENT_RUN_FAILED,
            "abandoned": types.EVENT_RUN_ABANDONED,
        }[status]
        writer = self._writer(
            product,
            run_id,
            resume_existing=True,
            slot_range=slot_range,
            slot_group=slot_group,
        )
        writer.append(event_type, record, meta=record.get("meta") or {}, flush=True)
        writer.finalize(status=status, error=error)
        self._repo._writers.pop((product, run_id), None)

    def record_run_started_with_replacement(
        self,
        *,
        product: str,
        run_id: str,
        replaces: list[str],
    ) -> None:
        record = {
            "replaces": list(replaces),
            "schema_version": SCHEMA_VERSION,
        }
        self._writer(product, run_id, resume_existing=True).append(
            types.EVENT_RUN_STARTED_WITH_REPLACEMENT,
            record,
            meta={},
            flush=True,
        )

    def record_maintenance_started(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
    ) -> None:
        self._validate_maintenance_op(op)
        record = self._build_maintenance_record(
            run_id=run_id,
            op=op,
            status="started",
            scope_meta=scope_meta or {},
        )
        self._writer(product, run_id).append(
            types.EVENT_MAINTENANCE_STARTED,
            record,
            meta=record.get("meta") or {},
            flush=True,
        )

    def record_maintenance_completed(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
    ) -> None:
        self._validate_maintenance_op(op)
        record = self._build_maintenance_record(
            run_id=run_id,
            op=op,
            status="complete",
            scope_meta=scope_meta or {},
        )
        writer = self._writer(product, run_id, resume_existing=True)
        writer.append(
            types.EVENT_MAINTENANCE_COMPLETED,
            record,
            meta=record.get("meta") or {},
            flush=True,
        )
        writer.finalize(status="complete")
        self._repo._writers.pop((product, run_id), None)

    def record_maintenance_failed(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
        error: str,
    ) -> None:
        self._validate_maintenance_op(op)
        record = self._build_maintenance_record(
            run_id=run_id,
            op=op,
            status="failed",
            scope_meta=scope_meta or {},
            error=error,
        )
        writer = self._writer(product, run_id, resume_existing=True)
        writer.append(
            types.EVENT_MAINTENANCE_FAILED,
            record,
            meta=record.get("meta") or {},
            flush=True,
        )
        writer.finalize(status="failed", error=error)
        self._repo._writers.pop((product, run_id), None)

    def record_replacement_committed(
        self,
        *,
        product: str,
        run_id: str,
        replacing_run_id: str,
        replaced_span_keys: list[str],
    ) -> None:
        try:
            run_entry = self._repo._get_run_entry(product=product, run_id=run_id)
        except ManifestError:
            run_entry = None

        if run_entry is not None:
            for event in self._repo._read_run_events(product, run_entry):
                if str(event.get("event_type", "")) != types.EVENT_REPLACEMENT_COMMITTED:
                    continue
                record = dict(event.get("record") or {})
                if str(record.get("replacing_run_id", "") or "") == replacing_run_id:
                    return

        record = {
            "replacing_run_id": replacing_run_id,
            "replaced_span_keys": list(replaced_span_keys),
            "schema_version": SCHEMA_VERSION,
        }
        self._writer(product, run_id, resume_existing=True).append(
            types.EVENT_REPLACEMENT_COMMITTED,
            record,
            meta={},
            flush=True,
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
        arrays = coverage.arrays if coverage else []
        ranges = coverage.time_index_ranges if coverage and coverage.time_index_ranges else []
        aligned = coverage.aligned if coverage else True
        state_array = coverage.state_array if coverage else None
        state_deleted_value = coverage.state_deleted_value if coverage else 2
        region_spec = coverage.region_spec if coverage else None
        write_strategy = coverage.write_strategy if coverage else None
        time_dim_name = coverage.time_dim_name if coverage else None
        record = build_span_entry(
            run_id=run_id,
            batch_id=batch_id,
            group=group,
            meta=meta or {},
            arrays=arrays,
            time_index_ranges=ranges,
            status=status,
            reason=reason,
            aligned=aligned,
            state_array=state_array,
            state_deleted_value=state_deleted_value,
            region_spec=region_spec,
            write_strategy=write_strategy,
            time_dim_name=time_dim_name,
        )
        event_type = {
            "active": types.EVENT_SPAN_COMMITTED,
            "failed": types.EVENT_SPAN_FAILED,
            "skipped": types.EVENT_SPAN_NOOP,
            "noop": types.EVENT_SPAN_NOOP,
            "replaced": types.EVENT_RECORD_REPLACED,
        }.get(status, types.EVENT_RECORD_UPSERT)
        self._writer(product, run_id, resume_existing=True).append(
            event_type,
            record,
            meta=record.get("meta") or {},
            flush=False,
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
        event_meta = meta or {}
        record = {
            "key": f"{run_id}:schema_verification:{group}",
            "type": types.EVENT_SCHEMA_VERIFICATION,
            "run_id": run_id,
            "group": group,
            "plugin": plugin,
            "schema_hash": schema_hash,
            "verified_at": verified_at,
            "expected_time_count": expected_time_count,
            "meta": event_meta,
        }
        self._writer(product, run_id, resume_existing=True).append(
            types.EVENT_SCHEMA_VERIFICATION,
            record,
            meta=event_meta,
            flush=False,
        )

    def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
        record = event.to_dict()
        self._writer(event.product, event.run_id, resume_existing=True).append(
            EVENT_INDEX_ENSURED,
            record,
            meta={},
            flush=False,
        )

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
        if event_type not in (
            EVENT_SLOT_INDEX_MODEL_RECORDED,
            EVENT_SLOT_INDEX_MODEL_VERIFIED,
        ):
            raise ValueError(
                f"event_type must be one of {EVENT_SLOT_INDEX_MODEL_RECORDED!r} or "
                f"{EVENT_SLOT_INDEX_MODEL_VERIFIED!r}, got {event_type!r}"
            )
        record: dict[str, Any] = {
            "identity_hash": identity_hash,
            "model_name": model_name,
        }
        if group is not None:
            record["group"] = group
        self._writer(product, run_id, resume_existing=True).append(
            event_type,
            record,
            meta=meta or {},
            flush=False,
        )

    def abandon_run(
        self,
        *,
        product: str,
        run_id: str,
        reason: str,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run_entry = self._repo._get_run_entry(product=product, run_id=run_id)
        info = self._repo._projection._run_info_from_entry(product, run_entry)
        if info.is_terminal:
            self._repo.log.debug(
                "run %s for %s is already terminal (status=%s), skipping abandon",
                run_id,
                product,
                info.status,
            )
            return {"product": product, "run_id": run_id, "status": info.status, "abandoned": False}

        terminal_meta = dict(meta or {})
        terminal_meta.setdefault("operation", "abandon_run")
        terminal_meta.setdefault("reason", reason)
        self._repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(
                run_entry.get("output_path")
                or run_entry.get("run_uri")
                or self._repo.get_product_root_uri(product)
            ),
            output_format=str(run_entry.get("output_format") or "control"),
            size=int(run_entry.get("size", 0) or 0),
            meta=terminal_meta,
            status="abandoned",
            error=reason,
        )
        return {"product": product, "run_id": run_id, "status": "abandoned", "abandoned": True}

    def mark_chunks_replaced(
        self, chunk_keys: list[str], product: str, replacement_timestamp: float
    ) -> dict[str, Any]:
        active_by_key = {
            chunk.key: chunk
            for chunk in self._repo.list_chunks(product=product, include_replaced=False)
            if chunk.is_active
        }
        if not active_by_key:
            return {"marked_count": 0}
        run_id = f"maintenance-{uuid.uuid4().hex}"
        self._repo.record_run_started(
            product=product,
            run_id=run_id,
            output_path=self._repo.get_product_root_uri(product),
            output_format="control",
            size=0,
            meta={"plugin": "firecube.chunks", "operation": "replace"},
        )
        count = 0
        for key in set(chunk_keys):
            chunk = active_by_key.get(key)
            if not chunk:
                continue
            record = dict(chunk.record or {})
            record["status"] = "replaced"
            record["replaced_at"] = float(replacement_timestamp)
            record["timestamp"] = float(replacement_timestamp)
            self._writer(product, run_id).append(
                types.EVENT_RECORD_REPLACED,
                record,
                meta=record.get("meta") or {},
                flush=False,
            )
            count += 1
        self._repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=self._repo.get_product_root_uri(product),
            output_format="control",
            size=0,
            meta={"plugin": "firecube.chunks", "operation": "replace"},
            status="complete",
        )
        return {"marked_count": count}

    def remove_from_manifest(self, chunks_to_remove: list[Any]) -> tuple[int, int]:
        if not chunks_to_remove:
            return 0, 0
        product = chunks_to_remove[0].product
        marker_timestamp = time.time()
        chunk_keys = [chunk.key for chunk in chunks_to_remove]
        result = self.mark_chunks_replaced(chunk_keys, product, marker_timestamp)
        removed_size = sum(int(chunk.size or 0) for chunk in chunks_to_remove)
        return result.get("marked_count", 0), removed_size

    def _writer(
        self,
        product: str,
        run_id: str,
        *,
        resume_existing: bool = False,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> RunEventWriter:
        self._repo._ensure_product_control_root(product)
        key = (product, run_id)
        writer = self._repo._writers.get(key)
        if writer is not None:
            return writer

        if self._repo._resolver is None:
            raise ManifestError("Repository not bound — call bind() first")
        if self._repo._fs is None:
            raise ManifestError("Repository not bound — call bind() first")
        control_path_uri, control_uri = self._repo._resolver(product)
        resume_meta = None
        if resume_existing:
            resume_meta = self._resume_meta_for_run(
                product=product,
                run_id=run_id,
                control_path=control_path_uri,
                control_uri=control_uri,
            )
        writer = RunEventWriter(
            fs=self._repo._fs,
            control_uri=control_path_uri,
            product=product,
            run_id=run_id,
            resume_meta=resume_meta,
            run_stale_threshold_s=self._repo.run_stale_threshold_s,
            slot_range=slot_range,
            slot_group=slot_group,
        )
        self._repo._writers[key] = writer
        return writer

    def _resume_meta_for_run(
        self,
        *,
        product: str,
        run_id: str,
        control_path: StorageUri,
        control_uri: StorageUri,
    ) -> dict[str, Any] | None:
        run_dir = control_path.join(types.RUNS_DIRNAME, run_id)
        run_uri = control_uri.join(types.RUNS_DIRNAME, run_id).to_str()
        self._repo._ensure_bound()
        assert self._repo._wal_reader is not None
        resume_meta = self._repo._wal_reader.read_run_entry(
            product=product, run_dir=run_dir, run_uri=run_uri
        )
        if resume_meta is not None:
            slot_range = resume_meta.get("slot_range")
            slot_group = resume_meta.get("slot_group")
            if slot_range is None or slot_group is None:
                parsed_range, parsed_group = parse_pod_run_id_slot(run_id)
                if slot_range is None and parsed_range is not None:
                    resume_meta["slot_range"] = [parsed_range[0], parsed_range[1]]
                if slot_group is None and parsed_group is not None:
                    resume_meta["slot_group"] = parsed_group
        return resume_meta

    @staticmethod
    def _validate_maintenance_op(op: str) -> None:
        if op not in MAINTENANCE_OPS:
            raise ManifestError(
                f"Unsupported maintenance op: {op!r}. Allowed: {sorted(MAINTENANCE_OPS)}"
            )

    @staticmethod
    def _build_maintenance_record(
        *,
        run_id: str,
        op: str,
        status: str,
        scope_meta: dict[str, Any],
        error: str | None = None,
    ) -> dict[str, Any]:
        scope_payload = dict(scope_meta or {})
        meta_payload = {
            **scope_payload,
            "run_id": run_id,
            "kind": MAINTENANCE_KIND,
            "op": op,
        }
        maintenance_payload: dict[str, Any] = {
            "op": op,
            "kind": MAINTENANCE_KIND,
            "scope": scope_payload,
        }
        if error:
            maintenance_payload["error"] = error
        return {
            "key": f"run_{run_id}",
            "type": "run",
            "size": 0,
            "timestamp": time.time(),
            "status": status,
            "meta": meta_payload,
            "maintenance": maintenance_payload,
            "schema_version": SCHEMA_VERSION,
        }

    @staticmethod
    def _build_run_record(
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
        payload: dict[str, Any] = {
            "key": f"run_{run_id}",
            "type": "run",
            "size": int(size),
            "timestamp": time.time(),
            "status": status,
            "meta": {**dict(meta), "run_id": run_id},
            "run": {
                "output_path": output_path,
                "output_format": output_format,
            },
            "schema_version": SCHEMA_VERSION,
        }
        if error:
            payload["run"]["error"] = error
        if slot_range is not None:
            payload["slot_range"] = [int(slot_range[0]), int(slot_range[1])]
        if slot_group is not None:
            payload["slot_group"] = slot_group
        return payload
