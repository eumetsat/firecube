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

"""Control-plane serialization codec for Firecube .tgm archives.

Serializes .firecube/ state into a numpy uint8 array for embedding in .tgm
archives, and deserializes/restores it on the restore path.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager

_log = logging.getLogger(__name__)


def serialize_controlplane(
    manager: ChunkManager,
    product: str,
    *,
    group: str | None = None,
) -> tuple[dict[str, Any], np.ndarray]:
    """Serialize control-plane state for a product into a uint8 numpy array.

    When *group* is specified only spans for that group are included.
    Claims (write locks) are always included in full — they have no group field.
    Run records are included only for runs referenced by the result set.

    Returns
    -------
    tuple[dict, np.ndarray]
        ``(descriptor_dict, uint8_array)`` suitable for ``TensogramFile.append()``.
    """
    list_kwargs: dict[str, Any] = {"product": product, "chunk_type": "span"}
    if group is not None:
        list_kwargs["meta"] = {"group": group}

    spans = manager.list_chunks(**list_kwargs)

    span_records: list[dict[str, Any]] = []
    run_ids_seen: set[str] = set()
    for chunk in spans:
        span_meta = chunk.meta or {}
        run_id = span_meta.get("run_id")
        if run_id:
            run_ids_seen.add(run_id)
        span_records.append(
            {
                "key": chunk.key,
                "type": chunk.chunk_type,
                "status": chunk.status,
                "meta": span_meta,
                "record": chunk.record or {},
            }
        )

    runs = manager.list_runs(product=product)
    run_records: list[dict[str, Any]] = []
    for run in runs:
        if run_ids_seen and run.run_id not in run_ids_seen:
            continue
        run_records.append(
            {
                "run_id": run.run_id,
                "status": run.status,
                "started_at": run.started_at,
                "updated_at": run.updated_at,
                "completed_at": run.completed_at,
                "events": run.events,
                "parts": run.parts,
                "product": run.product,
            }
        )

    claim_records: list[dict[str, Any]] = []
    try:
        claims = manager.list_claims(product=product)
        claim_records.extend(
            {
                "domain": claim.domain,
                "owner_id": claim.owner_id,
                "acquired_at": claim.acquired_at,
            }
            for claim in claims
        )
    except Exception as exc:
        _log.debug("Could not collect claims for %s: %s", product, exc)

    state: dict[str, Any] = {
        "schema_version": "v1",
        "product": product,
        "group_filter": group,
        "spans": span_records,
        "runs": run_records,
        "claims": claim_records,
    }

    data_bytes = json.dumps(state).encode("utf-8")
    arr = np.frombuffer(data_bytes, dtype=np.uint8).copy()
    descriptor: dict[str, Any] = {
        "type": "ntensor",
        "shape": [len(arr)],
        "dtype": "uint8",
        "compression": "none",
    }
    return descriptor, arr


def deserialize_controlplane(data: np.ndarray) -> dict[str, Any]:
    """Deserialize a control-plane uint8 array back into a state dict."""
    data_bytes = data.astype(np.uint8).tobytes()
    return json.loads(data_bytes.decode("utf-8"))


def restore_controlplane(
    state: dict[str, Any],
    target_path: str,
    *,
    storage_config: Any | None = None,
) -> None:
    """Write .firecube/ control-plane state to a target path.

    Replays run and span records as WAL events under
    ``<target_path>/.firecube/`` using ``RunEventWriter``.

    This is best-effort: individual run failures are logged as warnings
    and do not prevent other runs from being restored.

    Parameters
    ----------
    state:
        Deserialized control-plane state dict from ``deserialize_controlplane()``.
    target_path:
        Root path of the zarr store to restore ``.firecube/`` into.
    storage_config:
        Optional ``StorageConfig`` used to wire the underlying filesystem with
        the configured endpoint and credentials. When ``None``, ambient defaults
        apply — required for remote ``target_path`` values such as ``s3://...``.
    """
    from firecube.core.controlplane.events import RunEventWriter
    from firecube.core.filesystem import StorageFilesystemFull, create_filesystem
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.uri import StorageUri

    target_uri = (
        StorageUri.parse(target_path)
        if "://" in target_path
        else StorageUri.from_local_path(Path(target_path).resolve())
    )
    control_uri = target_uri.join(".firecube")
    driver = StorageDriverConfig.from_storage_config_or_default(storage_config)
    fs = cast(
        StorageFilesystemFull,
        create_filesystem(
            StorageBinding(
                identity=ProductIdentity.from_uri(target_uri, "zarr", product_name=target_path),
                driver=driver,
            )
        ),
    )

    product = state.get("product", "unknown")
    spans = state.get("spans", [])
    runs = state.get("runs", [])

    if not spans and not runs:
        _log.info("No control-plane records to restore for %s", product)
        return

    spans_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for span in spans:
        run_id = span.get("meta", {}).get("run_id", "unknown")
        spans_by_run[run_id].append(span)

    runs_by_id = {r["run_id"]: r for r in runs}

    for run_id, run_spans in spans_by_run.items():
        run_info = runs_by_id.get(run_id, {})
        try:
            writer = RunEventWriter(
                fs=fs,
                control_uri=control_uri,
                product=product,
                run_id=run_id,
                resume_meta=run_info if run_info else None,
            )
            for span in run_spans:
                writer.append(
                    event_type="span_recorded",
                    record=span.get("record", {}),
                    meta=span.get("meta", {}),
                )
            original_status = run_info.get("status", "complete")
            writer.finalize(status=original_status)
        except Exception as exc:
            _log.warning(
                "Could not restore WAL events for run %s in %s: %s",
                run_id,
                product,
                exc,
            )

    _log.info(
        "Restored .firecube/ state for %s: %d spans across %d runs",
        product,
        len(spans),
        len(runs_by_id),
    )
