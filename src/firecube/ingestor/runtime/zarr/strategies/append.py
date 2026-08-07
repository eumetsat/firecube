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

"""AppendStrategy — AppendWriteStrategy wrapping ``append_time_groups()``.

Delegates entirely to the existing orchestrator in
``firecube.ingestor.runtime.zarr.append``.  No append logic is reimplemented.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

import xarray as xr

from firecube.core.uris import storage_uri_from_target

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.core.filesystem.store_factory import ZarrStoreHandle
    from firecube.core.storage.session import StorageSession


def _session_for_store(store_uri: str, storage_config: StorageConfig) -> StorageSession:
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession

    uri = storage_uri_from_target(store_uri)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(uri, format="zarr", product_name=store_uri),
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )


class AppendStrategy:
    """Xarray-append write strategy for Zarr stores.

    Wraps :func:`~firecube.ingestor.runtime.zarr.append.append_time_groups`
    behind the ``AppendWriteStrategy`` Protocol so that
    ``GenericZarrIngestor`` can treat append and region writes uniformly.

    All static configuration is captured at construction time; only
    per-batch dynamic inputs are passed to :meth:`write_groups`.
    """

    def __init__(
        self,
        *,
        store: object,
        store_uri: str | None = None,
        store_handle: ZarrStoreHandle | None = None,
        resume_target_uri: str | None = None,
        arrays_for_group: Callable[[str], list[str]] | None = None,
        chunk_shape: dict[str, int] | None = None,
        shard_shape: dict[str, int] | None = None,
        sharding: bool = False,
        compression: bool = False,
        zarr_codecs: list[dict] | None = None,
        consolidate: bool = False,
        resume_existing: bool = False,
        append_dim: str = "timestamp",
        state_var_name: str = "firecube_timestamp_state",
        state_deleted_value: int = 2,
        logger: logging.Logger | None = None,
        storage_config: StorageConfig | None = None,
        session: StorageSession | None = None,
    ) -> None:
        # NOTE: append_dim is the already-resolved time dim name supplied by
        # the caller (typically GenericZarrIngestor via _resolve_time_dim_name()).
        # The strategy is host-free at runtime — never looks up the dim name
        # from an ingestor object. This keeps the write protocol decoupled and
        # the strategy testable in isolation.
        self._store = store
        self._store_uri = store_uri
        self._store_handle = store_handle
        self._resume_target_uri = resume_target_uri
        self._storage_config = storage_config
        self._arrays_for_group = arrays_for_group
        self._chunk_shape = chunk_shape
        self._shard_shape = shard_shape
        self._sharding = sharding
        self._compression = compression
        self._zarr_codecs = zarr_codecs
        self._consolidate = consolidate
        self._resume_existing = resume_existing
        self._append_dim = append_dim
        self._state_var_name = state_var_name
        self._state_deleted_value = state_deleted_value
        self._logger = logger
        self._session = session

    def write_groups(
        self,
        *,
        group_to_timestamps: Mapping[str, Sequence[Any]],
        dataset_for_batch: Callable[[str, Sequence[Any]], xr.Dataset | None],
        batch_size: int,
        claim_for_group: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        """Delegate to ``append_time_groups()`` and return its metrics dict."""
        from firecube.ingestor.runtime.zarr.append import append_time_groups

        effective_store = self._store
        effective_zarr_store: Any = self._store_handle
        effective_session: StorageSession | None = self._session

        if effective_zarr_store is None and self._storage_config and self._store_uri:
            if effective_session is None:
                effective_session = _session_for_store(self._store_uri, self._storage_config)
            effective_zarr_store = effective_session.zarr.create_store(
                uri=storage_uri_from_target(self._store_uri),
                mode="a",
            )

        if effective_zarr_store is not None:
            typed_handle = cast(Any, effective_zarr_store)
            effective_store = typed_handle.store

        if effective_zarr_store is None:
            raise ValueError(
                "AppendStrategy.write_groups requires either store_handle or "
                "(storage_config + store_uri) to construct a ZarrStoreHandle."
            )

        resume_zarr_store: Any = None
        resume_session: Any = None
        if self._resume_target_uri and self._storage_config:
            resume_session = _session_for_store(self._resume_target_uri, self._storage_config)
            resume_zarr_store = resume_session.zarr.create_store(
                uri=storage_uri_from_target(self._resume_target_uri),
                mode="r",
            )

        append_time_groups_fn = cast(Any, append_time_groups)
        append_dim = self._append_dim
        return append_time_groups_fn(
            store=effective_store,
            zarr_store=effective_zarr_store,
            group_to_timestamps=group_to_timestamps,
            dataset_for_batch=dataset_for_batch,
            resume_zarr_store=resume_zarr_store,
            resume_session=resume_session,
            arrays_for_group=self._arrays_for_group,
            chunk_shape=self._chunk_shape,
            shard_shape=self._shard_shape,
            sharding=self._sharding,
            compression=self._compression,
            zarr_codecs=self._zarr_codecs,
            consolidate=self._consolidate,
            resume_existing=self._resume_existing,
            batch_size=batch_size,
            append_dim=append_dim,
            state_var_name=self._state_var_name,
            state_deleted_value=self._state_deleted_value,
            logger=self._logger,
            claim_for_group=claim_for_group,
            session=effective_session,
        )
