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

"""ZarrStoreFactory — driver-aware zarr store construction."""

# pyright: reportAttributeAccessIssue=false

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from zarr.storage import LocalStore as _ZarrLocalStore

if TYPE_CHECKING:
    from obstore.store import S3Config

    from firecube.core.config import StorageConfig


@dataclass(frozen=True, slots=True)
class ZarrStoreHandle:
    """Uniform zarr-store handle for both storage drivers.

    Callers should splat ``handle.zarr_kwargs()`` into ``zarr.open_group()``,
    ``xr.open_zarr()``, or ``Dataset.to_zarr()``. This intentionally omits the
    ``storage_options`` key when it is ``None`` because some zarr/xarray call
    paths are stricter about unexpected ``storage_options=None`` kwargs.
    """

    store: Any
    storage_options: dict[str, Any] | None
    target_uri: str

    def zarr_kwargs(self) -> dict[str, Any]:
        """Return uniform kwargs for zarr/xarray store-opening APIs."""
        kwargs: dict[str, Any] = {"store": self.store}
        if self.storage_options is not None:
            kwargs["storage_options"] = self.storage_options
        return kwargs


class _ComparableLocalStore(_ZarrLocalStore):
    """LocalStore adapter with path-string compatibility for legacy callers."""

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return other == str(self.root) or other == str(self)
        return super().__eq__(other)


def create_obstore_store(uri: str, storage_config: StorageConfig) -> Any:
    """Construct an obstore store for the given URI and config.

    Returns S3Store for s3:// URIs, LocalStore for local paths.
    Import-guarded — requires obstore to be installed.
    """
    from firecube.core.filesystem._obstore_compat import (
        LocalStore,
        S3Store,
        require_obstore,
    )

    require_obstore()

    from firecube.core.storage.uri import StorageUri
    from firecube.core.uris import is_remote_target, local_path_from_target

    if is_remote_target(uri):
        config: dict[str, Any] = {}
        if storage_config.access_key:
            config["aws_access_key_id"] = storage_config.access_key
        if storage_config.secret_key:
            config["aws_secret_access_key"] = storage_config.secret_key
        if storage_config.endpoint_url:
            config["aws_endpoint"] = storage_config.endpoint_url
            if urlparse(storage_config.endpoint_url).scheme == "http":
                config["aws_allow_http"] = "true"
        if storage_config.region:
            config["aws_region"] = storage_config.region
        if storage_config.path_style:
            config["aws_virtual_hosted_style_request"] = "false"
        parsed = StorageUri.parse(uri)
        return S3Store(
            bucket=parsed.authority,
            prefix=parsed.path.lstrip("/").rstrip("/") or None,
            config=cast("S3Config", config),
        )
    else:
        local_path = str(local_path_from_target(uri))
        return LocalStore(prefix=local_path, mkdir=True)


def create_zarr_store(
    *,
    uri: str,
    storage_config: StorageConfig,
    mode: str = "w",
) -> ZarrStoreHandle:
    """Build a uniform zarr-store handle for the configured driver.

    Callers should not branch on driver; instead splat ``handle.zarr_kwargs()``
    into zarr/xarray APIs.
    """
    if storage_config.storage_driver == "fsspec":
        from firecube.core.uris import is_remote_target, local_path_from_target

        if is_remote_target(uri):
            storage_options: dict[str, Any] | None = None
            if storage_config.storage_type == "s3":
                from firecube.core.filesystem.ops import fs_kwargs_for_uri

                storage_options = fs_kwargs_for_uri(uri, storage_config=storage_config)
            return ZarrStoreHandle(
                store=uri,
                storage_options=storage_options,
                target_uri=uri,
            )

        return ZarrStoreHandle(
            store=_ComparableLocalStore(local_path_from_target(uri), read_only=(mode == "r")),
            storage_options=None,
            target_uri=uri,
        )

    from firecube.core.filesystem._obstore_compat import require_obstore

    require_obstore()

    from zarr.storage import ObjectStore

    raw_store = create_obstore_store(uri, storage_config)
    return ZarrStoreHandle(
        store=ObjectStore(raw_store, read_only=(mode == "r")),
        storage_options=None,
        target_uri=uri,
    )
