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

"""Module-level helpers for the control-plane repository.

These are pure functions (or a small filesystem cache helper) used by
`ManifestRepository` but with no dependency on its instance state. Kept
out of `repo.py` so that file can shrink and focus on the orchestration
class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from firecube.core.controlplane.types import CONTROL_DIRNAME, LATEST_POINTER
from firecube.core.errors import ManifestError
from firecube.core.filesystem import StorageFilesystem, create_filesystem
from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from firecube.core.storage.binding import StorageBinding


def fsspec_kwargs_from_binding(binding: StorageBinding, protocol: str) -> dict[str, Any]:
    """Build fsspec kwargs directly from a StorageBinding (creds via binding.driver.credentials)."""
    if protocol != "s3":
        return {}

    driver = binding.driver
    credentials = driver.credentials
    access_key = credentials.access_key if credentials is not None else None
    secret_key = credentials.secret_key if credentials is not None else None

    fs_kwargs: dict[str, Any] = {}
    client_kwargs: dict[str, Any] = {}
    config_kwargs: dict[str, Any] = {}

    if driver.endpoint_url:
        client_kwargs["endpoint_url"] = driver.endpoint_url
    if driver.region:
        client_kwargs["region_name"] = driver.region
    if client_kwargs:
        fs_kwargs["client_kwargs"] = client_kwargs

    if driver.path_style is None or bool(driver.path_style):
        config_kwargs.setdefault("s3", {})["addressing_style"] = "path"
    else:
        config_kwargs.setdefault("s3", {})["addressing_style"] = "virtual"
    if config_kwargs:
        fs_kwargs["config_kwargs"] = config_kwargs

    if access_key is not None:
        fs_kwargs["key"] = access_key
    if secret_key is not None:
        fs_kwargs["secret"] = secret_key

    return fs_kwargs


def open_controlplane_fs_cached(
    uri: str | StorageUri,
    *,
    binding: StorageBinding,
    cache: dict[tuple, StorageFilesystem],
) -> tuple[StorageFilesystem, StorageUri]:
    """Control-plane-local filesystem cache keyed off StorageBinding."""
    root_uri = uri if isinstance(uri, StorageUri) else StorageUri.parse(str(uri))
    cache_key = (binding.cache_key(),)
    fs = cache.get(cache_key)
    if fs is None:
        fs = create_filesystem(binding)
        cache[cache_key] = fs
    return fs, root_uri


def describe_control_plane(
    *,
    product: str | None = None,
    base_uri: str | None = None,
    product_uri: str | None = None,
) -> dict[str, str]:
    """Return canonical `.firecube/` control-plane locations for one product."""
    from firecube.core.product import ensure_product_uri

    if product_uri:
        resolved_product_uri = StorageUri.parse(str(product_uri))
    else:
        product_name = str(product or "").strip("/").strip()
        base_uri_raw = str(base_uri or "").rstrip("/")
        if not product_name or not base_uri_raw:
            raise ManifestError(
                "describe_control_plane() requires product_uri or both product and base_uri."
            )
        normalized_base = StorageUri.parse(base_uri_raw).to_str()
        resolved_product_uri = StorageUri.parse(ensure_product_uri(normalized_base, product_name))

    control_root = resolved_product_uri.join(CONTROL_DIRNAME)
    return {
        "product_root": resolved_product_uri.to_str(),
        "control_root": control_root.to_str(),
        "latest_pointer": control_root.join(LATEST_POINTER).to_str(),
    }
