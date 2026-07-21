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

"""Helpers for building Intake catalogs for Firecube products.

Firecube owns catalog generation. It discovers catalogable dataset groups for
supported store formats, lets plugins optionally annotate or hide those groups,
and renders a generic Intake catalog from the resulting source list.
"""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem import create_filesystem
from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target, storage_uri_from_target
from firecube.core.zarr.validation import discover_groups


@dataclass(slots=True)
class CatalogGroupInfo:
    """Optional plugin annotations for one discovered catalog group."""

    include: bool = True
    name: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CatalogSourceSpec:
    """Description of a single Intake source backed by one discovered group."""

    name: str
    description: str
    group: str
    data_format: str = "zarr"
    metadata: dict[str, Any] = field(default_factory=dict)


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _group_path(store_root: str, group: str) -> str:
    group_clean = (group or "").strip("/")
    return store_root.rstrip("/") if not group_clean else f"{store_root.rstrip('/')}/{group_clean}"


def _group_target_uri(store_uri: str, group: str) -> str:
    group_clean = (group or "").strip("/")
    return store_uri.rstrip("/") if not group_clean else f"{store_uri.rstrip('/')}/{group_clean}"


def _storage_config_for_target(
    target_uri: str,
    storage_config: Any | None,
    storage_session: StorageSession | None = None,
) -> StorageConfig:
    if storage_config is not None:
        return storage_config
    if storage_session is not None:
        credentials = storage_session.driver.credentials
        return StorageConfig(
            storage_type="s3" if storage_session.product.product_uri.is_remote() else "local",
            endpoint_url=storage_session.driver.endpoint_url,
            access_key=credentials.access_key if credentials is not None else None,
            secret_key=credentials.secret_key if credentials is not None else None,
            region=storage_session.driver.region,
            path_style=storage_session.driver.path_style,
            storage_driver=storage_session.driver.driver,
        )
    raise ValueError(
        "storage_config is required for catalog generation; pass --storage-type and --storage-driver"
    )


def _session_for_target(
    target_uri: str,
    storage_config: Any | None,
    storage_session: StorageSession | None = None,
) -> StorageSession:
    if storage_session is not None:
        return storage_session
    store_uri = storage_uri_from_target(target_uri)
    cfg = _storage_config_for_target(target_uri, storage_config)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(store_uri, format="zarr", product_name=target_uri),
            driver=StorageDriverConfig.from_storage_config(cfg),
        )
    )


def _native_path_from_storage_uri(uri: StorageUri) -> str:
    if uri.protocol == "file":
        return uri.path
    return f"{uri.authority}{uri.path}"


def _normalize_source_suffix(group: str) -> str:
    text = (group or "").strip("/")
    if not text:
        return "root"
    return _NON_ALNUM_RE.sub("_", text.lower()).strip("_") or "root"


def default_catalog_source_name(plugin_name: str, group: str) -> str:
    """Return a deterministic Intake source name for a catalog group."""
    return f"{plugin_name}_{_normalize_source_suffix(group)}"


def default_catalog_description(product: str, group: str) -> str:
    """Return a generic human-readable description for a catalog group."""
    group_label = (group or "").strip("/") or "/"
    return f"Firecube product '{product}' group '{group_label}'."


def default_catalog_metadata(plugin_name: str, product: str, group: str) -> dict[str, Any]:
    """Return generic structural metadata for a discovered group."""
    cleaned = (group or "").strip("/")
    segments = cleaned.split("/") if cleaned else []
    return {
        "plugin": plugin_name,
        "product": product,
        "group": cleaned or "/",
        "group_segments": segments,
        "group_depth": len(segments),
    }


def detect_catalog_data_format(store_uri: str) -> str:
    """Infer the catalog data format from the product URI suffix."""
    lowered = str(store_uri).lower().rstrip("/")
    if lowered.endswith(".zarr"):
        return "zarr"
    if lowered.endswith(".parquet"):
        return "parquet"
    raise ValueError(
        "Unsupported catalog product format. Expected a '.zarr' or '.parquet' product, "
        f"got '{store_uri}'."
    )


def _read_node_type(
    store_uri: str,
    group: str,
    storage_config: Any | None = None,
    storage_session: StorageSession | None = None,
) -> str | None:
    import zarr

    handle = _session_for_target(store_uri, storage_config, storage_session).zarr.create_store(
        uri=storage_uri_from_target(store_uri),
        mode="r",
    )
    try:
        node = zarr.open(**handle.zarr_kwargs(), path=(group or None), mode="r")
    except Exception:
        return None
    payload = getattr(node, "metadata", None)
    if payload is None:
        return None
    to_dict = getattr(payload, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
    if not isinstance(payload, dict):
        return None
    node_type = payload.get("node_type")
    return str(node_type) if node_type is not None else None


def _resolve_dataset_store_handle(
    target_uri: str,
    storage_config: Any | None,
    storage_session: StorageSession | None = None,
) -> ZarrStoreHandle:
    """Build a uniform zarr-store handle for ``target_uri``.

    Routes remote targets through the session Zarr API so the configured
    endpoint and credentials are honored on every zarr open. Local targets and
    config-less calls short-circuit to a plain ``ZarrStoreHandle`` with the URI
    as the store and ``storage_options=None`` (mirrors the converter's
    ``_resolve_source_store`` pattern).
    """
    if not is_remote_target(target_uri) or storage_config is None:
        return ZarrStoreHandle(store=target_uri, storage_options=None, target_uri=target_uri)
    return _session_for_target(target_uri, storage_config, storage_session).zarr.create_store(
        uri=storage_uri_from_target(target_uri),
        mode="r",
    )


def _contains_zarr_arrays(
    target_uri: str,
    storage_config: Any | None,
    storage_session: StorageSession | None = None,
) -> bool:
    import zarr

    handle = _resolve_dataset_store_handle(target_uri, storage_config, storage_session)
    try:
        node = zarr.open(**handle.zarr_kwargs(), mode="r")
    except Exception:
        return False
    if not isinstance(node, zarr.Group):
        return False
    return any(True for _name, _array in node.arrays())


def _is_readable_dataset_group(
    store_uri: str,
    group: str,
    *,
    storage_config: Any | None = None,
    storage_session: StorageSession | None = None,
) -> bool:
    target_uri = _group_target_uri(store_uri, group)
    handle = _resolve_dataset_store_handle(target_uri, storage_config, storage_session)
    open_kwargs = {**handle.zarr_kwargs(), "group": None, "chunks": None, "consolidated": False}
    try:
        ds = xr.open_zarr(**open_kwargs)  # pyright: ignore[reportArgumentType]
    except Exception:
        return _contains_zarr_arrays(target_uri, storage_config, storage_session)
    try:
        return len(ds.data_vars) > 0
    finally:
        with contextlib.suppress(Exception):
            ds.close()


def discover_catalog_groups(
    store_uri: str,
    *,
    storage_config: Any | None = None,
    storage_session: StorageSession | None = None,
    max_depth: int = 8,
) -> list[str]:
    """Discover catalogable groups within a supported store."""
    data_format = detect_catalog_data_format(store_uri)
    if data_format == "parquet":
        return discover_parquet_catalog_groups(
            store_uri,
            storage_config=storage_config,
            storage_session=storage_session,
        )

    storage_config_for_discovery = _storage_config_for_target(
        store_uri,
        storage_config,
        storage_session,
    )

    readable: list[str] = []
    for group in discover_groups(
        store_uri,
        storage_config=storage_config_for_discovery,
        max_depth=max_depth,
        strict=True,
    ):
        node_type = _read_node_type(
            store_uri,
            group,
            storage_config=storage_config_for_discovery,
            storage_session=storage_session,
        )
        if node_type == "array":
            continue
        if _is_readable_dataset_group(
            store_uri,
            group,
            storage_config=storage_config_for_discovery,
            storage_session=storage_session,
        ):
            readable.append(group)
    return sorted(set(readable))


def discover_parquet_catalog_groups(
    store_uri: str,
    *,
    storage_config: Any | None = None,
    storage_session: StorageSession | None = None,
) -> list[str]:
    """Discover leaf parquet groups within a parquet product root."""
    store_storage_uri = storage_uri_from_target(store_uri)
    if storage_session is not None:
        fs = storage_session.fs()
    else:
        cfg = _storage_config_for_target(store_uri, storage_config)
        binding = StorageBinding(
            identity=ProductIdentity.from_uri(
                store_storage_uri,
                format="parquet",
                product_name=store_uri,
            ),
            driver=StorageDriverConfig.from_storage_config(cfg),
        )
        fs = create_filesystem(binding)
    root_clean = _native_path_from_storage_uri(store_storage_uri).rstrip("/")
    if not root_clean:
        return []

    try:
        if fs.exists(store_storage_uri) and not fs.isdir(store_storage_uri):
            return [""]
    except Exception:
        pass

    try:
        candidates = fs.find(store_storage_uri)
    except Exception:
        candidates = []

    groups: set[str] = set()
    root_path = PurePosixPath(root_clean)
    for candidate in candidates:
        normalized = (
            _native_path_from_storage_uri(candidate)
            if isinstance(candidate, StorageUri)
            else str(candidate)
        ).rstrip("/")
        if not normalized.endswith(".parquet"):
            continue
        if "/.firecube/" in f"/{normalized}/":
            continue
        candidate_path = PurePosixPath(normalized)
        parent = candidate_path.parent
        try:
            relative = parent.relative_to(root_path)
        except ValueError:
            if normalized == root_clean:
                groups.add("")
            continue
        relative_text = str(relative)
        groups.add("" if relative_text == "." else relative_text.strip("/"))

    return sorted(groups)


def build_catalog_source_specs(
    *,
    plugin_name: str,
    product: str,
    store_uri: str,
    groups: list[str],
    group_info_resolver: Any | None = None,
    storage_config: Any | None = None,
    storage_session: StorageSession | None = None,
) -> list[CatalogSourceSpec]:
    """Build catalog source specs from discovered groups and optional plugin annotations."""
    data_format = detect_catalog_data_format(store_uri)
    resolver_storage_config = (
        _storage_config_for_target(store_uri, storage_config, storage_session)
        if group_info_resolver is not None
        else None
    )
    specs: list[CatalogSourceSpec] = []
    for group in groups:
        info = None
        if callable(group_info_resolver):
            info = group_info_resolver(group, store_uri, storage_config=resolver_storage_config)
        if info is None:
            info = CatalogGroupInfo()
        if not isinstance(info, CatalogGroupInfo):
            raise TypeError(
                "catalog_group_info() must return CatalogGroupInfo or None, "
                f"got {type(info).__name__}."
            )
        if not info.include:
            continue

        metadata = default_catalog_metadata(plugin_name, product, group)
        metadata["data_format"] = data_format
        metadata.update(info.metadata)
        specs.append(
            CatalogSourceSpec(
                name=info.name or default_catalog_source_name(plugin_name, group),
                description=info.description or default_catalog_description(product, group),
                group=group,
                data_format=data_format,
                metadata=metadata,
            )
        )
    return specs


def _catalog_entry_args(
    *,
    store_uri: str,
    spec: CatalogSourceSpec,
    storage_opts: dict[str, Any],
) -> dict[str, Any]:
    if spec.data_format == "parquet":
        args: dict[str, Any] = {
            "urlpath": _group_target_uri(store_uri, spec.group),
            "engine": "pyarrow",
        }
        if storage_opts:
            args["storage_options"] = storage_opts
        return args

    args = {
        "urlpath": store_uri,
        "group": spec.group,
        "consolidated": False,
        "chunks": "auto",
    }
    if storage_opts:
        args["storage_options"] = storage_opts
    return args


def _catalog_entry_driver(spec: CatalogSourceSpec) -> str:
    if spec.data_format == "parquet":
        return "parquet"
    # Use the registered Intake driver short name (entry point ``zarr`` ->
    # ``intake_xarray.xzarr:ZarrSource``). A full module path such as
    # ``intake_xarray.zarr.ZarrSource`` is not resolvable by Intake and fails
    # at open time with "No plugins loaded for this entry".
    return "zarr"


def _default_storage_options(store_uri: str) -> dict[str, Any]:
    """Build a generic storage_options block for Intake.

    Notes
    -----
    To keep the catalog portable and avoid embedding secrets, this helper
    uses environment placeholders. Users are expected to export the matching
    FIRECUBE_* variables in their environment when opening the catalog.

    Canonical variables (mirrors `export_storage_config_to_env`):
      - FIRECUBE_ENDPOINT_URL
      - FIRECUBE_ACCESS_KEY
      - FIRECUBE_SECRET_KEY
    """
    parsed = urlparse(store_uri)
    if parsed.scheme != "s3":
        # Local/other backends rely on fsspec/xarray defaults and env.
        return {}

    return {
        "anon": False,
        "client_kwargs": {
            "endpoint_url": "${FIRECUBE_ENDPOINT_URL}",
        },
        "key": "${FIRECUBE_ACCESS_KEY}",
        # env-var template placeholder rendered downstream, not a credential value
        "secret": "${FIRECUBE_SECRET_KEY}",  # nosec B105
    }


def build_intake_catalog(
    *,
    catalog_name: str,
    catalog_description: str,
    collection_id: str,
    store_uri: str,
    sources: list[CatalogSourceSpec],
    include_storage_options: bool = True,
) -> dict[str, Any]:
    """Construct an Intake catalog dictionary for the given product store.

    Parameters
    ----------
    catalog_name:
        Logical name for the catalog (e.g. plugin id or product name).
    catalog_description:
        High-level description of the catalog.
    store_uri:
        Root URI of the store, such as ``s3://bucket/product.zarr`` or
        ``s3://bucket/product.parquet``.
    sources:
        List of CatalogSourceSpec entries describing which groups to expose.
    include_storage_options:
        When True, attach a generic ``storage_options`` block for S3 stores
        based on FIRECUBE_* environment placeholders.
    """
    catalog: dict[str, Any] = {
        "metadata": {
            "version": 1,
            "name": catalog_name,
            "description": catalog_description,
            "collection_id": collection_id,
        },
        "sources": {},
    }

    storage_opts = _default_storage_options(store_uri) if include_storage_options else {}

    for spec in sources:
        source_entry: dict[str, Any] = {
            "description": spec.description,
            "driver": _catalog_entry_driver(spec),
            "args": _catalog_entry_args(store_uri=store_uri, spec=spec, storage_opts=storage_opts),
        }

        source_metadata = dict(spec.metadata)
        source_metadata["collection_id"] = collection_id
        if source_metadata:
            source_entry["metadata"] = source_metadata

        catalog["sources"][spec.name] = source_entry

    return catalog
