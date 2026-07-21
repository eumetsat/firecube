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

"""Filesystem helpers via fsspec with consistent StorageConfig wiring.

This module centralizes all fsspec operations so S3 credentials, endpoints,
and path-style behavior are consistent across CLI, plugins, and tools.

Functions:
- create_filesystem: Build a StorageFilesystem from a StorageBinding
- fs_kwargs_for_uri: Build fsspec kwargs from StorageConfig
- safe_exists: Check file existence with error handling
- safe_open: Open file with error handling
- path_stats: Get bytes/files count for a path (local or remote)
- delete_path: Delete a path (file or directory) from storage
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import fsspec

from firecube.core.filesystem.instrumentation import (
    InstrumentedFilesystem,
    active_filesystem_metrics,
)
from firecube.core.filesystem.protocol import StorageFilesystem, StorageFilesystemFull
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri

log = logging.getLogger(__name__)

__all__ = [
    "create_filesystem",
    "create_filesystem_for_uri",
    "create_session_zarr_store",
    "delete_path",
    "ensure_directory",
    "fs_kwargs_for_uri",
    "path_stats",
    "safe_exists",
    "safe_open",
]


def create_session_zarr_store(
    *,
    uri: StorageUri,
    storage_config: Any,
    mode: str,
) -> Any:
    from firecube.core.filesystem.store_factory import create_zarr_store

    return create_zarr_store(
        uri=uri.to_str(),
        storage_config=storage_config,
        mode=mode,
    )


def ensure_directory(path: Path | str) -> Path:
    """Ensure a directory exists, creating parents if needed."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _protocol_for_uri(uri: str) -> str:
    """Extract the protocol (e.g. 's3', 'file') from a URI."""
    from firecube.core.uris import infer_target_protocol

    return infer_target_protocol(uri)


def _s3_fs_kwargs_from_storage_config(storage_config: Any) -> dict[str, Any]:
    """Build s3fs kwargs from a StorageConfig-like object (duck-typed)."""
    endpoint_url = getattr(storage_config, "endpoint_url", None)
    region = getattr(storage_config, "region", None)
    access_key = getattr(storage_config, "access_key", None)
    secret_key = getattr(storage_config, "secret_key", None)
    path_style = getattr(storage_config, "path_style", True)

    fs_kwargs: dict[str, Any] = {}
    client_kwargs: dict[str, Any] = {}
    config_kwargs: dict[str, Any] = {}

    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        fs_kwargs["client_kwargs"] = client_kwargs

    if path_style is None or bool(path_style):
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


def _summarise_error(exc: Exception, fs: Any = None) -> str:
    """Extract a readable error message from nested exceptions."""
    visited = set()
    parts: list[str] = []
    current: BaseException | None = exc
    while current and current not in visited:
        visited.add(current)
        args = getattr(current, "args", ())
        text = next((str(arg) for arg in args if isinstance(arg, str) and arg), "")
        if text:
            parts.append(text)
        current = current.__cause__ or current.__context__
    if not parts:
        return f"{exc.__class__.__name__}"
    cleaned: list[str] = []
    for part in parts:
        if part not in cleaned:
            cleaned.append(part)
    return "; ".join(cleaned)


def _sanitize_subpath(subpath: str, *, allow_manifest_paths: bool = False) -> str:
    """Validate and normalize a relative subpath for deletion."""
    subpath = str(subpath or "").strip().strip("/")
    if not subpath:
        raise ValueError("subpath must be a non-empty relative path")
    parts = [p for p in subpath.split("/") if p]
    if any(p in {".", ".."} for p in parts):
        raise ValueError(f"Refusing unsafe subpath: {subpath!r}")
    if not allow_manifest_paths and parts[0] == ".firecube":
        raise ValueError("Refusing to delete internal Firecube control-plane paths.")
    return "/".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_filesystem(binding: StorageBinding) -> StorageFilesystemFull:
    """Create a StorageFilesystem for the given storage binding."""
    if binding.driver.driver == "obstore":
        from firecube.core.filesystem.obstore_backend import ObstoreFilesystem

        fs: StorageFilesystemFull = ObstoreFilesystem.from_binding(binding)
    else:
        from firecube.core.filesystem.fsspec_backend import FsspecFilesystem

        fs = FsspecFilesystem.from_binding(binding)
    if active_filesystem_metrics() is not None:
        return cast(StorageFilesystemFull, InstrumentedFilesystem(fs))
    return fs


def create_filesystem_for_uri(
    uri: str,
    storage_config: Any,
    *,
    format: str,
) -> tuple[StorageFilesystem, StorageUri]:
    """Create a driver-aware filesystem for a concrete product/output URI."""
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.driver_config import StorageDriverConfig

    uri_obj = (
        StorageUri.parse(uri)
        if "://" in str(uri)
        else StorageUri.from_local_path(Path(uri).expanduser().resolve())  # firecube: STORAGE-URI
    )
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri_obj, format, product_name=uri),
        driver=StorageDriverConfig.from_storage_config(storage_config),
    )
    return create_filesystem(binding), uri_obj


def fs_kwargs_for_uri(uri: str, storage_config: Any | None = None) -> dict[str, Any]:
    """Return fsspec kwargs for opening `uri` with an optional StorageConfig."""
    if not isinstance(uri, str) or not uri:
        return {}

    protocol = _protocol_for_uri(uri)
    if protocol != "s3" or storage_config is None:
        return {}

    return _s3_fs_kwargs_from_storage_config(storage_config)


def _open_fsspec_url(
    uri: str,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
) -> tuple[Any, str]:
    """Private transitional fsspec URL opener for legacy callers."""
    kwargs: dict[str, Any] = {}
    kwargs.update(fs_kwargs_for_uri(uri, storage_config))
    if storage_options:
        kwargs.update(storage_options)
    fs, root = fsspec.core.url_to_fs(uri, **kwargs)
    if active_filesystem_metrics() is not None and not isinstance(fs, InstrumentedFilesystem):
        fs = InstrumentedFilesystem(fs)
    return fs, root


def _build_fsspec_filesystem(protocol: str, kwargs: dict[str, Any]) -> Any:
    return fsspec.filesystem(protocol, **kwargs)


def safe_exists(fs: Any, path: str, *, filesystem: Any = None) -> bool:
    """Safely check if file exists, handling connectivity issues.

    Returns False if existence can't be determined (e.g., network error).
    """
    active_fs = filesystem if filesystem is not None else fs
    try:
        return active_fs.exists(path)
    except Exception as e:
        log.warning(f"Failed to check existence of {path}: {_summarise_error(e, active_fs)}")
        return False


def safe_open(fs: Any, path: str, mode: str = "r", **kwargs):
    """Safely open file with error handling.

    Returns None if the file can't be opened.
    """
    try:
        return fs.open(path, mode, **kwargs)
    except Exception as e:
        log.warning(f"Failed to open {path}: {_summarise_error(e, fs)}")
        return None


def path_stats(
    uri: str,
    *,
    storage_config: Any | None = None,
    exclude_substrings: Iterable[str] | None = None,
) -> dict[str, int]:
    """Get bytes and file count for a path (local or remote).

    Works with both local paths and S3 URIs.

    Args:
        uri: Local path or remote URI
        storage_config: Optional StorageConfig for S3 credentials
        exclude_substrings: Path substrings to exclude (default: manifest paths)

    Returns:
        Dict with 'bytes' and 'files' keys
    """
    uri = str(uri or "")
    if not uri:
        return {"bytes": 0, "files": 0}

    # Fast path for local directories
    from firecube.core.uris import is_remote_target

    if not is_remote_target(uri):
        path = Path(uri)  # firecube: STORAGE-URI
        if not path.exists():
            return {"bytes": 0, "files": 0}
        if path.is_file():
            return {"bytes": path.stat().st_size, "files": 1}

        excluded = tuple(exclude_substrings or (".firecube/",))
        total_bytes = 0
        total_files = 0
        for p in path.rglob("*"):
            if p.is_file():
                path_str = str(p)
                if excluded and any(token in path_str for token in excluded):
                    continue
                total_bytes += p.stat().st_size
                total_files += 1
        return {"bytes": total_bytes, "files": total_files}

    # Remote path - use fsspec
    excluded = tuple(exclude_substrings or ("/.firecube/",))

    if storage_config is not None:
        from firecube.core.product.identity import ProductIdentity
        from firecube.core.storage.driver_config import StorageDriverConfig

        uri_obj = StorageUri.parse(uri)
        binding = StorageBinding(
            identity=ProductIdentity.from_uri(uri_obj, "zarr", product_name=uri),
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
        fs_driver = cast(Any, create_filesystem(binding))
        try:
            entries = fs_driver.find(uri_obj)
        except Exception:
            return {"bytes": 0, "files": 0}

        total_bytes = 0
        total_files = 0
        for entry in entries:
            path_str = entry.to_str()
            if excluded and any(token in path_str for token in excluded):
                continue
            try:
                info = fs_driver.info(entry)
                size = int(info.get("size") or info.get("Size") or 0)
            except Exception:
                size = 0
            total_bytes += max(0, size)
            total_files += 1
        return {"bytes": int(total_bytes), "files": int(total_files)}

    fs, root = fsspec.core.url_to_fs(uri, **fs_kwargs_for_uri(uri, storage_config))
    try:
        listing = fs.find(root, detail=True)
    except Exception:
        return {"bytes": 0, "files": 0}

    total_bytes = 0
    total_files = 0

    if isinstance(listing, dict):
        items = listing.items()
    else:
        items = ((path, {}) for path in (listing or []))

    for path, info in items:
        path_str = str(path)
        if excluded and any(token in path_str for token in excluded):
            continue
        size = 0
        if isinstance(info, dict):
            try:
                size = int(info.get("size") or info.get("Size") or 0)
            except Exception:
                size = 0
        total_bytes += max(0, size)
        total_files += 1

    return {"bytes": int(total_bytes), "files": int(total_files)}


def delete_path(
    base_uri: str,
    subpath: str,
    *,
    storage_config: Any | None = None,
    allow_manifest_paths: bool = False,
    dry_run: bool = False,
    filesystem: Any = None,
) -> dict[str, Any]:
    """Delete a path (file or directory) from storage.

    Args:
        base_uri: Base URI (e.g., 's3://bucket/store.zarr' or '/data/store.zarr')
        subpath: Relative path within base_uri to delete (e.g., 'F072')
        storage_config: Optional StorageConfig for S3 credentials
        allow_manifest_paths: If True, allow deleting manifest paths
        dry_run: If True, only check existence without deleting

    Returns:
        Dict with keys: path, exists, deleted
    """
    base_uri = str(base_uri or "").rstrip("/")
    subpath = _sanitize_subpath(subpath, allow_manifest_paths=allow_manifest_paths)
    target_uri = f"{base_uri}/{subpath}"

    if filesystem is not None:
        exists = bool(filesystem.exists(target_uri))
        if not exists:
            return {"path": target_uri, "exists": False, "deleted": False}
        if dry_run:
            return {"path": target_uri, "exists": True, "deleted": False}
        filesystem.rm(target_uri, recursive=True)
        return {"path": target_uri, "exists": True, "deleted": True}

    fs, root = fsspec.core.url_to_fs(target_uri, **fs_kwargs_for_uri(target_uri, storage_config))
    exists = bool(fs.exists(root))
    if not exists:
        return {"path": target_uri, "exists": False, "deleted": False}
    if dry_run:
        return {"path": target_uri, "exists": True, "deleted": False}

    fs.rm(root, recursive=True)
    return {"path": target_uri, "exists": True, "deleted": True}
