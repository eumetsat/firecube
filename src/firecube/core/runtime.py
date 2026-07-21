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

"""Runtime wiring and global state for Firecube configuration.

This module handles side-effects like setting global variables and exporting
environment variables. It depends on `firecube.core.config` for types
and loading logic.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

from firecube.core.config import (
    StorageConfig,
    build_storage_config,
    derive_target_uri,
    load_config_file,
)
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.uri import StorageUri

log = logging.getLogger("firecube.core.runtime")

_global_storage_config: StorageConfig | None = None


def _target_uri_from_config(identity: ProductIdentity) -> str:
    """Render the FIRECUBE_TARGET_URI value from a `ProductIdentity`.

    Preserves legacy env-var semantics: bare local path for `file://` URIs,
    canonical URI form for remote (`s3://`, `gs://`) URIs.
    """
    if identity.product_uri.protocol == "file":
        return identity.product_uri.path
    return identity.product_uri.to_str()


def set_global_storage_config(config: StorageConfig) -> None:
    """Set the global storage configuration."""
    global _global_storage_config
    config.validate()
    _global_storage_config = config


def get_global_storage_config() -> StorageConfig:
    """Return the global storage configuration."""
    if _global_storage_config is None:
        raise RuntimeError("Global storage configuration not set. Initialise with CLI args first.")
    return _global_storage_config


def export_storage_config_to_env(
    config: StorageConfig,
    identity: ProductIdentity | None = None,
    *,
    env: MutableMapping[str, str] | None = None,
) -> None:
    """Publish storage configuration to environment variables for child processes.

    Parameters
    ----------
    config:
        Non-location storage config (storage_type, endpoint_url, credentials,
        region, path_style, storage_driver).
    identity:
        Optional product identity providing location fields (bucket /
        target_path / target_uri). When ``None``, location vars are omitted.
    env:
        Optional mutable mapping to write into; defaults to ``os.environ``.

    Notes
    -----
    We intentionally export both FIRECUBE_* and AWS_* style variables:
      - FIRECUBE_* is used by Firecube internals and helpers.
      - AWS_* allows libraries such as fsspec/xarray/zarr to discover
        credentials and endpoints when working with S3-compatible storage.
    """
    target_env: MutableMapping[str, str] = os.environ if env is None else env

    target_env["FIRECUBE_STORAGE_TYPE"] = config.storage_type
    if config.endpoint_url:
        target_env["FIRECUBE_ENDPOINT_URL"] = config.endpoint_url
    if config.access_key:
        target_env["FIRECUBE_ACCESS_KEY"] = config.access_key
    if config.secret_key:
        target_env["FIRECUBE_SECRET_KEY"] = config.secret_key
    if config.region:
        target_env["FIRECUBE_REGION"] = config.region
    target_env["FIRECUBE_PATH_STYLE"] = str(config.path_style).lower()
    target_env["FIRECUBE_STORAGE_DRIVER"] = config.storage_driver

    if identity is not None:
        if identity.product_uri.authority:
            target_env["FIRECUBE_BUCKET"] = identity.product_uri.authority
        if identity.product_uri.protocol == "file":
            target_env["FIRECUBE_TARGET_PATH"] = identity.product_uri.path
        target_env["FIRECUBE_TARGET_URI"] = _target_uri_from_config(identity)

    if config.access_key:
        target_env["AWS_ACCESS_KEY_ID"] = config.access_key
    if config.secret_key:
        target_env["AWS_SECRET_ACCESS_KEY"] = config.secret_key
    if config.region:
        target_env["AWS_DEFAULT_REGION"] = config.region
    if config.endpoint_url:
        target_env["AWS_ENDPOINT_URL"] = config.endpoint_url
    target_env["AWS_S3_ADDRESSING_STYLE"] = "path" if config.path_style else "virtual"


def identity_from_storage_config(storage_config: StorageConfig) -> ProductIdentity | None:
    """Synthesize a `ProductIdentity` from a resolved `StorageConfig`.

    Bridges the legacy bridge-attr storage view (``target_path`` / ``bucket``
    set on ``StorageConfig`` by ``build_storage_config``) to the
    ``ProductIdentity`` shape consumed by env-export and CLI bridge sites.
    The remaining bridge-attr reads live inside ``derive_target_uri`` (see
    config.py); this helper is the single seam between resolved config and
    identity.

    Returns ``None`` when the resolved config lacks a base URI (e.g. local
    storage_type with no ``target_path`` set yet).
    """
    try:
        base_uri_str = derive_target_uri(storage_config)
    except ValueError:
        return None

    if storage_config.storage_type == "local":
        base_uri = StorageUri.from_local_path(base_uri_str)
    else:
        base_uri = StorageUri.parse(base_uri_str)

    name = base_uri.path.rstrip("/").rsplit("/", 1)[-1]
    return ProductIdentity(
        product_name=name,
        product_uri=base_uri,
        control_root_uri=base_uri.join(".firecube"),
        format="zarr",
    )


def resolve_storage_config(
    *,
    config_file: Path | str | None = None,
    env: Mapping[str, str] = os.environ,  # type: ignore[assignment]
    overrides: dict[str, Any] | None = None,
    export_env: bool = True,
    set_global: bool = True,
) -> StorageConfig:
    """Resolve StorageConfig from config.toml + env + overrides.

    This is a convenience wrapper used by CLI entrypoints to keep the
    load/build/validate/export wiring consistent across commands.

    Notes
    -----
    - Precedence is implemented in `build_storage_config(...)`.
    - This helper does not depend on click (so it can be used by non-CLI code).
    """
    cfg = load_config_file(config_file)
    storage_config = build_storage_config(cfg, env, overrides or {})
    storage_config.validate()

    if set_global:
        set_global_storage_config(storage_config)
    if export_env:
        identity = identity_from_storage_config(storage_config)
        export_storage_config_to_env(storage_config, identity)

    return storage_config
