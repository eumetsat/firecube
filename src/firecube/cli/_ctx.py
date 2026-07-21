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

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import click

from firecube.core.config import StorageConfig, load_config_file
from firecube.core.runtime import identity_from_storage_config, resolve_storage_config

log = logging.getLogger("firecube.cli")


def get_config_file(ctx: click.Context) -> Path | None:
    ctx.ensure_object(dict)
    value = ctx.obj.get("config_file")
    return value if isinstance(value, Path) else None


def get_config(ctx: click.Context) -> dict[str, Any]:
    """Load config TOML as a plain dictionary."""
    return load_config_file(get_config_file(ctx))


def get_storage_config(
    ctx: click.Context,
    *,
    overrides: Mapping[str, str | None] | None = None,
    set_global: bool = False,
    cache: bool = True,
) -> StorageConfig:
    """Resolve StorageConfig from config + env + optional overrides.

    This centralizes common CLI behavior:
    - reads `--config-file` stored in `ctx.obj["config_file"]`
    - caches the resolved StorageConfig in `ctx.obj["storage_config"]`
    - wraps errors as Click-friendly exceptions
    """
    ctx.ensure_object(dict)
    if cache and overrides is None:
        cached = ctx.obj.get("storage_config")
        if isinstance(cached, StorageConfig):
            return cached

    try:
        storage_config = resolve_storage_config(
            config_file=get_config_file(ctx),
            overrides=dict(overrides) if overrides is not None else None,
            set_global=set_global,
        )
    except Exception as exc:
        raise click.ClickException(f"Failed to resolve storage configuration: {exc}") from exc

    if cache and overrides is None:
        ctx.obj["storage_config"] = storage_config
        identity = identity_from_storage_config(storage_config)
        product_uri = identity.product_uri if identity is not None else None
        bucket = product_uri.authority if product_uri is not None else None
        target_path = (
            product_uri.path if product_uri is not None and product_uri.protocol == "file" else None
        )
        log.debug(
            "Resolved StorageConfig (type=%s bucket=%s endpoint=%s target=%s)",
            storage_config.storage_type,
            bucket,
            storage_config.endpoint_url,
            target_path,
        )
    return storage_config
