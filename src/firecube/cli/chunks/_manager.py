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
import os
from pathlib import Path
from typing import overload

import click

from firecube.cli._ctx import get_config, get_storage_config
from firecube.cli._product import resolve_product_identity
from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.runtime import identity_from_storage_config
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

log = logging.getLogger("firecube.cli")


def resolve_manager(
    ctx: click.Context,
    workspace: Path | None,
    product_uri: StorageUri | None = None,
    *,
    product_name: str | None = None,
) -> ChunkManager:
    """Resolve a ChunkManager for chunk operations using shared config + overrides."""
    ctx.ensure_object(dict)
    base_workspace = workspace if workspace is not None else ctx.obj.get("workspace")
    cfg = get_config(ctx)
    storage_config: StorageConfig | None = None
    env_storage_type_present = bool(os.environ.get("FIRECUBE_STORAGE_TYPE"))
    if cfg.get("storage") or env_storage_type_present:
        storage_config = get_storage_config(ctx, set_global=False)

    if product_uri is not None and product_name is not None:
        driver = StorageDriverConfig.from_storage_config_or_default(storage_config)
        binding = StorageBinding(
            identity=ProductIdentity.from_uri(
                uri=product_uri,
                format="zarr",
                product_name=product_name,
            ),
            driver=driver,
        )
        manager = ChunkManager(binding=binding, workspace=base_workspace)
        ctx.call_on_close(manager.close)
        log.info("Chunks using product URI target (product_uri=%s)", manager.base_uri)
        return manager

    base_output = None
    if product_uri is not None:
        base_output = product_uri.to_str()

    if base_output is None and storage_config is not None:
        base_uri_from_config = _base_uri_from_storage_config(storage_config)
        base_output = base_uri_from_config.to_str() if base_uri_from_config is not None else None

    if base_output is None:
        msg = (
            "Chunk operations require a full product URI or a [storage] configuration in config.toml "
            "(no product URI or storage configuration found)."
        )
        log.error(msg)
        raise click.ClickException(msg)

    base_uri = _storage_uri_from_base(base_output)
    driver = StorageDriverConfig.from_storage_config_or_default(storage_config)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(
            base_uri.join("__firecube_controlplane__"),
            "zarr",
            product_name="__firecube_controlplane__",
        ),
        driver=driver,
    )
    manager = ChunkManager(binding=binding, workspace=base_workspace)
    ctx.call_on_close(manager.close)

    if storage_config is not None:
        config_identity = identity_from_storage_config(storage_config)
        bucket = config_identity.product_uri.authority if config_identity is not None else None
        log.info(
            "Chunks using storage configuration (storage_type=%s, bucket=%s, base_uri=%s)",
            storage_config.storage_type,
            bucket,
            manager.base_uri,
        )
        if not ctx.obj.get("chunks_quiet"):
            source = "env" if env_storage_type_present else "config"
            click.echo(
                f"[chunks] storage: type={storage_config.storage_type} "
                f"base_uri={manager.base_uri} (from {source})",
                err=True,
            )
    else:
        log.info("Chunks using product URI base (base_uri=%s)", manager.base_uri)

    return manager


def _storage_uri_from_base(raw: str) -> StorageUri:
    if "://" not in raw:
        raise click.UsageError(
            "Product target must resolve to a full URI like 's3://bucket/path' or "
            f"'file:///abs/path'. Bare paths are no longer supported. Got: {raw!r}"
        )
    return StorageUri.parse(raw)


def _base_uri_from_storage_config(storage_config: StorageConfig) -> StorageUri | None:
    identity = identity_from_storage_config(storage_config)
    return identity.product_uri if identity is not None else None


@overload
def resolve_cli_product(product: str, *, format: str = "zarr") -> tuple[str, StorageUri]: ...


@overload
def resolve_cli_product(product: None, *, format: str = "zarr") -> tuple[None, None]: ...


def resolve_cli_product(
    product: str | None, *, format: str = "zarr"
) -> tuple[str | None, StorageUri | None]:
    if product is None:
        return None, None
    if "://" not in product:
        return product, None
    product_label = product.rstrip("/").rsplit("/", 1)[-1]
    identity = resolve_product_identity(
        product, format=format, product_name=product_label, option_name="--product-name"
    )
    return identity.product_name, identity.product_uri


def storage_config_from_ctx(ctx: click.Context) -> StorageConfig:
    storage_config_obj: StorageConfig | None = ctx.obj.get("storage_config")
    if storage_config_obj is None:
        raise click.ClickException(
            "Storage configuration not available. Define [storage] in config.toml or use --manifest-only."
        )
    return storage_config_obj
