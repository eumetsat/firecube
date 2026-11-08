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
from dataclasses import dataclass
from pathlib import Path

import click

from firecube.cli._ctx import get_config, get_storage_config
from firecube.cli._uri_policy import parse_and_validate_uri
from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.product.open import Product, open_product
from firecube.core.runtime import identity_from_storage_config
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

log = logging.getLogger("firecube.cli")

# The path under the base URI that the scanning binding opens; it names no product.
_SCAN_LABEL = "__firecube_controlplane__"


@dataclass(frozen=True, eq=False)
class BoundProduct:
    """A ChunkManager bound to one product store.

    Attributes:
        manager: The manager bound to the store.
        label: The product label the store records.
        uri: The URI of the store.
    """

    manager: ChunkManager
    label: str
    uri: StorageUri


def require_one_scope(target: str | None, all_products: bool) -> None:
    """Refuse a call that passes both or neither of `--target` and `--all-products`."""
    if bool(target) == all_products:
        raise click.UsageError("Provide exactly one of --target or --all-products.")


def scoped_products(
    ctx: click.Context,
    workspace: Path | None,
    target: str | None,
    all_products: bool,
    *,
    product_filesystem: bool = False,
) -> list[BoundProduct]:
    """Bind the product at `target`, or with `all_products` every product under the base URI.

    With `product_filesystem`, each manager deletes objects through a filesystem bound to
    its own product.

    Raises:
        click.UsageError: Both or neither of `--target` and `--all-products` are given.
    """
    require_one_scope(target, all_products)
    if target:
        return [target_product(ctx, workspace, target, product_filesystem=product_filesystem)]
    return scan_products(ctx, workspace, product_filesystem=product_filesystem)


def target_product(
    ctx: click.Context, workspace: Path | None, target: str, *, product_filesystem: bool = False
) -> BoundProduct:
    """Bind a ChunkManager to the product at `target`, under the label the store records.

    The [storage] configuration supplies driver settings only, never a base URI.

    Raises:
        click.BadParameter: `target` is not an admitted product URI.
        ProductLabelMissingError: The store records no product label.
    """
    uri = parse_and_validate_uri(target, param_hint="--target")
    driver = StorageDriverConfig.from_storage_config_or_default(_optional_storage_config(ctx))
    product = ctx.with_resource(open_product(uri, format="zarr", driver=driver))
    manager = _product_manager(ctx, workspace, product, product_filesystem)
    log.debug("Chunks using product URI target (product_uri=%s)", uri.to_str())
    return BoundProduct(manager=manager, label=product.label, uri=uri)


def scan_products(
    ctx: click.Context, workspace: Path | None, *, product_filesystem: bool = False
) -> list[BoundProduct]:
    """Bind a ChunkManager to each product store under the [storage] base URI.

    Prints the base URI to stderr before any storage call. Binds each store by its own
    URI and label; skips a store that records no label, with a stderr line.

    Raises:
        click.UsageError: No [storage] configuration names a base URI.
    """
    storage_config = _optional_storage_config(ctx)
    base = _base_uri_from_storage_config(storage_config) if storage_config is not None else None
    if storage_config is None or base is None:
        raise click.UsageError(
            "--all-products scans the base URI of the [storage] configuration, and none is "
            "set. Define [storage] in config.toml, or pass --target."
        )
    base_uri = _storage_uri_from_base(base.to_str())
    click.echo(f"[chunks] scanning base {base_uri.to_str()}", err=True)
    driver = StorageDriverConfig.from_storage_config_or_default(storage_config)
    # The scan root names no product, so no label is expected of whatever is stored there.
    scan_root = ctx.with_resource(
        open_product(base_uri.join(_SCAN_LABEL), format="zarr", driver=driver, require_label=False)
    )
    scanner = _product_manager(ctx, workspace, scan_root, product_filesystem=False)
    products: list[BoundProduct] = []
    for uri, label in scanner.discover_products():
        if label is None:
            click.echo(f"[chunks] skipping {uri.to_str()}: it records no product label", err=True)
            continue
        store = ProductIdentity.from_uri(uri, format="zarr", product_name=label)
        manager = _bound_manager(ctx, workspace, store, driver, product_filesystem)
        products.append(BoundProduct(manager=manager, label=label, uri=uri))
    return products


def _optional_storage_config(ctx: click.Context) -> StorageConfig | None:
    """Return the [storage] configuration when the config file or the environment sets one."""
    if get_config(ctx).get("storage") or os.environ.get("FIRECUBE_STORAGE_TYPE"):
        return get_storage_config(ctx, set_global=False)
    return None


def _bound_manager(
    ctx: click.Context,
    workspace: Path | None,
    identity: ProductIdentity,
    driver: StorageDriverConfig,
    product_filesystem: bool = False,
) -> ChunkManager:
    """Return a ChunkManager for `identity` that closes with the command.

    With `product_filesystem`, the manager deletes objects through a filesystem bound to
    `identity`.

    Raises:
        ProductLabelMismatchError: The store records a label other than `identity`'s.
    """
    product = ctx.with_resource(
        open_product(
            identity.product_uri,
            format=identity.format,
            driver=driver,
            product_name=identity.product_name,
            require_label=False,
        )
    )
    return _product_manager(ctx, workspace, product, product_filesystem)


def _product_manager(
    ctx: click.Context, workspace: Path | None, product: Product, product_filesystem: bool
) -> ChunkManager:
    """Return a ChunkManager bound like `product`'s session that closes with the command.

    With `product_filesystem`, the manager deletes objects through the session's filesystem.
    """
    ctx.ensure_object(dict)
    base_workspace = workspace if workspace is not None else ctx.obj.get("workspace")
    filesystem = product.session.fs() if product_filesystem else None
    manager = ChunkManager(
        binding=product.session.binding, workspace=base_workspace, filesystem=filesystem
    )
    ctx.call_on_close(manager.close)
    return manager


def _storage_uri_from_base(raw: str) -> StorageUri:
    """Parse the base URI of the [storage] configuration; refuse a bare path."""
    if "://" not in raw:
        raise click.UsageError(
            "Product target must resolve to a full URI like 's3://bucket/path' or "
            f"'file:///abs/path'. Bare paths are no longer supported. Got: {raw!r}"
        )
    return StorageUri.parse(raw)


def _base_uri_from_storage_config(storage_config: StorageConfig) -> StorageUri | None:
    """Return the base URI the [storage] configuration names, or `None` when it names none."""
    identity = identity_from_storage_config(storage_config)
    return identity.product_uri if identity is not None else None
