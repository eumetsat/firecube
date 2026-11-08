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

"""Core helpers for building multi-resolution Zarr layers.

This module provides a generic, storage-aware multiresolution builder
that can operate on any existing Zarr product using the shared
StorageConfig and ChunkManager infrastructure.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from firecube.core.config import StorageConfig
from firecube.core.product.identity import ProductIdentity
from firecube.core.product.target import ProductTarget
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.layers import build_multires_layers

if TYPE_CHECKING:
    from firecube.core.controlplane import ChunkManager

log = logging.getLogger("firecube.core.zarr.multires")


@dataclass
class MultiresConfig:
    """Configuration for building multiresolution Zarr layers.

    `group` is the group that holds the dataset; empty means the root. `lat_name` and
    `lon_name` name the 1-D coordinates to coarsen.
    """

    product: str
    group: str
    resolutions: list[float]
    time_dim_name: str = "timestamp"
    lat_name: str = "lat"
    lon_name: str = "lon"


def _plan_layers(
    session: StorageSession, store_uri: str, cfg: MultiresConfig
) -> list[dict[str, Any]]:
    """Plan the layers `build_multires_layers` writes for *cfg*; read and write nothing.

    Each entry names the layer, its coarsening factors and whether a layer there
    exists already and would be replaced.

    Raises:
        ValueError: The root dataset cannot be opened.
        ConfigurationError: A named coordinate is missing, not 1-D, or not on its own dimension.
    """
    from firecube.core.errors import Param
    from firecube.core.zarr.layers import _require_axis

    uri = StorageUri.parse(store_uri)
    group = cfg.group.strip("/")
    if group:
        ds = session.zarr.open_dataset(uri, chunks="auto", group=group)
    else:
        try:
            ds = session.zarr.open_dataset(uri, chunks="auto")
        except Exception as exc:
            raise ValueError(
                f"Cannot open root dataset at {store_uri!r}. "
                "If the store uses a grouped layout, pass the group explicitly."
            ) from exc
    _require_axis(ds, cfg.lat_name, Param("lat_name"))
    _require_axis(ds, cfg.lon_name, Param("lon_name"))
    ds = ds.sortby([cfg.lat_name, cfg.lon_name])
    steps = {
        name: float(abs(ds[name].diff(name).mean().values)) if ds.sizes.get(name, 0) > 1 else 1.0
        for name in (cfg.lat_name, cfg.lon_name)
    }
    prefix = f"{group}/" if group else ""
    plan: list[dict[str, Any]] = []
    for res in cfg.resolutions:
        layer = f"{prefix}multires/{res}deg"
        plan.append(
            {
                "layer": layer,
                "lat_factor": max(1, round(res / steps[cfg.lat_name])),
                "lon_factor": max(1, round(res / steps[cfg.lon_name])),
                "replaces_existing": session.exists(uri.join(layer)),
            }
        )
    return plan


class ZarrMultiresBuilder:
    """Generic multiresolution builder for Zarr products.

    This operates on an existing Zarr product by:
      - resolving the store URI from StorageConfig
      - opening the target group
      - building multiresolution views using shared zarr_utils helpers

    It is intentionally generic; product-specific helpers (e.g. ExampleProductMultires)
    should build MultiresConfig instances and call .build().
    """

    def __init__(
        self,
        storage_config: StorageConfig,
        *,
        product_name: str,
        product_uri: StorageUri | None = None,
        chunk_manager: ChunkManager | None = None,
        session: StorageSession | None = None,
    ) -> None:
        if not product_name:
            raise ValueError("ZarrMultiresBuilder requires a non-empty product_name.")
        self.storage_config = storage_config
        self.product_name = product_name
        self.session = session
        resolved_product_uri = product_uri or (
            session.product.product_uri if session is not None else None
        )
        if resolved_product_uri is None:
            raise ValueError("ZarrMultiresBuilder requires product_uri or session.")
        self.product_uri: StorageUri = resolved_product_uri
        driver_config = StorageDriverConfig.from_storage_config(storage_config)
        # This binding intentionally uses the sentinel identity
        # "__firecube_controlplane__" so multires can address the cube root as a
        # product-agnostic control-plane scope instead of a real product name.
        # The sentinel bypasses the product-name short-circuit in
        # _ControlRootResolver. ChunkManager is currently instantiated but not
        # used in build(); it is reserved here for future control-plane hooks.
        from firecube.core.controlplane import ChunkManager

        self.chunk_manager = chunk_manager or ChunkManager(
            binding=StorageBinding(
                identity=ProductIdentity.from_uri(
                    self.product_uri.parent().join("__firecube_controlplane__"),
                    "zarr",
                    product_name="__firecube_controlplane__",
                ),
                driver=driver_config,
            )
        )

    def _store_uri_for(self, product: str) -> str:
        """Build the underlying store URI for a given product."""
        product_uri = (
            self.product_uri
            if product.strip("/") == self.product_name
            else self.product_uri.parent().join(product)
        )
        return product_uri.to_str()

    def _session_for(self, cfg: MultiresConfig) -> StorageSession:
        store_uri = self._store_uri_for(cfg.product)
        # A supplied session serves only the store it is bound to; otherwise the
        # maintenance check and the layer writes would address different stores.
        if self.session is not None and self.session.product.product_uri.to_str() == store_uri:
            return self.session

        driver_config = StorageDriverConfig.from_storage_config(self.storage_config)
        # Bind to the exact store `_store_uri_for` returns, so the maintenance
        # check and the layer writes address the same store.
        resolved_product = ProductTarget.resolve(
            store_uri,
            driver_config,
            product_name=cfg.product,
            plugin_default_format="zarr",
        )
        return StorageSession(
            StorageBinding(
                identity=ProductIdentity(
                    product_name=resolved_product.product_name,
                    product_uri=resolved_product.product_uri,
                    control_root_uri=resolved_product.control_root_uri,
                    format=resolved_product.format,
                ),
                driver=driver_config,
            )
        )

    def build(self, cfg: MultiresConfig, *, dry_run: bool = False) -> dict:
        """Build multiresolution layers for the given configuration.

        With `dry_run`, make the same checks from reads only, write nothing, and return
        the plan under `"plan"`, one entry per layer.
        """
        if not cfg.resolutions:
            raise ValueError("At least one resolution must be provided for multires.")

        session = self._session_for(cfg)
        manager = session.control_plane()
        try:
            manager.check_exclusive_maintenance(
                product=session.product.product_name, owner_id=f"multires-{uuid.uuid4().hex}"
            )
        finally:
            manager.close()

        store_uri = self._store_uri_for(cfg.product)
        if dry_run:
            plan = _plan_layers(session, store_uri, cfg)
            return {
                "dry_run": True,
                "product": cfg.product,
                "group": cfg.group,
                "store_uri": store_uri,
                "layers": [entry["layer"] for entry in plan],
                "plan": plan,
            }
        log.info(
            "Building Zarr multiresolution layers",
            extra={
                "product": cfg.product,
                "group": cfg.group,
                "store_uri": store_uri,
                "resolutions": cfg.resolutions,
            },
        )

        # Delegate to shared helper
        build_fn = cast(Any, build_multires_layers)
        layers = build_fn(
            store_uri,
            resolutions=cfg.resolutions,
            silent=False,
            logger=log,
            group=cfg.group,
            strict=True,
            session=session,
            time_dim_name=cfg.time_dim_name,
            lat_name=cfg.lat_name,
            lon_name=cfg.lon_name,
        )

        log.info(
            "Zarr multiresolution build complete",
            extra={
                "product": cfg.product,
                "group": cfg.group,
                "store_uri": store_uri,
                "layers": layers,
            },
        )

        return {
            "product": cfg.product,
            "group": cfg.group,
            "store_uri": store_uri,
            "layers": layers,
        }
