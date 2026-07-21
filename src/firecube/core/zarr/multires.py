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
from dataclasses import dataclass
from typing import Any, cast

from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.product.target import ProductTarget
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.layers import build_multires_layers

log = logging.getLogger("firecube.core.zarr.multires")


@dataclass
class MultiresConfig:
    """Configuration for building multiresolution Zarr layers."""

    product: str
    group: str
    resolutions: list[float]
    time_dim_name: str = "timestamp"


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
        if self.session is not None:
            return self.session

        driver_config = StorageDriverConfig.from_storage_config(self.storage_config)
        resolved_product = ProductTarget.resolve(
            cfg.product,
            driver_config,
            product_name=cfg.product,
            plugin_default_format="zarr",
            default_base_uri=self.product_uri.parent(),
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

    def build(self, cfg: MultiresConfig) -> dict:
        """Build multiresolution layers for the given configuration."""
        if not cfg.resolutions:
            raise ValueError("At least one resolution must be provided for multires.")

        store_uri = self._store_uri_for(cfg.product)
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
            session=self._session_for(cfg),
            time_dim_name=cfg.time_dim_name,
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
