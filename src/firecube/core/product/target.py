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

from dataclasses import dataclass
from pathlib import Path

from firecube.core.storage.driver_config import (
    StorageDriverConfig,
)
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import parse_uri

_OBSTORE_SUPPORTED_PROTOCOLS = {"s3", "file", "gs", "az"}


@dataclass(frozen=True, slots=True)
class ResolvedProduct:
    product_name: str
    product_uri: StorageUri
    output_base_uri: StorageUri
    control_root_uri: StorageUri
    format: str


class ProductTarget:
    @classmethod
    def resolve(
        cls,
        target_arg: str,
        driver_config: StorageDriverConfig,
        *,
        product_name: str,
        plugin_default_format: str | None = None,
        default_base_uri: StorageUri | None = None,
    ) -> ResolvedProduct:
        target_value = str(target_arg or "")
        if not product_name:
            raise ValueError("product_name is required; cannot be empty.")
        output_format = str(plugin_default_format or "zarr")
        parsed = parse_uri(target_value)
        protocol = parsed["protocol"]

        if cls._is_bare_name_target(target_value, protocol):
            if default_base_uri is None:
                raise ValueError(
                    "Bare-name targets require either a fully-qualified URI/path or default_base_uri."
                )

            product_uri = default_base_uri.join(target_value)
            cls._validate_driver_protocol(driver_config, product_uri.protocol)
            return ResolvedProduct(
                product_name=product_name,
                product_uri=product_uri,
                output_base_uri=default_base_uri,
                control_root_uri=cls._control_root(product_uri),
                format=output_format,
            )

        if protocol != "file":
            product_uri = StorageUri.parse(target_value)
            cls._validate_driver_protocol(driver_config, product_uri.protocol)
            return ResolvedProduct(
                product_name=product_name,
                product_uri=product_uri,
                output_base_uri=product_uri.parent(),
                control_root_uri=cls._control_root(product_uri),
                format=output_format,
            )

        if target_value.startswith("file://"):
            product_uri = StorageUri.parse(target_value)
            resolved_path = Path(product_uri.path).resolve()
        else:
            resolved_path = Path(target_value).resolve()
        product_uri = StorageUri.from_local_path(resolved_path)
        cls._validate_driver_protocol(driver_config, product_uri.protocol)
        return ResolvedProduct(
            product_name=product_name,
            product_uri=product_uri,
            output_base_uri=product_uri.parent(),
            control_root_uri=cls._control_root(product_uri),
            format=output_format,
        )

    @staticmethod
    def _is_bare_name_target(target_arg: str, protocol: str) -> bool:
        return (
            protocol == "file"
            and "/" not in target_arg
            and "\\" not in target_arg
            and not target_arg.startswith("file://")
        )

    @staticmethod
    def _control_root(product_uri: StorageUri) -> StorageUri:
        return product_uri.join(".firecube")

    @staticmethod
    def _validate_driver_protocol(
        driver_config: StorageDriverConfig,
        protocol: str,
    ) -> None:
        if driver_config.driver == "obstore" and protocol not in _OBSTORE_SUPPORTED_PROTOCOLS:
            raise ValueError(
                f"Storage driver 'obstore' does not support protocol '{protocol}'; "
                "use --storage-driver=fsspec instead."
            )
