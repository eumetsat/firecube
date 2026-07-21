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

from firecube.core.storage.uri import StorageUri

VALID_FORMATS = frozenset({"zarr", "parquet", "tensogram"})


@dataclass(frozen=True, slots=True)
class ProductIdentity:
    product_name: str
    product_uri: StorageUri
    control_root_uri: StorageUri
    format: str

    def __post_init__(self) -> None:
        if self.format not in VALID_FORMATS:
            raise ValueError(
                f"Invalid format {self.format!r}; expected one of {sorted(VALID_FORMATS)}"
            )

    @classmethod
    def from_uri(cls, uri: StorageUri, format: str, product_name: str) -> ProductIdentity:
        """Create a ProductIdentity from a URI with an explicit product name.

        The product_name must be provided explicitly; it is not derived from the
        URI basename. Use CLI --product-name, config default_product_name, or
        plugin PRODUCT_NAME as the source.
        """
        if not product_name:
            raise ValueError(f"product_name is required; cannot be empty. URI: {uri}")
        control_root = uri.join(".firecube")
        return cls(
            product_name=product_name,
            product_uri=uri,
            control_root_uri=control_root,
            format=format,
        )


def ensure_product_uri(base_uri: str, product: str) -> str:
    """Append `product` to `base_uri` if the last path segment doesn't exactly match.

    Uses URI path parsing to avoid substring false positives (e.g. 'daily' matching
    'daily_archive').
    """
    from firecube.core.uris import parse_uri

    base_uri = str(base_uri or "").rstrip("/")
    product = str(product or "")

    if not product:
        return base_uri

    parsed = parse_uri(base_uri)
    path = parsed.get("path", "").rstrip("/")
    last_segment = path.rsplit("/", 1)[-1] if "/" in path else path
    if last_segment == product:
        return base_uri

    return base_uri + "/" + product
