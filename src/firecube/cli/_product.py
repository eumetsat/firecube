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

import click

from firecube.core.product.identity import ProductIdentity
from firecube.core.product.resolver import ProductResolver


def require_full_uri(value: str, *, option_name: str) -> None:
    if value and "://" not in value:
        raise click.UsageError(
            f"{option_name} must be a full URI like 's3://bucket/path/x.zarr' or "
            f"'file:///abs/path/x.zarr'. Bare names are no longer supported. "
            f"Got: {value!r}"
        )


def resolve_product_identity(
    value: str,
    *,
    format: str,
    product_name: str,
    option_name: str = "--product",
) -> ProductIdentity:
    require_full_uri(value, option_name=option_name)
    try:
        return ProductResolver.resolve(value, format=format, product_name=product_name)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc


def resolve_product_name(value: str, *, format: str = "zarr", product_name: str) -> str:
    return resolve_product_identity(value, format=format, product_name=product_name).product_name
