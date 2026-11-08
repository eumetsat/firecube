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

import shlex
from typing import cast

import click

from firecube.cli._errors import render
from firecube.core.controlplane.product_label import recorded_product_label
from firecube.core.errors import CommandName, Message, Placeholder
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.product.identity import ProductIdentity
from firecube.core.product.resolver import ProductResolver
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri

_ADOPT_COMMAND = CommandName(("migrate", "adopt"), library="adopt_product")


def resolve_product_identity(
    uri: StorageUri,
    *,
    format: str,
    product_name: str,
) -> ProductIdentity:
    """Resolve an admitted product URI into a ProductIdentity.

    Raises:
        click.UsageError: The resolver refuses the URI.
    """
    try:
        return ProductResolver.resolve(uri.to_str(), format=format, product_name=product_name)
    except ValueError as exc:
        raise click.UsageError(render(exc)) from exc


def session_identity(
    uri: StorageUri, *, format: str, driver: StorageDriverConfig
) -> ProductIdentity:
    """Return the identity of the product at `uri`, labelled as the store records it.

    A store that records no label takes the normalised URI as its label. Reads only.

    Raises:
        ManifestError: The stamp or a control-plane record cannot be read.
        ProductLabelConflictError: An unstamped store's records name several labels.
    """
    label = _recorded_label(uri, format=format, driver=driver)
    return ProductIdentity.from_uri(uri, format=format, product_name=label or uri.to_str())


def recorded_identity(
    uri: StorageUri, *, format: str, driver: StorageDriverConfig
) -> ProductIdentity:
    """Return the identity of the product at `uri` under the label the store records.

    Reads only.

    Raises:
        click.UsageError: The store records no label; the text names the command that records one.
        ManifestError: The stamp or a control-plane record cannot be read.
        ProductLabelConflictError: An unstamped store's records name several labels.
    """
    label = _recorded_label(uri, format=format, driver=driver)
    if label is None:
        raise click.UsageError(
            render(
                Message(
                    "The product at {product_uri} records no product label. Record one with "
                    "{adopt} --target {target} --product-name {label}.",
                    product_uri=uri.to_str(),
                    adopt=_ADOPT_COMMAND,
                    target=shlex.quote(uri.to_str()),
                    label=Placeholder("<label>"),
                )
            )
        )
    return ProductIdentity.from_uri(uri, format=format, product_name=label)


def _recorded_label(uri: StorageUri, *, format: str, driver: StorageDriverConfig) -> str | None:
    """Return the label the store at `uri` records, or `None`; the read binding carries the URI label."""
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, format=format, product_name=uri.to_str()),
        driver=driver,
    )
    return recorded_product_label(cast(StorageFilesystemFull, StorageSession(binding).fs()), uri)
