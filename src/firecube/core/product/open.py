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

"""Open a product store under the label it records, for the length of one block.

The package `__init__` does not re-export this module: the control plane imports
`firecube.core.product`, and a re-export would close an import cycle.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

from firecube.core.controlplane.product_label import recorded_product_label
from firecube.core.errors import Message, ProductLabelMismatchError, ProductLabelMissingError
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager


class Product:
    """An open product store, bound to the label the store records.

    `open_product` builds it; do not construct it yourself. `.session` and `.manager`
    expose `StorageSession` and `ChunkManager`. They serve the engine and the operations
    and are not part of the public contract.

    Attributes:
        identity: The product's URI, control root, format and label.
        session: The storage session bound to `identity`.
    """

    __slots__ = ("_closed", "_manager", "identity", "session")

    def __init__(self, identity: ProductIdentity, session: StorageSession) -> None:
        """Bind the handle to `identity` and `session`; build no manager yet.

        Args:
            identity: The product's identity, labelled as the store records it.
            session: The storage session bound to `identity`.
        """
        self.identity = identity
        self.session = session
        self._manager: ChunkManager | None = None
        self._closed = False

    @property
    def label(self) -> str:
        """The product label of the open store."""
        return self.identity.product_name

    @property
    def manager(self) -> ChunkManager:
        """The control-plane manager of the store, built on first use.

        `open_product` closes it when the block ends.

        Raises:
            RuntimeError: The block of `open_product` has ended.
        """
        if self._closed:
            raise RuntimeError(
                f"the product at {self.identity.product_uri.to_str()} is closed; "
                "open it again to build a manager"
            )
        if self._manager is None:
            self._manager = self.session.control_plane()
        return self._manager

    def _close(self) -> None:
        """Close the manager this handle built, if any; refuse a manager after that."""
        self._closed = True
        manager, self._manager = self._manager, None
        if manager is not None:
            manager.close()


@contextmanager
def open_product(
    target: StorageUri | str,
    *,
    format: str,
    driver: StorageDriverConfig | None = None,
    product_name: str | None = None,
    require_label: bool = True,
) -> Iterator[Product]:
    """Open the product at `target` and yield its handle for the block.

    The store's recorded label is read before the handle exists, so a refusal writes
    nothing. When the block ends, also on an exception, the manager the handle built is
    closed. No storage setting is resolved from the process environment, and nothing is
    exported to it.

    Args:
        target: The product URI. A string is parsed as a `StorageUri`.
        format: The product format, one of `zarr`, `parquet` or `tensogram`.
        driver: The storage driver settings. `None` uses the defaults: `fsspec`, no
            endpoint, no credentials.
        product_name: The label the caller expects. A store that records another
            label is refused. A store that records none opens under this label.
        require_label: Refuse a store that records no label. When `False`, such a
            store opens under `product_name`, else under the normalised URI.

    Yields:
        The handle of the open product.

    Raises:
        ValueError: `target` is not a storage URI, `format` is unknown, or
            `product_name` is empty.
        ProductLabelMissingError: The store records no label and `require_label` is set.
        ProductLabelMismatchError: The store records a label other than `product_name`.
        ManifestError: The stamp or a control-plane record cannot be read.
        ProductLabelConflictError: An unstamped store's records name several labels.
    """
    uri = target if isinstance(target, StorageUri) else StorageUri.parse(target)
    if product_name is not None and not product_name:
        raise ValueError(f"product_name must not be empty. URI: {uri.to_str()}")
    driver_config = (
        driver if driver is not None else StorageDriverConfig.from_storage_config_or_default(None)
    )
    label = _label(
        uri,
        format=format,
        driver=driver_config,
        product_name=product_name,
        require_label=require_label,
    )
    identity = ProductIdentity.from_uri(uri, format=format, product_name=label)
    session = StorageSession(StorageBinding(identity=identity, driver=driver_config))
    product = Product(identity, session)
    try:
        yield product
    finally:
        product._close()


def _label(
    uri: StorageUri,
    *,
    format: str,
    driver: StorageDriverConfig,
    product_name: str | None,
    require_label: bool,
) -> str:
    """Return the label the product at `uri` opens under, or refuse the store.

    Raises:
        ProductLabelMissingError: The store records no label and `require_label` is set.
        ProductLabelMismatchError: The store records a label other than `product_name`.
        ManifestError: The stamp or a control-plane record cannot be read.
        ProductLabelConflictError: An unstamped store's records name several labels.
    """
    recorded = _recorded_label(uri, format=format, driver=driver)
    if recorded is None:
        if require_label:
            raise ProductLabelMissingError(uri.to_str())
        return product_name if product_name is not None else uri.to_str()
    if product_name is not None and product_name != recorded:
        raise ProductLabelMismatchError(
            Message(
                "Product label {given!r} is refused: the product at {product_uri} records "
                "the label {product!r}.",
                given=product_name,
                product=recorded,
                product_uri=uri.to_str(),
            ),
            given=product_name,
            recorded=recorded,
        )
    return recorded


def _recorded_label(uri: StorageUri, *, format: str, driver: StorageDriverConfig) -> str | None:
    """Return the label the store at `uri` records, or `None`; reads through a URI-labelled binding."""
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, format=format, product_name=uri.to_str()),
        driver=driver,
    )
    return recorded_product_label(cast(StorageFilesystemFull, StorageSession(binding).fs()), uri)
