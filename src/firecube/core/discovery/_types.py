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

"""Value types of source discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

DEFAULT_OPERATION = "STAC source discovery"

STAC_SCHEME: Final = "stac"
# The API root (any number of non-empty segments, none included), then /collections/<id>.
_COLLECTION_PATH: Final = re.compile(r"^(?P<root>(?:/[^/]+)*?)/collections/(?P<id>[^/]+)$")


@dataclass(frozen=True, slots=True)
class StacCollectionUri:
    """One collection of a STAC API, named as a source: ``stac://<host><api_root>/collections/<id>``.

    A source location, never a storage location: the API is reached at
    ``https://<host><api_root>`` and searched with ``collections=[<collection>]``.

    Attributes:
        host: The API host, with a port when one is given.
        api_root: The path of the API landing page, ``""`` at the host root, else
            starting with ``/`` and without a trailing ``/``.
        collection: The collection id.
    """

    host: str
    api_root: str
    collection: str

    @classmethod
    def parse(cls, raw: str) -> StacCollectionUri:
        """Parse a ``stac://`` source URI.

        The scheme is lowercase; the URI names a host and a path ending in
        ``/collections/<id>``, and has no ``?``, ``#`` or control character.

        Raises:
            ValueError: The first rule ``raw`` fails, as one sentence.
        """
        candidate = str(raw or "")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in candidate):
            raise ValueError("URI must not contain control characters.")
        scheme, marker, rest = candidate.partition("://")
        if not marker or scheme.lower() != STAC_SCHEME:
            raise ValueError(f"not a {STAC_SCHEME}:// URI: {candidate!r}")
        if scheme != STAC_SCHEME:
            raise ValueError(
                f"URI scheme must be lowercase ({STAC_SCHEME}://). "
                f"Did you mean {STAC_SCHEME}://{rest}?"
            )
        if "?" in rest or "#" in rest:
            raise ValueError("URI must not contain '?' or '#'.")
        host, _slash, _path = rest.partition("/")
        if not host:
            raise ValueError("stac:// URI must name a host.")
        path = rest[len(host) :]
        match = _COLLECTION_PATH.match(path)
        if match is None:
            raise ValueError(
                "stac:// URI path must end in /collections/<id> "
                f"(got '{path or '/'}'). Name one collection of a STAC API."
            )
        return cls(host=host, api_root=match["root"], collection=match["id"])

    @property
    def api_url(self) -> str:
        """The https URL of the API landing page."""
        return f"https://{self.host}{self.api_root}"

    def to_str(self) -> str:
        """The canonical ``stac://`` spelling, round-trippable through `parse`."""
        return f"{STAC_SCHEME}://{self.host}{self.api_root}/collections/{self.collection}"


@dataclass(frozen=True, slots=True)
class SourceItem:
    """One discovered source granule.

    A file-tree source (a local path, ``file://`` or ``s3://``) yields items that carry
    ``href`` and ``scheme`` only. A ``stac://`` source yields one item per selected asset
    of each matching STAC item, with the STAC fields filled in.

    Plugins receive ``href`` strings in ``filter_item`` and ``inspect_item``, unless the
    ingestor class declares ``SOURCE_ITEMS = True``; then they receive these items.

    Attributes:
        href: Where the granule is read from: a local path, an ``s3://`` URI, or the
            absolute asset href a STAC API publishes (often ``https://``).
        scheme: The scheme of the source that produced the item: ``"file"``, ``"s3"``
            or ``"stac"``.
        collection: STAC collection id, or ``None`` for a file-tree item.
        item_id: STAC item id, or ``None`` for a file-tree item.
        asset_key: Key of the asset within its STAC item, or ``None``.
        datetime: The item's ``datetime`` property as published (an ISO 8601 string),
            or ``None`` when the item has none or the item is from a file tree.
        bbox: The item's bounding box, or ``None``.
        media_type: The asset's media type, or ``None``.
        roles: The asset's roles, for example ``("data",)``; empty for a file-tree item.

    Examples:
        A STAC asset as a plugin declaring ``SOURCE_ITEMS = True`` receives it:

            SourceItem(
                href="https://example.com/S2A_T32TQM_20250601.nc",
                scheme="stac",
                collection="sentinel-2-l2a",
                item_id="S2A_T32TQM_20250601",
                asset_key="data",
                datetime="2025-06-01T10:20:31Z",
                bbox=(11.5, 48.0, 11.7, 48.2),
                media_type="application/netcdf",
                roles=("data",),
            )
    """

    href: str
    scheme: str
    collection: str | None = None
    item_id: str | None = None
    asset_key: str | None = None
    datetime: str | None = None
    bbox: tuple[float, ...] | None = None
    media_type: str | None = None
    roles: tuple[str, ...] = ()

    @property
    def identity(self) -> str:
        """The key discovery deduplicates on.

        ``collection/item_id/asset_key`` for a STAC item, so the same asset keeps its
        identity when an API changes its hrefs; ``href`` for every other item.
        """
        if self.collection is not None and self.item_id is not None:
            return f"{self.collection}/{self.item_id}/{self.asset_key}"
        return self.href


@dataclass(frozen=True, slots=True)
class DiscoveryRequest:
    """What `discover` resolves: the sources of one run and how to narrow them.

    Attributes:
        sources: Source locations in the order given: local paths, ``file://`` or
            ``s3://`` URIs walked as file trees, and ``stac://<host>/<api-root>/
            collections/<id>`` URIs searched as one STAC API collection each.
        storage_config: Storage settings used to reach a remote file-tree source.
        input_filters: Filename filters. On a file tree they add to the default suffix
            selection and ``!`` entries exclude, as in `discover_input_files`. On a STAC
            source they narrow the selected assets on the basename of the href: with a
            positive entry only matching assets are kept, and ``!`` entries exclude.
        stac_search: Item search filter applied to every STAC source: any of the keys
            ``datetime``, ``bbox``, ``intersects`` and ``ids``, as the STAC API item
            search defines them. ``collections`` always comes from the source URI.
        operation: Name of the operation, used in the message when the ``stac`` extra
            is missing.
    """

    sources: tuple[str, ...]
    storage_config: Any | None = None
    input_filters: Sequence[str] | None = None
    stac_search: Mapping[str, Any] | None = None
    operation: str = DEFAULT_OPERATION
