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

"""STAC API backend: the data assets of the items of one collection.

A ``stac://<host>/<api-root>/collections/<id>`` source is searched at
``https://<host>/<api-root>`` with ``collections=[<id>]`` and the run's search filter.
Every result page is read. Each asset whose roles contain ``data`` becomes one
`SourceItem`; the hrefs are handed through as the API publishes them. Requires the
optional ``stac`` extra (``pystac-client``).
"""

from __future__ import annotations

from typing import Any

from firecube.core.discovery import _stac_compat
from firecube.core.discovery._filters import narrow_by_basename
from firecube.core.discovery._types import DiscoveryRequest, SourceItem, StacCollectionUri
from firecube.core.discovery.errors import DiscoveryError, StacApiError, StacAssetHrefError

DATA_ROLE = "data"
# Seconds a single HTTP request to the API may take; pystac-client retries on top.
_REQUEST_TIMEOUT_S = 60


def _new_stac_io() -> Any:
    """Return the HTTP layer the client uses. Tests replace it with an in-process API."""
    return _stac_compat.StacApiIO(timeout=_REQUEST_TIMEOUT_S)


def parse_stac_source(source: str) -> StacCollectionUri:
    """Parse a ``stac://`` source with the rules the CLI admits it by.

    Raises:
        StacApiError: If the source is not a valid ``stac://`` collection URI.
    """
    try:
        return StacCollectionUri.parse(source)
    except ValueError as exc:
        raise StacApiError(source, str(exc)) from exc


def stac_source(source: str, request: DiscoveryRequest) -> list[SourceItem]:
    """Backend of ``stac://`` sources: one item per data asset, deduplicated, unsorted.

    Raises:
        ImportError: If the ``stac`` extra is not installed; the message names
            ``request.operation``.
        StacApiError: If the API cannot be opened or searched, or a page cannot be read.
        StacAssetHrefError: If a relative asset href cannot be made absolute.
    """
    _stac_compat.require_stac(request.operation)
    uri = parse_stac_source(source)
    collection = uri.collection
    search = {**(request.stac_search or {}), "collections": [collection]}

    items: dict[str, SourceItem] = {}
    try:
        client = _stac_compat.Client.open(uri.api_url, stac_io=_new_stac_io())
        for stac_item in client.search(**search).items():
            for item in _data_assets(source, collection, stac_item):
                items.setdefault(item.identity, item)
    except DiscoveryError:
        raise
    except Exception as exc:
        raise StacApiError(source, f"{type(exc).__name__}: {exc}") from exc

    return narrow_by_basename(items.values(), request.input_filters)


def _data_assets(source: str, collection: str, stac_item: Any) -> list[SourceItem]:
    """The assets of one STAC item whose roles contain ``data``, in asset-key order.

    An item that does not name its collection is taken to belong to the searched one.
    """
    bbox = tuple(float(value) for value in stac_item.bbox) if stac_item.bbox else None
    published = stac_item.properties.get("datetime")
    selected: list[SourceItem] = []
    for key, asset in sorted(stac_item.assets.items()):
        roles = tuple(asset.roles or ())
        if DATA_ROLE not in roles:
            continue
        href = asset.get_absolute_href()
        if href is None:
            raise StacAssetHrefError(source, stac_item.id, key)
        selected.append(
            SourceItem(
                href=href,
                scheme="stac",
                collection=stac_item.collection_id or collection,
                item_id=stac_item.id,
                asset_key=key,
                datetime=str(published) if published is not None else None,
                bbox=bbox,
                media_type=asset.media_type,
                roles=roles,
            )
        )
    return selected
