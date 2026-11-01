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
Every result page is read, up to the first page without a ``next`` link. A ``next`` link that repeats an earlier
request, or five empty pages in a row with a ``next`` link, is refused. Each asset whose roles contain ``data`` becomes one
`SourceItem`; the hrefs are handed through as the API publishes them. Requires the
optional ``stac`` extra (``pystac-client``), which is imported only when a search runs.
"""

# pyright: reportMissingImports=false

from __future__ import annotations

import functools
import json
from collections.abc import Iterator, Mapping
from typing import Any

from firecube.core.discovery import _stac_compat
from firecube.core.discovery._filters import narrow_by_basename
from firecube.core.discovery._types import DiscoveryRequest, SourceItem, StacCollectionUri
from firecube.core.discovery.errors import DiscoveryError, StacApiError, StacAssetHrefError

DATA_ROLE = "data"
# Seconds a single HTTP request to the API may take; pystac-client retries on top.
_REQUEST_TIMEOUT_S = 60
# Empty result pages in a row, each with a next link, after which paging is refused.
MAX_CONSECUTIVE_EMPTY_PAGES = 5


class _IncompletePagingError(Exception):
    """Paging cannot reach a page without a next link; the results may be incomplete."""


def _new_stac_io() -> Any:
    """Return the HTTP layer of one search: pystac-client's, with firecube's paging."""
    return _paging_stac_api_io()(timeout=_REQUEST_TIMEOUT_S)


@functools.cache
def _paging_stac_api_io() -> type[Any]:
    """Build, once, the `StacApiIO` subclass whose ``get_pages`` is `_follow_next_links`.

    Built on first use because pystac-client is imported only when a search runs.
    """
    from pystac_client.stac_api_io import StacApiIO

    class _PagingStacApiIO(StacApiIO):
        def get_pages(
            self,
            url: str,
            method: str | None = None,
            parameters: dict[str, Any] | None = None,
        ) -> Iterator[dict[str, Any]]:
            return _follow_next_links(self, url, method, parameters)

    return _PagingStacApiIO


def _follow_next_links(
    stac_io: Any, url: str, method: str | None, parameters: dict[str, Any] | None
) -> Iterator[dict[str, Any]]:
    """Yield every result page that has features or collections, following next links.

    Requests go out as in ``StacApiIO.get_pages``. Paging ends normally only at a page
    without a next link; an empty page with one does not end it.

    Raises:
        _IncompletePagingError: If a next link repeats a request of this paging, or
            `MAX_CONSECUTIVE_EMPTY_PAGES` empty pages in a row each carry a next link.
    """
    from pystac.link import Link

    seen = {_request_key(url, method, parameters, _sent_headers(stac_io, None))}
    page = stac_io.read_json(url, method=method, parameters=parameters)
    empty_in_a_row = 0
    while True:
        if page.get("features") or page.get("collections"):
            empty_in_a_row = 0
            yield page
        next_link = next(
            (link for link in page.get("links", []) if link.get("rel") == "next"), None
        )
        if next_link is None:
            return
        if not (page.get("features") or page.get("collections")):
            empty_in_a_row += 1
            if empty_in_a_row >= MAX_CONSECUTIVE_EMPTY_PAGES:
                raise _IncompletePagingError(
                    f"{empty_in_a_row} result pages in a row were empty and each had a next link"
                )
        key = _next_link_key(stac_io, next_link, parameters)
        if key in seen:
            raise _IncompletePagingError(
                f"a next link repeats an earlier request ({key[1]} {key[0]})"
            )
        seen.add(key)
        page = stac_io.read_json(Link.from_dict(next_link), parameters=parameters)


def _request_key(
    href: str,
    method: str | None,
    body: Mapping[str, Any] | None,
    headers: Mapping[str, Any],
) -> tuple[str, str, str, str]:
    """The href, method, canonical JSON body and sorted headers of one request.

    Header names compare case-insensitively, as HTTP defines them.
    """
    return (
        href,
        (method or "GET").upper(),
        json.dumps(dict(body or {}), sort_keys=True, default=str),
        json.dumps(sorted((str(name).lower(), str(value)) for name, value in headers.items())),
    )


def _sent_headers(stac_io: Any, request_headers: Mapping[str, Any] | None) -> Mapping[str, Any]:
    """The headers ``StacApiIO.request`` sends: the session's, overridden by the request's.

    The merge is the one ``requests`` applies when it prepares the request, so a request
    header set to ``None`` removes the session header.
    """
    from requests.sessions import merge_setting
    from requests.structures import CaseInsensitiveDict

    return merge_setting(
        dict(request_headers or {}), stac_io.session.headers, dict_class=CaseInsensitiveDict
    )


def _next_link_key(
    stac_io: Any, link: Mapping[str, Any], parameters: dict[str, Any] | None
) -> tuple[str, str, str, str]:
    """The `_request_key` of the request ``StacApiIO.read_text`` makes for a next link.

    A ``POST`` link sends its ``body``, merged over ``parameters`` when ``merge`` is set;
    any other method sends no body, because the href carries the query. The link's
    ``headers`` go out over the session's, whatever ``merge`` says.
    """
    method = link.get("method", "GET")
    body: Mapping[str, Any] = {}
    if method == "POST":
        link_body = link.get("body", {})
        body = {**(parameters or {}), **link_body} if link.get("merge", False) else link_body
    headers = _sent_headers(stac_io, link.get("headers"))
    return _request_key(str(link["href"]), method, body, headers)


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
        StacApiError: If the API cannot be opened or searched, a page cannot be read,
            or paging cannot reach a page without a next link.
        StacAssetHrefError: If a relative asset href cannot be made absolute.
    """
    _stac_compat.require_stac(request.operation)
    from pystac_client import Client

    uri = parse_stac_source(source)
    collection = uri.collection
    search = {**(request.stac_search or {}), "collections": [collection]}

    items: dict[str, SourceItem] = {}
    try:
        client = Client.open(uri.api_url, stac_io=_new_stac_io())
        for stac_item in client.search(**search).items():
            for item in _data_assets(source, collection, stac_item):
                items.setdefault(item.identity, item)
    except DiscoveryError:
        raise
    except _IncompletePagingError as exc:
        raise StacApiError(source, f"{exc}; the results may be incomplete") from exc
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
