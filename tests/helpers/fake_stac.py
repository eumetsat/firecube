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

"""An in-process STAC API for discovery tests: no network, real pystac-client paging.

`FakeStacIO` replaces the HTTP layer of pystac-client (`StacApiIO.request`). Each
`FakeStacApi` serves one API root: a landing page that conforms to item search, and a
POST ``/search`` that filters its items by the posted ``collections`` and pages them with
``next`` links whose body is merged into the next POST, as STAC APIs do. Every request is
recorded, so a test asserts on what the client actually sent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from pystac_client.exceptions import APIError
from pystac_client.stac_api_io import StacApiIO

_CONFORMS_TO = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
]


def stac_item(
    item_id: str,
    collection: str,
    assets: dict[str, tuple[str, list[str]]],
    *,
    base: str,
    datetime: str = "2025-06-01T00:00:00Z",
    bbox: tuple[float, float, float, float] = (10.0, 40.0, 11.0, 41.0),
) -> dict[str, Any]:
    """Return a STAC item document whose assets map key -> (href, roles)."""
    west, south, east, north = bbox
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "collection": collection,
        "bbox": list(bbox),
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[west, south], [east, south], [east, north], [west, north], [west, south]]
            ],
        },
        "properties": {"datetime": datetime},
        "links": [
            {
                "rel": "self",
                "href": f"{base}/collections/{collection}/items/{item_id}",
                "type": "application/geo+json",
            }
        ],
        "assets": {
            key: {"href": href, "roles": roles, "type": _media_type(href)}
            for key, (href, roles) in assets.items()
        },
    }


def _media_type(href: str) -> str:
    if href.endswith(".nc"):
        return "application/netcdf"
    if href.endswith(".xml"):
        return "application/xml"
    if href.endswith(".png"):
        return "image/png"
    return "image/tiff; application=geotiff"


@dataclass
class FakeStacApi:
    """One STAC API root serving ``items`` through item search, ``page_size`` per page."""

    base: str
    items: list[dict[str, Any]]
    page_size: int = 2
    fail_search: bool = False
    requests: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    @property
    def searches(self) -> list[dict[str, Any]]:
        """The body of every first-page search (pages after the first carry a token)."""
        return [
            params
            for method, href, params in self.requests
            if href == f"{self.base}/search" and "token" not in params
        ]

    @property
    def search_pages(self) -> int:
        return sum(1 for _m, href, _p in self.requests if href == f"{self.base}/search")

    def respond(self, href: str, method: str, parameters: dict[str, Any]) -> dict[str, Any]:
        self.requests.append((method, href, dict(parameters)))
        if href.rstrip("/") == self.base:
            return self._landing()
        if href == f"{self.base}/search":
            if self.fail_search:
                raise APIError("503 Service Unavailable")
            return self._page(parameters)
        raise APIError(f"404 Not Found: {href}")

    def _landing(self) -> dict[str, Any]:
        return {
            "type": "Catalog",
            "id": "fake-api",
            "stac_version": "1.0.0",
            "description": "In-process STAC API for tests",
            "conformsTo": _CONFORMS_TO,
            "links": [
                {"rel": "self", "href": self.base, "type": "application/json"},
                {"rel": "root", "href": self.base, "type": "application/json"},
                {
                    "rel": "search",
                    "href": f"{self.base}/search",
                    "type": "application/geo+json",
                    "method": "POST",
                },
            ],
        }

    def _page(self, parameters: dict[str, Any]) -> dict[str, Any]:
        wanted = set(parameters.get("collections") or ())
        ids = set(parameters.get("ids") or ())
        matching = [
            item
            for item in self.items
            if item["collection"] in wanted and (not ids or item["id"] in ids)
        ]
        index = int(str(parameters.get("token", "page-0")).split("-")[1])
        start = index * self.page_size
        page = matching[start : start + self.page_size]
        links: list[dict[str, Any]] = []
        if start + self.page_size < len(matching):
            links.append(
                {
                    "rel": "next",
                    "href": f"{self.base}/search",
                    "type": "application/geo+json",
                    "method": "POST",
                    "body": {"token": f"page-{index + 1}"},
                    "merge": True,
                }
            )
        return {"type": "FeatureCollection", "features": page, "links": links}


class FakeStacIO(StacApiIO):
    """`StacApiIO` that answers from `FakeStacApi` roots instead of the network."""

    def __init__(self, apis: list[FakeStacApi]):
        super().__init__()
        self._apis = apis

    def request(
        self,
        href: str,
        method: str | None = None,
        headers: dict[str, str] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        _ = headers
        # Round-trip through JSON, as the request body on the wire does.
        sent = json.loads(json.dumps(parameters or {}))
        for api in self._apis:
            if href.rstrip("/") == api.base or href.startswith(f"{api.base}/"):
                body = api.respond(href, (method or "GET").upper(), sent)
                return json.dumps(body)
        raise APIError(f"no fake STAC API serves {href}")


def install_fake_stac(monkeypatch: pytest.MonkeyPatch, *apis: FakeStacApi) -> None:
    """Route every STAC discovery request of the test to ``apis``."""
    monkeypatch.setattr(
        "firecube.core.discovery._stac._new_stac_io", lambda: FakeStacIO(list(apis))
    )
