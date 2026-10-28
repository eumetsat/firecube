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

"""STAC source discovery against an in-process STAC API (no network)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from firecube.core.discovery import (
    DiscoveryRequest,
    SourceItem,
    StacApiError,
    StacSearchError,
    UnknownSourceSchemeError,
    discover,
    validate_stac_search,
)
from tests.helpers.fake_stac import FakeStacApi, install_fake_stac, stac_item

pytestmark = pytest.mark.unit

API_A = "https://stac-a.test/api"
API_B = "https://stac-b.test/v1"
SRC_A = "stac://stac-a.test/api/collections/coll-a"
SRC_B = "stac://stac-b.test/v1/collections/coll-b"


def _granule_item(item_id: str, collection: str, base: str) -> dict:
    return stac_item(
        item_id,
        collection,
        {
            "data": (f"https://data.test/{collection}/{item_id}.nc", ["data"]),
            "metadata": (f"https://data.test/{collection}/{item_id}.xml", ["metadata"]),
            "thumbnail": (f"https://data.test/{collection}/{item_id}.png", ["thumbnail"]),
        },
        base=base,
    )


@pytest.fixture
def api_a() -> FakeStacApi:
    items = [_granule_item(f"a-{n}", "coll-a", API_A) for n in (3, 1, 2)]
    items.append(_granule_item("other-1", "coll-other", API_A))
    return FakeStacApi(API_A, items, page_size=2)


@pytest.fixture
def api_b() -> FakeStacApi:
    return FakeStacApi(API_B, [_granule_item("b-1", "coll-b", API_B)])


def test_search_body_takes_collections_from_the_uri_only(monkeypatch, api_a):
    install_fake_stac(monkeypatch, api_a)

    items = discover(
        DiscoveryRequest(
            sources=(SRC_A,),
            stac_search={"datetime": "2025-06-01T00:00:00Z/2025-06-30T23:59:59Z"},
        )
    )

    assert len(api_a.searches) == 1
    body = api_a.searches[0]
    assert body["collections"] == ["coll-a"]
    assert body["datetime"] == "2025-06-01T00:00:00Z/2025-06-30T23:59:59Z"
    assert {item.collection for item in items} == {"coll-a"}


@pytest.mark.parametrize(
    ("layout", "refused", "pages"),
    [
        pytest.param({}, False, 2, id="complete"),
        pytest.param({"empty_pages_at": (1,)}, False, 3, id="empty_page_in_the_middle"),
        pytest.param({"page_size": 0}, True, None, id="endless_empty_pages"),
        pytest.param(
            {"empty_pages_at": (2,), "repeat_next_at": {2: 2}},
            True,
            None,
            id="empty_last_page_repeats_its_own_next",
        ),
        pytest.param({"repeat_next_at": {1: 1}}, True, None, id="next_repeats_an_earlier_request"),
    ],
)
def test_next_links_are_followed_until_the_last_page(monkeypatch, layout, refused, pages):
    items = [_granule_item(f"a-{n}", "coll-a", API_A) for n in (3, 1, 2)]
    api = FakeStacApi(API_A, items, **{"page_size": 2, "max_search_requests": 20, **layout})
    install_fake_stac(monkeypatch, api)

    if refused:
        # An early or endless end of paging must be an error, not a silent short result.
        with pytest.raises(StacApiError, match=r"stac-a\.test"):
            discover(DiscoveryRequest(sources=(SRC_A,)))
        assert api.search_pages < api.max_search_requests
        return

    found = discover(DiscoveryRequest(sources=(SRC_A,)))

    assert api.search_pages == pages
    assert sorted(str(item.item_id) for item in found) == ["a-1", "a-2", "a-3"]


def test_data_assets_are_kept_and_metadata_assets_dropped(monkeypatch, api_a):
    install_fake_stac(monkeypatch, api_a)

    items = discover(DiscoveryRequest(sources=(SRC_A,)))

    assert {item.asset_key for item in items} == {"data"}
    assert all(item.href.endswith(".nc") for item in items)
    assert all("data" in item.roles for item in items)


def test_default_suffix_list_does_not_apply_to_stac(monkeypatch):
    api = FakeStacApi(
        API_A,
        [
            stac_item(
                "cog-1",
                "coll-a",
                {"visual": ("https://data.test/cog-1.tif", ["data", "visual"])},
                base=API_A,
            )
        ],
    )
    install_fake_stac(monkeypatch, api)

    items = discover(DiscoveryRequest(sources=(SRC_A,)))

    assert [item.href for item in items] == ["https://data.test/cog-1.tif"]


def test_input_filters_narrow_stac_items_on_the_href_basename(monkeypatch):
    api = FakeStacApi(
        API_A,
        [
            stac_item(
                f"s-{n}",
                "coll-a",
                {
                    "nc": (f"https://data.test/s-{n}.nc", ["data"]),
                    "tif": (f"https://data.test/s-{n}.tif", ["data"]),
                },
                base=API_A,
            )
            for n in (1, 2)
        ],
    )
    install_fake_stac(monkeypatch, api)

    unfiltered = discover(DiscoveryRequest(sources=(SRC_A,)))
    narrowed = discover(DiscoveryRequest(sources=(SRC_A,), input_filters=["*.nc"]))
    excluded = discover(DiscoveryRequest(sources=(SRC_A,), input_filters=["*.nc", "!s-2.*"]))

    assert len(unfiltered) == 4
    assert [item.href for item in narrowed] == [
        "https://data.test/s-1.nc",
        "https://data.test/s-2.nc",
    ]
    assert [item.href for item in excluded] == ["https://data.test/s-1.nc"]


def test_stac_item_fields_and_identity(monkeypatch, api_a):
    install_fake_stac(monkeypatch, api_a)

    items = discover(DiscoveryRequest(sources=(SRC_A,), stac_search={"ids": ["a-1"]}))

    assert items == [
        SourceItem(
            href="https://data.test/coll-a/a-1.nc",
            scheme="stac",
            collection="coll-a",
            item_id="a-1",
            asset_key="data",
            datetime="2025-06-01T00:00:00Z",
            bbox=(10.0, 40.0, 11.0, 41.0),
            media_type="application/netcdf",
            roles=("data",),
        )
    ]
    assert items[0].identity == "coll-a/a-1/data"


@pytest.mark.parametrize(
    "lookalikes",
    [
        pytest.param((), id="plain"),
        pytest.param((("b/c", "d"), ("b", "c/d")), id="slash_moves_between_id_and_asset"),
        pytest.param((("b/c", "data"), ("b%2Fc", "data")), id="slash_against_its_escape"),
    ],
)
def test_dedup_and_ordering_are_deterministic(monkeypatch, api_a, api_b, lookalikes):
    for n, (item_id, asset_key) in enumerate(lookalikes):
        href = f"https://data.test/coll-b/look-{n}.nc"
        api_b.items.append(stac_item(item_id, "coll-b", {asset_key: (href, ["data"])}, base=API_B))
    install_fake_stac(monkeypatch, api_a, api_b)

    forward = discover(DiscoveryRequest(sources=(SRC_A, SRC_B, SRC_A)))
    backward = discover(DiscoveryRequest(sources=(SRC_B, SRC_A)))

    assert forward == backward
    identities = [item.identity for item in forward]
    assert len(forward) == 4 + len(lookalikes)
    assert len(set(identities)) == len(forward)
    if not lookalikes:
        assert identities == [
            "coll-a/a-1/data",
            "coll-a/a-2/data",
            "coll-a/a-3/data",
            "coll-b/b-1/data",
        ]


def test_walk_and_stac_sources_mix_in_one_request(monkeypatch, api_b, tmp_path: Path):
    install_fake_stac(monkeypatch, api_b)
    (tmp_path / "local-0.nc").write_bytes(b"")

    items = discover(DiscoveryRequest(sources=(str(tmp_path), SRC_B)))

    assert [(item.scheme, item.href) for item in items] == [
        ("stac", "https://data.test/coll-b/b-1.nc"),
        ("file", str(tmp_path / "local-0.nc")),
    ]
    walked = items[1]
    assert walked.identity == walked.href
    assert (walked.collection, walked.item_id, walked.asset_key, walked.roles) == (
        None,
        None,
        None,
        (),
    )


def test_empty_collection_yields_no_items(monkeypatch):
    api = FakeStacApi(
        API_A,
        [
            stac_item(
                "meta-only",
                "coll-a",
                {"metadata": ("https://data.test/meta-only.xml", ["metadata"])},
                base=API_A,
            )
        ],
    )
    install_fake_stac(monkeypatch, api)

    assert discover(DiscoveryRequest(sources=(SRC_A,))) == []


def test_unknown_scheme_is_refused_by_the_registry():
    with pytest.raises(UnknownSourceSchemeError, match="ftp") as refused:
        discover(DiscoveryRequest(sources=("ftp://host/data",)))

    assert isinstance(refused.value, ValueError)
    assert refused.value.scheme == "ftp"


def test_api_failure_is_a_discovery_error_naming_the_source(monkeypatch, api_a):
    api_a.fail_search = True
    install_fake_stac(monkeypatch, api_a)

    with pytest.raises(StacApiError, match=r"stac-a\.test") as refused:
        discover(DiscoveryRequest(sources=(SRC_A,)))

    assert refused.value.source == SRC_A


def test_missing_extra_names_the_operation(monkeypatch):
    monkeypatch.setattr("firecube.core.discovery._stac_compat.HAS_PYSTAC_CLIENT", False)

    with pytest.raises(ImportError, match=r"^firecube ingest requires the stac extra"):
        discover(DiscoveryRequest(sources=(SRC_A,), operation="firecube ingest"))


@pytest.mark.parametrize(
    "value",
    [
        {"datetime": "2025-06-01/2025-06-02"},
        {"bbox": [1, 2, 3, 4], "ids": ["x"]},
        {"intersects": {"type": "Point", "coordinates": [1.0, 2.0]}},
        {},
    ],
)
def test_validate_stac_search_accepts_the_allowed_keys(value):
    assert validate_stac_search(value) == value


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ({"collections": ["x"]}, "collections"),
        ({"datetime": "x", "limit": 10}, "limit"),
        ({"query": {}}, "query"),
        (["datetime"], "JSON object"),
        ("datetime", "JSON object"),
    ],
)
def test_validate_stac_search_refuses_other_keys(monkeypatch, api_a, value, match):
    install_fake_stac(monkeypatch, api_a)

    with pytest.raises(StacSearchError, match=match):
        validate_stac_search(value)
    with pytest.raises(StacSearchError, match=match):
        discover(DiscoveryRequest(sources=(SRC_A,), stac_search=value))

    assert api_a.searches == []


@pytest.mark.contract
def test_core_api_import_does_not_load_the_stac_client():
    repo_root = Path(__file__).resolve().parents[2]

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, firecube.core.api; print('pystac_client' in sys.modules)",
        ],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env={**os.environ, "PYTHONPATH": str(repo_root / "src")},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"
