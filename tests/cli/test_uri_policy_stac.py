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

"""Admission of source URIs: ``stac://`` for source options only, refused everywhere else."""

from __future__ import annotations

import click
import pytest

from firecube.cli._uri_policy import (
    admit_input_sources,
    parse_and_validate_source_uri,
    parse_and_validate_uri,
)
from firecube.core.discovery import StacCollectionUri
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.contract

_ADMITTED = {
    "stac://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a": (
        "earth-search.aws.element84.com",
        "/v1",
        "sentinel-2-l2a",
    ),
    "stac://host/api/collections/X": ("host", "/api", "X"),
    "stac://host/collections/X": ("host", "", "X"),
    "stac://host:8080/a/b/collections/x.y-z_1": ("host:8080", "/a/b", "x.y-z_1"),
}


@pytest.mark.parametrize(("raw", "parts"), _ADMITTED.items())
def test_stac_uri_is_admitted_as_a_collection(raw: str, parts: tuple[str, str, str]) -> None:
    uri = parse_and_validate_source_uri(raw, param_hint="--input-data")

    assert uri == StacCollectionUri(*parts)
    assert isinstance(uri, StacCollectionUri)
    assert uri.to_str() == raw
    assert uri.api_url == f"https://{parts[0]}{parts[1]}"


@pytest.mark.parametrize("raw", ["file:///abs/p.zarr", "s3://b/p", "s3://b"])
def test_file_and_s3_return_the_same_storage_uri(raw: str) -> None:
    uri = parse_and_validate_source_uri(raw, param_hint="--input-data")

    assert isinstance(uri, StorageUri)
    assert uri == parse_and_validate_uri(raw, param_hint="--input-data")


@pytest.mark.parametrize(
    "raw",
    ["", "file:///a?x=1", "file://h/p", "file:///", "s3:///missing-bucket", "S3://b/x", "gs://b"],
)
def test_file_and_s3_keep_the_same_refusals(raw: str) -> None:
    with pytest.raises(click.UsageError) as storage:
        parse_and_validate_uri(raw, param_hint="--input-data")
    with pytest.raises(click.UsageError) as source:
        parse_and_validate_source_uri(raw, param_hint="--input-data")

    assert source.value.format_message() == storage.value.format_message()


_STAC_REFUSED = {
    r"must not contain '\?' or '#'": [
        "stac://host/api/collections/X?limit=1",
        "stac://host/api/collections/X#frag",
    ],
    "URI must not contain control characters": ["stac://host/api/collections/X\tY"],
    "stac:// URI must name a host": ["stac:///api/collections/X"],
    r"stac:// URI path must end in /collections/<id>": [
        "stac://host",
        "stac://host/",
        "stac://host/api",
        "stac://host/api/collections",
        "stac://host/api/collections/",
        "stac://host/api/collections/X/",
        "stac://host/api/collections/X/items",
        "stac://host//collections/X",
        "stac://host/api/search",
    ],
    "must be lowercase .* Did you mean stac://": [
        "STAC://host/api/collections/X",
        "Stac://host/api/collections/X",
    ],
}


@pytest.mark.parametrize(("raw", "msg"), [(r, m) for m, rs in _STAC_REFUSED.items() for r in rs])
def test_stac_uri_refuses_each_rule_naming_the_option(raw: str, msg: str) -> None:
    with pytest.raises(click.UsageError, match=msg) as refused:
        parse_and_validate_source_uri(raw, param_hint="--input-data")
    assert refused.value.format_message().startswith("Invalid value for '--input-data': ")


@pytest.mark.parametrize("hint", ["--target", "--source", "--archive", "A_URI"])
def test_parse_and_validate_uri_still_refuses_stac(hint: str) -> None:
    with pytest.raises(click.UsageError, match="URI scheme 'stac' not supported"):
        parse_and_validate_uri("stac://host/api/collections/X", param_hint=hint)


def test_storage_uri_never_holds_a_stac_location() -> None:
    with pytest.raises(ValueError, match="unsupported protocol: 'stac'"):
        StorageUri.parse("stac://host/api/collections/X")


def test_admit_input_sources_keeps_order_and_reports_stac() -> None:
    values = ("/data/raw", "stac://host/api/collections/X", "s3://bucket/prefix")

    admitted, has_stac = admit_input_sources(values, param_hint="--input-data")

    assert admitted == values
    assert has_stac is True


def test_admit_input_sources_without_stac_passes_values_through() -> None:
    values = ("relative/dir", "s3://bucket/prefix", "file:///abs/dir", "https://plugin/own")

    admitted, has_stac = admit_input_sources(values, param_hint="--input-data")

    assert admitted == values
    assert has_stac is False


def test_admit_input_sources_refuses_a_bad_stac_value() -> None:
    with pytest.raises(click.UsageError, match="must name a host"):
        admit_input_sources(("/data", "stac:///api/collections/X"), param_hint="--input-data")
