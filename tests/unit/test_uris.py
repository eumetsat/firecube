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

from importlib import import_module
from pathlib import Path

import pytest

StorageUri = import_module("firecube.core.storage.uri").StorageUri
local_path_from_target = import_module("firecube.core.uris").local_path_from_target
parse_target = import_module("firecube.core.uris").parse_target
storage_uri_from_target = import_module("firecube.core.uris").storage_uri_from_target


def test_parse_target_accepts_file_uri() -> None:
    uri = parse_target("file:///tmp/x.zarr")

    assert isinstance(uri, StorageUri)
    assert uri.protocol == "file"


def test_parse_target_accepts_s3_uri() -> None:
    uri = parse_target("s3://bucket/data/x.zarr")

    assert isinstance(uri, StorageUri)
    assert uri.protocol == "s3"
    assert uri.authority == "bucket"


def test_parse_target_accepts_bare_absolute_path() -> None:
    uri = parse_target("/tmp/x.zarr")

    assert uri == StorageUri.from_local_path("/tmp/x.zarr")


def test_parse_target_returns_storage_uri_identity() -> None:
    original = StorageUri.parse("file:///tmp/y.zarr")

    assert parse_target(original) is original


def test_parse_target_rejects_relative_path() -> None:
    with pytest.raises(ValueError, match="absolute path or URI"):
        parse_target("relative/path.zarr")


@pytest.mark.unit
def test_storage_uri_from_target_handles_remote_and_local_targets() -> None:
    s3_uri = storage_uri_from_target("s3://bucket/data/x.zarr")
    file_uri = storage_uri_from_target("file:///tmp/x.zarr")
    bare_uri = storage_uri_from_target("/tmp/x.zarr")

    assert s3_uri == StorageUri.parse("s3://bucket/data/x.zarr")
    assert file_uri == StorageUri.parse("file:///tmp/x.zarr")
    assert bare_uri == StorageUri.from_local_path("/tmp/x.zarr")


@pytest.mark.unit
def test_local_path_from_target_handles_file_localhost() -> None:
    assert local_path_from_target("file://localhost/tmp/x.zarr") == Path("/tmp/x.zarr")


@pytest.mark.unit
def test_local_path_from_target_handles_file_empty_authority() -> None:
    assert local_path_from_target("file:///tmp/x.zarr") == Path("/tmp/x.zarr")


@pytest.mark.unit
def test_local_path_from_target_handles_bare_path() -> None:
    assert local_path_from_target("/tmp/x.zarr") == Path("/tmp/x.zarr")
