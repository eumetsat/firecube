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

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from firecube.core.storage.uri import StorageUri


@pytest.mark.parametrize(
    (
        "raw",
        "expected_protocol",
        "expected_authority",
        "expected_path",
        "expected_str",
    ),
    [
        (
            "s3://bucket/product.zarr",
            "s3",
            "bucket",
            "/product.zarr",
            "s3://bucket/product.zarr",
        ),
        (
            "s3://bucket/prefix/product.zarr",
            "s3",
            "bucket",
            "/prefix/product.zarr",
            "s3://bucket/prefix/product.zarr",
        ),
        (
            "s3://bucket/prefix/product.zarr/",
            "s3",
            "bucket",
            "/prefix/product.zarr",
            "s3://bucket/prefix/product.zarr",
        ),
        ("s3://bucket/", "s3", "bucket", "/", "s3://bucket/"),
        (
            "S3://Bucket/Case.zarr",
            "s3",
            "Bucket",
            "/Case.zarr",
            "s3://Bucket/Case.zarr",
        ),
        (
            "s3://bucket//double//slashes.zarr",
            "s3",
            "bucket",
            "/double/slashes.zarr",
            "s3://bucket/double/slashes.zarr",
        ),
        (
            "s3://bucket/path%20encoded/x.zarr",
            "s3",
            "bucket",
            "/path%20encoded/x.zarr",
            "s3://bucket/path%20encoded/x.zarr",
        ),
        (
            "file:///abs/path/product.zarr",
            "file",
            None,
            "/abs/path/product.zarr",
            "file:///abs/path/product.zarr",
        ),
        (
            "file://localhost/abs/path/product.zarr",
            "file",
            None,
            "/abs/path/product.zarr",
            "file:///abs/path/product.zarr",
        ),
        (
            "gs://bucket/product.zarr",
            "gs",
            "bucket",
            "/product.zarr",
            "gs://bucket/product.zarr",
        ),
        (
            "memory:///test/product.zarr",
            "memory",
            None,
            "/test/product.zarr",
            "memory:///test/product.zarr",
        ),
    ],
)
def test_storage_uri_parse_canonical(
    raw: str,
    expected_protocol: str,
    expected_authority: str | None,
    expected_path: str,
    expected_str: str,
) -> None:
    uri = StorageUri.parse(raw)

    assert uri.protocol == expected_protocol
    assert uri.authority == expected_authority
    assert uri.path == expected_path
    assert uri.to_str() == expected_str


@pytest.mark.parametrize(
    ("raw", "error_fragment"),
    [
        ("", "empty"),
        ("s3:///bucket/x", "missing authority"),
        ("/abs/path", "file://"),
        ("relative/path", "scheme"),
        ("product.zarr", "scheme"),
        ("s3://bucket/path?q=1", "query"),
        ("s3://bucket/path#f", "fragment"),
        ("ftp://host/path", "unsupported"),
    ],
)
def test_storage_uri_parse_rejects(raw: str, error_fragment: str) -> None:
    with pytest.raises(ValueError, match=error_fragment):
        StorageUri.parse(raw)


@pytest.mark.parametrize(
    ("uri", "expected_parent"),
    [
        ("s3://bucket/prefix/x.zarr", "s3://bucket/prefix"),
        ("s3://bucket/x.zarr", "s3://bucket"),
        ("s3://bucket", "s3://bucket"),
        ("s3://bucket/", "s3://bucket/"),
        ("file:///abs/path/x.zarr", "file:///abs/path"),
        ("file:///x.zarr", "file:///"),
    ],
)
def test_storage_uri_parent(uri: str, expected_parent: str) -> None:
    assert StorageUri.parse(uri).parent().to_str() == expected_parent


@pytest.mark.parametrize(
    ("uri", "segments", "expected"),
    [
        ("s3://bucket/prefix", ("a", "b"), "s3://bucket/prefix/a/b"),
        ("s3://bucket/prefix/", ("a", "b"), "s3://bucket/prefix/a/b"),
        ("file:///abs", ("a/b/c",), "file:///abs/a/b/c"),
        ("s3://bucket", (".firecube",), "s3://bucket/.firecube"),
    ],
)
def test_storage_uri_join(uri: str, segments: tuple[str, ...], expected: str) -> None:
    assert StorageUri.parse(uri).join(*segments).to_str() == expected


def test_storage_uri_round_trip() -> None:
    canonical_inputs = [
        "s3://bucket/product.zarr",
        "s3://bucket/prefix/product.zarr",
        "s3://bucket/",
        "s3://Bucket/Case.zarr",
        "s3://bucket/path%20encoded/x.zarr",
        "gs://bucket/product.zarr",
        "gs://bucket/",
        "file:///abs/path/product.zarr",
        "file:///",
        "memory:///test/product.zarr",
        "memory:///",
    ]

    for raw in canonical_inputs:
        assert StorageUri.parse(raw).to_str() == raw


def test_storage_uri_direct_constructor_normalizes() -> None:
    uri = StorageUri(protocol="S3", authority="Bucket", path="//x//y/")

    assert uri.protocol == "s3"
    assert uri.authority == "Bucket"
    assert uri.path == "/x/y"
    assert uri.to_str() == "s3://Bucket/x/y"


def test_storage_uri_direct_constructor_rejects_file_authority() -> None:
    with pytest.raises(ValueError, match="authority"):
        StorageUri(protocol="file", authority="host", path="/x")


def test_storage_uri_direct_constructor_rejects_remote_without_authority() -> None:
    with pytest.raises(ValueError, match="authority"):
        StorageUri(protocol="s3", authority=None, path="/x")


def test_storage_uri_from_local_path_requires_absolute() -> None:
    with pytest.raises(ValueError, match="absolute"):
        StorageUri.from_local_path(Path("relative/path"))


def test_storage_uri_from_local_path() -> None:
    uri = StorageUri.from_local_path(Path("/tmp/product.zarr"))

    assert uri == StorageUri(protocol="file", authority=None, path="/tmp/product.zarr")
    assert uri.to_str() == "file:///tmp/product.zarr"


def test_storage_uri_with_protocol_preserves_location_shape() -> None:
    uri = StorageUri.parse("s3://bucket/x.zarr").with_protocol("gs")

    assert uri == StorageUri(protocol="gs", authority="bucket", path="/x.zarr")


def test_storage_uri_is_remote() -> None:
    assert StorageUri.parse("s3://bucket/x.zarr").is_remote()
    assert StorageUri.parse("gs://bucket/x.zarr").is_remote()
    assert not StorageUri.parse("file:///x.zarr").is_remote()
    assert not StorageUri.parse("memory:///x.zarr").is_remote()


def test_storage_uri_no_auto_str_coerce() -> None:
    result = f"{StorageUri.parse('s3://bucket/x.zarr')}/y"

    assert "s3://bucket/x.zarr/y" not in result
    assert "StorageUri" in result


def test_storage_uri_frozen_immutable() -> None:
    uri = StorageUri.parse("s3://bucket/path")

    with pytest.raises(FrozenInstanceError):
        uri.protocol = "file"  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        uri.authority = None  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        uri.path = "/other"  # type: ignore[misc]
