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

from pathlib import Path

import click
import pytest

from firecube.cli._uri_policy import (
    SCHEME_TO_STORAGE_TYPE,
    ParsedUri,
    apply_smart_default,
    parse_product_uri,
    resolve_storage_type,
    validate_uri_storage_coherence,
)
from firecube.core.storage.session import _storage_type_for_uri
from firecube.core.storage.uri import StorageUri


def test_scheme_mapping_matches_storage_session_for_file() -> None:
    uri = StorageUri.parse("file:///tmp/example.zarr")

    assert SCHEME_TO_STORAGE_TYPE["file"] == _storage_type_for_uri(uri)


def test_scheme_mapping_matches_storage_session_for_s3() -> None:
    uri = StorageUri.parse("s3://bucket/example.zarr")

    assert SCHEME_TO_STORAGE_TYPE["s3"] == _storage_type_for_uri(uri)


@pytest.mark.parametrize(
    ("raw", "expected_path"),
    [
        ("file:///abs/path.zarr", "/abs/path.zarr"),
    ],
)
def test_parse_product_uri_accepts_local_absolute_forms(raw: str, expected_path: str) -> None:
    parsed = parse_product_uri(raw)

    assert parsed == ParsedUri(
        raw=raw,
        scheme="file",
        normalized=Path(expected_path).as_uri(),
        local=True,
    )


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("file://", "file:// URI requires a non-empty path"),
        ("file:///", "file:// URI requires a non-empty path"),
    ],
)
def test_parse_product_uri_rejects_empty_file_path(raw: str, message: str) -> None:
    with pytest.raises(click.UsageError, match=message):
        parse_product_uri(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "s3://bucket/path.zarr",
        "s3://bucket",
        "s3://bucket/nested/path.zarr",
    ],
)
def test_parse_product_uri_accepts_s3(raw: str) -> None:
    parsed = parse_product_uri(raw)

    assert parsed == ParsedUri(raw=raw, scheme="s3", normalized=raw, local=False)


@pytest.mark.parametrize(
    "raw",
    ["/abs/path.zarr", "./rel/path.zarr", "rel/path.zarr", "x.zarr"],
)
def test_parse_product_uri_rejects_bare_paths_with_did_you_mean(raw: str) -> None:
    with pytest.raises(
        click.UsageError, match=r"URI scheme required \(file:// or s3://\)\. Did you mean file://"
    ):
        parse_product_uri(raw)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("file://hostname/path", r"URI scheme 'file' with non-local host 'hostname' not supported"),
        ("gs://bucket/x", "URI scheme 'gs' not supported"),
        ("abfs://container/x", "URI scheme 'abfs' not supported"),
        ("https://x.com/x", "URI scheme 'https' not supported"),
        ("memory://x", "URI scheme 'memory' not supported"),
        ("ftp://host/x", "URI scheme 'ftp' not supported"),
        ("s3:///missing-bucket", "authority (bucket) is required"),
    ],
)
def test_parse_product_uri_rejects_unsupported_or_malformed(raw: str, message: str) -> None:
    with pytest.raises(click.UsageError, match=message):
        parse_product_uri(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("file:///abs/path.zarr", "local"),
        ("s3://bucket/path.zarr", "s3"),
    ],
)
def test_resolve_storage_type_returns_scheme_mapping(raw: str, expected: str) -> None:
    assert resolve_storage_type(parse_product_uri(raw)) == expected


@pytest.mark.parametrize(
    ("raw", "storage_type"),
    [
        ("file:///abs/path.zarr", "local"),
        ("s3://bucket/path.zarr", "s3"),
    ],
)
def test_validate_uri_storage_coherence_accepts_matches(raw: str, storage_type: str) -> None:
    validate_uri_storage_coherence(parse_product_uri(raw), storage_type)


@pytest.mark.parametrize(
    ("raw", "storage_type", "scheme"),
    [
        ("file:///abs/path.zarr", "s3", "file"),
        ("s3://bucket/path.zarr", "local", "s3"),
    ],
)
def test_validate_uri_storage_coherence_rejects_mismatches(
    raw: str, storage_type: str, scheme: str
) -> None:
    with pytest.raises(click.UsageError, match=f"incompatible with URI scheme '{scheme}'"):
        validate_uri_storage_coherence(parse_product_uri(raw), storage_type)


def test_apply_smart_default_infers_local_from_file_uri() -> None:
    assert apply_smart_default(parse_product_uri("file:///abs/path.zarr"), None) == "local"


def test_apply_smart_default_infers_s3_from_uri_scheme() -> None:
    assert apply_smart_default(parse_product_uri("s3://bucket/path.zarr"), None) == "s3"


def test_apply_smart_default_accepts_explicit_local() -> None:
    uri = parse_product_uri("file:///abs/path.zarr")

    assert apply_smart_default(uri, "local") == "local"


def test_apply_smart_default_validates_after_resolution() -> None:
    uri = parse_product_uri("file:///abs/path.zarr")

    with pytest.raises(click.UsageError, match="incompatible with URI scheme 'file'"):
        apply_smart_default(uri, "s3")


def test_apply_smart_default_accepts_explicit_s3_storage_type() -> None:
    uri = parse_product_uri("s3://bucket/path.zarr")

    assert apply_smart_default(uri, "s3") == "s3"


# ---------------------------------------------------------------------------
# Archive subcommands: archive parameter must carry a URI scheme
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("subcommand", ["create", "restore"])
def test_archive_subcommand_rejects_bare_archive_path(
    subcommand: str,
    tmp_path: Path,
) -> None:
    """create and restore must reject bare local paths (no file:// scheme)."""
    archive_path = tmp_path / "archive.tgm"
    archive_path.write_bytes(b"not a real archive")
    base = ["archive", subcommand, "--archive", str(archive_path)]

    if subcommand == "create":
        base.extend(["--source", "file:///tmp/fake.zarr"])
        base.append("--dry-run")
    else:
        base.extend(["--target", "file:///tmp/restored.zarr"])
        base.append("--dry-run")

    result = CliRunner().invoke(cli, base)
    assert result.exit_code != 0
    assert "URI scheme required" in result.output


def test_archive_create_accepts_file_uri(tmp_path: Path) -> None:
    """create accepts a proper file:// URI for --archive."""
    archive_path = tmp_path / "archive.tgm"
    archive_path.write_bytes(b"not a real archive")

    result = CliRunner().invoke(
        cli,
        [
            "archive", "create",
            "--source", "file:///tmp/fake.zarr",
            "--archive", archive_path.as_uri(),
            "--dry-run",
        ],
    )
    # Should not fail with URI-scheme error (may fail for other reasons)
    assert "URI scheme required" not in result.output


def test_archive_restore_accepts_file_uri(tmp_path: Path) -> None:
    """restore accepts a proper file:// URI for --archive."""
    archive_path = tmp_path / "archive.tgm"
    archive_path.write_bytes(b"not a real archive")

    result = CliRunner().invoke(
        cli,
        [
            "archive", "restore",
            "--archive", archive_path.as_uri(),
            "--target", "file:///tmp/restored.zarr",
            "--dry-run",
        ],
    )
    # Should not fail with URI-scheme error
    assert "URI scheme required" not in result.output
