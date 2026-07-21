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

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

import click

from firecube.core.storage.uri import StorageUri

SCHEME_TO_STORAGE_TYPE: Final[Mapping[str, str]] = {"file": "local", "s3": "s3"}


@dataclass(frozen=True)
class ParsedUri:
    raw: str
    scheme: str
    normalized: str
    local: bool


def parse_product_uri(raw: str) -> ParsedUri:
    """Parse product URI strings accepted at the CLI boundary."""
    candidate = str(raw or "")
    if not candidate:
        raise click.UsageError("Product URI must not be empty.")

    if "://" not in candidate:
        suggestion = Path(candidate).expanduser().resolve().as_uri()
        raise click.UsageError(
            f"URI scheme required (file:// or s3://). Did you mean {suggestion}?"
        )

    parsed = urlparse(candidate)
    scheme = parsed.scheme.lower()
    if scheme == "file" and parsed.path in ("", "/"):
        raise click.UsageError("file:// URI requires a non-empty path")
    if scheme == "file":
        return _parse_file_uri(candidate, parsed.netloc)
    if scheme == "s3":
        return _parse_s3_uri(candidate)
    raise click.UsageError(f"URI scheme '{scheme}' not supported.")


def resolve_storage_type(uri: ParsedUri) -> str | None:
    """Return the storage type matching a parsed product URI, if known."""
    return SCHEME_TO_STORAGE_TYPE.get(uri.scheme)


def validate_uri_storage_coherence(uri: ParsedUri, storage_type: str) -> None:
    """Raise UsageError when an explicit storage type contradicts the URI scheme."""
    expected = resolve_storage_type(uri)
    if expected is None:
        raise click.UsageError(f"URI scheme '{uri.scheme}' not supported.")
    if expected == storage_type:
        return

    alternate = "an s3:// URI" if storage_type == "s3" else "a file:// URI"
    raise click.UsageError(
        f"--storage-type '{storage_type}' is incompatible with URI scheme '{uri.scheme}'. "
        f"Use --storage-type {expected} for {uri.scheme}:// targets, "
        f"or change --target to {alternate}."
    )


def apply_smart_default(uri: ParsedUri, storage_type: str | None) -> str:
    """Resolve storage_type: explicit CLI value wins, else inferred from scheme."""
    if storage_type is not None:
        validate_uri_storage_coherence(uri, storage_type)
        return storage_type
    return SCHEME_TO_STORAGE_TYPE[uri.scheme]


def _parse_file_uri(raw: str, host: str) -> ParsedUri:
    if host:
        raise click.UsageError(
            f"URI scheme 'file' with non-local host '{host}' not supported. "
            "Use file:///path (three slashes, no host)."
        )
    try:
        uri = StorageUri.parse(raw)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc
    return ParsedUri(raw=raw, scheme="file", normalized=Path(uri.path).as_uri(), local=True)


def _parse_s3_uri(raw: str) -> ParsedUri:
    try:
        uri = StorageUri.parse(raw)
    except ValueError as exc:
        if "authority (bucket) is required" in str(exc):
            raise click.UsageError("authority bucket is required for s3 URIs") from exc
        raise click.UsageError(str(exc)) from exc
    return ParsedUri(raw=raw, scheme="s3", normalized=uri.to_str(), local=False)
