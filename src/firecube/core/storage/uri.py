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

"""Canonical storage URI value object.

Canonical URI Contract:
- Protocol is always lowercased. Supported protocols are: s3, gs, file, memory.
- Authority is required for s3/gs (bucket) and absent for file/memory.
- file://localhost/... normalizes localhost to absent authority.
- Path always starts with /, collapses duplicate slashes, and strips trailing /
  except for root (/).
- Query strings and fragments are not supported.
- Bare paths without URI schemes are rejected; use file:///... for local paths.
- Percent-encoded characters are preserved as-is. Storage adapters own decoding.
- to_str() emits canonical round-trippable strings:
  file:///abs/path, memory:///test/path, s3://bucket/path, gs://bucket/path.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

SUPPORTED_PROTOCOLS = frozenset(
    {
        "s3",
        # gs retained for planned future GCS support; not currently advertised in CLI choices
        "gs",
        "file",
        "memory",
    }
)
REMOTE_PROTOCOLS = frozenset({"s3", "gs"})
LOCAL_PROTOCOLS = frozenset({"file", "memory"})


def _validate_protocol(protocol: str) -> str:
    normalized = str(protocol or "").strip().lower()
    if not normalized:
        raise ValueError("a URI scheme is required (use file:///… or s3://…)")
    if normalized not in SUPPORTED_PROTOCOLS:
        supported = ", ".join(sorted(SUPPORTED_PROTOCOLS))
        raise ValueError(f"unsupported protocol: {normalized!r}; supported: {supported}")
    return normalized


def _normalize_path(path: str) -> str:
    candidate = str(path or "")
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    candidate = re.sub(r"/+", "/", candidate)
    collapsed = posixpath.normpath(candidate)
    if collapsed == ".":
        return "/"
    return collapsed.rstrip("/") or "/"


@dataclass(frozen=True, slots=True)
class StorageUri:
    protocol: str
    authority: str | None
    path: str
    _root_had_slash: bool = field(default=True, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        protocol = _validate_protocol(self.protocol)
        authority = str(self.authority) if self.authority is not None else None
        if authority == "":
            authority = None
        if protocol == "file" and authority == "localhost":
            authority = None
        path = _normalize_path(self.path)

        if protocol in REMOTE_PROTOCOLS and authority is None:
            raise ValueError("authority (bucket) is required for s3/gs URIs")
        if protocol in LOCAL_PROTOCOLS and authority is not None:
            raise ValueError(f"authority is not supported for {protocol} URIs")

        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "authority", authority)
        object.__setattr__(self, "path", path)

    @classmethod
    def parse(cls, raw: str) -> StorageUri:
        candidate = str(raw or "")
        if not candidate.strip():
            raise ValueError("StorageUri input must not be empty")

        parsed = urlparse(candidate)
        if not parsed.scheme:
            if candidate.startswith("/"):
                raise ValueError("bare paths are not supported; use file:///…")
            raise ValueError("a URI scheme is required (use file:///… or s3://…)")
        if parsed.query:
            raise ValueError("query strings not supported in storage URIs")
        if parsed.fragment:
            raise ValueError("fragments not supported in storage URIs")

        protocol = _validate_protocol(parsed.scheme)
        authority = parsed.netloc or None
        if protocol == "file" and authority == "localhost":
            authority = None
        if protocol in REMOTE_PROTOCOLS and authority is None:
            raise ValueError("missing authority: authority (bucket) is required for s3/gs URIs")

        uri = cls(protocol=protocol, authority=authority, path=parsed.path)
        object.__setattr__(uri, "_root_had_slash", bool(parsed.path))
        return uri

    @classmethod
    def from_local_path(cls, abs_path: str | Path) -> StorageUri:
        path = Path(abs_path)
        if not path.is_absolute():
            raise ValueError("from_local_path requires an absolute path")
        return cls(protocol="file", authority=None, path=path.as_posix())

    def join(self, *segments: str) -> StorageUri:
        cleaned_segments = [segment.strip("/") for segment in segments if segment.strip("/")]
        if not cleaned_segments:
            return self
        joined_path = posixpath.join(self.path.rstrip("/"), *cleaned_segments)
        return StorageUri(
            protocol=self.protocol,
            authority=self.authority,
            path=joined_path,
        )

    def with_protocol(self, p: str) -> StorageUri:
        protocol = _validate_protocol(p)
        if protocol in REMOTE_PROTOCOLS and self.authority is None:
            raise ValueError("authority (bucket) is required for s3/gs URIs")
        if protocol in LOCAL_PROTOCOLS:
            return StorageUri(protocol=protocol, authority=None, path=self.path)
        return StorageUri(protocol=protocol, authority=self.authority, path=self.path)

    def parent(self) -> StorageUri:
        if self.path == "/":
            return self
        parent_path = posixpath.dirname(self.path)
        parent = StorageUri(
            protocol=self.protocol,
            authority=self.authority,
            path=parent_path or "/",
        )
        if parent.path == "/" and self.protocol in REMOTE_PROTOCOLS:
            object.__setattr__(parent, "_root_had_slash", False)
        return parent

    def is_remote(self) -> bool:
        return self.protocol in REMOTE_PROTOCOLS

    def to_str(self) -> str:
        if self.authority is None:
            return f"{self.protocol}://{self.path}"
        if self.path == "/":
            if not self._root_had_slash:
                return f"{self.protocol}://{self.authority}"
            return f"{self.protocol}://{self.authority}/"
        return f"{self.protocol}://{self.authority}{self.path}"
