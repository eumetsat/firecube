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

"""Read-only inspection of Tensogram (.tgm) archives: info, listing and validation."""

# pyright: reportMissingImports=false

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, cast

from firecube.core.errors import FirecubeError
from firecube.core.tensogram import _compat
from firecube.core.tensogram.schema import (
    ARCHIVE_VERSION,
    KEY_ARCHIVE_VERSION,
    ROLE_CONTROLPLANE,
)


class TensogramArchiveError(FirecubeError):
    """A .tgm archive cannot be interpreted as a Firecube archive.

    Attributes:
        path: Local path of the archive that was refused.
    """

    def __init__(self, message: str, *, path: str) -> None:
        self.path = path
        super().__init__(message)


@dataclass(frozen=True)
class TgmObject:
    """One object (array) of a data message, as the listing shows it.

    Attributes:
        index: Position of the object within its message.
        name: Object name, from the message base entry or the descriptor.
        shape: Object shape as a list.
        dtype: Descriptor dtype, unconverted.
        compression: Descriptor compression, unconverted.
    """

    index: int
    name: Any
    shape: list[Any]
    dtype: Any
    compression: Any


@dataclass(frozen=True)
class TgmMessage:
    """One message of a .tgm archive, with its metadata decoded.

    Attributes:
        index: Position of the message within the archive.
        is_controlplane: True for the control-plane message.
        product: Product name of a control-plane message, else empty.
        group_name: Group name, empty for the root, or ``message <index>`` when unrecorded.
        has_firecube_metadata: True when the message carries Firecube metadata.
        source_uri: Source URI recorded at archive time.
        compression: Group codec recorded at archive time.
    """

    index: int
    is_controlplane: bool
    product: Any
    group_name: str
    has_firecube_metadata: bool
    source_uri: Any
    compression: Any
    _read_objects: Callable[[], tuple[TgmObject, ...]] = field(repr=False, compare=False)

    def objects(self) -> tuple[TgmObject, ...]:
        """Decode and return the descriptors of a data message.

        A control-plane message has no objects and is not decoded.
        """
        if self.is_controlplane:
            return ()
        return self._read_objects()


@dataclass(frozen=True)
class TgmListing:
    """Lazy view over the messages of an open .tgm archive.

    Attributes:
        message_count: Number of messages in the archive.
    """

    message_count: int
    _messages: Callable[[], Iterator[TgmMessage]] = field(repr=False, compare=False)

    def messages(self) -> Iterator[TgmMessage]:
        """Yield each message, decoding its metadata when it is reached."""
        return self._messages()


@dataclass(frozen=True)
class TgmValidation:
    """Result of validating a .tgm archive.

    Attributes:
        message_count: Number of messages the validator reported.
        issues: ``(scope, issue)`` pairs in report order. The scope is ``"file"``
            or ``"msg <index>"``.
    """

    message_count: int
    issues: tuple[tuple[str, Any], ...]


def tgm_info(path: str, *, operation: str) -> dict[str, Any]:
    """Summarise a Firecube .tgm archive.

    Args:
        path: Local path of an existing archive.
        operation: Command name used in the missing-extras message.

    Returns:
        A dict with ``path``, ``format``, ``size_bytes``, ``groups``,
        ``has_controlplane`` and ``group_details``.

    Raises:
        ImportError: The tensogram extras are not installed.
        TensogramArchiveError: The archive has no messages or an unsupported version.
    """
    _compat.require_tensogram(operation)

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)

    with tensogram.TensogramFile.open(path) as f:
        count = f.message_count()
        if count == 0:
            raise TensogramArchiveError("Archive contains no messages.", path=path)

        first_meta = f.file_decode_metadata(0)
        first_extra = first_meta.extra or {}
        firecube_meta = first_extra.get("firecube", {})
        archive_version = str(firecube_meta.get(KEY_ARCHIVE_VERSION, ""))
        if archive_version != ARCHIVE_VERSION:
            raise TensogramArchiveError(
                f"Unsupported Firecube archive version: {archive_version or 'missing'}. "
                f"Expected {ARCHIVE_VERSION}.",
                path=path,
            )

        groups_info: list[dict[str, Any]] = []
        has_controlplane = False

        for i in range(count):
            meta = f.file_decode_metadata(i)
            extra = meta.extra or {}
            fc = extra.get("firecube", {})
            role = fc.get("role", "data")

            if role == ROLE_CONTROLPLANE:
                has_controlplane = True
                continue

            group_name = fc.get("group", "")
            base_entries = list(meta.base) if meta.base else []
            result = f.file_decode_descriptors(i)
            descriptors = result.get("descriptors", [])
            archived_coords = set(fc.get("coordinates", []))

            group_vars: dict[str, dict] = {}
            for j, desc in enumerate(descriptors):
                base_entry = base_entries[j] if j < len(base_entries) else {}
                obj_name = str(base_entry.get("name") or f"object_{j}")
                if obj_name not in archived_coords:
                    group_vars[obj_name] = {
                        "shape": list(desc.shape) if desc.shape else [],
                        "dtype": str(desc.dtype),
                        "dims": base_entry.get("dim_names", []),
                        "zarr_chunks": base_entry.get("zarr_chunks"),
                    }

            groups_info.append(
                {
                    "group": group_name,
                    "variables": group_vars,
                    "compression": fc.get("compression", ""),
                    "source_uri": fc.get("source_uri", ""),
                    "archived_at": fc.get("archived_at", ""),
                }
            )

        return {
            "path": path,
            "format": archive_version,
            "size_bytes": os.path.getsize(path),
            "groups": [g["group"] for g in groups_info],
            "has_controlplane": has_controlplane,
            "group_details": groups_info,
        }


@contextmanager
def open_tgm_listing(path: str, *, operation: str) -> Iterator[TgmListing]:
    """Open a .tgm archive for listing and close it on exit.

    The listing decodes lazily: ``messages()`` decodes a message's metadata when
    the caller reaches it, and ``TgmMessage.objects()`` decodes its descriptors
    when called. A decode failure therefore surfaces after the earlier messages
    were consumed.

    Args:
        path: Local path of an existing archive.
        operation: Command name used in the missing-extras message.

    Yields:
        The listing, valid only inside the ``with`` block.

    Raises:
        ImportError: The tensogram extras are not installed.
    """
    _compat.require_tensogram(operation)

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)

    with tensogram.TensogramFile.open(path) as f:
        count = f.message_count()

        def messages() -> Iterator[TgmMessage]:
            for i in range(count):
                meta = f.file_decode_metadata(i)
                extra = meta.extra or {}
                fc = extra.get("firecube", {})
                role = fc.get("role", "data")
                base_entries = list(meta.base) if meta.base else []

                def read_objects(
                    i: int = i, base_entries: list[Any] = base_entries
                ) -> tuple[TgmObject, ...]:
                    result = f.file_decode_descriptors(i)
                    descriptors = result.get("descriptors", [])
                    objects: list[TgmObject] = []
                    for j, desc in enumerate(descriptors):
                        base_entry = base_entries[j] if j < len(base_entries) else {}
                        objects.append(
                            TgmObject(
                                index=j,
                                name=base_entry.get("name")
                                or desc.params.get("name", desc.obj_type),
                                shape=list(desc.shape) if desc.shape else [],
                                dtype=desc.dtype,
                                compression=desc.compression,
                            )
                        )
                    return tuple(objects)

                yield TgmMessage(
                    index=i,
                    is_controlplane=role == ROLE_CONTROLPLANE,
                    product=fc.get("product", ""),
                    group_name=fc.get("group", f"message {i}"),
                    has_firecube_metadata=bool(fc),
                    source_uri=fc.get("source_uri", ""),
                    compression=fc.get("compression", ""),
                    _read_objects=read_objects,
                )

        yield TgmListing(message_count=count, _messages=messages)


def validate_tgm(path: str, *, quick: bool = False, operation: str) -> TgmValidation:
    """Validate the structure of a .tgm archive.

    Args:
        path: Local path of an existing archive.
        quick: Skip hash verification and check structure only.
        operation: Command name used in the missing-extras message.

    Returns:
        The message count and every issue, file issues first.

    Raises:
        ImportError: The tensogram extras are not installed.
    """
    _compat.require_tensogram(operation)

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)

    level = "quick" if quick else "default"
    report = tensogram.validate_file(path, level=level)

    file_issues = report.get("file_issues", [])
    messages = report.get("messages", [])

    issues: list[tuple[str, Any]] = [("file", i) for i in file_issues]
    if not messages:
        issues.append(("file", {"byte_offset": 0, "description": "Archive contains no messages."}))
    issues.extend(
        (f"msg {i}", issue)
        for i, msg_report in enumerate(messages)
        for issue in msg_report.get("issues", [])
    )
    return TgmValidation(message_count=len(messages), issues=tuple(issues))
