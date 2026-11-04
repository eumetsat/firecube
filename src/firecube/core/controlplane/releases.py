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

"""Named releases of a product, kept under .firecube/releases/.

A release binds a version string to one coherent metadata snapshot. Layout
below .firecube/releases/:

  <key>.json          one release record, created once and never rewritten
  refs/current.json   the version of the current release, published last

<key> is the version with `%` written `%25`, `/` written `%2F` and a leading
`.` written `%2E`; the empty version is the key `%`. The record keeps the
version verbatim. `check_storable` refuses a version whose record the
storage drivers cannot write, list or read. Only the ChunkManager facade
calls the I/O functions here.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

from firecube.core.controlplane.metadata_snapshots import _makedirs, _publish
from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    EVENT_RUN_STARTED,
    RELEASES_DIRNAME,
    canonical_json_bytes,
)
from firecube.core.errors import ConfigurationError, ControlPlaneCorruptionError, ManifestError
from firecube.core.filesystem.protocol import StorageFilesystemFull
from firecube.core.storage.uri import StorageUri

RELEASE_SCHEMA_VERSION: Final = "v1"
REFS_DIRNAME: Final = "refs"
CURRENT_REF_FILENAME: Final = "current.json"
EMPTY_VERSION_KEY: Final = "%"
PRODUCER_RECORDED: Final = "recorded"
PRODUCER_UNKNOWN: Final = "unknown"

_RECORD_SUFFIX: Final = ".json"
# The obstore local writer first creates `<name>#<n>`, where <n> is a decimal counter
# starting at 1. Its width is not established; `#` and 20 digits, the length of the
# largest unsigned 64-bit number, take 21 bytes. The fsspec temporaries have fixed names.
_WRITER_SUFFIX_RESERVE: Final = 21
# The longest object key that S3 takes, in UTF-8 bytes.
_S3_KEY_MAX_BYTES: Final = 1024
# obstore refuses an ASCII control character in a path, and its listing of a directory
# fails whole when one name there holds one.
_CONTROL_CHARACTERS: Final = re.compile(r"[\x00-\x1f\x7f]")
# s3fs reads `?versionId=` in a key as an object-version query and cuts the key there.
_S3FS_VERSION_QUERY: Final = "?versionId="
_RECORD_FIELDS: Final = (
    "version",
    "created_at",
    "previous",
    "snapshot",
    "runs_since_previous",
    "producer",
    "producer_status",
    "note",
    "created_by_run",
)


class ReleaseNotFoundError(ManifestError):
    """The product has no release of that version, or no current release.

    Attributes:
        version: The version asked for, or `None` when the current release was asked for.
    """

    def __init__(self, message: str, *, version: str | None) -> None:
        self.version = version
        super().__init__(message)


class ReleaseExistsError(ManifestError):
    """A record of the version exists already; a release record is created once.

    Attributes:
        version: The version.
        snapshot: The snapshot id the existing record names.
    """

    def __init__(self, message: str, *, version: str, snapshot: str) -> None:
        self.version = version
        self.snapshot = snapshot
        super().__init__(message)


class ReleaseVersionUnstorableError(ConfigurationError):
    """The storage drivers cannot write, list or read the record of the version.

    This is a storage limit, not a rule of version names.

    Attributes:
        version: The version.
    """

    def __init__(self, message: str, *, version: str) -> None:
        self.version = version
        super().__init__(message)


@dataclass(frozen=True)
class StoredRelease:
    """One release record, as stored in `.firecube/releases/<key>.json`.

    Attributes:
        version: The release name, verbatim.
        created_at: Unix time in seconds when the record was written.
        previous: The version that was current when the record was written, or `None`.
        snapshot: The id of the coherent metadata snapshot the release names.
        runs_since_previous: The ids of the runs that started after the previous release.
        producer: The `run_started` producer of the latest run that records one, or `None`.
        producer_status: `"recorded"` when `producer` is set, else `"unknown"`.
        note: The free text given at creation, or `None`.
        created_by_run: The id of the maintenance run that wrote the record.
    """

    version: str
    created_at: float
    previous: str | None
    snapshot: str
    runs_since_previous: tuple[str, ...]
    producer: Mapping[str, Any] | None
    producer_status: Literal["recorded", "unknown"]
    note: str | None
    created_by_run: str


@dataclass(frozen=True)
class ReleaseVerification:
    """How the live metadata documents differ from a release's snapshot.

    Only metadata documents are compared. An edit of chunk bytes is not seen.

    Attributes:
        version: The release version.
        snapshot: The snapshot id the release names.
        added: Paths the store holds and the snapshot lacks.
        removed: Paths the snapshot holds and the store lacks.
        changed: Paths both hold with different bytes.
    """

    version: str
    snapshot: str
    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[str, ...]

    @property
    def matches(self) -> bool:
        """Whether the live documents equal the snapshot's, path for path and byte for byte."""
        return not (self.added or self.removed or self.changed)


def releases_root(product_root: StorageUri) -> StorageUri:
    """Return the `.firecube/releases/` directory of a product root."""
    return product_root.join(CONTROL_DIRNAME, RELEASES_DIRNAME)


def release_key(version: str) -> str:
    """Return the file stem of a version's record: one path segment that no other version shares.

    Raises:
        TypeError: `version` is not a string.
    """
    if not isinstance(version, str):
        raise TypeError(f"a release version is a string, not {type(version).__name__}")
    if version == "":
        # No other version encodes to a bare `%`: every encoded `%` is followed by two hex digits.
        return EMPTY_VERSION_KEY
    key = version.replace("%", "%25").replace("/", "%2F")
    return f"%2E{key[1:]}" if key.startswith(".") else key


def _nearest_directory(path: str) -> str:
    """Return `path`, or its nearest ancestor that exists as a directory."""
    while not os.path.isdir(path):
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    return path


def _local_limit(directory: str, name: str) -> int | None:
    """Return the `os.pathconf` limit `name` of the filesystem holding `directory`, or `None`."""
    try:
        limit = os.pathconf(directory, name)
    except (OSError, ValueError):
        return None
    return limit if limit > 0 else None


def check_storable(root: StorageUri, version: str) -> None:
    """Refuse a version whose record the storage drivers cannot write, list or read.

    Run it before any write of a release. It refuses ASCII control characters, which
    obstore cannot hold in a path. A local record name needs 21 bytes spare for the
    obstore writer's temporary name. The limits come from `os.pathconf` on the target.
    An S3 object key takes at most 1024 bytes and no `?versionId=`, which s3fs cuts
    off. The version grammar stays open: only these storage limits refuse.

    Raises:
        TypeError: `version` is not a string.
        UnicodeEncodeError: `version` cannot be encoded as UTF-8.
        ReleaseVersionUnstorableError: A driver cannot write, list or read the record.
    """
    name = f"{release_key(version)}{_RECORD_SUFFIX}"
    record = root.join(name)
    size = len(name.encode("utf-8"))
    controls = sorted(set(_CONTROL_CHARACTERS.findall(version)))
    if controls:
        raise ReleaseVersionUnstorableError(
            f"release version {version!r} cannot be stored: it contains "
            f"{', '.join(repr(char) for char in controls)}, and the obstore driver cannot "
            "write, list or read a path that holds an ASCII control character",
            version=version,
        )
    if root.protocol == "file":
        directory = _nearest_directory(root.path)
        name_max = _local_limit(directory, "PC_NAME_MAX")
        if name_max is not None and size + _WRITER_SUFFIX_RESERVE > name_max:
            raise ReleaseVersionUnstorableError(
                f"release version {version!r} cannot be stored: its record name takes "
                f"{size} bytes, and the filesystem at {directory} takes names of at most "
                f"{name_max} bytes, {_WRITER_SUFFIX_RESERVE} of them kept for the "
                "writer's temporary name",
                version=version,
            )
        path_max = _local_limit(directory, "PC_PATH_MAX")
        path_size = len(record.path.encode("utf-8")) + _WRITER_SUFFIX_RESERVE
        if path_max is not None and path_size >= path_max:
            raise ReleaseVersionUnstorableError(
                f"release version {version!r} cannot be stored: the path of its record "
                f"and the writer's temporary name take {path_size} bytes, and the "
                f"filesystem at {directory} takes paths shorter than {path_max} bytes",
                version=version,
            )
    elif record.protocol == "s3":
        key_size = len(record.path.lstrip("/").encode("utf-8"))
        if key_size > _S3_KEY_MAX_BYTES:
            raise ReleaseVersionUnstorableError(
                f"release version {version!r} cannot be stored: the object key of its "
                f"record takes {key_size} bytes, and S3 takes keys of at most "
                f"{_S3_KEY_MAX_BYTES} bytes",
                version=version,
            )
        if _S3FS_VERSION_QUERY in name:
            raise ReleaseVersionUnstorableError(
                f"release version {version!r} cannot be stored: it contains "
                f"{_S3FS_VERSION_QUERY!r}, and the fsspec driver reads an S3 key from there "
                "on as an object-version query",
                version=version,
            )


def _record_uri(root: StorageUri, version: str) -> StorageUri:
    return root.join(f"{release_key(version)}{_RECORD_SUFFIX}")


def _current_uri(root: StorageUri) -> StorageUri:
    return root.join(REFS_DIRNAME, CURRENT_REF_FILENAME)


def record_payload(release: StoredRelease) -> dict[str, Any]:
    """Return the JSON object stored for a release record."""
    return {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "version": release.version,
        "created_at": release.created_at,
        "previous": release.previous,
        "snapshot": release.snapshot,
        "runs_since_previous": list(release.runs_since_previous),
        "producer": None if release.producer is None else dict(release.producer),
        "producer_status": release.producer_status,
        "note": release.note,
        "created_by_run": release.created_by_run,
    }


def _load_json(data: bytes, uri: StorageUri) -> dict[str, Any]:
    try:
        content = json.loads(data)
    except (ValueError, RecursionError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not valid JSON: {exc}") from exc
    if not isinstance(content, dict):
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not a JSON object")
    if content.get("schema_version") != RELEASE_SCHEMA_VERSION:
        raise ControlPlaneCorruptionError(
            f"Unsupported release schema version in {uri.to_str()}: "
            f"{content.get('schema_version')!r}"
        )
    return content


def _optional_str(value: Any, name: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise TypeError(f"{name} is not a string or null")
    return value


def _release_from_payload(content: Mapping[str, Any], uri: StorageUri) -> StoredRelease:
    try:
        missing = [name for name in _RECORD_FIELDS if name not in content]
        if missing:
            raise KeyError(", ".join(missing))
        version, snapshot, run = content["version"], content["snapshot"], content["created_by_run"]
        if not all(isinstance(value, str) for value in (version, snapshot, run)):
            raise TypeError("version, snapshot and created_by_run must be strings")
        runs = content["runs_since_previous"]
        if not isinstance(runs, list) or not all(isinstance(item, str) for item in runs):
            raise TypeError("runs_since_previous is not a list of run ids")
        producer = content["producer"]
        if producer is not None and not isinstance(producer, dict):
            raise TypeError("producer is not an object or null")
        status = content["producer_status"]
        if status != (PRODUCER_UNKNOWN if producer is None else PRODUCER_RECORDED):
            raise ValueError(f"producer_status {status!r} does not fit the producer")
        return StoredRelease(
            version=version,
            created_at=float(content["created_at"]),
            previous=_optional_str(content["previous"], "previous"),
            snapshot=snapshot,
            runs_since_previous=tuple(runs),
            producer=producer,
            producer_status=status,
            note=_optional_str(content["note"], "note"),
            created_by_run=run,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlPlaneCorruptionError(f"{uri.to_str()} is not a release record: {exc}") from exc


def find_release(fs: StorageFilesystemFull, root: StorageUri, version: str) -> StoredRelease | None:
    """Return the record of `version`, or `None` when the product has none.

    Raises:
        ControlPlaneCorruptionError: The record does not parse or names another version.
    """
    uri = _record_uri(root, version)
    try:
        data = fs.read_bytes(uri)
    except FileNotFoundError:
        return None
    release = _release_from_payload(_load_json(data, uri), uri)
    if release.version != version:
        raise ControlPlaneCorruptionError(
            f"{uri.to_str()} records version {release.version!r}, not {version!r}"
        )
    return release


def create_record(fs: StorageFilesystemFull, root: StorageUri, release: StoredRelease) -> None:
    """Create the record of a release once; an existing record is never rewritten.

    Raises:
        ReleaseExistsError: A record of the version exists; nothing is written.
    """
    uri = _record_uri(root, release.version)
    _makedirs(fs, uri.parent())
    try:
        fs.atomic_writer.write_atomic(uri, canonical_json_bytes(record_payload(release)))
    except FileExistsError:
        existing = find_release(fs, root, release.version)
        raise ReleaseExistsError(
            f"release {release.version!r} exists already and names snapshot "
            f"{'<unreadable>' if existing is None else existing.snapshot}",
            version=release.version,
            snapshot="" if existing is None else existing.snapshot,
        ) from None


def read_current_version(fs: StorageFilesystemFull, root: StorageUri) -> str | None:
    """Return the version `refs/current.json` names, or `None` when the product has no release.

    Raises:
        ControlPlaneCorruptionError: The ref does not parse.
    """
    uri = _current_uri(root)
    try:
        data = fs.read_bytes(uri)
    except FileNotFoundError:
        return None
    version = _load_json(data, uri).get("version")
    if not isinstance(version, str):
        raise ControlPlaneCorruptionError(f"{uri.to_str()} names no release version")
    return version


def publish_current(fs: StorageFilesystemFull, root: StorageUri, version: str) -> None:
    """Replace `refs/current.json` so it names `version`; a reader sees the old or the new ref."""
    payload = {"schema_version": RELEASE_SCHEMA_VERSION, "version": version}
    _publish(fs, _current_uri(root), canonical_json_bytes(payload))


def list_records(fs: StorageFilesystemFull, root: StorageUri) -> list[StoredRelease]:
    """Read every release record, sorted by `created_at` and then by version.

    A name that starts with `.` is an interrupted write's temporary and is skipped.
    Each record is read by its listed name, so `#`, `?` and `%` stay literal.
    """
    # ``ls`` is on both drivers but not on the protocol. ``refresh=True`` bypasses the
    # s3fs listing cache. ``find`` would turn each listed path back into a URI, and the
    # fsspec driver reads `#` and `?` there as a fragment and a query.
    try:
        entries = fs.ls(root, detail=True, refresh=True)  # pyright: ignore[reportAttributeAccessIssue]
    except FileNotFoundError:
        return []
    releases: list[StoredRelease] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") != "file":
            continue
        name = str(entry.get("name") or "").rstrip("/").rpartition("/")[2]
        if name.startswith(".") or not name.endswith(_RECORD_SUFFIX):
            continue
        uri = root.join(name)
        data = fs.read_bytes(uri)
        release = _release_from_payload(_load_json(data, uri), uri)
        if f"{release_key(release.version)}{_RECORD_SUFFIX}" != name:
            raise ControlPlaneCorruptionError(
                f"{uri.to_str()} records version {release.version!r}, which has another key"
            )
        releases.append(release)
    return sorted(releases, key=lambda release: (release.created_at, release.version))


def runs_since(runs: Iterable[tuple[str, float]], since: float | None) -> tuple[str, ...]:
    """Return the ids of `(run_id, started_at)` pairs started after `since`, oldest first."""
    kept = sorted(
        (started_at, run_id) for run_id, started_at in runs if since is None or started_at > since
    )
    return tuple(run_id for _started_at, run_id in kept)


def latest_producer(
    entries: Sequence[Mapping[str, Any]],
    read_events: Callable[[Mapping[str, Any]], Iterable[Mapping[str, Any]]],
) -> tuple[str, dict[str, Any]] | None:
    """Return `(run_id, producer)` of the latest-started run whose `run_started` event records one.

    `entries` are run entries with `run_id` and `started_at`; `read_events` reads one
    run's WAL events. Runs are read newest first, and the first producer found wins.
    """
    ordered = sorted(
        entries,
        key=lambda entry: (float(entry.get("started_at", 0.0) or 0.0), str(entry.get("run_id"))),
        reverse=True,
    )
    for entry in ordered:
        for event in read_events(entry):
            if event.get("event_type") != EVENT_RUN_STARTED:
                continue
            meta = event.get("meta")
            producer = meta.get("producer") if isinstance(meta, Mapping) else None
            if isinstance(producer, dict):
                return str(entry.get("run_id")), dict(producer)
    return None


def compare_documents(
    version: str,
    snapshot: str,
    captured: Iterable[tuple[str, str]],
    live: Iterable[tuple[str, str]],
) -> ReleaseVerification:
    """Compare `(path, digest)` pairs of the live documents with those a snapshot captured."""
    old, new = dict(captured), dict(live)
    return ReleaseVerification(
        version=version,
        snapshot=snapshot,
        added=tuple(sorted(new.keys() - old.keys())),
        removed=tuple(sorted(old.keys() - new.keys())),
        changed=tuple(sorted(path for path in old.keys() & new.keys() if old[path] != new[path])),
    )
