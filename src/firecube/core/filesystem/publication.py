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


"""Driver-neutral helpers for publishing an object under a name readers can trust.

The ``AtomicPublish`` capability of both storage adapters delegates here. This
module is the one place that knows the reserved temporary name and the local
rename dance; the listings of both adapters and the staged-metadata seeder use
``is_publication_temp`` to leave in-flight temporaries out. The fsspec atomic
writer stages its local writes under the same reserved name.
"""

from __future__ import annotations

import contextlib
import os
import stat
import uuid
from collections.abc import Callable

from firecube.core.storage.uri import StorageUri

_PUBLICATION_TEMP_PREFIX = ".firecube-publish-"
_PUBLICATION_TEMP_SUFFIX = ".tmp"


def publication_temp_name() -> str:
    """Return a fresh reserved file name for an object that is being published.

    A local object is written under this name beside its destination and then
    renamed onto it (see ``publish_atomic``). Names of this form are reserved:
    ``find`` and ``ls`` of both drivers leave them out, so a listing taken
    while a peer is publishing never returns the half-written temporary.
    """
    return f"{_PUBLICATION_TEMP_PREFIX}{uuid.uuid4().hex}{_PUBLICATION_TEMP_SUFFIX}"


def is_publication_temp(path: str) -> bool:
    """True if the last segment of ``path`` is a reserved publication temporary."""
    name = path.rstrip("/").rsplit("/", 1)[-1]
    return name.startswith(_PUBLICATION_TEMP_PREFIX) and name.endswith(_PUBLICATION_TEMP_SUFFIX)


def publish_atomic(dst_uri: StorageUri, write: Callable[[StorageUri], None]) -> None:
    """Run ``write`` so that a concurrent reader of ``dst_uri`` never sees a torn object.

    S3 and GCS publish a PUT or a completed multipart upload whole, and an
    in-memory filesystem has no other process reading it, so for anything but
    a ``file`` target ``write`` targets ``dst_uri`` directly. A local file
    written in place is truncated first: a peer reading it meanwhile gets zero
    or partial bytes (staged pods re-publish the shared root ``zarr.json`` and
    time coordinate chunk while other pods seed from them). On a local target
    ``write`` goes to a reserved temporary in the destination's directory,
    hidden from ``find`` and ``ls``, which ``os.replace`` then moves onto
    ``dst_uri`` in one step. An existing regular file's permission bits are
    copied onto the temporary first, as the old in-place write kept them; a new
    destination gets the umask-derived mode. If ``write`` or the rename fails,
    the temporary is removed and ``dst_uri`` keeps its previous bytes.
    """
    if dst_uri.protocol != "file":
        write(dst_uri)
        return

    tmp_uri = dst_uri.parent().join(publication_temp_name())
    try:
        write(tmp_uri)
        try:
            dst_stat = os.lstat(dst_uri.path)
        except FileNotFoundError:
            pass
        else:
            if stat.S_ISREG(dst_stat.st_mode):
                os.chmod(tmp_uri.path, stat.S_IMODE(dst_stat.st_mode) & 0o777)
        os.replace(tmp_uri.path, dst_uri.path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_uri.path)
        raise
