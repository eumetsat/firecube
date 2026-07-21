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

"""Characterization tests for ``derive_target_uri``.

These tests pin the behavior of
``firecube.core.config.derive_target_uri`` after the T16 cleanup: the
function now branches **only** on ``storage_type`` (``local`` reads
``target_path``; ``s3`` reads ``bucket``).  The legacy
``getattr(storage_config, "target_uri", None)`` fallback that historically
let bridge subclasses (e.g. ``_StorageConfigView``, removed in T15) carry a
fully-prefixed base URI is gone.

The "target_uri attribute no longer used" test below is the safety net for
that contract: it sets a ``target_uri`` attribute on a plain ``StorageConfig``
and asserts the function ignores it in favour of the ``bucket``-derived
value.  If someone reintroduces the getattr fallback (or any other
duck-typed ``target_uri`` short-circuit), this test fails loudly.
"""

from __future__ import annotations

import pytest

from firecube.core.config import StorageConfig, derive_target_uri


def test_derive_target_uri_local_with_target_path() -> None:
    config = StorageConfig(storage_type="local")
    config.target_path = "/data/products"  # type: ignore[attr-defined]

    assert derive_target_uri(config) == "/data/products"


def test_derive_target_uri_s3_with_bucket() -> None:
    config = StorageConfig(storage_type="s3")
    config.bucket = "my-bucket"  # type: ignore[attr-defined]

    assert derive_target_uri(config) == "s3://my-bucket"


def test_derive_target_uri_target_uri_attribute_no_longer_used() -> None:
    """Tombstone: ``target_uri`` attribute is IGNORED after T16.

    Before T16, a truthy ``target_uri`` attribute short-circuited every
    other branch (the legacy bridge fallback for ``_StorageConfigView``).
    After T16, the function branches solely on ``storage_type``; the
    ``target_uri`` attribute is no longer consulted.

    A duck-typed config carrying both ``bucket`` and ``target_uri`` must
    resolve to the ``bucket``-derived ``s3://...`` form — proving the
    getattr fallback is gone.
    """
    config = StorageConfig(storage_type="s3")
    config.bucket = "my-bucket"  # type: ignore[attr-defined]
    config.target_uri = "s3://ignored/full/path"  # type: ignore[attr-defined]

    assert derive_target_uri(config) == "s3://my-bucket"


def test_derive_target_uri_local_missing_target_path_raises() -> None:
    config = StorageConfig(storage_type="local")

    with pytest.raises(ValueError, match="Local StorageConfig must have target_path set"):
        derive_target_uri(config)


def test_derive_target_uri_s3_missing_bucket_raises() -> None:
    config = StorageConfig(storage_type="s3")

    with pytest.raises(ValueError, match="S3 StorageConfig must have bucket set"):
        derive_target_uri(config)
