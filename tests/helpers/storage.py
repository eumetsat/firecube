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

"""Storage test helpers (storage-flat-uri, T24).

Replace recurring StorageBinding / StorageSession / IngestContext
boilerplate. Helpers accept typed entry points only (``tmp_path`` or
``protocol``+``authority``); fixtures in ``tests/conftest.py`` call them.
"""

from __future__ import annotations

import importlib
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.types.context import IngestContext, StorageContext

# All module-level import sites that create a local binding to ``_open_fsspec_url``.
# Function-local imports are caught by patching the source (first entry) because
# ``from foo import bar`` *inside a function* re-resolves ``foo.bar`` on every call.
#
# DISCOVERY: regenerate via:
#   grep -rEn "(_open_fsspec_url|\bopen_fsspec_url)" src/firecube/ --include="*.py" \
#     | grep -E "^[^:]+:[0-9]+:(from|import) "
#
# A sentinel test in ``tests/helpers/test_storage_helpers.py`` AST-scans the
# source tree and FAILS if a new module-level import site is introduced without
# being added here — this is the regression guard for Finding 4 (T5.1).
_OPEN_FSSPEC_URL_PATCH_TARGETS: tuple[str, ...] = (
    # Source — also catches function-local ``from … import _open_fsspec_url``
    # in ``firecube/core/formats/netcdf.py`` and ``firecube/ingestor/runtime/workspace.py``.
    "firecube.core.filesystem.ops._open_fsspec_url",
    # Module-level bindings under the original name.
    "firecube.core.zarr.validation._open_fsspec_url",
    "firecube.core.tensogram.converter._open_fsspec_url",
)


def _storage_uri_from_target(target_uri: str | StorageUri) -> StorageUri:
    if isinstance(target_uri, StorageUri):
        return target_uri
    if "://" in target_uri:
        return StorageUri.parse(target_uri)
    return StorageUri.from_local_path(Path(target_uri))


def make_local_session(target_uri: str | StorageUri) -> StorageSession:
    """Build a local fsspec-backed StorageSession for a target URI."""
    uri = _storage_uri_from_target(target_uri)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    return StorageSession(binding)


def local_zarr_handle(target_uri: str | Path, mode: str = "a") -> Any:
    """Build a local fsspec-backed ZarrStoreHandle for a target URI."""
    from firecube.core.config import StorageConfig
    from firecube.core.filesystem.store_factory import create_zarr_store

    return create_zarr_store(
        uri=str(target_uri),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode=mode,
    )


@contextmanager
def assert_no_fsspec_bypass():
    """Assert the legacy fsspec URL opener is not used inside the block.

    Patches every known import site (``_OPEN_FSSPEC_URL_PATCH_TARGETS``).
    Function-local ``from ... import _open_fsspec_url`` callers are covered
    by the source patch since their binding is resolved at call-time.

    Yields ``list[Mock]`` (one per patch target, in declaration order).
    """
    # Eagerly import every target module so module-local bindings resolve to
    # the real function before any patching begins. This prevents a later lazy
    # import from snapshotting an already-active mock as its "original".
    for target in _OPEN_FSSPEC_URL_PATCH_TARGETS:
        module_path = target.rsplit(".", 1)[0]
        importlib.import_module(module_path)

    with ExitStack() as stack:
        mocks = [
            stack.enter_context(patch(target, create=True))
            for target in _OPEN_FSSPEC_URL_PATCH_TARGETS
        ]
        yield mocks
        offenders = [
            (target, mock)
            for target, mock in zip(_OPEN_FSSPEC_URL_PATCH_TARGETS, mocks, strict=True)
            if mock.call_count
        ]
        if offenders:
            details = "; ".join(
                f"{target}: {mock.call_count} call(s) {mock.call_args_list!r}"
                for target, mock in offenders
            )
            total = sum(mock.call_count for _, mock in offenders)
            raise AssertionError(f"Expected no _open_fsspec_url calls; observed {total}: {details}")


def make_obstore_test_session(target_uri: str | StorageUri) -> StorageSession:
    """Build an obstore-backed StorageSession."""
    uri = _storage_uri_from_target(target_uri)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
        driver=StorageDriverConfig(driver="obstore"),
    )
    return StorageSession(binding)


def make_test_binding(
    tmp_path: Path,
    *,
    product: str = "product.zarr",
    format: str = "zarr",
    driver: Literal["fsspec", "obstore"] = "fsspec",
    protocol: str = "file",
    authority: str | None = None,
) -> StorageBinding:
    if protocol == "file":
        uri = StorageUri.from_local_path(tmp_path / product)
    else:
        if not authority:
            raise ValueError(f"authority (bucket) is required for protocol={protocol!r}")
        uri = StorageUri(protocol=protocol, authority=authority, path=f"/{product}")
    identity = ProductIdentity.from_uri(uri, format, product_name=product)
    driver_config = StorageDriverConfig(driver=driver)
    return StorageBinding(identity=identity, driver=driver_config)


def make_test_session(
    tmp_path: Path,
    *,
    product: str = "product.zarr",
    format: str = "zarr",
    driver: Literal["fsspec", "obstore"] = "fsspec",
    protocol: str = "file",
    authority: str | None = None,
) -> StorageSession:
    binding = make_test_binding(
        tmp_path,
        product=product,
        format=format,
        driver=driver,
        protocol=protocol,
        authority=authority,
    )
    return StorageSession(binding)


def make_test_context(
    tmp_path: Path,
    *,
    source: str = "/dev/null",
    product: str = "product.zarr",
    format: str = "zarr",
    driver: Literal["fsspec", "obstore"] = "fsspec",
    protocol: str = "file",
    authority: str | None = None,
    options: dict[str, Any] | None = None,
) -> IngestContext:
    session = make_test_session(
        tmp_path,
        product=product,
        format=format,
        driver=driver,
        protocol=protocol,
        authority=authority,
    )
    return IngestContext(
        source=source,
        target=session.product.product_uri.to_str(),
        output_format=format,
        options=options or {},
        storage=StorageContext(output=session),
    )
