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

import contextlib
import sys
from pathlib import Path
from types import TracebackType
from typing import Any
from unittest.mock import patch

import fsspec
import xarray as xr
import zarr

from firecube.core.product.identity import ProductIdentity
from firecube.core.product.target import ProductTarget
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri

BT2_APPROVED_MODULES = frozenset(
    {
        "firecube.core.filesystem.fsspec_backend",
        "firecube.core.filesystem.ops",
        "firecube.core.filesystem.store_factory",
        "firecube.core.storage.session",
        "firecube.core.uris",
        "firecube.core.zarr.strategies.indexed_region",
        "firecube.core.zarr.strategies.append",
        "firecube.core.zarr.validation",
        "firecube.core.zarr.scrub",
        "firecube.core.zarr.state",
        "firecube.core.zarr.multires",
        "firecube.core.tensogram.converter",
        "firecube.core.tensogram.metadata",
        "firecube.core.tensogram.restore",
    }
)

BT7Roots = tuple[
    StorageUri | None,
    StorageUri | None,
    StorageUri | None,
    StorageUri | None,
    StorageUri | None,
    StorageUri | None,
]


def _infer_format(target: StorageUri) -> str:
    name = Path(target.path).name
    if name.endswith(".zarr"):
        return "zarr"
    if name.endswith(".parquet"):
        return "parquet"
    return "zarr"


def bt7_root_factory(target: StorageUri, command: str, **kwargs: Any) -> BT7Roots:
    """Return six canonical roots for a command harness probe.

    Slot order:
      1. ``resolved_product.product_uri``
      2. ``ChunkManager`` product root
      3. zarr-write root (URI handed to store/open-group seams)
      4. archive-source-product root
      5. catalog root
      6. validation root

    Commands that do not traverse a slot return ``None`` there. Cells assert
    equality only across non-``None`` slots.
    """

    _ = kwargs.pop("storage_driver", "fsspec")
    driver_config = StorageDriverConfig.from_storage_config_or_default(None)
    target_base_uri = target.parent()
    resolved_product = ProductTarget.resolve(
        target.to_str(),
        driver_config,
        product_name="product",
        plugin_default_format=str(kwargs.pop("plugin_default_format", _infer_format(target))),
        default_base_uri=target_base_uri,
    )
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(
                resolved_product.product_uri,
                resolved_product.format,
                product_name=resolved_product.product_name,
            ),
            driver=driver_config,
        )
    )
    chunk_manager = session.control_plane()
    product_root = StorageUri.parse(chunk_manager.get_product_root(resolved_product.product_name))

    zarr_write_root: StorageUri | None = None
    archive_source_root: StorageUri | None = None
    catalog_root: StorageUri | None = None
    validation_root: StorageUri | None = None

    normalized = command.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized == "ingest":
        zarr_write_root = resolved_product.product_uri
    elif normalized in {"archive_create", "archivecreate"}:
        archive_source_root = resolved_product.product_uri
    elif normalized in {"archive_restore", "archiverestore"}:
        zarr_write_root = resolved_product.product_uri
    elif normalized == "advise":
        # Advise reads via session.zarr.open_group(resolved_product.product_uri),
        # so the zarr-store seam URI is the product URI itself.
        zarr_write_root = resolved_product.product_uri
    elif normalized in {"catalog", "catalog_intake", "catalogintake"}:
        catalog_root = resolved_product.output_base_uri.join(resolved_product.product_name)
    elif normalized in {"zarr", "zarr_validate", "validate", "zarrvalidate"}:
        validation_root = resolved_product.output_base_uri.join(resolved_product.product_name)
    else:
        raise ValueError(f"Unsupported BT7 command: {command!r}")

    return (
        resolved_product.product_uri,
        product_root,
        zarr_write_root,
        archive_source_root,
        catalog_root,
        validation_root,
    )


_FIRECUBE_MODULE_PREFIX = "firecube."


def _caller_module_name() -> str:
    # Walk up the stack to find the closest firecube frame so that intermediate
    # third-party frames (xarray/zarr internals) do not mask the actual firecube
    # caller. External-only stacks (no firecube frame, e.g. test scaffolding)
    # return ``<external>`` so the BT2 wrapper can pass the call through.
    depth = 3
    while depth < 50:
        try:
            module_name = sys._getframe(depth).f_globals.get("__name__")
        except ValueError:
            break
        if isinstance(module_name, str) and module_name.startswith(_FIRECUBE_MODULE_PREFIX):
            return module_name
        depth += 1
    return "<external>"


class BT2PoisonContext:
    def __init__(self, *, approved_modules: frozenset[str] | None = None) -> None:
        self._approved_modules = approved_modules or BT2_APPROVED_MODULES
        self._stack = contextlib.ExitStack()
        self._original_fsspec_filesystem = fsspec.filesystem
        self._original_fsspec_get_mapper = fsspec.get_mapper
        self._original_fsspec_open = fsspec.open
        self._original_fsspec_url_to_fs = fsspec.url_to_fs
        self._original_zarr_open_group = zarr.open_group
        self._original_xarray_open_zarr = xr.open_zarr
        self._original_dataset_to_zarr = xr.Dataset.to_zarr

    def __enter__(self) -> BT2PoisonContext:
        self._stack.enter_context(
            patch("fsspec.filesystem", self._make_passthrough_or_poison("fsspec.filesystem"))
        )
        self._stack.enter_context(
            patch("fsspec.get_mapper", self._make_passthrough_or_poison("fsspec.get_mapper"))
        )
        self._stack.enter_context(
            patch("fsspec.open", self._make_passthrough_or_poison("fsspec.open"))
        )
        self._stack.enter_context(
            patch("fsspec.url_to_fs", self._make_passthrough_or_poison("fsspec.url_to_fs"))
        )
        self._stack.enter_context(patch("zarr.open_group", self._poison_zarr_open_group))
        self._stack.enter_context(patch("xarray.open_zarr", self._poison_xarray_open_zarr))
        # Wrap as a plain function so xarray's instance-method dispatch passes
        # ``ds`` as the first positional argument; bound-method patching would
        # swallow the ``self``-equivalent and break ``ds.to_zarr(...)`` calls.
        context = self

        def _to_zarr_replacement(ds: xr.Dataset, *args: Any, **kwargs: Any) -> Any:
            return context._poison_dataset_to_zarr(ds, *args, **kwargs)

        self._stack.enter_context(patch.object(xr.Dataset, "to_zarr", _to_zarr_replacement))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stack.close()

    def _raise_or_call(self, name: str, original: Any, *args: Any, **kwargs: Any) -> Any:
        caller = _caller_module_name()
        if caller == "<external>" or caller in self._approved_modules:
            return original(*args, **kwargs)
        raise RuntimeError(f"BT2_POISON: {name} called from disallowed module {caller}")

    def _make_passthrough_or_poison(self, name: str):
        attr_name = f"_original_{name.replace('.', '_')}"
        original = getattr(self, attr_name)

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self._raise_or_call(name, original, *args, **kwargs)

        return wrapper

    def _poison_zarr_open_group(self, *args: Any, **kwargs: Any) -> Any:
        candidate = args[0] if args else kwargs.get("store")
        if isinstance(candidate, (str, StorageUri)):
            return self._raise_or_call(
                "zarr.open_group",
                self._original_zarr_open_group,
                *args,
                **kwargs,
            )
        return self._original_zarr_open_group(*args, **kwargs)

    def _poison_xarray_open_zarr(self, *args: Any, **kwargs: Any) -> Any:
        if "storage_options" in kwargs:
            return self._raise_or_call(
                "xarray.open_zarr",
                self._original_xarray_open_zarr,
                *args,
                **kwargs,
            )
        return self._original_xarray_open_zarr(*args, **kwargs)

    def _poison_dataset_to_zarr(self, ds: xr.Dataset, *args: Any, **kwargs: Any) -> Any:
        if "storage_options" in kwargs:
            return self._raise_or_call(
                "xarray.Dataset.to_zarr",
                self._original_dataset_to_zarr,
                ds,
                *args,
                **kwargs,
            )
        return self._original_dataset_to_zarr(ds, *args, **kwargs)


def bt2_poison(*, approved_modules: frozenset[str] | None = None) -> BT2PoisonContext:
    return BT2PoisonContext(approved_modules=approved_modules)


def bt2_allowlisted_filesystem_call(protocol: str = "file") -> Any:
    """Self-test helper for BT2 pass-through checks.

    Tests may extend the approved-module set with this helper module instead of
    relying on a production engine seam to trigger a raw fsspec call.
    """

    return fsspec.filesystem(protocol)
