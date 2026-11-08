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

"""Runtime wiring and global state for Firecube configuration.

This module handles side-effects like setting global variables and exporting
environment variables. It depends on `firecube.core.config` for types
and loading logic.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

from firecube.core.config import (
    StorageConfig,
    build_storage_config,
    derive_target_uri,
    load_config_file,
)
from firecube.core.errors import Message, Param, SlotWindowError
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.uri import StorageUri

log = logging.getLogger("firecube.core.runtime")

_global_storage_config: StorageConfig | None = None


def _target_uri_from_config(identity: ProductIdentity) -> str:
    """Render the FIRECUBE_TARGET_URI value from a `ProductIdentity`.

    Preserves legacy env-var semantics: bare local path for `file://` URIs,
    canonical URI form for remote (`s3://`, `gs://`) URIs.
    """
    if identity.product_uri.protocol == "file":
        return identity.product_uri.path
    return identity.product_uri.to_str()


def set_global_storage_config(config: StorageConfig) -> None:
    """Set the global storage configuration."""
    global _global_storage_config
    config.validate()
    _global_storage_config = config


def get_global_storage_config() -> StorageConfig:
    """Return the global storage configuration."""
    if _global_storage_config is None:
        raise RuntimeError("Global storage configuration not set. Initialise with CLI args first.")
    return _global_storage_config


def export_storage_config_to_env(
    config: StorageConfig,
    identity: ProductIdentity | None = None,
    *,
    env: MutableMapping[str, str] | None = None,
) -> None:
    """Publish storage configuration to environment variables for child processes.

    Args:
        config: Non-location storage config (storage_type, endpoint_url,
            credentials, region, path_style, storage_driver).
        identity: Optional product identity providing location fields (bucket /
            target_path / target_uri). When ``None``, location vars are omitted.
        env: Optional mutable mapping to write into; defaults to ``os.environ``.

    We intentionally export both FIRECUBE_* and AWS_* style variables:
      - FIRECUBE_* is used by Firecube internals and helpers.
      - AWS_* allows libraries such as fsspec/xarray/zarr to discover
        credentials and endpoints when working with S3-compatible storage.
    """
    target_env: MutableMapping[str, str] = os.environ if env is None else env

    target_env["FIRECUBE_STORAGE_TYPE"] = config.storage_type
    if config.endpoint_url:
        target_env["FIRECUBE_ENDPOINT_URL"] = config.endpoint_url
    if config.access_key:
        target_env["FIRECUBE_ACCESS_KEY"] = config.access_key
    if config.secret_key:
        target_env["FIRECUBE_SECRET_KEY"] = config.secret_key
    if config.region:
        target_env["FIRECUBE_REGION"] = config.region
    target_env["FIRECUBE_PATH_STYLE"] = str(config.path_style).lower()
    target_env["FIRECUBE_STORAGE_DRIVER"] = config.storage_driver
    target_env["FIRECUBE_S3_ANONYMOUS"] = str(config.anonymous).lower()

    if identity is not None:
        if identity.product_uri.authority:
            target_env["FIRECUBE_BUCKET"] = identity.product_uri.authority
        if identity.product_uri.protocol == "file":
            target_env["FIRECUBE_TARGET_PATH"] = identity.product_uri.path
        target_env["FIRECUBE_TARGET_URI"] = _target_uri_from_config(identity)

    if config.access_key:
        target_env["AWS_ACCESS_KEY_ID"] = config.access_key
    if config.secret_key:
        target_env["AWS_SECRET_ACCESS_KEY"] = config.secret_key
    if config.region:
        target_env["AWS_DEFAULT_REGION"] = config.region
    if config.endpoint_url:
        target_env["AWS_ENDPOINT_URL"] = config.endpoint_url
    target_env["AWS_S3_ADDRESSING_STYLE"] = "path" if config.path_style else "virtual"


def identity_from_storage_config(storage_config: StorageConfig) -> ProductIdentity | None:
    """Synthesize a `ProductIdentity` from a resolved `StorageConfig`.

    Bridges the legacy bridge-attr storage view (``target_path`` / ``bucket``
    set on ``StorageConfig`` by ``build_storage_config``) to the
    ``ProductIdentity`` shape consumed by env-export and CLI bridge sites.
    The remaining bridge-attr reads live inside ``derive_target_uri`` (see
    config.py); this helper is the single seam between resolved config and
    identity.

    Returns ``None`` when the resolved config lacks a base URI (e.g. local
    storage_type with no ``target_path`` set yet).
    """
    try:
        base_uri_str = derive_target_uri(storage_config)
    except ValueError:
        return None

    if storage_config.storage_type == "local":
        base_uri = StorageUri.from_local_path(base_uri_str)
    else:
        base_uri = StorageUri.parse(base_uri_str)

    name = base_uri.path.rstrip("/").rsplit("/", 1)[-1]
    return ProductIdentity(
        product_name=name,
        product_uri=base_uri,
        control_root_uri=base_uri.join(".firecube"),
        format="zarr",
    )


def resolve_storage_config(
    *,
    config_file: Path | str | None = None,
    env: Mapping[str, str] = os.environ,  # type: ignore[assignment]
    overrides: dict[str, Any] | None = None,
    export_env: bool = False,
    set_global: bool = True,
) -> StorageConfig:
    """Resolve StorageConfig from config.toml + env + overrides.

    This is a convenience wrapper used by CLI entrypoints to keep the
    load/build/validate/export wiring consistent across commands.

    - Precedence is implemented in `build_storage_config(...)`.
    - This helper does not depend on click (so it can be used by non-CLI code).
    - ``export_env=True`` publishes the result to ``os.environ``; the default
      leaves the process environment untouched.
    """
    cfg = load_config_file(config_file)
    storage_config = build_storage_config(cfg, env, overrides or {})
    storage_config.validate()

    if set_global:
        set_global_storage_config(storage_config)
    if export_env:
        identity = identity_from_storage_config(storage_config)
        export_storage_config_to_env(storage_config, identity)

    return storage_config


def _parse_slot_int(value: str, *, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise SlotWindowError(f"{name} must be an integer, got {value!r}") from exc


def resolve_slot_window(
    cli_slot_start: int | None,
    cli_slot_end: int | None,
    cli_slot_size: int | None,
    cli_slot_group: str | None,
    *,
    env: Mapping[str, str],
) -> tuple[int | None, int | None, str | None]:
    """Resolve ``(slot_start, slot_end, slot_group)`` from CLI values and ``env``.

    Window precedence: CLI start and end, then ``FIRECUBE_SLOT_START`` with
    ``FIRECUBE_SLOT_END``, then ``JOB_COMPLETION_INDEX`` times the CLI slot size
    or ``FIRECUBE_SLOT_SIZE``, then no window. The group is the CLI value, then a
    non-blank ``FIRECUBE_SLOT_GROUP``, then ``None``.

    Raises:
        SlotWindowError: Only one CLI bound is given, or an env value is not an integer.
    """
    if (cli_slot_start is None) ^ (cli_slot_end is None):
        raise SlotWindowError(
            Message(
                "{start} and {end} must be provided together",
                start=Param("slot_start"),
                end=Param("slot_end"),
            )
        )

    slot_group = cli_slot_group
    env_slot_group = env.get("FIRECUBE_SLOT_GROUP")
    if slot_group is None and env_slot_group is not None and env_slot_group.strip():
        slot_group = env_slot_group

    if cli_slot_start is not None and cli_slot_end is not None:
        return cli_slot_start, cli_slot_end, slot_group

    env_slot_start = env.get("FIRECUBE_SLOT_START")
    env_slot_end = env.get("FIRECUBE_SLOT_END")
    if env_slot_start is not None and env_slot_end is not None:
        start = _parse_slot_int(env_slot_start, name="FIRECUBE_SLOT_START")
        end = _parse_slot_int(env_slot_end, name="FIRECUBE_SLOT_END")
        log.info(
            "slot_range resolved from env vars FIRECUBE_SLOT_START/END: [%s, %s)",
            start,
            end,
        )
        return start, end, slot_group

    job_completion_index = env.get("JOB_COMPLETION_INDEX")
    if job_completion_index is None:
        return None, None, slot_group

    slot_size = cli_slot_size
    if slot_size is None:
        env_slot_size = env.get("FIRECUBE_SLOT_SIZE")
        if env_slot_size is not None:
            slot_size = _parse_slot_int(env_slot_size, name="FIRECUBE_SLOT_SIZE")
    if slot_size is None:
        log.warning("JOB_COMPLETION_INDEX detected but no slot size; parallel mode NOT activated")
        return None, None, slot_group

    index = _parse_slot_int(job_completion_index, name="JOB_COMPLETION_INDEX")
    start = index * slot_size
    end = (index + 1) * slot_size
    log.info(
        "slot_range resolved from JOB_COMPLETION_INDEX=%s * slot_size=%s: [%s, %s)",
        index,
        slot_size,
        start,
        end,
    )
    return start, end, slot_group
