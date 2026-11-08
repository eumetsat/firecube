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

"""Options and manager construction shared by the ``firecube snapshot`` commands."""

from __future__ import annotations

from typing import Any, Literal

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._product import recorded_identity
from firecube.cli._shared_options import (
    format_option,
    storage_driver_option,
    storage_type_option,
    target_uri_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_and_validate_uri
from firecube.core.controlplane import ChunkManager
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession


def snapshot_options(tier: Literal["write", "inspect"]) -> Any:
    """Return the decorator that adds the options every snapshot command takes.

    The options are ``--target``, ``--storage-type``,
    ``--storage-driver``, ``--storage-anonymous`` and ``--format``.
    """

    def decorator(func: Any) -> Any:
        func = format_option(("table", "json"))(func)
        func = storage_driver_option(required=False)(func)
        func = storage_type_option(
            required=False,
            extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3).",
        )(func)
        return target_uri_option(tier)(func)

    return decorator


def open_manager(
    ctx: click.Context,
    *,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    writable: bool,
) -> tuple[ChunkManager, str]:
    """Admit ``--target`` and return the manager of the product with the recorded product label.

    The manager closes when the Click context closes.

    Raises:
        click.BadParameter: The target fails the URI rule.
        click.UsageError: The storage type contradicts the scheme, the store records no
            product label, or a writing command is given anonymous access.
    """
    session = open_session(
        ctx,
        target=target,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=writable,
    )
    manager = session.control_plane()
    ctx.call_on_close(manager.close)
    return manager, session.product.product_name


def open_session(
    ctx: click.Context,
    *,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    writable: bool,
) -> StorageSession:
    """Admit ``--target`` and return the storage session bound to the recorded product label.

    Raises:
        click.BadParameter: The target fails the URI rule.
        click.UsageError: The storage type contradicts the scheme, the store records no
            product label, or a writing command is given anonymous access.
    """
    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )
    if writable and storage_config.anonymous:
        raise click.UsageError(
            "snapshot take writes under .firecube/metadata/ and needs write access; "
            "anonymous storage access is not supported."
        )
    driver = StorageDriverConfig.from_storage_config(storage_config)
    identity = recorded_identity(target_uri, format="zarr", driver=driver)
    return StorageSession(StorageBinding(identity=identity, driver=driver))
