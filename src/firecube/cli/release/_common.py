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

"""Options, session construction and output helpers shared by the ``firecube release`` commands."""

from __future__ import annotations

import json
from typing import Any, Literal

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import render
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    product_name_option,
    storage_driver_option,
    storage_type_option,
    target_uri_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_and_validate_uri
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession


def format_option(func: Any) -> Any:
    """Add the ``-f/--format`` option."""
    return click.option(
        "-f",
        "--format",
        "output_format",
        type=click.Choice(["text", "json"], case_sensitive=False),
        default="text",
        show_default=True,
        help="Output format; json prints sorted keys.",
    )(func)


def echo_json(payload: Any) -> None:
    """Print *payload* as JSON with sorted keys."""
    click.echo(json.dumps(payload, indent=2, sort_keys=True))


def echo_warning(line: str) -> None:
    """Print a bookkeeping warning line to stderr, rendered for the running command."""
    click.echo(render(line), err=True)


def echo_progress(line: str) -> None:
    """Print a progress line to stderr, so stdout stays machine-readable."""
    click.echo(line, err=True)


def release_options(tier: Literal["write", "inspect"]) -> Any:
    """Return the decorator that adds the options every release command takes.

    The options are ``--target``, ``--product-name``, ``--storage-type``,
    ``--storage-driver``, ``--storage-anonymous`` and ``--format``.
    """

    def decorator(func: Any) -> Any:
        func = format_option(func)
        func = storage_driver_option(required=False)(func)
        func = storage_type_option(
            required=False,
            extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3).",
        )(func)
        func = product_name_option(required=True)(func)
        return target_uri_option(tier)(func)

    return decorator


def open_session(
    ctx: click.Context,
    *,
    command: str,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    writable: bool,
) -> StorageSession:
    """Admit ``--target`` and return the storage session bound to the product.

    Raises:
        click.BadParameter: The target fails the URI rule.
        click.UsageError: The storage type contradicts the scheme, the resolver refuses the
            URI, or a writing command is given anonymous access.
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
            f"{command} writes under .firecube/ and needs write access; "
            "anonymous storage access is not supported."
        )
    identity = resolve_product_identity(
        target_uri,
        format="zarr",
        product_name=product_name,
    )
    return StorageSession(
        StorageBinding(
            identity=identity,
            driver=StorageDriverConfig.from_storage_config(storage_config),
        )
    )
