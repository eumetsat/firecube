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

"""Options, session construction and step-option merging for ``firecube migrate``."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any, Literal

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import render
from firecube.cli._product import recorded_identity, resolve_product_identity
from firecube.cli._shared_options import (
    format_option,
    product_name_option,
    storage_driver_option,
    storage_type_option,
    target_uri_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_and_validate_uri
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession

SET_ATTRS_ID = "firecube:set-attrs"
"""The built-in step that the ``--set``, ``--remove`` and ``--node`` flags belong to."""


def echo_json(payload: Any) -> None:
    """Print *payload* as JSON with sorted keys."""
    click.echo(json.dumps(payload, indent=2, sort_keys=True))


def echo_warning(line: str) -> None:
    """Print a bookkeeping warning line to stderr, rendered for the running command."""
    click.echo(render(line), err=True)


def echo_report(line: str) -> None:
    """Print a progress line to stdout, rendered for the running command."""
    click.echo(render(line))


def echo_progress(line: str) -> None:
    """Print a progress line to stderr, rendered for the running command.

    Stdout stays machine-readable.
    """
    click.echo(render(line), err=True)


def migrate_options(tier: Literal["write", "inspect"], *, product_name: bool = False) -> Any:
    """Return the decorator that adds the options every product-bound migrate command takes.

    The options are ``--target``, ``--storage-type``, ``--storage-driver``,
    ``--storage-anonymous`` and ``--format``. With ``product_name``, ``--product-name`` joins them.
    """

    def decorator(func: Any) -> Any:
        func = format_option(("table", "json"))(func)
        func = storage_driver_option(required=False)(func)
        func = storage_type_option(
            required=False,
            extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3).",
        )(func)
        if product_name:
            func = product_name_option(required=True)(func)
        return target_uri_option(tier)(func)

    return decorator


def open_session(
    ctx: click.Context,
    *,
    command: str,
    target: str,
    product_name: str | None = None,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    writable: bool,
) -> StorageSession:
    """Admit ``--target`` and return the storage session bound to the product.

    The session carries ``product_name`` when given, else the label the store records.

    Raises:
        click.BadParameter: The target fails the URI rule.
        click.UsageError: The storage type contradicts the scheme, the resolver refuses the
            URI, the store records no label and none is given, or a writing command is given
            anonymous access.
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
    driver = StorageDriverConfig.from_storage_config(storage_config)
    if product_name is None:
        identity = recorded_identity(target_uri, format="zarr", driver=driver)
    else:
        identity = resolve_product_identity(target_uri, format="zarr", product_name=product_name)
    return StorageSession(StorageBinding(identity=identity, driver=driver))


def _split_pair(item: str, flag: str) -> tuple[str, str]:
    key, sep, value = item.partition("=")
    if not sep or not key:
        raise click.BadParameter(f"expected KEY=VALUE, got {item!r}", param_hint=flag)
    return key, value


def merge_step_options(
    step_id: str,
    *,
    options: Iterable[str],
    sets: Sequence[str],
    removes: Sequence[str],
    node: str | None,
) -> dict[str, Any]:
    """Merge ``--option``, ``--set``, ``--remove`` and ``--node`` into one mapping.

    A key given once by ``--option`` is a string; a key given more than once is a list in
    the order given. ``--set`` and ``--remove`` fill the keys ``set`` and ``remove`` as
    lists, appended to any ``--option`` values of those keys. ``--node`` fills ``node``.
    For the built-in step, ``set`` and ``remove`` are lists even when ``--option`` gives
    them once, and ``node`` is a string.

    Raises:
        click.BadParameter: An ``--option`` item is not ``KEY=VALUE``.
        click.UsageError: A set-attrs flag is given for another step, or ``node`` is given
            both by ``--node`` and by ``--option``.
    """
    pairs: dict[str, list[str]] = {}
    for item in options:
        key, value = _split_pair(item, "--option")
        pairs.setdefault(key, []).append(value)
    merged: dict[str, Any] = {k: v[0] if len(v) == 1 else v for k, v in pairs.items()}

    flagged = bool(sets or removes or node is not None)
    if flagged and step_id != SET_ATTRS_ID:
        raise click.UsageError(
            f"--set, --remove and --node apply to the step {SET_ATTRS_ID} only; "
            "pass options of another step with --option KEY=VALUE."
        )
    if step_id == SET_ATTRS_ID:
        if len(pairs.get("node", [])) > 1:
            raise click.UsageError(f"give --option node=PATH once for the step {SET_ATTRS_ID}.")
        for key, values in (("set", sets), ("remove", removes)):
            if values or key in pairs:
                merged[key] = [*pairs.get(key, []), *values]
    if node is not None:
        if "node" in merged:
            raise click.UsageError("give the node with --node or --option node=PATH, not both.")
        merged["node"] = node
    return merged
