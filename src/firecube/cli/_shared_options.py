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

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import click

__all__ = [
    "all_products_flag",
    "archive_uri_option",
    "config_file_option",
    "dry_run_flag",
    "format_option",
    "product_name_option",
    "storage_driver_option",
    "storage_type_option",
    "target_uri_option",
    "workspace_option",
    "write_mode_option",
    "yes_flag",
]


def _uri_shell_complete(ctx: click.Context, param: click.Parameter, incomplete: str) -> list[Any]:
    from click.shell_completion import CompletionItem

    return [CompletionItem(incomplete, type="file")]


def workspace_option(f: Any) -> Any:
    return click.option(
        "--workspace",
        type=click.Path(path_type=Path),
        help="workspace directory override",
    )(f)


def config_file_option(f: Any) -> Any:
    return click.option(
        "--config-file",
        type=click.Path(path_type=Path),
        help="Firecube TOML config file (default: ~/.config/firecube/config.toml)",
    )(f)


def storage_type_option(
    f: Any = None,
    *,
    required: bool = True,
    extra_help: str = "",
) -> Any:
    def decorator(func: Any) -> Any:
        help_text = f"Storage locality/class. {extra_help}".strip()
        return click.option(
            "--storage-type",
            "storage_type",
            required=required,
            type=click.Choice(["local", "s3"], case_sensitive=False),
            help=help_text,
        )(func)

    if callable(f):
        return decorator(f)
    return decorator


def storage_driver_option(
    f: Any = None,
    *,
    required: bool = True,
    extra_help: str = "",
) -> Any:
    def decorator(func: Any) -> Any:
        help_text = f"Storage backend driver. {extra_help}".strip()
        with_driver = click.option(
            "--storage-driver",
            "storage_driver",
            required=required,
            type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
            help=help_text,
        )(func)
        return click.option(
            "--storage-anonymous",
            "storage_anonymous",
            is_flag=True,
            default=None,
            help=(
                "Use anonymous access for public S3 buckets. Overrides any credentials "
                "in the environment or config file."
            ),
        )(with_driver)

    if callable(f):
        return decorator(f)
    return decorator


def write_mode_option(
    f: Any = None,
    *,
    required: bool = True,
    extra_help: str = "",
) -> Any:
    def decorator(func: Any) -> Any:
        help_text = (
            f"Write strategy (staged=workspace-first, direct=stream to target). {extra_help}"
        ).strip()
        return click.option(
            "-w",
            "--write-mode",
            "write_mode",
            required=required,
            type=click.Choice(["staged", "direct"], case_sensitive=False),
            help=help_text,
        )(func)

    if callable(f):
        return decorator(f)
    return decorator


def _legacy_product_uri_option(tier: Literal["write", "inspect"]) -> Any:
    """Add the retired `-p, --product` URI option; a command family drops it for `--target`."""
    help_text = (
        "Firecube product URI (file:///abs/path or s3://bucket/key); storage flags inferred from URI scheme."
        if tier == "inspect"
        else "Firecube product URI; --storage-type must match URI scheme."
    )

    def decorator(func: Any) -> Any:
        return click.option(
            "-p",
            "--product",
            "product",
            required=True,
            type=str,
            help=help_text,
            shell_complete=_uri_shell_complete,
        )(func)

    return decorator


def target_uri_option(tier: Literal["write", "inspect"], *, required: bool = True) -> Any:
    """Add `-t, --target`, the product URI option, as parameter `target`.

    Args:
        tier: `inspect` says storage flags follow the URI scheme; `write` says they must match it.
        required: Make Click refuse a call without the option.
    """
    help_text = (
        "Target Firecube product URI (file:///abs/path or s3://bucket/key); storage flags inferred from URI scheme."
        if tier == "inspect"
        else "Target Firecube product URI; --storage-type must match URI scheme."
    )

    def decorator(func: Any) -> Any:
        return click.option(
            "-t",
            "--target",
            "target",
            required=required,
            type=str,
            help=help_text,
            shell_complete=_uri_shell_complete,
        )(func)

    return decorator


def all_products_flag(f: Any) -> Any:
    """Add `--all-products`, which selects every product under the configured storage base."""
    return click.option(
        "--all-products",
        "all_products",
        is_flag=True,
        default=False,
        help="Act on every product under the [storage] base URI of the config file. Excludes --target.",
    )(f)


def archive_uri_option(tier: Literal["write", "inspect"]) -> Any:
    """Shared -a, --archive URI flag for write and inspect commands."""
    help_text = {
        "write": "Target archive URI; runtime decides what is supported (file:///abs/path or s3://bucket/key).",
        "inspect": "Archive URI to inspect (file:///abs/path or s3://bucket/key).",
    }[tier]

    def decorator(func: Any) -> Any:
        return click.option(
            "-a",
            "--archive",
            "archive",
            type=str,
            required=True,
            help=help_text,
            shell_complete=_uri_shell_complete,
        )(func)

    return decorator


FORMAT_CHOICES: tuple[str, ...] = ("table", "json", "csv")
"""The output formats a command may offer through `format_option`."""


def format_option(choices: Sequence[str] = FORMAT_CHOICES, default: str = "table") -> Any:
    """Add the shared case-insensitive `-f, --format` option, bound to `output_format`.

    Raises:
        ValueError: `choices` is not a subset of `FORMAT_CHOICES` or lacks `default`.
    """
    if not set(choices) <= set(FORMAT_CHOICES) or default not in choices:
        raise ValueError(f"format choices {tuple(choices)!r} and default {default!r} are invalid")

    def decorator(f: Any) -> Any:
        return click.option(
            "-f",
            "--format",
            "output_format",
            type=click.Choice(list(choices), case_sensitive=False),
            default=default,
            show_default=True,
            help="Output format.",
        )(f)

    return decorator


def _legacy_product_filter_option(required: bool = False) -> Any:
    """Add the retired `-n, --product-name` filter option; `--target` replaces it."""

    def decorator(f: Any) -> Any:
        return click.option(
            "-n",
            "--product-name",
            "product_name",
            required=required,
            help="Filter by product name.",
        )(f)

    return decorator


def product_name_option(required: bool = True) -> Any:
    """Add the long-only `--product-name` option for a command that may record the label."""

    def decorator(f: Any) -> Any:
        return click.option(
            "--product-name",
            "product_name",
            required=required,
            help="Logical product name.",
        )(f)

    return decorator


def dry_run_flag(f: Any) -> Any:
    return click.option(
        "--dry-run",
        "dry_run",
        is_flag=True,
        default=False,
        help="Show what would happen without making any changes.",
    )(f)


def yes_flag(f: Any) -> Any:
    return click.option(
        "--yes-i-really-mean-it",
        "yes_i_really_mean_it",
        is_flag=True,
        default=False,
        help="Skip confirmation prompts. Required for destructive operations in non-TTY.",
    )(f)


# Transitional names, outside `__all__`: each goes once no module imports it.
product_uri_option = _legacy_product_uri_option
product_filter_option = _legacy_product_filter_option
