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

from pathlib import Path
from typing import Any, Literal

import click

__all__ = [
    "archive_uri_option",
    "config_file_option",
    "dry_run_flag",
    "format_option",
    "product_filter_option",
    "product_name_option",
    "product_uri_option",
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
        return click.option(
            "--storage-driver",
            "storage_driver",
            required=required,
            type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
            help=help_text,
        )(func)

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


def product_uri_option(tier: Literal["write", "inspect"]) -> Any:
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


def target_uri_option(tier: Literal["write", "inspect"]) -> Any:
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
            required=True,
            type=str,
            help=help_text,
            shell_complete=_uri_shell_complete,
        )(func)

    return decorator


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


def format_option(default: str = "table") -> Any:
    def decorator(f: Any) -> Any:
        return click.option(
            "-f",
            "--format",
            "output_format",
            type=click.Choice(["table", "json", "csv"], case_sensitive=False),
            default=default,
            show_default=True,
            help="Output format.",
        )(f)

    return decorator


def product_filter_option(required: bool = False) -> Any:
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
