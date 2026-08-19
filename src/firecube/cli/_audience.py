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

from typing import Literal

import click

USER_FACING_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("ingest",),
        ("zarr", "validate"),
        ("zarr", "multires"),
        ("zarr", "preallocate"),
        ("zarr", "slots"),
        ("parquet", "validate"),
        ("parquet", "consolidate"),
        ("plugins", "list"),
        ("plugins", "describe"),
        ("plugins", "explain"),
        ("plugins", "create"),
        ("plugins", "install"),
        ("plugins", "uninstall"),
        ("advise", "batch-size"),
        ("advise", "compliance"),
        ("completion",),
    }
)

OPERATOR_FACING_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("chunks", "list"),
        ("chunks", "delete"),
        ("chunks", "delete-span"),
        ("chunks", "claims", "list"),
        ("chunks", "claims", "clear"),
        ("chunks", "runs", "list"),
        ("chunks", "runs", "abandon"),
        ("chunks", "snapshots", "rebuild"),
        ("chunks", "snapshots", "status"),
        ("archive", "create"),
        ("archive", "restore"),
        ("archive", "info"),
        ("archive", "validate"),
        ("archive", "list"),
        ("catalog", "intake"),
    }
)

TIER_1_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("chunks", "delete"),
        ("chunks", "delete-span"),
        ("archive", "create"),
        ("archive", "restore"),
    }
)

TIER_2_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("chunks", "runs", "abandon"),
        ("chunks", "claims", "clear"),
    }
)

TIER_3_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("chunks", "snapshots", "rebuild"),
        ("zarr", "preallocate"),
    }
)

INTERNAL_TOKENS_USER_FORBIDDEN = frozenset(
    {
        ".firecube",
        "WAL",
        "control-plane",
        "DirectZarrIngestor",
        "JOB_COMPLETION_INDEX",
        "SchemaSizeMismatchError",
    }
)

GROUP_AUDIENCE: dict[str, Literal["user", "operator"]] = {
    "zarr": "user",
    "archive": "operator",
    "parquet": "user",
    "chunks": "operator",
    "advise": "user",
    "plugins": "user",
    "catalog": "operator",
}

_AUDIENCE_PATHS: dict[tuple[str, ...], Literal["user", "operator"]] = dict.fromkeys(
    USER_FACING_PATHS, "user"
)
_AUDIENCE_PATHS.update(dict.fromkeys(OPERATOR_FACING_PATHS, "operator"))


def classify(command_path: tuple[str, ...]) -> Literal["user", "operator"]:
    """Return audience class for a command path.

    Raises ``click.ClickException`` for unknown paths so the CLI boundary
    renders a clean error instead of a Python traceback.
    """

    try:
        return _AUDIENCE_PATHS[command_path]
    except KeyError as exc:
        raise click.ClickException(f"Unknown command path: {command_path!r}") from exc


__all__ = [
    "GROUP_AUDIENCE",
    "INTERNAL_TOKENS_USER_FORBIDDEN",
    "OPERATOR_FACING_PATHS",
    "TIER_1_PATHS",
    "TIER_2_PATHS",
    "TIER_3_PATHS",
    "USER_FACING_PATHS",
    "classify",
]
