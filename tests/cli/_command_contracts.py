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

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

TierLiteral = Literal[
    "write",
    "inspect",
    "artifact",
    "artifact-output",
    "control-plane",
    "config-scoped",
    "tool",
]


@dataclass(frozen=True)
class ContractEntry:
    path: tuple[str, ...]
    tier: TierLiteral
    uri_roles: dict[str, str]
    required_storage_flags: list[str]
    smart_default_eligible: bool
    expected_failures: list[tuple[list[str], str]] = field(default_factory=list)


CLI_COMMAND_CONTRACTS: Mapping[tuple[str, ...], ContractEntry] = {
    ("ingest",): ContractEntry(
        path=("ingest",),
        tier="write",
        uri_roles={"--target": "product-output"},
        required_storage_flags=["--write-mode"],
        smart_default_eligible=False,
        expected_failures=[
            (
                [
                    "--target",
                    "file:///tmp/x.zarr",
                    "--storage-type",
                    "s3",
                    "--storage-driver",
                    "fsspec",
                    "--write-mode",
                    "staged",
                    "--product-name",
                    "pn",
                ],
                "incompatible",
            ),
            ([], "--target is required"),
        ],
    ),
    ("zarr", "slots"): ContractEntry(
        path=("zarr", "slots"),
        tier="write",
        uri_roles={"--target": "product-output"},
        required_storage_flags=["--write-mode"],
        smart_default_eligible=False,
        expected_failures=[
            (
                [
                    "--target",
                    "file:///tmp/x.zarr",
                    "--storage-type",
                    "s3",
                    "--storage-driver",
                    "fsspec",
                ],
                "incompatible",
            ),
        ],
    ),
    ("zarr", "multires"): ContractEntry(
        path=("zarr", "multires"),
        tier="write",
        uri_roles={"--target": "product-input-output"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[
            (
                [
                    "--target",
                    "file:///tmp/x.zarr",
                    "--product-name",
                    "pn",
                    "--storage-type",
                    "s3",
                    "--storage-driver",
                    "fsspec",
                ],
                "incompatible",
            ),
        ],
    ),
    ("zarr", "preallocate"): ContractEntry(
        path=("zarr", "preallocate"),
        tier="write",
        uri_roles={"--target": "product-output"},
        required_storage_flags=["--write-mode"],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("zarr", "validate"): ContractEntry(
        path=("zarr", "validate"),
        tier="inspect",
        uri_roles={"--product": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("parquet", "validate"): ContractEntry(
        path=("parquet", "validate"),
        tier="inspect",
        uri_roles={"--product": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("parquet", "consolidate"): ContractEntry(
        path=("parquet", "consolidate"),
        tier="artifact-output",
        uri_roles={"--product": "product-input", "--output": "artifact-output"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("catalog", "intake"): ContractEntry(
        path=("catalog", "intake"),
        tier="artifact-output",
        uri_roles={"--product": "product-input", "--output": "artifact-output"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("advise", "batch-size"): ContractEntry(
        path=("advise", "batch-size"),
        tier="inspect",
        uri_roles={"--product": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("advise", "compliance"): ContractEntry(
        path=("advise", "compliance"),
        tier="inspect",
        uri_roles={"--product": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("archive", "create"): ContractEntry(
        path=("archive", "create"),
        tier="artifact",
        uri_roles={"--source": "product-input", "--archive": "artifact-output"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[
            (
                [
                    "--source",
                    "file:///tmp/x.zarr",
                    "--archive",
                    "file:///tmp/x.tgm",
                    "--storage-type",
                    "s3",
                    "--storage-driver",
                    "fsspec",
                ],
                "incompatible",
            ),
        ],
    ),
    ("archive", "restore"): ContractEntry(
        path=("archive", "restore"),
        tier="artifact",
        uri_roles={"--archive": "artifact-input", "--target": "product-output"},
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[
            (
                [
                    "--archive",
                    "file:///tmp/x.tgm",
                    "--target",
                    "file:///tmp/out.zarr",
                    "--storage-type",
                    "s3",
                    "--storage-driver",
                    "fsspec",
                ],
                "incompatible",
            ),
        ],
    ),
    ("archive", "info"): ContractEntry(
        path=("archive", "info"),
        tier="artifact",
        uri_roles={"--archive": "artifact-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("archive", "validate"): ContractEntry(
        path=("archive", "validate"),
        tier="artifact",
        uri_roles={"--archive": "artifact-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("archive", "list"): ContractEntry(
        path=("archive", "list"),
        tier="artifact",
        uri_roles={"--archive": "artifact-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("zarr", "index", "show"): ContractEntry(
        path=("zarr", "index", "show"),
        tier="control-plane",
        uri_roles={"--target": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("zarr", "index", "verify"): ContractEntry(
        path=("zarr", "index", "verify"),
        tier="control-plane",
        uri_roles={"--target": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("zarr", "index", "rebuild"): ContractEntry(
        path=("zarr", "index", "rebuild"),
        tier="control-plane",
        uri_roles={"--target": "product-input"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "create"): ContractEntry(
        path=("plugins", "create"),
        tier="artifact",
        uri_roles={"--target-dir": "artifact-output"},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "list"): ContractEntry(
        path=("plugins", "list"),
        tier="config-scoped",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "describe"): ContractEntry(
        path=("plugins", "describe"),
        tier="config-scoped",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "explain"): ContractEntry(
        path=("plugins", "explain"),
        tier="config-scoped",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "install"): ContractEntry(
        path=("plugins", "install"),
        tier="config-scoped",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("plugins", "uninstall"): ContractEntry(
        path=("plugins", "uninstall"),
        tier="config-scoped",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "list"): ContractEntry(
        path=("chunks", "list"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "delete"): ContractEntry(
        path=("chunks", "delete"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "delete-span"): ContractEntry(
        path=("chunks", "delete-span"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "claims", "list"): ContractEntry(
        path=("chunks", "claims", "list"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "claims", "clear"): ContractEntry(
        path=("chunks", "claims", "clear"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "runs", "list"): ContractEntry(
        path=("chunks", "runs", "list"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "runs", "abandon"): ContractEntry(
        path=("chunks", "runs", "abandon"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "snapshots", "rebuild"): ContractEntry(
        path=("chunks", "snapshots", "rebuild"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("chunks", "snapshots", "status"): ContractEntry(
        path=("chunks", "snapshots", "status"),
        tier="control-plane",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("completion",): ContractEntry(
        path=("completion",),
        tier="tool",
        uri_roles={},
        required_storage_flags=[],
        smart_default_eligible=False,
        expected_failures=[],
    ),
}


def command_contracts() -> list[tuple[tuple[str, ...], ContractEntry]]:
    return list(CLI_COMMAND_CONTRACTS.items())
