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


LabelSource = Literal["cli", "recorded", "session", "none"]
"""Where a command gets the product label: its ``--product-name``, the store's record,
the record or else the URI (pure-storage readers), or nowhere (no product identity)."""


@dataclass(frozen=True)
class ContractEntry:
    path: tuple[str, ...]
    tier: TierLiteral
    uri_roles: dict[str, str]
    label_source: LabelSource
    required_storage_flags: list[str]
    smart_default_eligible: bool
    expected_failures: list[tuple[list[str], str]] = field(default_factory=list)
    extra_options: list[str] = field(default_factory=list)


_RW = "product-input-output"
_ALL = ("--all-products",)
_ARCHIVE_IN = {"--archive": "artifact-input"}
_TARGET_DIR = {"--target-dir": "artifact-output"}


def _row(
    path: tuple[str, ...],
    tier: TierLiteral,
    label_source: LabelSource,
    role: str | None = "product-input",
    *,
    extra_roles: Mapping[str, str] | None = None,
    extra_options: tuple[str, ...] = (),
) -> tuple[tuple[str, ...], ContractEntry]:
    """Contract row of a command without storage-flag or failure cases; ``role`` None means no URI.

    ``extra_roles`` adds URI options beside ``--target``; ``extra_options`` lists
    non-URI options the help must show.
    """
    return path, ContractEntry(
        path=path,
        tier=tier,
        uri_roles={**({} if role is None else {"--target": role}), **(extra_roles or {})},
        label_source=label_source,
        required_storage_flags=["--product-name"] if label_source == "cli" else [],
        smart_default_eligible=False,
        extra_options=list(extra_options),
    )


CLI_COMMAND_CONTRACTS: Mapping[tuple[str, ...], ContractEntry] = {
    ("ingest",): ContractEntry(
        path=("ingest",),
        tier="write",
        uri_roles={"--target": "product-output"},
        label_source="cli",
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
        label_source="cli",
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
        label_source="recorded",
        required_storage_flags=[],
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
    ("zarr", "preallocate"): ContractEntry(
        path=("zarr", "preallocate"),
        tier="write",
        uri_roles={"--target": "product-output"},
        label_source="cli",
        required_storage_flags=["--write-mode"],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("zarr", "validate"): ContractEntry(
        path=("zarr", "validate"),
        tier="inspect",
        uri_roles={"--target": "product-input"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("zarr", "compare"): ContractEntry(
        path=("zarr", "compare"),
        tier="inspect",
        uri_roles={"A_URI": "product-input", "B_URI": "product-input"},
        label_source="none",
        required_storage_flags=["--storage-type", "--storage-driver"],
        smart_default_eligible=False,
        expected_failures=[],
    ),
    ("parquet", "validate"): ContractEntry(
        path=("parquet", "validate"),
        tier="inspect",
        uri_roles={"--target": "product-input"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("parquet", "consolidate"): ContractEntry(
        path=("parquet", "consolidate"),
        tier="artifact-output",
        uri_roles={"--target": "product-input", "--output": "artifact-output"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("catalog", "intake"): ContractEntry(
        path=("catalog", "intake"),
        tier="artifact-output",
        uri_roles={"--target": "product-input", "--output": "artifact-output"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("advise", "batch-size"): ContractEntry(
        path=("advise", "batch-size"),
        tier="inspect",
        uri_roles={"--target": "product-input"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("advise", "compliance"): ContractEntry(
        path=("advise", "compliance"),
        tier="inspect",
        uri_roles={"--target": "product-input"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[],
    ),
    ("archive", "create"): ContractEntry(
        path=("archive", "create"),
        tier="artifact",
        uri_roles={"--target": "product-input", "--archive": "artifact-output"},
        label_source="session",
        required_storage_flags=[],
        smart_default_eligible=True,
        expected_failures=[
            (
                [
                    "--target",
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
        label_source="cli",
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
    **dict(
        [
            _row(("archive", "info"), "artifact", "none", None, extra_roles=_ARCHIVE_IN),
            _row(("archive", "validate"), "artifact", "none", None, extra_roles=_ARCHIVE_IN),
            _row(("archive", "list"), "artifact", "none", None, extra_roles=_ARCHIVE_IN),
            _row(("zarr", "consolidate-time-coord"), "write", "recorded", "product-input-output"),
            _row(("zarr", "index", "show"), "control-plane", "recorded"),
            _row(("zarr", "index", "verify"), "control-plane", "recorded"),
            _row(("zarr", "index", "rebuild"), "control-plane", "cli"),
            _row(("plugins", "create"), "artifact", "none", None, extra_roles=_TARGET_DIR),
            _row(("plugins", "list"), "config-scoped", "none", None),
            _row(("plugins", "describe"), "config-scoped", "none", None),
            _row(("plugins", "explain"), "config-scoped", "none", None),
            _row(("plugins", "install"), "config-scoped", "none", None),
            _row(("plugins", "uninstall"), "config-scoped", "none", None),
            _row(("chunks", "list"), "control-plane", "recorded", extra_options=_ALL),
            _row(("chunks", "delete"), "control-plane", "recorded", _RW, extra_options=_ALL),
            _row(("chunks", "delete-span"), "control-plane", "recorded", _RW),
            _row(("chunks", "claims", "list"), "control-plane", "recorded", extra_options=_ALL),
            _row(("chunks", "claims", "clear"), "control-plane", "recorded", _RW),
            _row(("chunks", "runs", "list"), "control-plane", "recorded"),
            _row(("chunks", "runs", "abandon"), "control-plane", "recorded", _RW),
            _row(("chunks", "compact", "rebuild"), "control-plane", "recorded", _RW),
            _row(("chunks", "compact", "status"), "control-plane", "recorded"),
            _row(("chunks", "snapshots", "rebuild"), "control-plane", "recorded", _RW),
            _row(("chunks", "snapshots", "status"), "control-plane", "recorded"),
            _row(("completion",), "tool", "none", None),
            _row(("snapshot", "take"), "control-plane", "recorded", _RW),
            _row(("snapshot", "list"), "control-plane", "recorded"),
            _row(("snapshot", "show"), "control-plane", "recorded"),
            _row(("snapshot", "diff"), "control-plane", "recorded"),
            _row(("migrate", "run"), "write", "recorded", _RW),
            _row(("migrate", "list"), "config-scoped", "none", None),
            _row(("migrate", "status"), "control-plane", "recorded"),
            _row(("migrate", "adopt"), "write", "cli", _RW),
            _row(
                ("release", "create"),
                "write",
                "recorded",
                _RW,
                extra_roles={"--archive": "artifact-output"},
                extra_options=("--archive-full", "--retain-versions"),
            ),
            _row(("release", "list"), "control-plane", "recorded"),
            _row(("release", "show"), "control-plane", "recorded"),
            _row(("release", "verify"), "control-plane", "recorded", extra_options=("--sample",)),
        ]
    ),
}


def command_contracts() -> list[tuple[tuple[str, ...], ContractEntry]]:
    return list(CLI_COMMAND_CONTRACTS.items())
