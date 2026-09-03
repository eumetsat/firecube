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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from tests.cli._command_contracts import CLI_COMMAND_CONTRACTS, ContractEntry, command_contracts

ALL_CONTRACT_CASES = command_contracts()
WRITE_STORAGE_CONTRACTS = [
    (path, entry)
    for path, entry in ALL_CONTRACT_CASES
    if entry.tier == "write" and entry.required_storage_flags
]
LOCAL_PRODUCT_CONTRACTS = [
    (path, entry)
    for path, entry in ALL_CONTRACT_CASES
    if entry.tier in ("inspect", "artifact-output") and "--product" in entry.uri_roles
]


def _contract_ids(cases: list[tuple[tuple[str, ...], ContractEntry]]) -> list[str]:
    return [".".join(path) for path, _entry in cases]


def _write_parquet(path: Path) -> None:
    pq.write_table(pa.table({"a": [1, 2, 3]}), path)


def _help_flags_for(entry: ContractEntry) -> list[str]:
    return entry.required_storage_flags


def _write_args_without_storage(path: tuple[str, ...], tmp_path: Path) -> list[str]:
    target = (tmp_path / "out.zarr").as_uri()
    match path:
        case ("ingest",):
            return ["cli_test_plugin", "--target", target, "--product-name", "pn"]
        case ("zarr", "slots"):
            return ["cli_test_plugin", "--target", target, "--product-name", "pn"]
        case ("zarr", "multires"):
            return ["--target", target, "--product-name", "pn"]
        case ("zarr", "consolidate-time-coord"):
            return ["--target", target]
        case ("zarr", "preallocate"):
            return ["cli_test_plugin", "--target", target, "--product-name", "pn"]
        case _:
            raise AssertionError(f"unexpected write-tier command: {path!r}")


def _write_args_with_required_storage(path: tuple[str, ...], tmp_path: Path) -> list[str]:
    target = (tmp_path / "out.zarr").as_uri()
    match path:
        case ("ingest",):
            return [
                "cli_test_plugin",
                "--target",
                target,
                "--product-name",
                "pn",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
            ]
        case ("zarr", "slots"):
            return [
                "cli_test_plugin",
                "--target",
                target,
                "--product-name",
                "pn",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
            ]
        case ("zarr", "multires"):
            return [
                "--target",
                target,
                "--product-name",
                "pn",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
            ]
        case ("zarr", "consolidate-time-coord"):
            return [
                "--target",
                target,
                "--product-name",
                "pn",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
            ]
        case ("zarr", "preallocate"):
            return [
                "cli_test_plugin",
                "--target",
                target,
                "--product-name",
                "pn",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
            ]
        case _:
            raise AssertionError(f"unexpected write-tier command: {path!r}")


def _without_option(args: list[str], flag: str) -> list[str]:
    stripped = list(args)
    index = stripped.index(flag)
    del stripped[index : index + 2]
    return stripped


def _local_product_args(
    path: tuple[str, ...],
    entry: ContractEntry,
    tmp_local_zarr: Path,
    tmp_path: Path,
) -> list[str]:
    if "--product" not in entry.uri_roles:
        raise AssertionError(f"command has no product input: {path!r}")

    product_uri = tmp_local_zarr.as_uri()
    match path:
        case ("zarr", "validate"):
            return ["-p", product_uri, "-g", "g1"]
        case ("parquet", "validate"):
            parquet_path = tmp_path / "x.parquet"
            _write_parquet(parquet_path)
            return ["-p", parquet_path.as_uri()]
        case ("parquet", "consolidate"):
            parquet_path = tmp_path / "x.parquet"
            _write_parquet(parquet_path)
            return ["-p", parquet_path.as_uri(), "-o", (tmp_path / "out.parquet").as_uri()]
        case ("catalog", "intake"):
            return [
                "cli_test_plugin",
                "-p",
                product_uri,
                "-o",
                (tmp_path / "catalog.yaml").as_uri(),
                "--collection-id",
                "test-coll",
            ]
        case ("advise", "batch-size"):
            return ["-p", product_uri, "-g", "g1"]
        case ("advise", "compliance"):
            return ["--profile", "cf-18", "-p", product_uri, "-g", "g1"]
        case _:
            raise AssertionError(f"unexpected inspect/artifact-output command: {path!r}")


def _expected_failure_cases() -> list[tuple[tuple[str, ...], ContractEntry, list[str], str]]:
    return [
        (path, entry, args, expected)
        for path, entry in CLI_COMMAND_CONTRACTS.items()
        for args, expected in entry.expected_failures
    ]


def _with_required_positionals(path: tuple[str, ...], args: list[str]) -> list[str]:
    if path == ("ingest",):
        return ["cli_test_plugin", *args]
    if path == ("zarr", "slots"):
        return ["cli_test_plugin", "--product-name", "pn", "--write-mode", "direct", *args]
    return args


@pytest.mark.parametrize("path,entry", ALL_CONTRACT_CASES, ids=_contract_ids(ALL_CONTRACT_CASES))
def test_contract_help_mentions_declared_flags(path: tuple[str, ...], entry: ContractEntry) -> None:
    result = CliRunner().invoke(cli, [*path, "--help"])

    assert result.exit_code == 0, result.output
    for flag in _help_flags_for(entry):
        if not flag.startswith("--"):
            continue
        assert flag in result.output, (path, flag)


@pytest.mark.parametrize(
    "path,entry",
    WRITE_STORAGE_CONTRACTS,
    ids=_contract_ids(WRITE_STORAGE_CONTRACTS),
)
def test_write_tier_requires_storage_flags(
    path: tuple[str, ...],
    entry: ContractEntry,
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(cli, [*path, *_write_args_without_storage(path, tmp_path)])

    assert result.exit_code == 2, result.output
    assert any(flag in result.output for flag in entry.required_storage_flags)


@pytest.mark.parametrize(
    "path,entry",
    WRITE_STORAGE_CONTRACTS,
    ids=_contract_ids(WRITE_STORAGE_CONTRACTS),
)
def test_write_tier_requires_each_declared_storage_flag_individually(
    path: tuple[str, ...],
    entry: ContractEntry,
    tmp_path: Path,
) -> None:
    base_args = _write_args_with_required_storage(path, tmp_path)
    for flag in entry.required_storage_flags:
        result = CliRunner().invoke(cli, [*path, *_without_option(base_args, flag)])

        assert result.exit_code == 2, (path, flag, result.output)
        assert flag in result.output, (path, flag, result.output)


@pytest.mark.parametrize(
    "path,entry",
    LOCAL_PRODUCT_CONTRACTS,
    ids=_contract_ids(LOCAL_PRODUCT_CONTRACTS),
)
def test_inspect_tier_accepts_local_product_without_storage_flags(
    path: tuple[str, ...],
    entry: ContractEntry,
    tmp_local_zarr: Path,
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        cli,
        [*path, *_local_product_args(path, entry, tmp_local_zarr, tmp_path)],
    )

    assert "Missing option '--storage-type'" not in result.output
    assert "Missing option '--storage-driver'" not in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    ("path", "entry", "args", "expected"),
    _expected_failure_cases(),
)
def test_expected_failure_contracts_are_enforced(
    path: tuple[str, ...],
    entry: ContractEntry,
    args: list[str],
    expected: str,
) -> None:
    result = CliRunner().invoke(cli, [*path, *_with_required_positionals(path, args)])

    assert result.exit_code != 0, (entry, result.output)
    assert expected in result.output
    assert "Traceback" not in result.output
