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

import re
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import exclusive_maintenance_domain
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from tests.helpers.rows import table_row
from tests.helpers.storage import make_test_binding

_CONFIG_TEMPLATE = """\
[storage]
type = "local"
target_path = "{target_path}"
"""


def _write_config(tmp_path: Path, *, target_path: Path) -> Path:
    config_file = tmp_path / "firecube-test.toml"
    config_file.write_text(
        _CONFIG_TEMPLATE.format(target_path=target_path),
        encoding="utf-8",
    )
    return config_file


def _stub_list_chunks(monkeypatch) -> None:
    monkeypatch.setattr(
        "firecube.core.controlplane.manager.ChunkManager.list_chunks",
        lambda self, **kwargs: [],
    )


_SCAN = "[chunks] scanning base file://"
_ROWS = [
    (
        "test_chunks_scan_banner_shown_with_config_base",
        "config",
        ["--all-products"],
        True,
        [_SCAN],
        [],
    ),
    ("test_chunks_scan_banner_shown_with_env_base", "env", ["--all-products"], True, [_SCAN], []),
    (
        "test_chunks_target_prints_no_scan_banner",
        "config",
        ["--target", "file://{wd}/p.zarr"],
        True,
        [],
        ["scanning base"],
    ),
    (
        "test_chunks_all_products_without_a_base_names_the_missing_config",
        "none",
        ["--all-products"],
        False,
        ["--all-products scans the base URI of the [storage] configuration, and none is set"],
        ["scanning base"],
    ),
]


def test_chunks_scan_banner(subtests: Any, monkeypatch, tmp_path: Path) -> None:
    for row_id, source, list_args, exit_ok, present, absent in _ROWS:
        with table_row(subtests, row_id, tmp_path, monkeypatch) as (workdir, mp):
            assert mp is not None
            config_file = _write_config(workdir, target_path=workdir)
            for name in ("FIRECUBE_STORAGE_TYPE", "FIRECUBE_TARGET_PATH", "FIRECUBE_BUCKET"):
                mp.delenv(name, raising=False)
            mp.setenv(
                "FIRECUBE_CONFIG",
                str(config_file if source == "config" else workdir / "missing.toml"),
            )
            if source == "env":
                mp.setenv("FIRECUBE_STORAGE_TYPE", "local")
                mp.setenv("FIRECUBE_TARGET_PATH", str(workdir))
            _stub_list_chunks(mp)
            if "--target" in list_args:  # a labelled store, so the targeted listing succeeds
                seeded = ChunkManager(
                    binding=make_test_binding(workdir, product="p.zarr"), workspace=workdir / "ws"
                )
                seeded.record_run_started(
                    product="p.zarr",
                    run_id="r1",
                    output_path="",
                    output_format="zarr",
                    size=0,
                    meta={},
                )
                seeded.close()
            # The scan's first storage call is discover_products: record stderr as it starts.
            stderr_at_first_storage_call: list[str] = []
            real_discover = ChunkManager.discover_products

            def spy(self, log=stderr_at_first_storage_call, real=real_discover):
                sys.stderr.flush()
                log.append(sys.stderr.buffer.getvalue().decode())  # type: ignore[attr-defined]
                return real(self)

            mp.setattr(ChunkManager, "discover_products", spy)

            result = CliRunner().invoke(
                cli,
                [
                    "chunks",
                    "--workspace",
                    str(workdir / "ws"),
                    "list",
                    *(arg.format(wd=workdir) for arg in list_args),
                    "--format",
                    "json",
                ],
            )

            assert (result.exit_code == 0) is exit_ok, result.stderr
            for text in present:
                assert text in result.stderr
            for text in absent:
                assert text not in result.stderr
            scans = "--all-products" in list_args and source != "none"
            banner = f"[chunks] scanning base {StorageUri.from_local_path(workdir).to_str()}\n"
            assert [banner in seen for seen in stderr_at_first_storage_call] == [True] * scans


@pytest.mark.parametrize("command", [["list"], ["claims", "list"]])
@pytest.mark.parametrize("flags", [[], ["--target", "file:///tmp/p.zarr", "--all-products"]])
def test_chunks_scope_needs_exactly_one_of_target_and_all_products(command, flags) -> None:
    result = CliRunner().invoke(cli, ["chunks", *command, *flags])

    assert result.exit_code == 2
    assert "Provide exactly one of --target or --all-products." in result.output


def test_chunks_bare_n_gets_the_target_rename_hint() -> None:
    result = CliRunner().invoke(cli, ["chunks", "list", "-n", "file:///tmp/p.zarr"])

    assert result.exit_code == 2
    assert "Use --target (-t) instead" in result.output


@pytest.mark.parametrize("command", [["list"], ["claims", "list"], ["delete", "--dry-run"]])
def test_all_products_reports_each_stores_recorded_label_and_uri(
    command, monkeypatch, tmp_path: Path
) -> None:
    for name in ("FIRECUBE_CONFIG", "FIRECUBE_STORAGE_DRIVER"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "local")
    monkeypatch.setenv("FIRECUBE_TARGET_PATH", str(tmp_path))
    stores = [("dirA", "labelX"), ("dirB", "labelY"), ("dirC", "shared"), ("dirD", "shared")]
    for name, label in stores:
        uri = StorageUri.from_local_path(tmp_path / name)
        identity = ProductIdentity.from_uri(uri, "zarr", product_name=label)
        driver = StorageDriverConfig.from_storage_config_or_default(None)
        manager = ChunkManager(
            binding=StorageBinding(identity=identity, driver=driver),
            workspace=tmp_path / "ws" / name,
        )
        manager.record_run_started(
            product=label, run_id="r1", output_path="", output_format="zarr", size=0, meta={}
        )
        manager.record_span(label, "r1", "b1", "G", "active", meta={"group": "G"})
        manager.record_run_terminal(
            product=label,
            run_id="r1",
            output_path="",
            output_format="zarr",
            size=0,
            meta={},
            status="complete",
        )
        manager.acquire_claim(
            product=label, domain=exclusive_maintenance_domain(label), owner_id="o"
        )
        manager.close()

    result = CliRunner().invoke(cli, ["chunks", *command, "--all-products"])

    assert result.exit_code == 0, result.output
    found = re.findall(r"(\w+) \((file://[^)]+)\)", result.output)
    assert sorted(found) == sorted(
        (label, StorageUri.from_local_path(tmp_path / name).to_str()) for name, label in stores
    )
