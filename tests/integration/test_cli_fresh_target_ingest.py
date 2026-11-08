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

"""T4.3 — CLI fresh-target smoke test for the storage-flat-uri regression (fixed in T1.2)."""

from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from click.testing import CliRunner
from direct_zarr_capable_test_plugin import DirectZarrCapableTestIngestor

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.ingestor.registry import loader
from firecube.sdk import GenericParquetIngestor
from tests.helpers.storage import make_test_binding

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RUNNER = REPO_ROOT / "tests" / "fixtures" / "test_stub_ingestor" / "run_firecube.py"
STUB_PLUGIN_NAME = "fc_test_stub"
pytestmark = [pytest.mark.integration, pytest.mark.contract]


def _run_ingest(
    target_uri: str, source_dir: Path, *, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix
    if extra_env:
        env.update(extra_env)

    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "ingest",
        STUB_PLUGIN_NAME,
        "--input-data",
        source_dir.as_uri(),
        "--target",
        target_uri,
        "--write-mode",
        "direct",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--option",
        "no_progress=true",
    ]

    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))


def test_fresh_local_target(tmp_path: Path) -> None:
    target_dir = tmp_path / "fresh.zarr"
    target_uri = f"file://{target_dir}"
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    proc = _run_ingest(target_uri, source_dir)

    assert proc.returncode == 0, (
        f"Ingest failed (exit={proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    assert "FileNotFoundError" not in proc.stderr, (
        f"Original regression detected in stderr:\n{proc.stderr}"
    )
    assert target_dir.exists(), f"Target dir not created at {target_dir}"

    zarr_metas = list(target_dir.rglob("zarr.json"))
    assert zarr_metas, (
        f"No zarr.json found under {target_dir} (zarr v3 metadata missing). "
        f"Tree: {sorted(p.relative_to(target_dir) for p in target_dir.rglob('*'))}"
    )

    control_root = target_dir / ".firecube"
    assert control_root.is_dir(), f".firecube/ control plane not created at {control_root}"
    assert (control_root / "schema.json").exists(), "Control plane schema.json missing"

    runs_dir = control_root / "runs"
    assert runs_dir.is_dir(), f"WAL runs directory missing at {runs_dir}"
    wal_files = list(runs_dir.rglob("events-*.jsonl"))
    assert wal_files, (
        f"No events-*.jsonl WAL file found under {runs_dir}. "
        f"Run dirs: {[p.name for p in runs_dir.iterdir()]}"
    )

    default_group = target_dir / "default"
    assert default_group.exists(), (
        "Direct write mode must produce output under target_dir "
        f"(staged would route via temp workspace); {default_group} missing"
    )


def test_fresh_local_target_obstore_parity(tmp_path: Path) -> None:
    target_dir = tmp_path / "fresh-obstore.zarr"
    target_uri = f"file://{target_dir}"
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix

    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "ingest",
        STUB_PLUGIN_NAME,
        "--input-data",
        source_dir.as_uri(),
        "--target",
        target_uri,
        "--write-mode",
        "direct",
        "--storage-type",
        "local",
        "--storage-driver",
        "obstore",
        "--option",
        "no_progress=true",
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))

    assert proc.returncode == 0, (
        f"Obstore parity ingest failed (exit={proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    assert "FileNotFoundError" not in proc.stderr, proc.stderr
    assert any(target_dir.rglob("zarr.json")), "obstore driver: no zarr.json produced"
    assert (target_dir / ".firecube").is_dir(), "obstore driver: .firecube/ missing"


CAPABLE = "direct_zarr_capable_test_plugin"
PRODUCT = "direct_zarr_capable_test_product"
RESUME = ["--option", "resume_existing=true"]


class _Parquet(GenericParquetIngestor):
    PRODUCT_NAME = name = "dry_run_parquet"

    def discover_source_files(self, ctx):
        return ["a", "b", "c"]

    def build_dataset(self, group, batch, ctx):
        return pa.table({"item": list(batch.items)})


class _OtherCadence(DirectZarrCapableTestIngestor):
    name = "dry_run_other_cadence"

    def index_spec(self, ctx):
        spec = super().index_spec(ctx)
        return dataclasses.replace(spec, groups={"data": dataclasses.replace(cast(Any, spec.groups["data"]), cadence_s=2)})  # fmt: skip


def _ingest(root, plugin, *extra, dry=True, driver="fsspec"):
    args = ["ingest", plugin, "--input-data", root.as_uri(), "--write-mode", "direct"]
    args += ["--target", (root / "t.zarr").as_uri(), "--storage-type", "local", "--option", "no_progress=true"]  # fmt: skip
    result = CliRunner().invoke(cli, [*args, "--storage-driver", driver, *extra, *(["--dry-run"] if dry else [])])  # fmt: skip
    return result, json.loads(result.stdout[result.stdout.index("{") :])


def _tree(root):
    return {str(p.relative_to(root)): p.is_file() and p.read_bytes() for p in root.rglob("*")}


@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_dry_run_on_fresh_target_creates_nothing(tmp_path, monkeypatch, driver):
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "tmp"))
    before = _tree(tmp_path)
    result, manifest = _ingest(tmp_path, CAPABLE, "--option", f"workspace={tmp_path / 'ws'}", driver=driver)  # fmt: skip
    assert result.exit_code == 0 and manifest["dry_run"] is True, result.output
    assert _tree(tmp_path) == before, "dry run left a target, workspace or firecube_* temp dir"


@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_dry_run_on_existing_cube_records_no_byte_run_or_claim(tmp_path, driver):
    assert _ingest(tmp_path, CAPABLE, dry=False, driver=driver)[0].exit_code == 0
    binding = make_test_binding(tmp_path, product="t.zarr", label=PRODUCT)
    manager = ChunkManager(binding=binding, workspace=tmp_path / "ws")
    before, runs = _tree(tmp_path / "t.zarr"), manager.list_runs(product=PRODUCT)
    result, manifest = _ingest(tmp_path, CAPABLE, *RESUME, driver=driver)
    assert result.exit_code == 0 and manifest["plan"]["index"]["status"] == "unchanged"
    assert _tree(tmp_path / "t.zarr") == before
    assert manager.list_runs(product=PRODUCT) == runs
    assert list(manager.list_claims(product=PRODUCT)) == []


def test_dry_run_plan_equals_real_run_of_same_fixture(tmp_path):
    window = ["--slot-start", "0", "--slot-end", "100"]
    plan = _ingest(tmp_path, CAPABLE, *window)[1]["plan"]
    real = _ingest(tmp_path, CAPABLE, *window, dry=False)[1]["metrics"]
    assert plan["index"]["status"] == "would_create" and plan["slot_range"] == [0, 100]
    assert plan["batches_total"] == real["pipeline"]["batches_total"] == 10
    assert plan["groups"] == sorted({c["group"] for c in real["zarr"]["coverage"]})
    binding = make_test_binding(tmp_path, product="t.zarr", label=PRODUCT)
    manager = ChunkManager(binding=binding, workspace=tmp_path / "ws")
    assert [r.slot_range for r in manager.list_runs(product=PRODUCT)] == [tuple(plan["slot_range"])]
    assert manager.get_resolved_index(product=PRODUCT) is not None, "plan said would_create"


@pytest.mark.parametrize(("size", "total", "truncated"), [(2, 100, False), (1, 200, True)])
def test_dry_run_lists_at_most_100_batches_and_flags_truncation(tmp_path, size, total, truncated):
    plan = _ingest(tmp_path, CAPABLE, "--option", f"pipeline_batch_size={size}")[1]["plan"]
    assert (plan["batches_total"], len(plan["batches"]), plan["batches_truncated"]) == (total, 100, truncated)  # fmt: skip


@pytest.mark.parametrize("blocker", ["resume_guard", "index", "fresh_output", "maintenance"])
def test_dry_run_exits_1_and_names_the_blocking_check(tmp_path, monkeypatch, blocker):
    for cls in (_Parquet, _OtherCadence):
        monkeypatch.setitem(loader.AVAILABLE_INGESTORS, cls.name, cls)
    real = _Parquet.name if blocker == "fresh_output" else CAPABLE
    plugin = _OtherCadence.name if blocker == "index" else real
    assert _ingest(tmp_path, real, dry=False)[0].exit_code == 0
    binding = make_test_binding(tmp_path, product="t.zarr", label=PRODUCT)
    manager = ChunkManager(binding=binding, workspace=tmp_path / "ws")
    if blocker == "maintenance":
        manager.acquire_exclusive_maintenance(product=PRODUCT, run_id="held")
    if blocker == "resume_guard":  # a peer run that started and never finished
        target = str(tmp_path / "t.zarr")
        manager.record_run_started(product=PRODUCT, run_id="peer", output_path=target, output_format="zarr", size=0, meta={})  # fmt: skip
    before = _tree(tmp_path / "t.zarr")
    result, manifest = _ingest(tmp_path, plugin, *RESUME)
    assert result.exit_code == 1, result.output
    assert manifest["plan"]["blocked"] == [blocker] and manifest["plan"][blocker]["reason"]
    assert _tree(tmp_path / "t.zarr") == before


@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
@pytest.mark.parametrize("stale", [False, True], ids=["live", "stale"])
def test_dry_run_refuses_a_held_fresh_output_claim_the_real_run_refuses(tmp_path, monkeypatch, driver, stale):  # fmt: skip
    monkeypatch.setitem(loader.AVAILABLE_INGESTORS, _Parquet.name, _Parquet)
    label = _Parquet.PRODUCT_NAME
    binding = make_test_binding(tmp_path, product="t.zarr", label=label, driver=driver, format="parquet")  # fmt: skip
    manager = ChunkManager(binding=binding, workspace=tmp_path / "ws")
    domain = WriteDomain(product=label, category="fresh_output", name="product")
    handle = manager.acquire_claim(product=label, domain=domain, owner_id="peer")
    if stale:  # age the heartbeat so the claim reads as stale
        path = Path(handle.info.claim_path.removeprefix("file://"))
        record = json.loads(path.read_bytes())
        record["last_heartbeat_at"] = record["acquired_at"] - 100000
        path.write_bytes(json.dumps(record, separators=(",", ":")).encode())
        aged = manager.repo.read_claim_by_domain(product=label, domain=domain.identifier)
        assert aged is not None and aged.stale
    before = _tree(tmp_path)
    result, manifest = _ingest(tmp_path, _Parquet.name, *RESUME, driver=driver)
    reason = manifest["plan"]["fresh_output"]["reason"]
    assert result.exit_code == 1, result.output
    assert manifest["plan"]["blocked"] == ["fresh_output"], manifest["plan"]
    assert "fresh_output:product" in reason and "peer" in reason and ("stale" if stale else "live") in reason  # fmt: skip
    assert _tree(tmp_path) == before, "preview changed the input, target or .firecube"
    real = _ingest_real_exit(tmp_path, driver)
    assert real.exit_code == 1 and "fresh_output:product" in real.output, real.output
    manager.close()


def _ingest_real_exit(root, driver):
    args = ["ingest", _Parquet.name, "--input-data", root.as_uri(), "--write-mode", "direct"]
    args += ["--target", (root / "t.zarr").as_uri(), "--storage-type", "local", "--option", "no_progress=true", "--storage-driver", driver, *RESUME]  # fmt: skip
    return CliRunner().invoke(cli, args)
