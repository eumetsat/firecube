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

"""``ingest`` with repeatable ``--input-data``, ``stac://`` sources and ``--stac-search``."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo, RegularTimeAxis
from firecube.core.controlplane import ChunkManager
from firecube.core.product.resolver import ProductResolver
from firecube.core.runtime import resolve_storage_config
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexedWrite,
    OutputPaths,
    PipelineResult,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader
from firecube.ingestor.runtime.configure import configure_ingestor
from firecube.ingestor.runtime.preallocate import preallocate_product
from tests.helpers.fake_stac import FakeStacApi, install_fake_stac, stac_item

pytestmark = pytest.mark.integration

API_A = "https://stac-a.test/api"
API_B = "https://stac-b.test/v1"
SRC_A = "stac://stac-a.test/api/collections/coll-a"
SRC_B = "stac://stac-b.test/v1/collections/coll-b"
SEARCH = {"datetime": "2025-06-01T00:00:00Z/2025-06-30T23:59:59Z", "bbox": [10, 40, 12, 42]}
# The day of June 2025 each fake item is observed on; the plugins below place items by it.
_ITEM_DAY = {"a-1": 1, "a-2": 2, "b-1": 3}
_STAC_ITEM_IDS = ["coll-a/a-1", "coll-a/a-2", "coll-b/b-1"]


def _api(base: str, collection: str, ids: tuple[str, ...]) -> FakeStacApi:
    return FakeStacApi(
        base,
        [
            stac_item(
                item_id,
                collection,
                {
                    "data": (f"https://data.test/{collection}/{item_id}.nc", ["data"]),
                    "metadata": (f"https://data.test/{collection}/{item_id}.xml", ["metadata"]),
                },
                base=base,
                datetime=f"2025-06-{_ITEM_DAY[item_id]:02d}T00:00:07Z",
            )
            for item_id in ids
        ],
    )


def _observed(item: Any) -> dt.datetime:
    return dt.datetime.fromisoformat(item.datetime.replace("Z", "+00:00"))


class _StacPlugin(DirectZarrIngestor):
    """Three STAC items placed by their datetime; no write intent reads an href."""

    PRODUCT_NAME: ClassVar[str] = "stac_plugin"
    SOURCE_ITEMS: ClassVar[bool] = True
    time_dim_name: ClassVar[str] = "time"
    groups: ClassVar[tuple[str, ...]] = ("g",)
    auto: ClassVar[bool] = True
    owned: ClassVar[bool] = False

    def index_spec(self, ctx):
        axis = (
            IrregularTimeAxis(coordinate="time", values=AUTO)
            if self.auto
            else RegularTimeAxis(
                coordinate="time",
                epoch="2025-06-01T00:00:00Z",
                cadence_s=86400,
                mode="floor",
                slot_count=5,
            )
        )
        return IndexSpec(name="stac_plugin_v1", groups=dict.fromkeys(self.groups, axis))

    def inspect_item(self, item, ctx):
        values = {"owned": float(_ITEM_DAY[item.item_id])} if self.owned else None
        return ItemInfo(coordinate=_observed(item), values=values)

    def zarr_schema(self, ctx):
        n = 3 if self.auto else 5

        def array(name, dtype, **kwargs):
            return ZarrArraySpec(
                name=name, shape=(n,), dtype=dtype, expected_time_count=n, time_indexed=True,
                dimension_names=("time",), **kwargs,
            )  # fmt: skip

        arrays = [array("values", "float32", chunks=(1,), fill_value=0.0)]
        if not self.auto:
            arrays.append(array("time", "datetime64[ns]", fill_value=np.datetime64("NaT", "ns")))
        if self.owned:
            arrays.append(array("owned", "float64", fill_value=np.nan, engine_owned=True))
        return [ZarrGroupSpec(group=group, arrays=arrays) for group in self.groups]

    def build_write_intents(self, batch, ctx):
        return [
            IndexedWrite.slot(
                group=self.groups[0],
                array="values",
                coordinate=_observed(item),
                data=np.asarray(1.0, dtype="float32"),
            )
            for item in batch.items
            if self.auto
        ]


class _StacAutoAxis(_StacPlugin):
    pass


class _StacAutoAxisOwned(_StacPlugin):
    owned = True


class _StacObservedAxis(_StacPlugin):
    groups = ("g1", "g2")
    auto = False
    owned = True


@pytest.fixture(autouse=True)
def _register_stac_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, cls in (
        ("stac_auto_axis", _StacAutoAxis),
        ("stac_auto_axis_owned", _StacAutoAxisOwned),
        ("stac_observed_axis", _StacObservedAxis),
    ):
        cls.name = name
        monkeypatch.setitem(loader.AVAILABLE_INGESTORS, name, cls)


@pytest.fixture(autouse=True)
def _keep_target_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    # cli_test_plugin reports Path(ctx.target) as its output, which turns file:///x into
    # file:/x and fails the run after its batches; report the target URI unchanged.
    import cli_test_plugin

    def process_batch(self, batch, ctx):
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")))

    monkeypatch.setattr(cli_test_plugin.CliTestIngestor, "_process_batch", process_batch)


@pytest.fixture
def apis(monkeypatch: pytest.MonkeyPatch) -> tuple[FakeStacApi, FakeStacApi]:
    api_a = _api(API_A, "coll-a", ("a-2", "a-1"))
    api_b = _api(API_B, "coll-b", ("b-1",))
    install_fake_stac(monkeypatch, api_a, api_b)
    return api_a, api_b


def _ingest(
    tmp_path: Path, *extra: str, command: str = "ingest", plugin: str = "cli_test_plugin"
) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        *command.split(),
        plugin,
        "--target",
        (tmp_path / "out.zarr").as_uri(),
        "--product-name",
        "out",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        *extra,
    ]


def _run_started_meta(target: Path) -> dict[str, Any]:
    for wal in sorted((target / ".firecube").rglob("events-*.jsonl")):
        for line in wal.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            if event.get("event_type") == "run_started":
                return event["meta"]
    raise AssertionError(f"no run_started event under {target}")


def _event_metas(target: Path, event_type: str) -> list[dict[str, Any]]:
    """The ``meta`` of every WAL event of one type under the product's ``.firecube``."""
    return [
        event["meta"]
        for wal in sorted((target / ".firecube").rglob("events-*.jsonl"))
        for event in map(json.loads, wal.read_text(encoding="utf-8").splitlines())
        if event.get("event_type") == event_type
    ]


def _no_requests(*apis: FakeStacApi) -> bool:
    return all(not api.requests for api in apis)


def test_stac_search_without_a_stac_source_exits_2_before_any_access(tmp_path, apis):
    result = CliRunner().invoke(
        cli,
        _ingest(
            tmp_path,
            "--input-data",
            "s3://bucket/a",
            "--input-data",
            "s3://bucket/b",
            "--stac-search",
            json.dumps(SEARCH),
        ),
    )

    assert result.exit_code == 2, result.output
    assert "--stac-search" in result.output
    assert "stac://" in result.output
    assert _no_requests(*apis)
    assert not (tmp_path / "out.zarr").exists()


@pytest.mark.parametrize(
    ("search", "refused"),
    [
        ({"datetime": "2025-06-01/2025-06-02", "limit": 5}, "limit"),
        ({"collections": ["other"]}, "collections"),
        ({"query": {"eo:cloud_cover": {"lt": 10}}}, "query"),
    ],
)
def test_stac_search_with_a_disallowed_key_exits_2(tmp_path, apis, search, refused):
    result = CliRunner().invoke(
        cli,
        _ingest(tmp_path, "--input-data", SRC_A, "--stac-search", json.dumps(search)),
    )

    assert result.exit_code == 2, result.output
    assert "--stac-search" in result.output
    assert refused in result.output
    assert _no_requests(*apis)
    assert not (tmp_path / "out.zarr").exists()


def test_stac_search_that_is_not_a_json_object_exits_2(tmp_path, apis):
    result = CliRunner().invoke(
        cli, _ingest(tmp_path, "--input-data", SRC_A, "--stac-search", "[1, 2]")
    )

    assert result.exit_code == 2, result.output
    assert "JSON object" in result.output
    assert _no_requests(*apis)


def test_a_bad_stac_uri_exits_2_naming_input_data(tmp_path, apis):
    result = CliRunner().invoke(
        cli, _ingest(tmp_path, "--input-data", "stac://stac-a.test/api/search")
    )

    assert result.exit_code == 2, result.output
    assert "Invalid value for '--input-data'" in result.output
    assert "/collections/<id>" in result.output
    assert _no_requests(*apis)


@pytest.mark.parametrize("plugin", ["cli_test_plugin", "stac_auto_axis"])
def test_valid_filter_reaches_both_stac_searches_and_the_run_record(
    tmp_path, apis, subtests, plugin
):
    api_a, api_b = apis

    result = CliRunner().invoke(
        cli,
        _ingest(
            tmp_path,
            "--input-data",
            SRC_A,
            "--input-data",
            SRC_B,
            "--stac-search",
            json.dumps(SEARCH),
            plugin=plugin,
        ),
    )

    assert result.exit_code == 0, result.output
    for api, collection in ((api_a, "coll-a"), (api_b, "coll-b")):
        with subtests.test(msg=collection):
            assert len(api.searches) == 1
            body = api.searches[0]
            assert body["collections"] == [collection]
            assert body["datetime"] == SEARCH["datetime"]
            assert body["bbox"] == SEARCH["bbox"]

    recorded = _run_started_meta(tmp_path / "out.zarr")["stac_discovery"]
    assert recorded == {"sources": [SRC_A, SRC_B], "search": SEARCH, "item_ids": _STAC_ITEM_IDS}


def test_failed_ingest_records_stac_provenance(tmp_path, apis, monkeypatch):
    import cli_test_plugin

    def fail_in_batch_planning(self, items, ctx):
        raise RuntimeError("batch planning failed")

    monkeypatch.setattr(cli_test_plugin.CliTestIngestor, "get_batch_groups", fail_in_batch_planning)

    result = CliRunner().invoke(
        cli,
        _ingest(tmp_path, "--input-data", SRC_A, "--stac-search", json.dumps(SEARCH)),
    )

    target = tmp_path / "out.zarr"
    assert result.exit_code != 0
    assert "batch planning failed" in result.output + str(result.exception)
    assert _event_metas(target, "run_started")
    (failed,) = _event_metas(target, "run_failed")
    assert failed["stac_discovery"] == {
        "sources": [SRC_A],
        "search": SEARCH,
        "item_ids": ["coll-a/a-1", "coll-a/a-2"],
    }


def _group(target: Path, path: str) -> Any:
    return zarr.open_group(str(target), mode="r", use_consolidated=False)[path]


@pytest.mark.parametrize(
    ("plugin", "dry_run"),
    [
        pytest.param("stac_auto_axis_owned", False, id="auto_axis"),
        pytest.param("stac_observed_axis", False, id="observed_axis"),
        pytest.param("stac_observed_axis", True, id="observed_axis_dry_run"),
    ],
)
def test_preallocate_discovers_once_and_records_stac_provenance(
    tmp_path, apis, subtests, plugin, dry_run
):
    api_a, api_b = apis
    target = tmp_path / "out.zarr"

    args = _ingest(
        tmp_path,
        "--input-data",
        SRC_A,
        "--input-data",
        SRC_B,
        "--stac-search",
        json.dumps(SEARCH),
        *(["--dry-run"] if dry_run else []),
        command="zarr preallocate",
        plugin=plugin,
    )

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    for api in (api_a, api_b):
        with subtests.test(msg=api.base):
            assert len(api.searches) == 1
    if dry_run:
        assert not target.exists()
        return

    expected = {"sources": [SRC_A, SRC_B], "search": SEARCH, "item_ids": _STAC_ITEM_IDS}
    for event_type in ("run_started", "run_completed"):
        with subtests.test(msg=event_type):
            metas = _event_metas(target, event_type)
            assert metas
            assert [meta.get("stac_discovery") for meta in metas] == [expected] * len(metas)
    observed = np.array([f"2025-06-0{day}T00:00:07" for day in (1, 2, 3)], dtype="datetime64[ns]")
    if plugin == "stac_auto_axis_owned":
        with subtests.test(msg="g"):
            np.testing.assert_array_equal(_group(target, "g/owned")[:], [1, 2, 3])
            np.testing.assert_array_equal(_group(target, "g/time")[:], observed)
        return
    for group in ("g1", "g2"):
        with subtests.test(msg=group):
            np.testing.assert_array_equal(
                _group(target, f"{group}/owned")[:], [1, 2, 3, np.nan, np.nan]
            )
            np.testing.assert_array_equal(_group(target, f"{group}/time")[:3], observed)


def test_file_and_stac_sources_mix_in_one_run(tmp_path, apis, monkeypatch):
    local = tmp_path / "raw"
    local.mkdir()
    (local / "local-1.nc").write_bytes(b"")
    seen: list[Any] = []

    import cli_test_plugin

    def record(self, item, ctx):
        seen.append(item)
        return True

    monkeypatch.setattr(cli_test_plugin.CliTestIngestor, "filter_item", record)

    result = CliRunner().invoke(
        cli, _ingest(tmp_path, "--input-data", str(local), "--input-data", SRC_B)
    )

    assert result.exit_code == 0, result.output
    assert seen == ["https://data.test/coll-b/b-1.nc", str(local / "local-1.nc")]
    assert "stac_discovery" in _run_started_meta(tmp_path / "out.zarr")


def test_a_run_over_local_sources_records_no_stac_discovery(tmp_path, apis):
    local = tmp_path / "raw"
    local.mkdir()
    (local / "local-1.nc").write_bytes(b"")

    result = CliRunner().invoke(cli, _ingest(tmp_path, "--input-data", str(local)))

    assert result.exit_code == 0, result.output
    assert "stac_discovery" not in _run_started_meta(tmp_path / "out.zarr")
    assert _no_requests(*apis)


def test_missing_stac_extra_names_the_command_before_any_access(tmp_path, apis, monkeypatch):
    monkeypatch.setattr("firecube.core.discovery._stac_compat.HAS_PYSTAC_CLIENT", False)

    result = CliRunner().invoke(cli, _ingest(tmp_path, "--input-data", SRC_A))

    assert result.exit_code == 1, result.output
    assert "firecube ingest requires the stac extra" in result.output
    assert "firecube[stac]" in result.output
    assert _no_requests(*apis)
    assert not (tmp_path / "out.zarr").exists()


def test_an_empty_stac_collection_ends_with_the_no_source_files_refusal(tmp_path, monkeypatch):
    api = FakeStacApi(
        API_A,
        [
            stac_item(
                "meta-only",
                "coll-a",
                {"metadata": ("https://data.test/meta-only.xml", ["metadata"])},
                base=API_A,
            )
        ],
    )
    install_fake_stac(monkeypatch, api)

    result = CliRunner().invoke(cli, _ingest(tmp_path, "--input-data", SRC_A))

    assert result.exit_code != 0
    assert "No source files found" in result.output
    assert "Traceback" not in result.output
    assert len(api.searches) == 1


@pytest.mark.parametrize("command", ["zarr preallocate", "zarr slots"])
def test_zarr_commands_refuse_stac_search_without_a_stac_source(tmp_path, apis, command):
    result = CliRunner().invoke(
        cli,
        _ingest(
            tmp_path,
            "--input-data",
            "s3://bucket/a",
            "--stac-search",
            json.dumps(SEARCH),
            command=command,
        ),
    )

    assert result.exit_code == 2, result.output
    assert "--stac-search applies to stac:// sources only" in result.output
    assert _no_requests(*apis)
    assert not (tmp_path / "out.zarr").exists()


@pytest.mark.parametrize("command", ["zarr preallocate", "zarr slots"])
def test_zarr_commands_admit_stac_uris_through_the_uri_rule(tmp_path, apis, command):
    result = CliRunner().invoke(
        cli,
        _ingest(tmp_path, "--input-data", "stac:///api/collections/x", command=command),
    )

    assert result.exit_code == 2, result.output
    assert "Invalid value for '--input-data': stac:// URI must name a host" in result.output
    assert _no_requests(*apis)


def test_second_preallocate_on_one_instance_discovers_again(tmp_path, apis, subtests):
    api_a, api_b = apis
    target = tmp_path / "out.zarr"
    storage_config = resolve_storage_config(
        config_file=None,
        overrides={"storage_type": "local", "storage_driver": "fsspec"},
        set_global=False,
    )
    identity = ProductResolver.resolve(target.as_uri(), format="zarr", product_name="out")
    binding = StorageBinding(
        identity=identity, driver=StorageDriverConfig.from_storage_config(storage_config)
    )
    chunk_manager = ChunkManager(binding=binding)
    try:
        ingestor = _StacObservedAxis(chunk_manager=chunk_manager)

        def preallocate(source: str, run_id: str) -> None:
            plugin_ctx = configure_ingestor(
                ingestor,
                target=target.as_uri(),
                options={"no_progress": True},
                run_id=run_id,
                sources=(source,),
            )
            preallocate_product(
                ingestor=ingestor,
                plugin_ctx=plugin_ctx,
                plugin_name="stac_observed_axis",
                chunk_manager=chunk_manager,
                session=StorageSession(binding),
                storage_config=storage_config,
                run_id=run_id,
                write_mode="direct",
                input_data=source,
            )

        preallocate(SRC_A, "first")
        searched_by_first = (len(api_a.searches), len(api_b.searches))
        preallocate(SRC_B, "second")
    finally:
        chunk_manager.close()

    assert len(api_a.searches) == searched_by_first[0]
    assert len(api_b.searches) > searched_by_first[1]
    for group in ("g1", "g2"):
        with subtests.test(msg=group):
            np.testing.assert_array_equal(
                _group(target, f"{group}/owned")[:], [1, 2, 3, np.nan, np.nan]
            )
