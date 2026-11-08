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
import importlib
import json
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo, RegularTimeAxis
from firecube.core.discovery import DiscoveryRequest, discover
from firecube.core.product.open import open_product
from firecube.core.runtime import resolve_storage_config
from firecube.ingestor.api import (
    DirectZarrIngestor,
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
_REMOTE_SOURCE_OPENS: list[str] = []  # filled by the `apis` fixture's spy
REPLACING = {"datetime": "2025-06-01T00:00:00Z/2025-06-15T00:00:00Z"}
# (--stac-search, stac_search of the plugin configuration, the search the API must receive).
# Only the flag replaces the configured table, whole; the two never merge.
_SEARCH_ORIGINS = [
    pytest.param(SEARCH, None, SEARCH, id="search_from_flag"),
    pytest.param(None, SEARCH, SEARCH, id="search_from_config"),
    pytest.param(REPLACING, SEARCH, REPLACING, id="flag_replaces_config"),
]
# (--stac-search, configured stac_search, text naming where the refused search came from).
_REFUSED_ORIGINS = [
    pytest.param(
        SEARCH, None, "--stac-search applies to stac:// sources only", id="search_from_flag"
    ),
    pytest.param(
        None,
        SEARCH,
        "stac_search in the plugin configuration applies to stac:// sources only",
        id="search_from_config",
    ),
]


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


class _StacPlugin(DirectZarrIngestor):
    """Three STAC items placed by their datetime; the batches write nothing."""

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
        observed = dt.datetime.fromisoformat(item.datetime.replace("Z", "+00:00"))
        return ItemInfo(coordinate=observed, values=values)

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
        return []


class _StacAutoAxisOwned(_StacPlugin):
    owned = True


class _StacObservedAxis(_StacPlugin):
    groups = ("g1", "g2")
    auto = False
    owned = True


@pytest.fixture(autouse=True)
def _register_stac_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, cls in (
        ("stac_auto_axis", _StacPlugin),
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
    _REMOTE_SOURCE_OPENS.clear()
    for module in ("firecube.core.discovery._walk", "firecube.ingestor.runtime.workspace"):
        original = importlib.import_module(module).open_source_filesystem

        def spy(uri, storage_config, _original=original):
            if "://" in uri and not uri.startswith("file://"):
                _REMOTE_SOURCE_OPENS.append(uri)
            return _original(uri, storage_config)

        monkeypatch.setattr(f"{module}.open_source_filesystem", spy)
    return api_a, api_b


def _ingest(
    tmp_path: Path,
    *extra: str,
    command: str = "ingest",
    plugin: str = "cli_test_plugin",
    search: dict[str, Any] | None = None,
    config_search: dict[str, Any] | None = None,
) -> list[str]:
    """CLI args; ``search`` is the ``--stac-search`` flag, ``config_search`` the plugin's table."""
    config = tmp_path / "config.toml"
    # A JSON scalar or array is also valid TOML.
    table = [f"{key} = {json.dumps(value)}" for key, value in (config_search or {}).items()]
    config.write_text(
        f"[plugins.{plugin}.stac_search]\n" + "\n".join(table) + "\n" if table else "",
        encoding="utf-8",
    )
    return [
        "--config-file",
        str(config),
        *command.split(),
        plugin,
        *(["--stac-search", json.dumps(search)] if search is not None else []),
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


def _event_metas(target: Path, event_type: str) -> list[dict[str, Any]]:
    """The ``meta`` of every WAL event of one type under the product's ``.firecube``."""
    return [
        event["meta"]
        for wal in sorted((target / ".firecube").rglob("events-*.jsonl"))
        for event in map(json.loads, wal.read_text(encoding="utf-8").splitlines())
        if event.get("event_type") == event_type
    ]


def _no_requests(*apis: FakeStacApi) -> bool:
    """No STAC request was served and no remote source filesystem was opened."""
    return all(not api.requests for api in apis) and not _REMOTE_SOURCE_OPENS


@pytest.mark.parametrize("command", ["ingest", "zarr preallocate", "zarr slots"])
@pytest.mark.parametrize(("flag", "config", "refusal"), _REFUSED_ORIGINS)
def test_stac_search_without_a_stac_source_exits_2_before_any_access(
    tmp_path, apis, command, flag, config, refusal
):
    result = CliRunner().invoke(
        cli,
        _ingest(
            tmp_path,
            "--input-data",
            "s3://bucket/a",
            "--input-data",
            "s3://bucket/b",
            command=command,
            search=flag,
            config_search=config,
        ),
    )

    assert result.exit_code == 2, result.output
    assert refusal in result.output
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


@pytest.mark.parametrize("command", ["ingest", "zarr preallocate", "zarr slots"])
@pytest.mark.parametrize(
    "value",
    [
        "stac://stac-a.test/api/search",
        "/data/raw",
        "relative/dir",
        "https://plugin/own",
        "gs://b/x",
        "S3://b/x",
    ],
)
def test_a_value_that_is_not_a_source_uri_exits_2_naming_input_data(
    tmp_path, apis, monkeypatch, command, value
):
    for module in ("firecube.core.discovery._walk", "firecube.ingestor.runtime.workspace"):

        def refuse_open(uri, storage_config):
            _REMOTE_SOURCE_OPENS.append(uri)
            raise RuntimeError(f"opened {uri}")

        monkeypatch.setattr(f"{module}.open_source_filesystem", refuse_open)
    plugin = "cli_test_plugin" if command == "ingest" else "stac_auto_axis"

    result = CliRunner().invoke(
        cli, _ingest(tmp_path, "--input-data", value, command=command, plugin=plugin)
    )

    assert result.exit_code == 2, result.output
    assert "Invalid value for '--input-data'" in result.output
    if value.startswith("stac://"):
        assert "/collections/<id>" in result.output
    assert _no_requests(*apis)
    assert not (tmp_path / "out.zarr").exists()


@pytest.mark.parametrize("plugin", ["cli_test_plugin", "stac_auto_axis"])
@pytest.mark.parametrize(("flag", "config", "expected"), _SEARCH_ORIGINS)
def test_valid_filter_reaches_both_stac_searches_and_the_run_record(
    tmp_path, apis, subtests, plugin, flag, config, expected
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
            plugin=plugin,
            search=flag,
            config_search=config,
        ),
    )

    assert result.exit_code == 0, result.output
    for api, collection in ((api_a, "coll-a"), (api_b, "coll-b")):
        with subtests.test(msg=collection):
            assert len(api.searches) == 1
            body = api.searches[0]
            assert body == {"collections": [collection], **expected}

    recorded = _event_metas(tmp_path / "out.zarr", "run_started")[0]["stac_discovery"]
    assert recorded == {"sources": [SRC_A, SRC_B], "search": expected, "item_ids": _STAC_ITEM_IDS}


def test_failed_ingest_records_stac_provenance(tmp_path, apis, monkeypatch):
    import cli_test_plugin

    def fail_in_batch_planning(self, items, ctx):
        raise RuntimeError("batch planning failed")

    monkeypatch.setattr(cli_test_plugin.CliTestIngestor, "get_batch_groups", fail_in_batch_planning)

    result = CliRunner().invoke(
        cli,
        _ingest(tmp_path, "--input-data", SRC_A, "--stac-search", json.dumps(SEARCH)),
    )

    assert result.exit_code != 0
    assert "batch planning failed" in result.output + str(result.exception)
    (failed,) = _event_metas(tmp_path / "out.zarr", "run_failed")
    assert failed["stac_discovery"] == {
        "sources": [SRC_A],
        "search": SEARCH,
        "item_ids": ["coll-a/a-1", "coll-a/a-2"],
    }
    (started,) = _event_metas(tmp_path / "out.zarr", "run_started")
    assert started["producer"]["plugin"]["distribution"] == "firecube-test-plugins"
    assert failed["producer"] == started["producer"]  # one capture per run


@pytest.mark.parametrize(
    ("plugin", "groups", "dry_run"),
    [
        pytest.param("stac_auto_axis_owned", ("g",), False, id="auto_axis"),
        pytest.param("stac_observed_axis", ("g1", "g2"), False, id="observed_axis"),
        pytest.param("stac_observed_axis", ("g1", "g2"), True, id="observed_axis_dry_run"),
    ],
)
def test_preallocate_discovers_once_and_records_stac_provenance(
    tmp_path, apis, subtests, plugin, groups, dry_run
):
    target = tmp_path / "out.zarr"
    both = ("--input-data", SRC_A, "--input-data", SRC_B)
    dry = ["--dry-run"] if dry_run else []
    args = _ingest(tmp_path, *both, *dry, command="zarr preallocate", plugin=plugin, search=SEARCH)

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    for api in apis:
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
    # These plugins have no entry point, so the record names the plugin and an unknown source.
    projected = [
        (m["producer"]["plugin"]["name"], m["producer"]["source"]["kind"])
        for m in _event_metas(target, "run_started")
    ]
    assert projected == [(plugin, "unknown")] * len(projected) != []
    observed = np.array([f"2025-06-0{day}T00:00:07" for day in (1, 2, 3)], dtype="datetime64[ns]")
    expected_owned = [1, 2, 3] if groups == ("g",) else [1, 2, 3, np.nan, np.nan]
    for group in groups:
        with subtests.test(msg=group):
            owned = zarr.open_array(str(target), path=f"{group}/owned", mode="r")
            times = zarr.open_array(str(target), path=f"{group}/time", mode="r")
            np.testing.assert_array_equal(owned[:], expected_owned)
            np.testing.assert_array_equal(times[:3], observed)


def test_preallocate_records_the_distribution_of_an_installed_plugin(tmp_path):
    plugin = "direct_zarr_capable_test_plugin"
    args = _ingest(tmp_path, command="zarr preallocate", plugin=plugin)

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    producers = [
        m["producer"]["plugin"] for m in _event_metas(tmp_path / "out.zarr", "run_started")
    ]
    assert producers
    assert {(p["name"], p["distribution"]) for p in producers} == {
        (plugin, "firecube-test-plugins")
    }


@pytest.mark.parametrize(
    ("command", "plugin"),
    [
        ("ingest", "cli_test_plugin"),
        ("zarr preallocate", "stac_auto_axis"),
        ("zarr slots", "stac_auto_axis"),
    ],
)
def test_each_source_reaches_the_plugin_as_given(tmp_path, apis, monkeypatch, command, plugin):
    local = tmp_path / "raw"
    local.mkdir()
    seen: list[tuple[str, ...]] = []
    items = [] if command == "ingest" else ["/x/a.nc"]
    plugin_class = loader.AVAILABLE_INGESTORS[plugin]

    def discover(self, ctx):
        seen.append(tuple(ctx.sources))
        return items

    def inspect(self, item, ctx):
        return ItemInfo(coordinate=dt.datetime(2025, 6, 1, tzinfo=dt.UTC))

    monkeypatch.setattr(plugin_class, "discover_source_files", discover)
    if command != "ingest":
        monkeypatch.setattr(plugin_class, "inspect_item", inspect)
    given = (local.as_uri(), "s3://bucket/x", SRC_A, "s3://bucket/x")
    args = [arg for value in (*given[:3], "", given[3]) for arg in ("--input-data", value)]

    result = CliRunner().invoke(cli, _ingest(tmp_path, *args, command=command, plugin=plugin))

    assert result.exit_code == 0, result.output
    assert seen
    assert set(seen) == {given}
    assert _no_requests(*apis)


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
        cli, _ingest(tmp_path, "--input-data", local.as_uri(), "--input-data", SRC_B)
    )

    assert result.exit_code == 0, result.output
    assert seen == ["https://data.test/coll-b/b-1.nc", str(local / "local-1.nc")]
    assert "stac_discovery" in _event_metas(tmp_path / "out.zarr", "run_started")[0]


def test_a_run_over_local_sources_records_no_stac_discovery(tmp_path, apis):
    local = tmp_path / "raw"
    local.mkdir()
    (local / "local-1.nc").write_bytes(b"")

    result = CliRunner().invoke(cli, _ingest(tmp_path, "--input-data", local.as_uri()))

    assert result.exit_code == 0, result.output
    assert "stac_discovery" not in _event_metas(tmp_path / "out.zarr", "run_started")[0]
    assert _no_requests(*apis)


def test_a_walked_file_beside_stac_items_is_identified_by_its_path(tmp_path, apis):
    (tmp_path / "local-0.nc").write_bytes(b"")

    walked = discover(DiscoveryRequest(sources=(str(tmp_path), SRC_B)))[-1]

    fields = (walked.scheme, walked.collection, walked.item_id, walked.asset_key, walked.roles)
    assert fields == ("file", None, None, None, ())
    assert walked.identity == walked.href == str(tmp_path / "local-0.nc")


def test_missing_stac_extra_names_the_command_before_any_access(tmp_path, apis, monkeypatch):
    monkeypatch.setattr("firecube.core.discovery._stac_compat.HAS_PYSTAC_CLIENT", False)

    result = CliRunner().invoke(cli, _ingest(tmp_path, "--input-data", SRC_A))

    assert result.exit_code == 4, result.output
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
@pytest.mark.parametrize(("flag", "config", "expected"), _SEARCH_ORIGINS)
def test_zarr_commands_admit_stac_uris_through_the_uri_rule(
    tmp_path, apis, subtests, command, flag, config, expected
):
    api_a, _api_b = apis

    result = CliRunner().invoke(
        cli,
        _ingest(
            tmp_path,
            "--input-data",
            SRC_A,
            command=command,
            plugin="stac_auto_axis",
            search=flag,
            config_search=config,
        ),
    )

    assert result.exit_code == 0, result.output
    assert api_a.searches
    for number, body in enumerate(api_a.searches):
        with subtests.test(msg=f"search {number}"):
            assert body == {"collections": ["coll-a"], **expected}


def test_second_preallocate_on_one_instance_discovers_again(tmp_path, apis):
    api_a, api_b = apis
    target = tmp_path / "out.zarr"
    storage_config = resolve_storage_config(
        config_file=None,
        overrides={"storage_type": "local", "storage_driver": "fsspec"},
        set_global=False,
    )
    with open_product(
        target.as_uri(), format="zarr", product_name="out.zarr", require_label=False
    ) as product:
        ingestor = _StacObservedAxis(chunk_manager=product.manager)

        def preallocate(source: str, run_id: str) -> None:
            plugin_ctx = configure_ingestor(
                ingestor,
                target=target.as_uri(),
                options={"no_progress": True},
                run_id=run_id,
                sources=(source,),
            )
            preallocate_product(
                product,
                ingestor,
                plugin_ctx=plugin_ctx,
                plugin_name="stac_observed_axis",
                storage_config=storage_config,
                run_id=run_id,
                write_mode="direct",
                input_data=source,
            )

        preallocate(SRC_A, "first")
        searched_by_first = (len(api_a.searches), len(api_b.searches))
        preallocate(SRC_B, "second")

    assert len(api_a.searches) == searched_by_first[0]
    assert len(api_b.searches) > searched_by_first[1]
    owned = zarr.open_array(str(target), path="g1/owned", mode="r")
    np.testing.assert_array_equal(owned[:], [1, 2, 3, np.nan, np.nan])
