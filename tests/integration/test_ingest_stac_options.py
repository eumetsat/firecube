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

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import OutputPaths, PipelineResult
from tests.helpers.fake_stac import FakeStacApi, install_fake_stac, stac_item

pytestmark = pytest.mark.integration

API_A = "https://stac-a.test/api"
API_B = "https://stac-b.test/v1"
SRC_A = "stac://stac-a.test/api/collections/coll-a"
SRC_B = "stac://stac-b.test/v1/collections/coll-b"
SEARCH = {"datetime": "2025-06-01T00:00:00Z/2025-06-30T23:59:59Z", "bbox": [10, 40, 12, 42]}


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
            )
            for item_id in ids
        ],
    )


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


def _ingest(tmp_path: Path, *extra: str, command: str = "ingest") -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        *command.split(),
        "cli_test_plugin",
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


def test_valid_filter_reaches_both_stac_searches_and_the_run_record(tmp_path, apis):
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
        ),
    )

    assert result.exit_code == 0, result.output
    for api, collection in ((api_a, "coll-a"), (api_b, "coll-b")):
        assert len(api.searches) == 1
        body = api.searches[0]
        assert body["collections"] == [collection]
        assert body["datetime"] == SEARCH["datetime"]
        assert body["bbox"] == SEARCH["bbox"]

    recorded = _run_started_meta(tmp_path / "out.zarr")["stac_discovery"]
    assert recorded == {
        "sources": [SRC_A, SRC_B],
        "search": SEARCH,
        "item_ids": ["coll-a/a-1", "coll-a/a-2", "coll-b/b-1"],
    }


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
