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

"""CLI contract tests for ``firecube zarr index rebuild``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import click
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.index import _manager
from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    IndexEnsuredEvent,
    ResolvedIndexRecord,
)
from firecube.core.errors import ManifestError, ResolvedIndexAttrsConflictError
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis
from firecube.core.product.open import open_product
from firecube.core.slot_index import SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR
from firecube.ingestor.registry import loader as ingestor_loader
from firecube.ingestor.runtime.index_record import IndexRecordError, rebuild_index_record

pytestmark = pytest.mark.integration


class _RebuildPlugin:
    PRODUCT_NAME: ClassVar[str] = "index_rebuild_product"
    time_dim_name: ClassVar[str] = "time"
    name = "index_rebuild_plugin"
    size: ClassVar[int] = 3
    template_config_class: ClassVar[None] = None
    plugin_config_class: ClassVar[None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def index_spec(self, ctx: Any) -> IndexSpec:
        return IndexSpec(
            name="index_rebuild_spec",
            groups={"data": IntegerAxis(slot_count=self.size)},
        )


class _DivergentRebuildPlugin(_RebuildPlugin):
    name = "index_rebuild_plugin_divergent"
    size: ClassVar[int] = 5


class _RegularRebuildPlugin(_RebuildPlugin):
    name = "index_rebuild_plugin_regular"

    def index_spec(self, ctx: Any) -> IndexSpec:
        return _regular_spec()


def _register_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    plugins = {
        "index-rebuild": _RebuildPlugin,
        "index-rebuild-divergent": _DivergentRebuildPlugin,
        "index-rebuild-regular": _RegularRebuildPlugin,
    }
    monkeypatch.setattr(ingestor_loader, "AVAILABLE_INGESTORS", plugins.copy())
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)


def _args(cube_dir: Path, plugin: str = "index-rebuild") -> list[str]:
    return _rebuild_args(cube_dir, plugin=plugin, product_name="index_rebuild_product")


def _rebuild_args(cube_dir: Path, *, plugin: str, product_name: str) -> list[str]:
    return [
        "zarr",
        "index",
        "rebuild",
        "--target",
        f"file://{cube_dir}",
        "--plugin",
        plugin,
        "--product-name",
        product_name,
    ]


def _current_record(cube_dir: Path) -> ResolvedIndexRecord:
    current_json = cube_dir / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME
    return ResolvedIndexRecord.from_json_bytes(current_json.read_bytes())


def _non_terminal_runs(cube_dir: Path, product: str = "index_rebuild_product") -> list[Any]:
    manager = _manager(f"file://{cube_dir}", product)
    try:
        return manager.list_runs(product=product, non_terminal=True)
    finally:
        manager.close()


def _expected_record(run_id: str = "rebuild", size: int = 3) -> ResolvedIndexRecord:
    spec = IndexSpec(name="index_rebuild_spec", groups={"data": IntegerAxis(slot_count=size)})
    return resolve_index_spec(spec, time_dim_name="time").as_resolved_index_record(run_id=run_id)


def _regular_spec() -> IndexSpec:
    return IndexSpec(
        name="index_rebuild_regular_spec",
        groups={
            "data": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                slot_count=3,
            )
        },
    )


def _expected_regular_record(run_id: str = "rebuild") -> ResolvedIndexRecord:
    return resolve_index_spec(_regular_spec(), time_dim_name="time").as_resolved_index_record(
        run_id=run_id
    )


def _expected_regular_legacy_hash() -> str:
    legacy_model = resolve_index_spec(
        _regular_spec(), time_dim_name="time"
    ).as_legacy_slot_index_model()
    assert legacy_model is not None
    return legacy_model.identity_hash


def _seed_legacy_only(cube_dir: Path) -> None:
    legacy_dir = cube_dir / ".firecube" / SLOT_INDEX_DIRNAME
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / SLOT_INDEX_CURRENT_FILENAME).write_text("{}")


def _set_legacy_attr(cube_dir: Path, value: str) -> None:
    root = zarr.open(str(cube_dir), mode="a")
    root.attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] = value


def test_rebuild_help_lists_required_flags() -> None:
    result = CliRunner().invoke(cli, ["zarr", "index", "rebuild", "--help"])

    assert result.exit_code == 0, result.output
    assert "--target" in result.output
    assert "--plugin" in result.output
    assert "--product-name" in result.output


@pytest.mark.contract
def test_rebuild_dry_run_writes_no_record_run_or_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "dry_cube"
    zarr.open_group(str(cube), mode="w", zarr_format=3).create_array("a", shape=(3,), dtype="f8")
    before = {p: p.read_bytes() for p in cube.rglob("*") if p.is_file()}

    result = CliRunner().invoke(cli, [*_args(cube), "--dry-run"])

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert not (cube / ".firecube").exists()
    assert {p: p.read_bytes() for p in cube.rglob("*") if p.is_file()} == before
    assert _non_terminal_runs(cube) == []


def test_rebuild_fresh_cube_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, subtests: pytest.Subtests
) -> None:
    """One cube through fresh rebuild, replay, divergent refusal and the next ingest.

    Each step is a former test of this module (step id = old test name). The
    steps are sequential on purpose: each one starts from the state the
    previous step left behind.
    """
    _register_plugins(monkeypatch)
    cube = tmp_path / "fresh_cube"
    cube.mkdir()
    current_json = cube / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME

    with subtests.test(msg="test_rebuild_fresh_cube_writes_new_record"):
        result = CliRunner().invoke(cli, _args(cube))

        assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
        assert "Rebuilt index record" in result.stdout
        record = _current_record(cube)
        assert record.recorded_by_run_id.startswith("rebuild-"), record.recorded_by_run_id
        assert record.identity_hash == _expected_record().identity_hash
        assert record.identity_hash[:16] in result.stdout

    with subtests.test(msg="test_rebuild_success_does_not_strand_run"):
        assert _non_terminal_runs(cube) == []

    with subtests.test(msg="test_rebuild_matching_record_is_noop"):
        original_bytes = current_json.read_bytes()

        result = CliRunner().invoke(cli, _args(cube))

        assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
        assert "Index record unchanged" in result.stdout
        assert current_json.read_bytes() == original_bytes
        assert _current_record(cube).identity_hash[:16] in result.stdout

    with subtests.test(msg="test_rebuild_divergent_record_exits_one_without_overwrite"):
        original_bytes = current_json.read_bytes()
        stored_hash = _current_record(cube).identity_hash
        incoming_hash = _expected_record(size=5).identity_hash

        result = CliRunner().invoke(cli, _args(cube, plugin="index-rebuild-divergent"))

        assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
        assert "plugin declares incompatible resolved index" in result.stderr
        assert stored_hash[:16] in result.stderr
        assert incoming_hash[:16] in result.stderr
        assert current_json.read_bytes() == original_bytes

    with subtests.test(msg="test_rebuild_conflict_does_not_strand_run"):
        assert _non_terminal_runs(cube) == []

    with subtests.test(msg="test_rebuild_run_id_is_uuid_per_invocation"):
        cube_b = tmp_path / "cube_b"
        cube_b.mkdir()

        second = CliRunner().invoke(cli, _args(cube_b))

        assert second.exit_code == 0, second.output
        first_id = _current_record(cube).recorded_by_run_id
        second_id = _current_record(cube_b).recorded_by_run_id
        assert first_id.startswith("rebuild-"), first_id
        assert second_id.startswith("rebuild-"), second_id
        assert first_id != second_id, (
            f"Each rebuild invocation must produce a distinct run_id; both were {first_id!r}"
        )

    with subtests.test(msg="test_rebuild_success_allows_next_ingest"):
        from cli_test_plugin import CliTestIngestor

        config = tmp_path / "config.toml"
        config.write_text("", encoding="utf-8")
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        monkeypatch.setattr(
            ingestor_loader,
            "AVAILABLE_INGESTORS",
            {"cli_test_plugin": CliTestIngestor},
        )
        ingested = CliRunner().invoke(
            cli,
            [
                "--config-file",
                str(config),
                "ingest",
                "cli_test_plugin",
                "--target",
                f"file://{cube}",
                "--product-name",
                "index_rebuild_product",
                "--input-data",
                input_dir.as_uri(),
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
                "--option",
                "allow_empty_source=true",
            ],
        )

        assert ingested.exit_code == 0, ingested.output
        assert "ResumeConflictError" not in ingested.output


def test_rebuild_pre_ensure_failure_does_not_create_or_strand_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "pre_ensure_failure_cube"
    cube.mkdir()

    def fail_before_ensure(plugin: str, manager: Any) -> None:
        _ = plugin, manager
        raise click.ClickException("failed before ensure")

    monkeypatch.setattr("firecube.cli.index._load_plugin", fail_before_ensure)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "failed before ensure" in result.output
    assert _non_terminal_runs(cube) == []


def test_rebuild_legacy_only_cube_is_migration_entry_point(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "legacy_cube"
    cube.mkdir()
    _seed_legacy_only(cube)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "Rebuilt index record" in result.stdout
    assert "Legacy index record" not in result.stderr
    assert _current_record(cube).identity_hash == _expected_record().identity_hash


def test_rebuild_refuses_on_drifted_legacy_attr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "drifted_legacy_attr_cube"
    cube.mkdir()
    _seed_legacy_only(cube)
    drifted_hash = "0" * 64
    declared_hash = _expected_regular_legacy_hash()
    assert drifted_hash != declared_hash
    _set_legacy_attr(cube, drifted_hash)
    current_json = cube / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME

    result = CliRunner().invoke(cli, _args(cube, plugin="index-rebuild-regular"))

    assert result.exit_code == 2, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert drifted_hash[:16] in result.output
    assert declared_hash[:16] in result.output
    assert "Refusing to rebuild" in result.output
    assert not current_json.exists()


def test_rebuild_migrates_on_matching_legacy_attr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "matching_legacy_attr_cube"
    cube.mkdir()
    _seed_legacy_only(cube)
    legacy_json = cube / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME
    original_legacy_bytes = legacy_json.read_bytes()
    _set_legacy_attr(cube, _expected_regular_legacy_hash())

    result = CliRunner().invoke(cli, _args(cube, plugin="index-rebuild-regular"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "Rebuilt index record" in result.stdout
    assert _current_record(cube).identity_hash == _expected_regular_record().identity_hash
    assert legacy_json.read_bytes() == original_legacy_bytes


def test_rebuild_mixed_spec_skips_drift_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from index_spec_integer_test_plugin import MixedAxisIngestor

    monkeypatch.setattr(
        ingestor_loader,
        "AVAILABLE_INGESTORS",
        {"index_spec_integer_mixed": MixedAxisIngestor},
    )
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    cube = tmp_path / "mixed_spec_drifted_legacy_attr_cube"
    cube.mkdir()
    _seed_legacy_only(cube)
    _set_legacy_attr(cube, "0" * 64)

    result = CliRunner().invoke(
        cli,
        _rebuild_args(
            cube,
            plugin="index_spec_integer_mixed",
            product_name="index_spec_integer_mixed_test",
        ),
    )

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "Refusing to rebuild" not in result.output
    assert "Rebuilt index record" in result.stdout


def test_rebuild_emits_rebuilt_outcome(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "rebuilt_outcome_cube"
    cube.mkdir()

    class _FakeChunkManager:
        storage_config = None

        def __init__(self) -> None:
            self.wal_events: list[IndexEnsuredEvent] = []
            self.ensure_calls: list[dict[str, Any]] = []

        def read_slot_index_attrs_hash(self, *, product: str) -> None:
            _ = product
            return None

        def ensure_resolved_index(
            self, *, product: str, record: ResolvedIndexRecord, run_id: str | None = None
        ) -> tuple[ResolvedIndexRecord, str]:
            self.ensure_calls.append({"product": product, "record": record, "run_id": run_id})
            return record, "created"

        def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
            self.wal_events.append(event)

        def record_run_terminal(
            self,
            *,
            product: str,
            run_id: str,
            output_path: str,
            output_format: str,
            size: int,
            meta: dict[str, Any],
            status: str,
        ) -> None:
            _ = product, run_id, output_path, output_format, size, meta, status

        def record_run_failed(
            self,
            *,
            product: str,
            run_id: str,
            output_path: str,
            output_format: str,
            size: int,
            meta: dict[str, Any],
            error: str,
        ) -> None:
            _ = product, run_id, output_path, output_format, size, meta, error

        def close(self) -> None:
            pass

    manager = _FakeChunkManager()
    monkeypatch.setattr("firecube.cli.index._manager", lambda target, product_name: manager)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert len(manager.wal_events) == 1
    event = manager.wal_events[0]
    assert event.outcome == "rebuilt"
    assert event.product == "index_rebuild_product"
    assert event.run_id.startswith("rebuild-"), event.run_id
    assert event.identity_hash == _expected_record().identity_hash


def test_rebuild_none_spec_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class _NoneSpecPlugin:
        PRODUCT_NAME: ClassVar[str] = "index_rebuild_product"
        time_dim_name: ClassVar[str] = "time"
        name = "index-rebuild-none"
        template_config_class: ClassVar[None] = None
        plugin_config_class: ClassVar[None] = None

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def index_spec(self, ctx: Any) -> None:
            _ = ctx
            return None

    monkeypatch.setattr(
        ingestor_loader, "AVAILABLE_INGESTORS", {"index-rebuild-none": _NoneSpecPlugin}
    )
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    cube = tmp_path / "none_spec_cube"
    cube.mkdir()

    result = CliRunner().invoke(cli, _args(cube, plugin="index-rebuild-none"))

    assert result.exit_code == 2, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "does not opt into parallel ingestion" in result.output
    assert "index_spec() returned None" in result.output
    current_json = cube / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME
    assert not current_json.exists()


def test_load_plugin_real_typeerror_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _BrokenInitPlugin:
        PRODUCT_NAME: ClassVar[str] = "index_rebuild_product"
        time_dim_name: ClassVar[str] = "time"
        name = "index-rebuild-broken"

        def __init__(self, chunk_manager: Any = None, **kwargs: Any) -> None:
            _ = (chunk_manager, kwargs)
            raise TypeError("my_custom_message")

        def index_spec(self, ctx: Any) -> IndexSpec:
            _ = ctx
            return IndexSpec(name="unused", groups={"data": IntegerAxis(slot_count=1)})

    monkeypatch.setattr(
        ingestor_loader, "AVAILABLE_INGESTORS", {"index-rebuild-broken": _BrokenInitPlugin}
    )
    monkeypatch.setattr(ingestor_loader, "_LOADED", True)
    cube = tmp_path / "broken_init_cube"
    cube.mkdir()

    result = CliRunner().invoke(cli, _args(cube, plugin="index-rebuild-broken"))

    assert result.exit_code == 70, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "TypeError" in result.stderr, (
        f"Real plugin-side TypeError must not be masked; got: {result.stderr!r}"
    )
    assert "my_custom_message" in result.stderr, (
        f"TypeError message must survive; got: {result.stderr!r}"
    )


def _open_cube(cube: Path) -> Any:
    return open_product(
        f"file://{cube}", format="zarr", product_name="index_rebuild_product", require_label=False
    )


def test_rebuild_library_fresh_cube_records_complete_run_and_prints_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cube = tmp_path / "library_cube"
    cube.mkdir()
    with _open_cube(cube) as product:
        result = rebuild_index_record(product, _RebuildPlugin(), plugin_name="index-rebuild")
        runs = product.manager.list_runs(product="index_rebuild_product", status="complete")

    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")
    assert result.outcome == "created"
    assert _current_record(cube).identity_hash == result.identity_hash
    assert [run.run_id for run in runs] == [result.run_id]


def test_rebuild_library_reports_outcome_before_run_is_recorded_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cube = tmp_path / "order_cube"
    cube.mkdir()
    events: list[str] = []
    record_terminal = ChunkManager.record_run_terminal

    def spy(self: ChunkManager, **kwargs: Any) -> None:
        events.append("terminal")
        record_terminal(self, **kwargs)

    monkeypatch.setattr(ChunkManager, "record_run_terminal", spy)
    with _open_cube(cube) as product:
        rebuild_index_record(
            product,
            _RebuildPlugin(),
            plugin_name="index-rebuild",
            report=lambda message: events.append(f"report:{message}"),
        )

    assert len(events) == 2 and events[0].startswith("report:Rebuilt index record"), events
    assert events[1] == "terminal"


def test_rebuild_library_none_spec_raises_and_leaves_no_run_record(tmp_path: Path) -> None:
    class _NoneSpecPlugin(_RebuildPlugin):
        def index_spec(self, ctx: Any) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
            return None

    cube = tmp_path / "library_none_spec_cube"
    cube.mkdir()
    with _open_cube(cube) as product:
        with pytest.raises(IndexRecordError, match="index_spec\\(\\) returned None"):
            rebuild_index_record(product, _NoneSpecPlugin(), plugin_name="index-rebuild")
        assert product.manager.list_runs(product="index_rebuild_product") == []


@pytest.mark.contract
def test_rebuild_library_attrs_drift_raises_the_manifest_error_unchanged(tmp_path: Path) -> None:
    cube = tmp_path / "library_drift_cube"
    cube.mkdir()
    with _open_cube(cube) as product:
        rebuild_index_record(product, _RebuildPlugin(), plugin_name="index-rebuild")
        zarr.open(str(cube), mode="a").attrs[RESOLVED_INDEX_IDENTITY_HASH_ATTR] = "f" * 64
        with pytest.raises(ManifestError) as refused:
            rebuild_index_record(product, _RebuildPlugin(), plugin_name="index-rebuild")

    assert type(refused.value) is ResolvedIndexAttrsConflictError
    assert not isinstance(refused.value, IndexRecordError)


def test_repro_index_option_rebuild(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    spec = IndexSpec(
        name="regular_axis_calendar_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="time",
                epoch="2049-01-01T00:00:00Z",
                cadence_s=86400,
                slot_count=5,
                calendar="360_day",
            )
        },
    )
    expected = resolve_index_spec(spec, time_dim_name="time").as_resolved_index_record(run_id="x")

    result = CliRunner().invoke(
        cli,
        [
            *_rebuild_args(
                cube, plugin="regular_axis_calendar", product_name="regular_axis_calendar"
            ),
            "--option",
            "slot_count=5",
        ],
    )

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert _current_record(cube).identity_hash == expected.identity_hash


@pytest.mark.parametrize("state", ["without-record", "drift"])
def test_repro_rebuild_attrs_conflict_exits_with_error_line_not_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    _register_plugins(monkeypatch)
    cube = tmp_path / "cube"
    assert CliRunner().invoke(cli, _args(cube)).exit_code == 0
    if state == "drift":
        zarr.open(str(cube), mode="a").attrs[RESOLVED_INDEX_IDENTITY_HASH_ATTR] = "f" * 64
    else:
        (cube / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME).unlink()
    result = CliRunner().invoke(cli, _args(cube))
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.stderr.startswith("Error: "), result.stderr
