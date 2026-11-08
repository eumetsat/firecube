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

import json
import subprocess
import sys
import warnings
from collections.abc import Iterator
from types import ModuleType

import pytest

_P2_MESSAGE = (
    r"firecube\.(?:core|ingestor)\.api is deprecated and will be removed in v0\.2\.0\. "
    r"Import from firecube\.sdk instead\."
    r"(?: firecube\.sdk does not carry \S+; open an issue if your plugin needs it\.)?$"
)


@pytest.fixture
def old_doors() -> Iterator[tuple[ModuleType, ModuleType]]:
    """Yield ``(firecube.core.api, firecube.ingestor.api)`` with only the P2 message ignored.

    The imports happen here, not at module level, because a ``pytestmark`` filter does not
    apply at collection time. Any other warning still surfaces.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=_P2_MESSAGE, category=DeprecationWarning)
        import firecube.core.api as core_api
        import firecube.ingestor.api as ingestor_api

        yield core_api, ingestor_api


def test_extent_unknown_error_exported_from_both_facades(old_doors) -> None:
    core_api, ingestor_api = old_doors
    assert core_api.ExtentUnknownError is ingestor_api.ExtentUnknownError


def test_time_axis_exported_from_both_facades(old_doors) -> None:
    core_api, ingestor_api = old_doors
    assert core_api.TimeAxis is ingestor_api.TimeAxis


def test_reserved_attrs_exported_from_both_facades(old_doors) -> None:
    core_api, ingestor_api = old_doors
    core_static_written_attr = core_api.FIRECUBE_STATIC_WRITTEN_ATTR
    ingestor_static_written_attr = ingestor_api.FIRECUBE_STATIC_WRITTEN_ATTR
    core_reserved_array_attrs = core_api.RESERVED_ARRAY_ATTRS
    ingestor_reserved_array_attrs = ingestor_api.RESERVED_ARRAY_ATTRS
    assert core_static_written_attr == "firecube_static_written"
    assert ingestor_static_written_attr == "firecube_static_written"
    assert core_static_written_attr is ingestor_static_written_attr
    assert core_static_written_attr in core_reserved_array_attrs
    assert ingestor_static_written_attr in ingestor_reserved_array_attrs
    assert core_reserved_array_attrs is ingestor_reserved_array_attrs

    for assert_attrs_safe in (core_api.assert_attrs_safe, ingestor_api.assert_attrs_safe):
        assert_attrs_safe({"my_custom_attr": 42})

        try:
            assert_attrs_safe({"firecube_static_written": True})
        except ValueError:
            pass
        else:
            raise AssertionError("reserved attrs must be rejected")


def test_coord_managed_marker_is_core_only(old_doors) -> None:
    # Coordinate lifecycle markers are operator-tooling surface: plugins never
    # read or stamp them, so the plugin facade must not export them.
    core_api, ingestor_api = old_doors
    assert core_api.ATTR_COORD_MANAGED == "firecube_coord_managed"
    assert "ATTR_COORD_MANAGED" in core_api.__all__
    assert "ATTR_COORD_MANAGED" not in ingestor_api.__all__


def test_internal_helpers_hidden(old_doors) -> None:
    hidden_names = (
        "CoordLifecycleState",
        "resolve_coord_lifecycle",
        "raise_if_invalid",
    )

    for module in old_doors:
        for name in hidden_names:
            assert name not in module.__all__


def test_resolve_index_spec_reexported_from_ingestor_api(old_doors) -> None:
    core_api, ingestor_api = old_doors
    assert core_api.resolve_index_spec is ingestor_api.resolve_index_spec


@pytest.mark.contract
def test_sdk_import_loads_no_cli_module_and_no_click() -> None:
    code = (
        "import sys, firecube.sdk\n"
        "bad = sorted(m for m in sys.modules if m == 'click' or m.startswith(('click.', 'firecube.cli')))\n"
        "print(bad)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"


# The names the external plugin repositories import from the two old doors
# (.ultron/evidence/2026-11-08/sdk-split.md); frozen here so a trim of the sdk fails.
_EXTERNAL_NAMES_TEXT = """
    BatchResourceRegistry ConfigurationError DirectZarrIngestor EngineConfig
    GenericZarrIngestor IndexSpec IndexedWrite IngestContext ItemInfo PipelineBatch
    PipelineRunState PluginConfig PluginContext RESERVED_ARRAY_ATTRS RegularTimeAxis
    ResolvedIndex ResumeConflictError RuntimeIngestContext StorageContext TimeAxis WriteIntent
    ZarrArraySpec ZarrGroupSpec ZarrTemplateConfig compare_zarr_stores discover_ingestors
    extract_all_from_zips is_remote_target local_path_from_target normalize_epoch_iso
    register_ingestor resolve_index_spec
"""
_EXTERNALLY_IMPORTED_NAMES = frozenset(_EXTERNAL_NAMES_TEXT.split())


@pytest.mark.contract
def test_sdk_carries_every_externally_imported_name() -> None:
    import firecube.sdk as sdk

    assert len(_EXTERNALLY_IMPORTED_NAMES) == 32
    assert not _EXTERNALLY_IMPORTED_NAMES - set(sdk.__all__)


_OLD = "firecube.ingestor.api"
_TEXT = "DeprecationWarning: {} is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
_NOT_CARRIED = " firecube.sdk does not carry {}; open an issue if your plugin needs it."


def _run(tmp_path, *body: str) -> tuple[list[list], dict, str]:
    """Run ``body`` in a fresh interpreter (first body line is script line 5); return the
    recorded warnings as ``["Category: message", filename, lineno]``, the body's ``state`` dict and the script path."""
    head = "import json, warnings\nstate = {}\nwith warnings.catch_warnings(record=True) as w:\n"
    tail = "print(json.dumps([[[f'{x.category.__name__}: {x.message}', x.filename, x.lineno] for x in w], state]))\n"
    script = tmp_path / "caller.py"
    script.write_text(
        head + "".join(f"    {s}\n" for s in ("warnings.simplefilter('always')", *body)) + tail
    )
    out = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, check=True)
    return *json.loads(out.stdout), str(script)


@pytest.mark.contract
def test_importing_old_module_alone_warns_nothing(tmp_path) -> None:
    assert _run(tmp_path, f"import {_OLD}")[0] == []


@pytest.mark.contract
def test_first_old_name_warns_once_at_the_callers_line(tmp_path) -> None:
    records, _, script = _run(tmp_path, f"from {_OLD} import register_ingestor")
    assert records == [[_TEXT.format(_OLD), script, 5]]


@pytest.mark.contract
def test_second_old_name_from_same_module_warns_nothing(tmp_path) -> None:
    body = (f"from {_OLD} import register_ingestor", f"from {_OLD} import IndexSpec")
    assert len(_run(tmp_path, *body)[0]) == 1


@pytest.mark.contract
def test_core_api_warns_once_on_its_own_after_ingestor_api(tmp_path) -> None:
    records, _, _ = _run(
        tmp_path,
        f"from {_OLD} import register_ingestor",
        "from firecube.core.api import TimeAxis",
        "from firecube.core.api import ExtentUnknownError",
    )
    assert [r[0] for r in records] == [_TEXT.format(_OLD), _TEXT.format("firecube.core.api")]


@pytest.mark.contract
def test_first_name_outside_sdk_adds_the_not_carried_suffix(tmp_path) -> None:
    records, _, _ = _run(tmp_path, f"from {_OLD} import IndexedRegionStrategy")
    assert [r[0] for r in records] == [
        _TEXT.format(_OLD) + _NOT_CARRIED.format("IndexedRegionStrategy")
    ]


@pytest.mark.contract
def test_later_name_outside_sdk_does_not_add_the_suffix(tmp_path) -> None:
    body = (f"from {_OLD} import register_ingestor", f"from {_OLD} import IndexedRegionStrategy")
    assert [r[0] for r in _run(tmp_path, *body)[0]] == [_TEXT.format(_OLD)]


@pytest.mark.contract
def test_star_import_binds_the_real_objects_and_warns_once(tmp_path) -> None:
    records, state, _ = _run(
        tmp_path,
        f"from {_OLD} import *",
        f"import {_OLD} as m",
        "import firecube.ingestor.errors as e, firecube.ingestor.runtime.zarr.strategies.indexed_region as r",
        "state['all_bound'] = all(n in globals() for n in m.__all__)",
        "state['same'] = ConfigurationError is e.ConfigurationError and IndexedRegionStrategy is r.IndexedRegionStrategy",
    )
    assert (state, len(records)) == ({"all_bound": True, "same": True}, 1)


@pytest.mark.contract
def test_same_names_from_sdk_warn_nothing(tmp_path) -> None:
    body = ("from firecube.sdk import register_ingestor, TimeAxis", "from firecube.sdk import *")
    assert _run(tmp_path, *body)[0] == []
