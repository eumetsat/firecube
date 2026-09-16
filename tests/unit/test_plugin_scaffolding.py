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

"""Contract tests for ``firecube plugins create`` output.

Every template is rendered, imported, and driven through its hooks. The
generated code is a complete plugin with one reader function unimplemented,
so these tests assert two things a plugin author depends on: everything the
engine calls before it needs source data works on a fresh scaffold, and the
first call that needs data fails loudly at the reader. Rendered projects are
also linted with their own generated config and typechecked, which is the
only guard against the templates drifting from the Firecube API.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import math
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import (
    IndexSpec,
    PipelineResult,
    ZarrTemplateConfig,
    resolve_index_spec,
)
from firecube.ingestor.devtools.scaffolding import _load_template, create_plugin_structure

TEMPLATES = ["base", "zarr", "parquet", "direct_zarr"]

TEMPLATE_FILE_PER_TYPE: dict[str, str] = {
    "base": "ingestor_base.py.tpl",
    "zarr": "ingestor_zarr.py.tpl",
    "parquet": "ingestor_parquet.py.tpl",
    "direct_zarr": "ingestor_direct_zarr.py.tpl",
}

READER_PER_TEMPLATE: dict[str, str] = {
    "base": "write_product_item",
    "zarr": "read_dataset",
    "parquet": "read_table",
    "direct_zarr": "read_product_item",
}

EXPECTED_DEPS: dict[str, list[str]] = {
    "base": ["firecube>=0.1.5"],
    "zarr": ["firecube>=0.1.5", "xarray"],
    "parquet": ["firecube>=0.1.5", "pyarrow"],
    "direct_zarr": ["firecube>=0.1.5", "numpy"],
}

FORBIDDEN_DEPS: dict[str, list[str]] = {
    "base": ["xarray", "pandas", "numpy", "pyarrow"],
    "zarr": ["pandas", "numpy", "pyarrow"],
    "parquet": ["xarray", "numpy"],
    "direct_zarr": ["xarray", "pandas", "pyarrow"],
}

OUTPUT_FORMAT_PER_TEMPLATE: dict[str, str] = {
    "base": "zarr",
    "zarr": "zarr",
    "parquet": "parquet",
    "direct_zarr": "zarr",
}

# Minimal stand-in for PluginContext: the generated hooks only touch these two.
_CTX = SimpleNamespace(materialize=lambda item: Path(item), target="file:///tmp/demo_foo_out")
_ITEM = "observation.nc"


def _render_and_exec(
    template_str: str,
    plugin_name: str = "test_plugin",
    class_name: str = "TestPlugin",
) -> dict[str, Any]:
    source = template_str.format(
        plugin_name=plugin_name,
        class_name=class_name,
        copyright_header="# Copyright 2024 Test Author\n# SPDX-License-Identifier: Apache-2.0",
    )
    ns: dict[str, Any] = {}
    exec(compile(source, "<scaffold>", "exec"), ns)
    return ns


def _generate(tmp_path: Path, template_type: str) -> tuple[Path, ModuleType]:
    """Render one template into ``tmp_path`` and import its ingestor module."""
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    ingestor_path = root / "src" / "firecube_demo_foo" / "ingestor.py"
    module_name = f"firecube_demo_foo_{template_type}.ingestor"
    spec = importlib.util.spec_from_file_location(module_name, ingestor_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return root, module


def _reader_calls(template_type: str, instance: Any) -> list[Callable[[], Any]]:
    """Hook invocations that must reach the unimplemented reader."""
    batch = SimpleNamespace(items=[_ITEM])
    if template_type == "base":
        return [lambda: instance._process_batch(batch, _CTX)]
    if template_type == "zarr":
        type(instance).time_dim_name = "timestamp"  # the author sets TIME_DIM first
        return [lambda: instance.build_dataset("default", [_ITEM], _CTX)]
    if template_type == "parquet":
        return [lambda: instance.build_dataset("default", batch, _CTX)]
    return [
        lambda: instance.inspect_item(_ITEM, _CTX),
        lambda: instance.build_write_intents(batch, _CTX),
    ]


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_template_class_declares_product_name(template_type: str) -> None:
    ns = _render_and_exec(_load_template(TEMPLATE_FILE_PER_TYPE[template_type]))
    assert ns["TestPlugin"].PRODUCT_NAME == "test_plugin"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generates_expected_project_layout(template_type: str, tmp_path: Path) -> None:
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    for relative in (
        "pyproject.toml",
        "README.md",
        "src/firecube_demo_foo/__init__.py",
        "src/firecube_demo_foo/ingestor.py",
        "tests/__init__.py",
        "tests/test_ingestor.py",
    ):
        assert (root / relative).exists(), relative


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_pyproject_declares_deps_entry_point_and_tooling(
    template_type: str, tmp_path: Path
) -> None:
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    data = tomllib.loads((root / "pyproject.toml").read_text())

    deps: list[str] = data["project"]["dependencies"]
    for forbidden in FORBIDDEN_DEPS[template_type]:
        assert not any(d.startswith(forbidden) for d in deps), (forbidden, deps)
    for expected in EXPECTED_DEPS[template_type]:
        assert expected in deps, (expected, deps)

    assert data["project"]["entry-points"]["firecube.plugins"] == {"demo_foo": "firecube_demo_foo"}
    # Day-one tooling so `uv run ruff` / `uv run pyright` in the plugin match
    # what the scaffolding tests check here.
    assert data["tool"]["ruff"]["line-length"] == 100
    assert data["tool"]["pyright"]["include"] == ["src", "tests"]


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_readme_names_reader_and_documents_the_flow(
    template_type: str, tmp_path: Path
) -> None:
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    readme = (root / "README.md").read_text()

    assert f"Implement `{READER_PER_TEMPLATE[template_type]}()`" in readme
    assert readme.startswith("<!-- Generated by `firecube plugins create` with firecube ")
    for flag in (
        "--input-data /path/to/your/input",
        "--target file:///tmp/demo_foo_out",
        "--product-name demo_foo",
        f"--output-format {OUTPUT_FORMAT_PER_TEMPLATE[template_type]}",
        "--write-mode",
        "--input-filters",
    ):
        assert flag in readme, flag
    # Storage type and driver are inferred from the file:// URI.
    assert "--storage-type" not in readme
    assert "--storage-driver" not in readme
    for command in ("plugins install --editable .", "plugins list", "plugins describe demo_foo"):
        assert f"uv run firecube {command}" in readme, command

    assert readme.index("uv run pytest") < readme.index("## Install into Firecube")
    assert readme.index("## Install into Firecube") < readme.index("## Run a local ingestion")


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_test_file_is_one_registration_test(template_type: str, tmp_path: Path) -> None:
    """One real contract test, no placeholders that pass for free."""
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    test_source = (root / "tests" / "test_ingestor.py").read_text()

    assert test_source.count("def test_") == 1
    assert "discover_ingestors()" in test_source
    assert 'PRODUCT_NAME == "demo_foo"' in test_source
    assert "pass" not in test_source.splitlines()


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_ingestor_imports_and_declares_product_name(
    template_type: str, tmp_path: Path
) -> None:
    _, module = _generate(tmp_path, template_type)
    assert module.DemoFooIngestor().PRODUCT_NAME == "demo_foo"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_hooks_fail_loudly_at_the_reader(template_type: str, tmp_path: Path) -> None:
    """A fresh scaffold fails at the one function the author must write, and says so."""
    _, module = _generate(tmp_path, template_type)
    instance = module.DemoFooIngestor()
    reader = READER_PER_TEMPLATE[template_type]

    for call in _reader_calls(template_type, instance):
        with pytest.raises(NotImplementedError) as exc_info:
            call()
        assert f"{reader}()" in str(exc_info.value)
        assert _ITEM in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_hooks_handle_an_empty_batch_without_the_reader(
    template_type: str, tmp_path: Path
) -> None:
    """The documented skip contract holds before the reader exists."""
    _, module = _generate(tmp_path, template_type)
    instance = module.DemoFooIngestor()
    empty = SimpleNamespace(items=[])

    if template_type == "zarr":
        assert instance.build_dataset("default", [], _CTX) is None
    elif template_type == "parquet":
        assert instance.build_dataset("default", empty, _CTX) is None
    elif template_type == "direct_zarr":
        assert instance.build_write_intents(empty, _CTX) == []
    else:
        result = instance._process_batch(empty, _CTX)
        assert isinstance(result, PipelineResult)
        assert result.outputs is not None and result.outputs.primary == Path("/tmp/demo_foo_out")


@pytest.mark.unit
def test_generated_direct_zarr_plans_without_a_reader(tmp_path: Path) -> None:
    """``zarr slots`` and ``preallocate`` need only ``index_spec`` and ``zarr_schema``.

    Both are live code the engine calls at startup, so a stale axis keyword
    (as when ``end`` became ``end_date``) crashes every fresh scaffold. Resolve
    the axis and check the schema against it the way the engine does.
    """
    _, module = _generate(tmp_path, "direct_zarr")
    instance = module.DemoFooIngestor()

    index_spec = instance.index_spec(None)
    assert isinstance(index_spec, IndexSpec)
    resolved = resolve_index_spec(index_spec, time_dim_name=instance.time_dim_name)
    slot_count = resolved.size("data")
    assert slot_count == 1008  # one week of ten-minute slots

    assert instance.resolved_index(None).size("data") == slot_count
    (group_spec,) = instance.zarr_schema(None)
    assert group_spec.group == "data"
    for array in group_spec.arrays:
        assert array.time_indexed
        assert array.shape[0] == slot_count, array.name
        assert array.chunks is not None
        assert slot_count % array.chunks[0] == 0, f"{array.name}: chunk would be partial"
    (value,) = (array for array in group_spec.arrays if array.name == "value")
    assert math.isnan(value.fill_value), "unwritten slots must not read as 0.0"

    with pytest.raises(NotImplementedError):
        module.read_product_item(Path(_ITEM))


@pytest.mark.unit
@pytest.mark.parametrize("template_type", TEMPLATES)
def test_generated_project_passes_ruff_and_pyright(template_type: str, tmp_path: Path) -> None:
    """Rendered code must lint with its own config and typecheck against the current API.

    Ruff cannot see signature drift, and code after a bare ``raise`` is
    invisible to pyright, which is how an earlier template went stale. The
    generated hooks are reachable code, so pyright covers every line.
    """
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    checks = [
        [sys.executable, "-m", "ruff", "check", "src", "tests"],
        [sys.executable, "-m", "ruff", "format", "--check", "src", "tests"],
        [sys.executable, "-m", "pyright", "--pythonpath", sys.executable],
    ]
    for command in checks:
        completed = subprocess.run(command, cwd=root, capture_output=True, text=True)
        assert completed.returncode == 0, (
            f"{' '.join(command[2:])} failed for {template_type}:\n"
            f"{completed.stdout}{completed.stderr}"
        )


@pytest.mark.unit
def test_cli_create_defaults_to_the_zarr_template(tmp_path: Path) -> None:
    """``firecube plugins create`` without ``--template`` scaffolds a GenericZarrIngestor."""
    result = CliRunner().invoke(
        cli,
        ["plugins", "create", "demo-foo", "--target-dir", str(tmp_path), "--non-interactive"],
    )
    assert result.exit_code == 0, result.output

    ingestor = tmp_path / "firecube-demo-foo" / "src" / "firecube_demo_foo" / "ingestor.py"
    assert "class DemoFooIngestor(GenericZarrIngestor)" in ingestor.read_text()


@pytest.mark.unit
@pytest.mark.parametrize("name", ["my plugin", "9lives", "my.plugin", "-plugin", "_plugin", ""])
def test_invalid_plugin_names_are_rejected_before_anything_is_written(
    name: str, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="Invalid plugin name"):
        create_plugin_structure(name, tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "plugin_name", "class_name"),
    [
        ("my-plugin", "my_plugin", "MyPluginIngestor"),
        ("MyCoolPlugin", "my_cool_plugin", "MyCoolPluginIngestor"),
        ("HTTPServer", "http_server", "HttpServerIngestor"),
        ("My-Cool_Plugin", "my_cool_plugin", "MyCoolPluginIngestor"),
        ("sentinel3-frp", "sentinel3_frp", "Sentinel3FrpIngestor"),
    ],
)
def test_plugin_id_package_and_class_derive_from_one_snake_case_name(
    name: str, plugin_name: str, class_name: str, tmp_path: Path
) -> None:
    root = create_plugin_structure(name, tmp_path)

    assert root.name == f"firecube-{plugin_name.replace('_', '-')}"
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["entry-points"]["firecube.plugins"] == {plugin_name: f"firecube_{plugin_name}"}
    ingestor = (root / "src" / f"firecube_{plugin_name}" / "ingestor.py").read_text()
    assert f"class {class_name}(GenericZarrIngestor)" in ingestor


@pytest.mark.unit
def test_quotes_in_author_and_license_still_produce_a_valid_pyproject(tmp_path: Path) -> None:
    root = create_plugin_structure(
        "demo-foo",
        tmp_path,
        author_name='Jane "JD" Doe',
        author_email="jd@example.com",
        license='Foo "bar"',
    )

    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["authors"] == [{"name": 'Jane "JD" Doe', "email": "jd@example.com"}]
    assert project["license"] == "LicenseRef-Foo-bar"


@pytest.mark.unit
def test_pyproject_license_is_the_spdx_expression_in_the_header(tmp_path: Path) -> None:
    root = create_plugin_structure("demo-foo", tmp_path, license="apache-2.0")

    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["license"] == "Apache-2.0"
    ingestor = (root / "src" / "firecube_demo_foo" / "ingestor.py").read_text()
    assert "# SPDX-License-Identifier: Apache-2.0" in ingestor


def _install_entry_point_metadata(site: Path, entry_point_line: str) -> None:
    """Stand in for ``uv sync``: installed metadata with one plugin entry point."""
    dist_info = site / "firecube_demo_foo-0.1.0.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: firecube-demo-foo\nVersion: 0.1.0\n"
    )
    (dist_info / "entry_points.txt").write_text(f"[firecube.plugins]\n{entry_point_line}\n")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("entry_point_line", "passes"),
    [
        ("demo_foo = firecube_demo_foo", True),
        ("demo_foo = firecube_demo_fo", False),
        ("demo_fo = firecube_demo_foo", False),
    ],
)
def test_generated_registration_test_checks_the_installed_entry_point(
    entry_point_line: str, passes: bool, tmp_path: Path
) -> None:
    """A wrong entry point name or module must fail the generated test.

    Importing the package registers the class on its own, so a test that only
    imports it passes while ``firecube plugins list`` cannot find the plugin.
    """
    root = create_plugin_structure("demo-foo", tmp_path / "project")
    site = tmp_path / "site"
    _install_entry_point_metadata(site, entry_point_line)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(site), str(root / "src")])}

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert (completed.returncode == 0) is passes, completed.stdout + completed.stderr


@pytest.mark.unit
@pytest.mark.parametrize("template", ["parquet", "base"])
def test_cli_rejects_write_strategy_outside_the_zarr_template(
    template: str, tmp_path: Path
) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "plugins",
            "create",
            "demo-foo",
            "--target-dir",
            str(tmp_path),
            "--template",
            template,
            "--write-strategy",
            "zarr-python",
            "--non-interactive",
        ],
    )

    assert result.exit_code == 2, result.output
    assert "--write-strategy applies only to --template zarr" in result.output
    assert not (tmp_path / "firecube-demo-foo").exists()


@pytest.mark.unit
def test_cli_reports_an_invalid_name_as_a_usage_error(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        ["plugins", "create", "my plugin", "--target-dir", str(tmp_path), "--non-interactive"],
    )

    assert result.exit_code == 2, result.output
    assert "Invalid plugin name 'my plugin'" in result.output
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
def test_cli_wizard_asks_again_after_an_invalid_name(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        ["plugins", "create", "placeholder", "--target-dir", str(tmp_path)],
        input="my plugin\nmy-plugin\n\n\n\n\n\n",
    )

    assert result.exit_code == 0, result.output
    assert "Invalid plugin name 'my plugin'" in result.output
    assert (tmp_path / "firecube-my-plugin").is_dir()


@pytest.mark.unit
def test_generated_zarr_ingestor_refuses_a_dataset_without_the_time_dimension(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concatenating on a missing dimension would invent it and store a mostly-NaN cube."""
    _, module = _generate(tmp_path, "zarr")
    dataset = xr.Dataset(
        {"value": (("time",), [1.0])}, coords={"time": [np.datetime64("2024-01-01")]}
    )
    monkeypatch.setattr(module, "read_dataset", lambda path: dataset)
    monkeypatch.setattr(module.DemoFooIngestor, "time_dim_name", "timestamp")

    with pytest.raises(ValueError, match="without TIME_DIM 'timestamp'"):
        module.DemoFooIngestor().build_dataset("default", [_ITEM], _CTX)


@pytest.mark.unit
def test_generated_zarr_ingestor_pins_concat_keywords(tmp_path: Path) -> None:
    """xr.concat pins four keywords; drift silently produces inconsistent stores."""
    root = create_plugin_structure("demo-foo", tmp_path, template_type="zarr")
    source = (root / "src" / "firecube_demo_foo" / "ingestor.py").read_text()

    for keyword in ('data_vars="minimal"', 'coords="minimal"', 'compat="equals"', 'join="exact"'):
        assert keyword in source, keyword


@pytest.mark.unit
@pytest.mark.parametrize("target", ["s3://bucket/demo_foo_out", None])
def test_generated_base_ingestor_refuses_a_remote_or_missing_target(
    target: str | None, tmp_path: Path
) -> None:
    _, module = _generate(tmp_path, "base")
    ctx = SimpleNamespace(materialize=lambda item: Path(item), target=target)

    with pytest.raises(ValueError, match="local file:// target"):
        module.DemoFooIngestor()._process_batch(SimpleNamespace(items=[]), ctx)


_COMMENTED_SETTING = re.compile(
    r"^(\s+)# ((?:zarr_\w+|shards|compressors)\b.*|with xr\.open_dataset\(.*|\s{4}\S.*|\).*)$",
    re.MULTILINE,
)


@pytest.mark.unit
@pytest.mark.parametrize(("template_type", "setting_lines"), [("zarr", 13), ("direct_zarr", 6)])
def test_commented_storage_settings_are_valid_code_once_uncommented(
    template_type: str, setting_lines: int, tmp_path: Path
) -> None:
    """The storage and reader hints are commented out, hiding them from ruff and pyright.

    Uncomment every one and check the result, so the hints cannot drift from
    ``ZarrTemplateConfig``, ``ZarrArraySpec``, and xarray.
    """
    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    ingestor = root / "src" / "firecube_demo_foo" / "ingestor.py"
    source, uncommented = _COMMENTED_SETTING.subn(r"\1\2", ingestor.read_text())
    assert uncommented == setting_lines
    ingestor.write_text(source)

    config_fields = {field.name for field in dataclasses.fields(ZarrTemplateConfig)}
    for name in re.findall(r"^    (zarr_\w+):", source, re.MULTILINE):
        assert name in config_fields, name
    for command in (
        [sys.executable, "-m", "ruff", "check", "src"],
        [sys.executable, "-m", "pyright", "--pythonpath", sys.executable],
    ):
        completed = subprocess.run(command, cwd=root, capture_output=True, text=True)
        assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.unit
def test_generated_zarr_ingestor_stops_until_time_dim_is_set(tmp_path: Path) -> None:
    """A fresh scaffold names ``TIME_DIM`` before it reaches the unimplemented reader."""
    _, module = _generate(tmp_path, "zarr")

    assert module.TIME_DIM == ""
    assert module.DemoFooIngestor.time_dim_name == module.TIME_DIM
    with pytest.raises(NotImplementedError, match="TIME_DIM is not set"):
        module.DemoFooIngestor().build_dataset("default", [_ITEM], _CTX)


_DOCS_URL = re.compile(r"https://eumetsat\.github\.io/firecube/latest/([a-z0-9/-]+)/")


@pytest.mark.unit
def test_guide_links_in_the_generated_zarr_ingestor_point_at_existing_pages(
    tmp_path: Path,
) -> None:
    root = create_plugin_structure("demo-foo", tmp_path, template_type="zarr")
    source = (root / "src" / "firecube_demo_foo" / "ingestor.py").read_text()
    docs_root = Path(__file__).resolve().parents[2] / "docs"

    pages = _DOCS_URL.findall(source)
    assert len(pages) == 5
    for page in pages:
        assert (docs_root / f"{page}.md").is_file(), page
