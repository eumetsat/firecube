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

import importlib.util
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import IndexSpec, PipelineResult, resolve_index_spec
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
    source = template_str.format(plugin_name=plugin_name, class_name=class_name)
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
        "--storage-type local",
        "--storage-driver fsspec",
        f"--output-format {OUTPUT_FORMAT_PER_TEMPLATE[template_type]}",
        "--write-mode",
    ):
        assert flag in readme, flag
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
