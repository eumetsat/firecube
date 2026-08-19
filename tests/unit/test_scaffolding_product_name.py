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
from typing import Any

import pytest

from firecube.ingestor.devtools.scaffolding import _load_template

EXPECTED_HOOKS_PER_TEMPLATE: dict[str, list[str]] = {
    "base": ["_process_batch"],
    "zarr": ["build_dataset"],
    "parquet": ["build_dataset"],
    "direct_zarr": ["zarr_schema", "build_write_intents"],
}

EXPECTED_DEPS: dict[str, list[str]] = {
    "base": ["firecube>=0.1.0"],
    "zarr": ["firecube>=0.1.0", "xarray"],
    "parquet": ["firecube>=0.1.0", "pyarrow"],
    "direct_zarr": ["firecube>=0.1.0", "numpy"],
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

HOOK_CALL_ARGS: dict[str, tuple] = {
    "_process_batch": (None, None),  # (batch, ctx)
    "build_dataset": (None, None, None),  # (group, items/batch, ctx)
    "zarr_schema": (None,),  # (ctx,)
    "build_write_intents": (None, None),  # (batch, ctx)
}

TEMPLATE_FILE_PER_TYPE: dict[str, str] = {
    "base": "ingestor_base.py.tpl",
    "zarr": "ingestor_zarr.py.tpl",
    "parquet": "ingestor_parquet.py.tpl",
    "direct_zarr": "ingestor_direct_zarr.py.tpl",
}


def _render_and_exec(
    template_str: str,
    plugin_name: str = "test_plugin",
    class_name: str = "TestPlugin",
) -> dict[str, Any]:
    source = template_str.format(plugin_name=plugin_name, class_name=class_name)
    ns: dict[str, Any] = {}
    exec(compile(source, "<scaffold>", "exec"), ns)
    return ns


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_template_class_declares_product_name(template_type: str) -> None:
    ns = _render_and_exec(_load_template(TEMPLATE_FILE_PER_TYPE[template_type]))
    cls = ns["TestPlugin"]

    assert cls.PRODUCT_NAME == "test_plugin"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_create_plugin_structure_generates_expected_files(
    template_type: str, tmp_path: Path
) -> None:
    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    assert (root / "pyproject.toml").exists()
    assert (root / "README.md").exists()
    assert (root / "src" / "firecube_demo_foo" / "__init__.py").exists()
    assert (root / "src" / "firecube_demo_foo" / "ingestor.py").exists()
    assert (root / "tests" / "__init__.py").exists()
    assert (root / "tests" / "test_ingestor.py").exists()


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_pyproject_has_template_specific_deps(template_type: str, tmp_path: Path) -> None:
    import tomllib

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    data = tomllib.loads((root / "pyproject.toml").read_text())
    deps: list[str] = data["project"]["dependencies"]
    # Check no forbidden deps present (strict check by pkg name prefix)
    for forbidden in FORBIDDEN_DEPS[template_type]:
        assert not any(d.startswith(forbidden) for d in deps), (
            f"Forbidden dep '{forbidden}' found in {template_type} template: {deps}"
        )
    # Check all expected deps present
    for expected in EXPECTED_DEPS[template_type]:
        assert any(d.startswith(expected.split(">=")[0]) for d in deps), (
            f"Expected dep '{expected}' missing from {template_type} template: {deps}"
        )


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_pyproject_entry_point_is_correct(template_type: str, tmp_path: Path) -> None:
    import tomllib

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    data = tomllib.loads((root / "pyproject.toml").read_text())
    eps = data["project"]["entry-points"]["firecube.plugins"]
    assert "demo_foo" in eps
    assert eps["demo_foo"] == "firecube_demo_foo"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_readme_has_template_specific_markers(template_type: str, tmp_path: Path) -> None:
    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    readme = (root / "README.md").read_text()
    expected_flags = {
        "--input-data /path/to/your/input",
        "--target file:///tmp/demo_foo_out",
        "--product-name demo_foo",
        "--storage-type local",
        "--storage-driver fsspec",
        f"--output-format {OUTPUT_FORMAT_PER_TEMPLATE[template_type]}",
        "--write-mode",
    }

    for flag in expected_flags:
        assert flag in readme

    assert readme.index("uv run pytest") < readme.index("## Install into Firecube")
    assert readme.index("## Install into Firecube") < readme.index("## Run a local ingestion")


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_test_file_has_no_test_functions(template_type: str, tmp_path: Path) -> None:
    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    test_content = (root / "tests" / "test_ingestor.py").read_text()
    assert "def test_" not in test_content, (
        "Generated test file must not contain test function definitions"
    )
    # Should have some guidance content
    assert len(test_content) > 50, "Generated test file appears empty"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_ingestor_imports_successfully(template_type: str, tmp_path: Path) -> None:
    import importlib.util
    import sys

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    ingestor_path = root / "src" / "firecube_demo_foo" / "ingestor.py"
    spec = importlib.util.spec_from_file_location("firecube_demo_foo.ingestor", ingestor_path)
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert hasattr(mod, "DemoFooIngestor"), "Generated class DemoFooIngestor not found"
    instance = mod.DemoFooIngestor()
    assert instance.PRODUCT_NAME == "demo_foo"


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_hook_raises_not_implemented(template_type: str, tmp_path: Path) -> None:
    import importlib.util
    import sys

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    ingestor_path = root / "src" / "firecube_demo_foo" / "ingestor.py"
    spec = importlib.util.spec_from_file_location("firecube_demo_foo.ingestor", ingestor_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = mod  # type: ignore[union-attr]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    instance = mod.DemoFooIngestor()
    for hook_name in EXPECTED_HOOKS_PER_TEMPLATE[template_type]:
        hook = getattr(instance, hook_name)
        args = HOOK_CALL_ARGS[hook_name]
        with pytest.raises(NotImplementedError) as exc_info:
            hook(*args)
        assert hook_name in str(exc_info.value), (
            f"NotImplementedError message for {hook_name} should mention the hook name"
        )


@pytest.mark.unit
@pytest.mark.parametrize("template_type", ["base", "zarr", "parquet", "direct_zarr"])
def test_generated_ingestor_passes_ruff(template_type: str, tmp_path: Path) -> None:
    """Generated ingestor.py must stay ruff-clean as templates evolve.

    A template edit that breaks lint or formatting would otherwise only
    surface when a real user's freshly scaffolded plugin fails their own
    `ruff check`, not in this repo's CI.
    """
    import subprocess
    import sys

    from firecube.ingestor.devtools.scaffolding import create_plugin_structure

    root = create_plugin_structure("demo-foo", tmp_path, template_type=template_type)
    ingestor_path = root / "src" / "firecube_demo_foo" / "ingestor.py"

    check = subprocess.run(
        [sys.executable, "-m", "ruff", "check", str(ingestor_path)],
        capture_output=True,
        text=True,
    )
    assert check.returncode == 0, (
        f"ruff check failed for {template_type} template:\n{check.stdout}{check.stderr}"
    )

    fmt = subprocess.run(
        [sys.executable, "-m", "ruff", "format", "--check", str(ingestor_path)],
        capture_output=True,
        text=True,
    )
    assert fmt.returncode == 0, (
        f"ruff format --check failed for {template_type} template:\n{fmt.stdout}{fmt.stderr}"
    )
