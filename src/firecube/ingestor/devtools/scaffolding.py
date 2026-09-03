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

"""Plugin scaffolding generator using uv and standards-compliant metadata."""

import re
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files as _resource_files
from pathlib import Path

from firecube.core.filesystem import ensure_directory


def _load_template(name: str) -> str:
    """Load a scaffold template file by name from the _templates/ package resource."""
    try:
        return (
            _resource_files("firecube.ingestor.devtools._templates")
            .joinpath(name)
            .read_text(encoding="utf-8")
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Missing scaffold template resource: {name}. Reinstall firecube."
        ) from exc


# PEP 621 compliant pyproject.toml template
PYPROJECT_TEMPLATE = """[project]
name = "{package_name}"
version = "0.1.0"
description = "Firecube ingestor plugin for {start_case_name}"
readme = "README.md"
requires-python = ">=3.12"
license = {{ text = "{license}" }}
authors = [
    {{ name = "{author_name}", email = "{author_email}" }}
]
dependencies = [
{dependencies}
]

[project.entry-points."firecube.plugins"]
{plugin_name} = "{import_name}"

# Optional: develop against a local Firecube checkout instead of the
# released package. Adjust the path, then re-run `uv sync`.
# [tool.uv.sources]
# firecube = {{ path = "../firecube", editable = true }}

# Optional: expose custom "firecube {plugin_name} ..." subcommands by
# pointing this at a click.Group. Most plugins don't need it.
# [project.entry-points."firecube.plugin_cli"]
# {plugin_name} = "{import_name}.plugin_cli:cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[dependency-groups]
dev = [
    "pytest",
    "ruff",
    "pyright",
]

[tool.ruff]
target-version = "py312"
line-length = 100
src = ["src", "tests"]

[tool.pyright]
include = ["src", "tests"]
"""

PLUGIN_INIT_TEMPLATE = """from .ingestor import {class_name}

# Importing the package registers the ingestor via @register_ingestor.
__all__ = ["{class_name}"]
"""

TEST_INGESTOR_TEMPLATE = '''"""Tests for {class_name}.

The registration test is real: it fails when the ``firecube.plugins`` entry
point in ``pyproject.toml`` and ``@register_ingestor`` disagree, which is the
usual reason a plugin is missing from ``firecube plugins list``.

Add behaviour tests as you implement the reader: call the generated hook with
a small fixture file and assert the returned shape, dtype, or row count, then
cover empty, partial, and malformed input.
"""

from firecube.ingestor.api import discover_ingestors

from {import_name} import {class_name}


def test_plugin_is_registered() -> None:
    assert discover_ingestors()["{plugin_name}"] is {class_name}
    assert {class_name}.PRODUCT_NAME == "{plugin_name}"
'''

_DEPENDENCY_HINT = (
    '    # "h5netcdf",  # example: uncomment and add your source-format reading library here'
)

_DEPS_STRING_PER_TEMPLATE: dict[str, str] = {
    "base": f'    "firecube>=0.1.5",\n{_DEPENDENCY_HINT}',
    "zarr": f'    "firecube>=0.1.5",\n    "xarray",\n{_DEPENDENCY_HINT}',
    "parquet": (
        '    "firecube>=0.1.5",\n'
        '    "pyarrow",\n'
        '    # "pandas",  # uncomment if your build_dataset returns pandas.DataFrame'
    ),
    "direct_zarr": f'    "firecube>=0.1.5",\n    "numpy",\n{_DEPENDENCY_HINT}',
}

_DIRECT_ZARR_EXTRA_INCOMPLETE_NOTE = (
    "\n>\n"
    "> `index_spec` and `zarr_schema` are a working example layout (a ten-minute "
    "axis over one week and one `value` array of four samples). Adapt both to "
    "the product before running `firecube zarr preallocate`, which materializes "
    "whatever `zarr_schema` declares."
)

_README_SUBSTITUTIONS_PER_TEMPLATE: dict[str, dict[str, str]] = {
    "base": {
        "hook_summary": "write_product_item()",
        "output_format": "zarr",
        "output_extension": "zarr",
        "write_mode_default": "staged",
        "extra_incomplete_note": "",
    },
    "zarr": {
        "hook_summary": "read_dataset()",
        "output_format": "zarr",
        "output_extension": "zarr",
        "write_mode_default": "staged",
        "extra_incomplete_note": "",
    },
    "parquet": {
        "hook_summary": "read_table()",
        "output_format": "parquet",
        "output_extension": "parquet",
        "write_mode_default": "staged",
        "extra_incomplete_note": "",
    },
    "direct_zarr": {
        "hook_summary": "read_product_item()",
        "output_format": "zarr",
        "output_extension": "zarr",
        "write_mode_default": "direct",
        "extra_incomplete_note": _DIRECT_ZARR_EXTRA_INCOMPLETE_NOTE,
    },
}

_INGESTOR_TPL_PER_TEMPLATE: dict[str, str] = {
    "base": "ingestor_base.py.tpl",
    "zarr": "ingestor_zarr.py.tpl",
    "parquet": "ingestor_parquet.py.tpl",
    "direct_zarr": "ingestor_direct_zarr.py.tpl",
}


def _firecube_version() -> str:
    try:
        return version("firecube")
    except PackageNotFoundError:
        return "unknown"


def to_snake_case(name: str) -> str:
    name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return name.replace("-", "_").replace(" ", "_")


def to_pascal_case(name: str) -> str:
    return "".join(word.title() for word in name.replace("-", "_").split("_"))


def create_plugin_structure(
    name: str,
    target_dir: Path,
    author_name: str = "Firecube Developer",
    author_email: str = "dev@example.com",
    license: str = "MIT",
    template_type: str = "zarr",
) -> Path:
    """Generate a new plugin project structure."""

    if template_type not in _INGESTOR_TPL_PER_TEMPLATE:
        raise ValueError(
            f"Unknown template type: {template_type}. "
            f"Choices: {list(_INGESTOR_TPL_PER_TEMPLATE.keys())}"
        )

    plugin_name = to_snake_case(name)
    class_name = f"{to_pascal_case(name)}Ingestor"
    package_name = f"firecube-{plugin_name.replace('_', '-')}"
    import_name = f"firecube_{plugin_name}"

    root = target_dir / package_name
    src_dir = root / "src" / import_name

    if root.exists():
        raise FileExistsError(f"Directory {root} already exists")

    ensure_directory(src_dir)
    ensure_directory(root / "tests")

    # Write pyproject.toml with per-template dependencies
    (root / "pyproject.toml").write_text(
        PYPROJECT_TEMPLATE.format(
            package_name=package_name,
            start_case_name=to_pascal_case(name),
            plugin_name=plugin_name,
            import_name=import_name,
            author_name=author_name,
            author_email=author_email,
            license=license,
            dependencies=_DEPS_STRING_PER_TEMPLATE[template_type],
        )
    )

    # Write template-specific README
    readme_tpl = _load_template("readme.md.tpl")
    (root / "README.md").write_text(
        readme_tpl.format(
            start_case_name=to_pascal_case(name),
            plugin_name=plugin_name,
            firecube_version=_firecube_version(),
            **_README_SUBSTITUTIONS_PER_TEMPLATE[template_type],
        )
    )

    # Write source files
    (src_dir / "__init__.py").write_text(PLUGIN_INIT_TEMPLATE.format(class_name=class_name))

    ingestor_tpl = _load_template(_INGESTOR_TPL_PER_TEMPLATE[template_type])
    (src_dir / "ingestor.py").write_text(
        ingestor_tpl.format(class_name=class_name, plugin_name=plugin_name)
    )

    # Create test stub (placeholder comments only — no fake tests)
    (root / "tests" / "__init__.py").touch()
    (root / "tests" / "test_ingestor.py").write_text(
        TEST_INGESTOR_TEMPLATE.format(
            class_name=class_name,
            import_name=import_name,
            plugin_name=plugin_name,
        )
    )

    return root
