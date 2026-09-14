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

import datetime
import json
import re
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files as _resource_files
from pathlib import Path

from packaging.licenses import InvalidLicenseExpression, canonicalize_license_expression

from firecube.core.filesystem import ensure_directory

_DEFAULT_AUTHOR_NAME = "Firecube Developer"
_TEMPLATE_LICENSE_HEADER_END = "# FIRECUBE_TEMPLATE_LICENSE_HEADER_END"


def _strip_template_license_header(template: str) -> str:
    if not template.startswith("# FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN"):
        return template

    _, separator, body = template.partition(f"{_TEMPLATE_LICENSE_HEADER_END}\n")
    if not separator:
        return template
    return body


def _load_template(name: str) -> str:
    """Load a scaffold template file by name from the _templates/ package resource."""
    try:
        template = (
            _resource_files("firecube.ingestor.devtools._templates")
            .joinpath(name)
            .read_text(encoding="utf-8")
        )
        return _strip_template_license_header(template)
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
license = {license}
authors = [
    {{ name = {author_name}, email = {author_email} }}
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

The registration test loads the ``firecube.plugins`` entry point declared in
``pyproject.toml``, the same way ``firecube`` finds the plugin. It fails when
the entry point name or module is wrong, or when ``@register_ingestor`` names a
different plugin, which are the usual reasons a plugin is missing from
``firecube plugins list``. Run ``uv sync`` first so the entry point is installed.

Add behaviour tests as you implement the reader: call the generated hook with
a small fixture file and assert the returned shape, dtype, or row count, then
cover empty, partial, and malformed input.
"""

from importlib.metadata import entry_points

from firecube.ingestor.api import discover_ingestors

from {import_name} import {class_name}


def test_entry_point_registers_the_ingestor() -> None:
    found = entry_points(group="firecube.plugins", name="{plugin_name}")
    assert found, "pyproject.toml declares no firecube.plugins entry point named {plugin_name}"
    for entry_point in found:
        entry_point.load()
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

_ZARR_RUN_AGAIN = """A second run into the same store is refused. Add
`--option resume_existing=true` to skip timestamps already written and append new
ones, or `--option force_reingest=true` to rewrite them."""

_ZARR_STORAGE_NOTE = """For an `s3://bucket/key.zarr` target, use `--write-mode staged`: the run
builds the store in a local workspace and uploads it at the end. Chunking,
compression, and sharding defaults live in `ZarrStorageConfig` in the ingestor."""

_README_SUBSTITUTIONS_PER_TEMPLATE: dict[str, dict[str, str]] = {
    "base": {
        "hook_summary": "write_product_item()",
        "output_format": "zarr",
        "output_suffix": "",
        "write_mode_default": "direct",
        "extra_incomplete_note": "",
        "output_check": """The files `write_product_item()` writes are below the target directory:

```bash
ls /tmp/{plugin_name}_out
```""",
        "run_again": """A second run into the same target is refused. Add
`--option resume_existing=true` to run again: it calls `write_product_item()`
for every item, including those already written, so overwriting must be safe.""",
        "parallel_section": "",
        "storage_note": """The example writer in this template writes into a local `file://` target
directly, in both write modes. Replace it before writing to object storage or
relying on `--write-mode staged`.""",
    },
    "zarr": {
        "hook_summary": "read_dataset()",
        "output_format": "zarr",
        "output_suffix": ".zarr",
        "write_mode_default": "direct",
        "extra_incomplete_note": (
            " Set `TIME_DIM` at the top of the ingestor too: ingestion stops with "
            "`NotImplementedError` until it names the time dimension your reader returns."
        ),
        "output_check": """The data is in the store's `default` group:

```python
import xarray as xr

print(xr.open_zarr("/tmp/{plugin_name}_out.zarr", group="default", consolidated=False))
```""",
        "run_again": _ZARR_RUN_AGAIN,
        "parallel_section": "",
        "storage_note": _ZARR_STORAGE_NOTE,
    },
    "parquet": {
        "hook_summary": "read_table()",
        "output_format": "parquet",
        "output_suffix": ".parquet",
        "write_mode_default": "direct",
        "extra_incomplete_note": "",
        "output_check": """Each batch becomes one Parquet part file below the target:

```python
import pyarrow.dataset as ds

print(ds.dataset("/tmp/{plugin_name}_out.parquet", format="parquet").to_table())
```""",
        "run_again": """A Parquet target accepts one run. A second run, or a retry after a failed
run, is refused, and `resume_existing` and `force_reingest` are not supported.
Point every run at a new, empty target, and do not test against a target you
want to keep.""",
        "parallel_section": "",
        "storage_note": """For an `s3://bucket/key.parquet` target, use `--write-mode staged`: the run
writes the part files in a local workspace and uploads them at the end.""",
    },
    "direct_zarr": {
        "hook_summary": "read_product_item()",
        "output_format": "zarr",
        "output_suffix": ".zarr",
        "write_mode_default": "direct",
        "extra_incomplete_note": _DIRECT_ZARR_EXTRA_INCOMPLETE_NOTE,
        "output_check": """The data is in the store's `data` group:

```python
import xarray as xr

print(xr.open_zarr("/tmp/{plugin_name}_out.zarr", group="data", consolidated=False))
```""",
        "run_again": """A second run into the same store is refused. Add
`--option resume_existing=true` to continue writing into it, or
`--option force_reingest=true` to overwrite what is already there.""",
        "parallel_section": """
## Run in parallel

`index_spec` fixes every slot of the store in advance, so separate processes can
write separate slot ranges of one store. Create the store before the first
ingestion: a store that a plain run created cannot be preallocated afterwards.

```bash
uv run firecube zarr preallocate {plugin_name} \\
  --input-data /path/to/your/input \\
  --target file:///tmp/{plugin_name}_out.zarr \\
  --product-name {plugin_name} \\
  --write-mode direct \\
  --slot-start 0 \\
  --slot-end 1008

uv run firecube zarr slots {plugin_name} \\
  --target file:///tmp/{plugin_name}_out.zarr \\
  --product-name {plugin_name} \\
  --write-mode direct
```

`zarr slots` prints the ranges. Start one ingestion per range, each in its own
process, with the same input data:

```bash
uv run firecube ingest {plugin_name} \\
  --input-data /path/to/your/input \\
  --target file:///tmp/{plugin_name}_out.zarr \\
  --product-name {plugin_name} \\
  --output-format zarr \\
  --write-mode direct \\
  --slot-start 0 \\
  --slot-end 24
```

Ranges must be whole multiples of the time chunk declared in `zarr_schema`. See
the Firecube "Run Parallel Zarr Writes" guide for the full workflow.
""",
        "storage_note": """For an `s3://bucket/key.zarr` target, use `--write-mode staged`: the run
builds the store in a local workspace and uploads it at the end. Chunk and shard
shapes come from `zarr_schema`; plugin-wide compression defaults live in
`ZarrStorageConfig` in the ingestor.""",
    },
}

_INGESTOR_TPL_PER_TEMPLATE: dict[str, str] = {
    "base": "ingestor_base.py.tpl",
    "zarr": "ingestor_zarr.py.tpl",
    "parquet": "ingestor_parquet.py.tpl",
    "direct_zarr": "ingestor_direct_zarr.py.tpl",
}


def _make_copyright_header(author: str, license_id: str) -> str:
    year = datetime.date.today().year
    safe_author = author.strip() or _DEFAULT_AUTHOR_NAME
    spdx_license_id = _spdx_license_id(license_id)
    return f"# Copyright {year} {safe_author}\n# SPDX-License-Identifier: {spdx_license_id}"


def _spdx_license_id(license_id: str) -> str:
    """Return the value for the generated ``SPDX-License-Identifier`` header line.

    A valid SPDX license identifier or expression (including ``LicenseRef-``
    forms) is returned in its canonical spelling. Anything else is recorded as
    ``LicenseRef-<value>`` with characters outside the SPDX id alphabet
    replaced by ``-``, or ``LicenseRef-Custom`` when nothing usable remains.
    """
    license_id = license_id.strip()
    try:
        return canonicalize_license_expression(license_id)
    except InvalidLicenseExpression:
        license_ref = re.sub(r"[^A-Za-z0-9.-]+", "-", license_id).strip("-.")
        return f"LicenseRef-{license_ref or 'Custom'}"


def _firecube_version() -> str:
    try:
        return version("firecube")
    except PackageNotFoundError:
        return "unknown"


PLUGIN_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


def validate_plugin_name(name: str) -> str:
    """Return ``name`` unchanged if it can become a plugin ID, package, and class.

    Raises:
        ValueError: If ``name`` does not start with a letter or contains characters
            other than letters, digits, ``-`` and ``_``.
    """
    if not PLUGIN_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"Invalid plugin name {name!r}: start with a letter and use only letters, "
            "digits, '-' and '_' (for example 'my-plugin' or 'sea_ice')."
        )
    return name


def to_snake_case(name: str) -> str:
    """Convert a plugin name to its snake-case plugin ID.

    Camel case splits where a lowercase letter or digit meets an uppercase letter
    and before the last capital of an acronym, so ``MyCoolPlugin`` becomes
    ``my_cool_plugin`` and ``HTTPServer`` becomes ``http_server``. Dashes become
    underscores, and repeated underscores collapse.
    """
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"_+", "_", name.replace("-", "_").lower())
    return name.strip("_")


def to_pascal_case(name: str) -> str:
    """Convert a plugin name to PascalCase via its snake-case form."""
    return "".join(part.capitalize() for part in to_snake_case(name).split("_") if part)


def _toml_string(value: str) -> str:
    """Quote ``value`` as a TOML basic string; JSON string escapes are valid TOML."""
    return json.dumps(value, ensure_ascii=False)


def create_plugin_structure(
    name: str,
    target_dir: Path,
    author_name: str = _DEFAULT_AUTHOR_NAME,
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

    validate_plugin_name(name)
    author_name = author_name.strip() or _DEFAULT_AUTHOR_NAME

    plugin_name = to_snake_case(name)
    start_case_name = to_pascal_case(name)
    class_name = f"{start_case_name}Ingestor"
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
            start_case_name=start_case_name,
            plugin_name=plugin_name,
            import_name=import_name,
            author_name=_toml_string(author_name),
            author_email=_toml_string(author_email),
            license=_toml_string(_spdx_license_id(license)),
            dependencies=_DEPS_STRING_PER_TEMPLATE[template_type],
        )
    )

    # Write template-specific README
    readme_tpl = _load_template("readme.md.tpl")
    (root / "README.md").write_text(
        readme_tpl.format(
            start_case_name=start_case_name,
            plugin_name=plugin_name,
            firecube_version=_firecube_version(),
            **{
                key: value.format(plugin_name=plugin_name)
                for key, value in _README_SUBSTITUTIONS_PER_TEMPLATE[template_type].items()
            },
        )
    )

    # Write source files
    (src_dir / "__init__.py").write_text(PLUGIN_INIT_TEMPLATE.format(class_name=class_name))

    copyright_header = _make_copyright_header(author_name, license)
    ingestor_tpl = _load_template(_INGESTOR_TPL_PER_TEMPLATE[template_type])
    (src_dir / "ingestor.py").write_text(
        ingestor_tpl.format(
            class_name=class_name,
            plugin_name=plugin_name,
            copyright_header=copyright_header,
        )
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
