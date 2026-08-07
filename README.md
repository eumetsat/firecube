<p align="center">
  <img src="docs/assets/firecube-logo-title.png" alt="Firecube Logo" width="165">
</p>

<p align="center">
  <strong>Plugin-driven CLI for turning Earth Observation products into analysis-ready datacubes</strong>
</p>

<p align="center">
  <a href="docs/index.md">Documentation</a> |
  <a href="docs/quickstart/index.md">Quickstart</a> |
  <a href="docs/guides/plugins/index.md">Plugin Development</a>
</p>


## Installation

Firecube requires **Python 3.12+**. Install the released package from PyPI in
an isolated environment:

```bash
mkdir firecube-quickstart
cd firecube-quickstart
uv venv --python 3.12
uv pip install firecube
uv run firecube --version
```

See the [Installation guide](docs/quickstart/installation.md) for the user
quickstart. To change Firecube itself, use the setup in
[Contributing to Firecube](docs/contributing/firecube-contributors.md).

## Use A Plugin

Install an external plugin using the package name or local checkout documented
by that plugin:

```bash
uv run firecube plugins install <plugin-package>
uv run firecube plugins list
uv run firecube plugins describe <plugin>
```

Then follow the plugin documentation for its source data and options. Ingestion
requires a target and write mode, plus a product name from the command or plugin
class. This example also spells out the storage and output choices:

```bash
uv run firecube ingest <plugin> \
  --input-data ./source-data \
  --target "file://${PWD}/my_product.zarr" \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

```bash
# use another supported storage driver explicitly
uv run firecube ingest <plugin> \
  --input-data ./source-data \
  --target "file://${PWD}/my_product.zarr" \
  --product-name my_product \
  --storage-type local \
  --storage-driver obstore \
  --output-format zarr \
  --write-mode direct
```

For the full first-run path, use the
[Quickstart](docs/quickstart/index.md).

## Documentation

- [Installation](docs/quickstart/installation.md)
- [Install a plugin](docs/quickstart/plugins.md)
- [Run ingestion](docs/quickstart/ingestion.md)
- [Configure S3 access](docs/quickstart/configuration.md)
- [CLI reference](docs/reference/cli.md)

Build or serve the docs locally:

```bash
uv sync --group docs
uv run mkdocs serve
uv run mkdocs build
```

## Development

Install test extras and run the main local gate:

```bash
uv sync --extra test
uv run pytest --strict-deps -m "not slow and not s3"
```

## Software Bill Of Materials

Regenerate the CycloneDX 1.5 SBOM and the dependency-license report locally with:

```bash
mkdir -p reports
uv export --format cyclonedx1.5 --all-groups --all-extras --output-file reports/sbom.cdx.json
uv run --isolated --all-groups --all-extras --with hatchling --with pip-licenses pip-licenses --format=json --with-urls > reports/dependency-licenses.json
```

The SBOM covers all uv dependency groups and extras. Use the dependency license
report and package metadata for license review; do not rely on CycloneDX license
fields alone.

Included third-party components: none declared.

### Direct Runtime And Optional Dependencies

The following dependencies are not included in the package but are required or
available as optional runtime extras:

| dependency | version | license | copyright | home_url | comments |
| --- | --- | --- | --- | --- | --- |
| `duckdb` | 1.5.4 | MIT |  | https://github.com/duckdb/duckdb-python | Direct dependency. |
| `pandas` | 3.0.3 | BSD-3-Clause |  | https://pandas.pydata.org | Direct dependency. |
| `numpy` | 2.5.0 | BSD-3-Clause |  | https://numpy.org | Direct dependency. |
| `pyarrow` | 24.0.0 | Apache-2.0 |  | https://arrow.apache.org/ | Direct dependency. |
| `xarray` | 2026.4.0 | Apache-2.0 |  | https://xarray.dev/ | Direct dependency. |
| `zarr` | 3.2.1 | MIT |  | https://github.com/zarr-developers/zarr-python | Direct dependency. |
| `psutil` | 7.2.2 | BSD-3-Clause |  | https://github.com/giampaolo/psutil | Direct dependency. |
| `fsspec` | 2026.6.0 | BSD-3-Clause |  | https://github.com/fsspec/filesystem_spec | Direct dependency. |
| `s3fs` | 2026.6.0 | BSD-3-Clause |  | http://github.com/fsspec/s3fs/ | Direct dependency. |
| `botocore` | 1.43.0 | Apache-2.0 |  | https://github.com/boto/botocore | Direct dependency. |
| `netcdf4` | 1.7.4 | MIT |  | https://github.com/Unidata/netcdf4-python | Direct dependency. |
| `h5netcdf` | 1.8.1 | BSD-3-Clause |  | https://h5netcdf.org | Direct dependency. |
| `h5py` | 3.16.0 | BSD-3-Clause |  | https://www.h5py.org/ | Direct dependency. |
| `dask` | 2026.6.0 | BSD-3-Clause |  | https://github.com/dask/dask/ | Direct dependency. |
| `prometheus-client` | 0.25.0 | Apache-2.0 |  | https://github.com/prometheus/client_python | Direct dependency. |
| `opentelemetry-sdk` | 1.44.0 | Apache-2.0 |  | https://github.com/open-telemetry/opentelemetry-python/tree/main/opentelemetry-sdk | Direct dependency. |
| `opentelemetry-exporter-otlp` | 1.44.0 | Apache-2.0 |  | https://github.com/open-telemetry/opentelemetry-python/tree/main/exporter/opentelemetry-exporter-otlp | Direct dependency. |
| `obstore` | 0.11.0 | MIT |  | https://developmentseed.org/obstore | Direct optional `obstore` extra dependency. |
| `tensogram` | 0.22.0 | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `tensogram` extra dependency. |
| `tensogram-xarray` | 0.22.0 | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `tensogram` extra dependency. |
| `tensogram-zarr` | 0.22.0 | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `tensogram` extra dependency. |
| `healpix-geo` | 0.2.1 | Apache-2.0 |  | https://pypi.org/project/healpix-geo/ | Direct optional `healpix` extra dependency. |
| `virtualizarr` | 2.7.1 | Apache-2.0 |  | https://github.com/zarr-developers/VirtualiZarr | Direct optional `virtualzarr` extra dependency. |

### Direct Build, Edit, And Test Dependencies

The following dependencies are only required for building, editing,
documentation, or testing:

| dependency | version | sw type | license | copyright | home_url | comments |
| --- | --- | --- | --- | --- | --- | --- |
| `hatchling` | 1.30.1 | Development tools | MIT |  | https://hatch.pypa.io/latest/ | Build backend dependency. |
| `tensogram` | 0.22.0 | Development tools | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `test` extra dependency. |
| `tensogram-xarray` | 0.22.0 | Development tools | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `test` extra dependency. |
| `tensogram-zarr` | 0.22.0 | Development tools | Apache-2.0 |  | https://sites.ecmwf.int/docs/tensogram/main | Direct optional `test` extra dependency. |
| `obstore` | 0.11.0 | Development tools | MIT |  | https://developmentseed.org/obstore | Direct optional `test` extra dependency. |
| `healpix-geo` | 0.2.1 | Development tools | Apache-2.0 |  | https://pypi.org/project/healpix-geo/ | Direct optional `test` extra dependency. |
| `moto` | 5.2.2 | Development tools | Apache-2.0 |  | https://github.com/getmoto/moto | Direct optional `test` extra dependency. |
| `pytest` | 9.1.1 | Development tools | MIT |  | https://docs.pytest.org/en/latest/ | Direct `dev` dependency group entry. |
| `ruff` | 0.15.20 | Development tools | MIT |  | https://docs.astral.sh/ruff | Direct `dev` dependency group entry. |
| `pytest-cov` | 7.1.0 | Development tools | MIT |  | https://pytest-cov.readthedocs.io/en/latest/changelog.html | Direct `dev` dependency group entry. |
| `pytest-xdist` | 3.8.0 | Development tools | MIT |  | https://github.com/pytest-dev/pytest-xdist | Direct `dev` dependency group entry. |
| `pytest-mock` | 3.15.1 | Development tools | MIT |  | https://github.com/pytest-dev/pytest-mock/ | Direct `dev` dependency group entry. |
| `pytest-asyncio` | 1.4.0 | Development tools | Apache-2.0 |  | https://github.com/pytest-dev/pytest-asyncio | Direct `dev` dependency group entry. |
| `pyright` | 1.1.411 | Development tools | MIT |  | https://github.com/RobertCraigie/pyright-python | Direct `dev` dependency group entry. |
| `mkdocs` | 1.6.1 | Development tools | BSD-2-Clause |  | https://github.com/mkdocs/mkdocs | Direct `docs` dependency group entry. |
| `mkdocs-material` | 9.7.7 | Development tools | MIT |  | https://github.com/squidfunk/mkdocs-material | Direct `docs` dependency group entry. |
| `mkdocstrings` | 1.0.4 | Development tools | ISC |  | https://mkdocstrings.github.io | Direct `docs` dependency group entry. |
| `mkdocs-mermaid2-plugin` | 1.2.3 | Development tools | MIT |  | https://github.com/fralau/mkdocs-mermaid2-plugin | Direct `docs` dependency group entry. |
| `mkdocs-click` | 0.9.0 | Development tools | Apache-2.0 |  | https://github.com/mkdocs/mkdocs-click | Direct `docs` dependency group entry. |
| `mkdocs-macros-plugin` | 1.5.0 | Development tools | MIT |  | https://github.com/fralau/mkdocs_macros_plugin | Direct `docs` dependency group entry. |
| `mike` | 2.2.0 | Development tools | BSD-3-Clause |  | https://github.com/jimporter/mike | Direct `docs` dependency group entry. |


## Copyright and License

Copyright © EUMETSAT 2025-2026

The provided code and instructions are licensed under [Apache License, Version 2.0](./LICENSE).

Contact [EUMETSAT](http://www.eumetsat.int) for details on the usage and distribution terms.

## Authors

See [AUTHORS.md](./AUTHORS.md).
