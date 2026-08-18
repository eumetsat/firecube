# Create an Intake Catalog

Generate an [Intake](https://intake.readthedocs.io/) catalog for an existing
Zarr or Parquet product, then confirm that Intake can resolve a source.

## Prerequisites

- A completed Zarr or Parquet ingestion.
- The Firecube environment activated.
- The plugin name and full product URI.
- A product URI ending in `.zarr` or `.parquet`.

## Install The Catalog Reader

For a Zarr product, install the Intake and xarray readers into the Firecube
environment:

```bash
uv pip install intake intake-xarray jinja2
```

For a Parquet product, install the Parquet reader as well:

```bash
uv pip install intake-parquet
```

## Generate A Local Catalog

Replace `PLUGIN_NAME` if the product came from another plugin:

```bash
mkdir -p catalogs

firecube catalog intake PLUGIN_NAME \
  --product "file://${PWD}/quickstart-output/weather.zarr" \
  --collection-id quickstart_weather \
  --output "file://${PWD}/catalogs/quickstart_weather.yaml" \
  --no-storage-options
```

Expected result:

```text
Intake catalog written to: .../catalogs/quickstart_weather.yaml
```

For an S3 product, pass its full `s3://` URI and omit
`--no-storage-options` so the catalog contains storage-option placeholders.

## Verify The Catalog

Open the catalog and resolve its first source:

```bash
python - <<'PY'
import intake

catalog = intake.open_catalog("catalogs/quickstart_weather.yaml")
source_names = list(catalog)
assert source_names, "catalog contains no sources"

source = catalog[source_names[0]]
print(source_names[0])
PY
```

The command should print at least one source name. Each discovered Zarr or
Parquet group becomes a source in the generated catalog.

## Next Steps

- **[Configure S3 Access](s3-access.md)**: configure credentials for S3-backed
  products.
- **[Intake documentation](https://intake.readthedocs.io/)**: load and use the
  catalog sources.
