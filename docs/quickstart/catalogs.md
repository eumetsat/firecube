# Create an Intake Catalog

Generate an [Intake](https://intake.readthedocs.io/) catalog for the Zarr or
Parquet product created in [Run Ingestion](ingestion.md), then confirm that
Intake can open a discovered source.

## Prerequisites

- A completed Zarr or Parquet ingestion.
- `FIRECUBE_PLUGIN_NAME`, `FIRECUBE_PRODUCT_NAME`, and either
  `FIRECUBE_LOCAL_TARGET` or `FIRECUBE_S3_TARGET` set in the current shell.
- A product URI ending in `.zarr` or `.parquet`.

## Install The Catalog Reader

For a Zarr product, install the Intake and xarray readers:

```bash
uv pip install intake intake-xarray jinja2
```

For a Parquet product, install the Parquet reader as well:

```bash
uv pip install intake-parquet
```

## Generate The Local Catalog

Use the local product URI from the ingestion step:

```bash
uv run firecube catalog intake "$FIRECUBE_PLUGIN_NAME" \
  --product "$FIRECUBE_LOCAL_TARGET" \
  --collection-id "$FIRECUBE_PRODUCT_NAME" \
  --output "file://${PWD}/catalogs/${FIRECUBE_PRODUCT_NAME}.yaml" \
  --no-storage-options
```

Expected result:

```text
Intake catalog written to: .../catalogs/my_product.yaml
```

## Generate An S3 Catalog

For S3 products, use the S3 product URI and include storage option placeholders:

```bash
uv run firecube catalog intake "$FIRECUBE_PLUGIN_NAME" \
  --product "$FIRECUBE_S3_TARGET" \
  --collection-id "$FIRECUBE_PRODUCT_NAME" \
  --output "file://${PWD}/catalogs/${FIRECUBE_PRODUCT_NAME}-s3.yaml"
```

## Verify The Catalog

Open the local catalog and resolve its first discovered source:

```python
import intake

cat = intake.open_catalog("catalogs/my_product.yaml")
source_names = list(cat)
assert source_names, "catalog contains no sources"

source = cat[source_names[0]]
print(source_names[0])
```

The command should print at least one source name. Each discovered Zarr or
Parquet group becomes a source in the generated catalog.

## Next Steps

- **[Configure S3 Access](configuration.md)** — provide credentials for S3 products
- **[Intake documentation](https://intake.readthedocs.io/)** — load and work with catalog sources
