# Intake Catalogs

Firecube provides tooling to generate [Intake](https://intake.readthedocs.io/) catalog items from supported product stores. This makes analysis-ready data easier to discover and use after ingestion.

Zarr groups are written as `intake-xarray` sources, while Parquet groups are written as [`intake-parquet`](https://intake-parquet.readthedocs.io/) sources.

One generated catalog file represents one collection. The Zarr or Parquet groups discovered inside that product become entries within the collection.

## Why Use Catalogs?

Catalogs give users a discovery layer on top of the ingested product. They help hide storage details and expose the discovered data groups in a consistent way.

A generated catalog item can include:

- Storage options, such as S3 credentials and endpoints.
- Group navigation for Zarr and Parquet products.
- Metadata.
- A collection identity through `metadata.collection_id`.

Install the catalog readers in the Python environment that opens the catalog:

```bash
uv pip install intake intake-xarray jinja2
```

For Parquet-backed catalogs, also install `intake-parquet`:

```bash
uv pip install intake-parquet
```

## Generate A Local Catalog Item

Use the local product URI from the ingestion step:

```bash
uv run firecube catalog intake "$FIRECUBE_PLUGIN" \
  --product "$FIRECUBE_LOCAL_TARGET" \
  --collection-id "$FIRECUBE_PRODUCT_NAME" \
  --output "file://${PWD}/catalogs/${FIRECUBE_PRODUCT_NAME}.yaml" \
  --no-storage-options
```

## Generate An S3 Catalog Item

For S3 products, use the S3 product URI and include storage option placeholders:

```bash
uv run firecube catalog intake "$FIRECUBE_PLUGIN" \
  --product "$FIRECUBE_S3_TARGET" \
  --collection-id "$FIRECUBE_PRODUCT_NAME" \
  --output "file://${PWD}/catalogs/${FIRECUBE_PRODUCT_NAME}-s3.yaml"
```

Recommended layout for backend serving:

```bash
~/.config/firecube/catalogs/
  collection-a.yml
  collection-b.yml
```

Point your catalog server at the containing directory.

## Use The Catalog

You can now load your analysis-ready data in a Jupyter notebook without worrying about storage paths or discovered groups:

```python
import intake

# 1. Open the catalog
cat = intake.open_catalog("catalogs/my_product.yaml")

# 2. Select one discovered source
source = cat[list(cat)[0]]

# 3. Explore the source
print(source)
```


## Next Steps

- **[Configuration](configuration.md)** — set S3 credentials, endpoints, or plugin defaults
