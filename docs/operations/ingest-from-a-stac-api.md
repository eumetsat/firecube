# Ingest From A STAC API

Ingest the granules of one collection of a STAC API instead of a local
directory or an S3 prefix.

## When To Use This

Use this page when a STAC API, such as Earth Search, publishes the source
granules your plugin reads. Firecube searches the collection and hands the
plugin the location of each data asset. Everything after discovery runs as
for a local source.

## Prerequisites

- Firecube and a compatible plugin installed in the same environment, with
  the `stac` extra:

  ```bash
  source .venv/bin/activate
  uv pip install 'firecube[stac]'
  ```

- The plugin can open the asset locations the API publishes. Firecube does
  not open `https://` hrefs itself. An `s3://` href is read like any other S3
  source, with the credentials in [Configure S3 Access](s3-access.md).
- The API supports STAC item search. A static `catalog.json` catalogue is not
  supported.

## Steps

1. Name the collection as a `stac://` URI. Take the API landing page URL,
   replace `https://` with `stac://`, and append `/collections/<id>`:

   | API landing page | Collection | Source |
   |---|---|---|
   | `https://earth-search.aws.element84.com/v1` | `cop-dem-glo-30` | `stac://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30` |

   The URI has no `?` and no `#`. Pass one collection per URI.

2. Run the ingestion. `--stac-search` takes one JSON object with any of the
   keys `datetime`, `bbox`, `intersects` and `ids`:

   ```bash
   firecube ingest PLUGIN_NAME \
     --input-data stac://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30 \
     --stac-search '{"bbox": [11.5, 48.0, 11.7, 48.2]}' \
     --target file:///data/products/dem.zarr \
     --product-name dem \
     --storage-type local \
     --storage-driver fsspec \
     --write-mode direct
   ```

   Firecube reads every result page and keeps each asset whose roles contain
   `data`. The default file-suffix list does not apply. To keep only some
   assets, add `--input-filters`; it matches the last segment of each href:

   ```bash
     --input-filters '["*_DEM.tif"]'
   ```

3. To read several collections, or a collection and a local directory, repeat
   `--input-data`. `--stac-search` applies to every `stac://` source:

   ```bash
   firecube ingest PLUGIN_NAME \
     --input-data stac://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30 \
     --input-data /data/raw/extra-tiles \
     --stac-search '{"datetime": "2021-01-01T00:00:00Z/2021-12-31T23:59:59Z"}' \
     ...
   ```

   Granules found twice are ingested once. The order of the sources does not
   change the order of the granules.

## Verify

List the runs of the product. The new run is recorded with the `stac://`
sources, the search filter as given, and the ids of the items it found.

```bash
firecube chunks runs list --product-name file:///data/products/dem.zarr
```

Expected result: one run with status `complete`. Open the product with the
plugin's usual check, for example the output summary that `firecube ingest`
printed.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Error: No source files found after applying input filters.` | The search matched no item, or no asset with the `data` role survived `--input-filters` | Widen `--stac-search` or `--input-filters`. Check the collection's assets on the API's `/collections/<id>/items` page |
| `Invalid value for '--input-data': stac:// URI path must end in /collections/<id>` | The URI names the landing page, `/search` or an item | Append `/collections/<id>` to the API root |
| `Invalid value for '--stac-search': ... accepts only the keys datetime, bbox, intersects, ids` | The filter holds another key, for example `collections` or `limit` | Remove the key. The collection comes from the `stac://` URI |
| `--stac-search applies to stac:// sources only` | No `--input-data` is a `stac://` URI | Drop `--stac-search`, or add a `stac://` source |
| `firecube ingest requires the stac extra` | `pystac-client` is not installed | Run `uv pip install 'firecube[stac]'` |
| `Cannot search STAC source stac://...` | The API did not answer, or refused the search | Open the landing page in a browser and retry; check the host and API root |

## Next Steps

- [Configure S3 Access](s3-access.md) when the API publishes `s3://` hrefs or
  the product is written to S3.
- [Run Parallel Zarr Writes](parallel-zarr-writes.md): `firecube zarr
  preallocate` and `firecube zarr slots` take the same `--input-data` and
  `--stac-search` options.
