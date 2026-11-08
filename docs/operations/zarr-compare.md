# Compare Zarr Stores

## Purpose

Check whether two Zarr stores are operationally equivalent after migration,
re-ingestion, staging promotion, or a storage-driver change. The command is
read-only and boolean: it either confirms equivalence or lists mismatches and
exits nonzero, so it can gate a promotion step in a pipeline.

## Prerequisites

- Both inputs are complete Zarr store URIs (`file://` or `s3://`).
- For `s3://` stores, credentials are configured as in
  [Configure S3 Access](s3-access.md).

## Procedure

1. Run the comparison:

   ```bash
   firecube zarr compare \
      file:///data/products/before.zarr \
      file:///data/products/after.zarr
   ```

2. Check the result:

   - **Exit 0, no output**: the stores are fully equivalent.
   - **Exit 0, WARNING on stderr**: the stores have layout-only differences
     (chunk shape or codec differences). Values are equivalent. The WARNING
     looks like:

     ```text
     WARNING: layout differences only (chunks/codecs differ, values equivalent). Use separate encoding options to realign.
     ```

   - **Exit 1, mismatches on stderr**: content differs (values, shapes,
     dtypes, attrs, or missing arrays). One line per difference:

      ```text
      array data/values: shape (2, 2) != (3, 2)
      array data/lat: firecube_static_written True != None
      array data/data: attrs[long_name] 'test data' != <missing>
      array data/data: attrs[zarr_fill_value] <missing> != 0.0
      ```

The comparison covers array paths, shape, dtype, chunks, dimension names,
public attrs, Firecube's static-array marker, and values. Runtime trace attrs
are ignored. Any other nonzero exit status means the command could not run,
for example because a URI is missing, a store has no Zarr metadata, or the
storage flags do not match the URI scheme.

Firecube reports each differing attribute key on its own line as
`attrs[<key>] <left> != <right>`. A key that one store lacks shows as
`<missing>`. Two attributes that hold the same NaN value are equal.

## Compare A Store With Its Restored Archive

A ZIP archive restores the store file for file. Compare the source with the
restored store:

```bash
firecube zarr compare \
   file:///data/products/MY_PRODUCT.zarr \
   file:///data/restored/MY_PRODUCT.zarr
```

For a ZIP archive the command prints nothing and exits 0.

A Tensogram archive stores the arrays in its own encoding. A restore decodes
them back into a Zarr store, but the store is not identical to the source. The
command exits 1 and lists the differences. The restored store differs in
these ways:

- The names of the coordinate arrays and the dimension names.
- The dtype of the time coordinate.
- The attributes of an array. The restore nests the user attributes inside one
  attribute named `attrs`. The restored array gains the keys `name`,
  `zarr_chunks`, `zarr_compressor` and `zarr_fill_value`.
- The attributes of the time coordinate. The restored coordinate does not keep
  its own attributes, for example `axis` and `standard_name`.

A source array with `units='K'` and `long_name='test data'` gives these lines:

```text
array temp: attrs[attrs] <missing> != {'units': 'K', 'long_name': 'test data'}
array temp: attrs[long_name] 'test data' != <missing>
array temp: attrs[units] 'K' != <missing>
```

A time coordinate also changes its dtype:

```text
array data/timestamp: dtype datetime64[ns] != float64
```

A Tensogram archive that records no product label needs `--product-name <label>`
on `archive restore`.

Do not use `zarr compare` to gate a Tensogram restore. To check the archive
itself, run `firecube archive validate`.

## Failure Recovery

| Symptom | Meaning | Recovery |
| --- | --- | --- |
| `Missing Zarr store metadata` | One input does not point at a Zarr store root. | Check the URI and rerun with the store directory, not a parent directory or array path. |
| `shape`, `dtype`, `chunks`, or `dimension_names` mismatch | The stores use different array schemas. | Compare the ingestion configuration and plugin schema before copying or promoting either store. |
| `attrs[<key>] <left> != <right>` | The user-visible attribute `<key>` differs after Firecube-managed attrs were removed. | Inspect the array metadata and decide which store owns the intended metadata. |
| `values differ` | Array payloads differ. | Re-run the source verification or re-ingest the affected product before promotion. |
| `firecube_static_written` mismatch | A static array has different write-once marker state. | Treat this as a resume-safety difference. Recreate or repair the affected store instead of ignoring the marker. |

## Operational Notes

The check is strict equivalence: no tolerances, no repair, no JSON output. Use
it where a mismatch must stop the workflow, not as a diff tool.

## See Also

- [Run Parallel Zarr Writes](parallel-zarr-writes.md)
- [Inspect And Manage The Resolved Index](firecube-index.md)
