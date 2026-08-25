# Compare Zarr Stores

## Purpose

Check whether two Zarr stores are operationally equivalent after migration,
re-ingestion, staging promotion, or a storage-driver change. The command is
read-only and boolean: it either confirms equivalence or lists mismatches and
exits nonzero, so it can gate a promotion step in a pipeline.

## Prerequisites

- Both inputs are complete Zarr store URIs (`file://` or `s3://`).
- One storage type and one storage driver apply to both stores:
  `--storage-type local` for `file://` stores, `--storage-type s3` for
  `s3://` stores, and `--storage-driver fsspec` or `--storage-driver obstore`.
- For `s3://` stores, credentials are configured as in
  [Configure S3 Access](s3-access.md).

## Procedure

1. Run the comparison with both required flags:

   ```bash
   uv run firecube zarr compare \
     file:///data/products/before.zarr \
     file:///data/products/after.zarr \
     --storage-type local \
     --storage-driver fsspec
   ```

2. Check the result. No output and exit status `0` mean the stores matched.
   On mismatch, the command writes one line per difference to stderr and
   exits with status `3`:

   ```text
   array data/values: shape (2, 2) != (3, 2)
   array data/lat: firecube_static_written True != None
   ```

The comparison covers array paths, shape, dtype, chunks, dimension names,
public attrs, Firecube's static-array marker, and values. Runtime trace attrs
are ignored. Any other nonzero exit status means the command could not run,
for example because a URI is missing, a store has no Zarr metadata, or the
storage flags do not match the URI scheme.

## Failure Recovery

| Symptom | Meaning | Recovery |
| --- | --- | --- |
| `Missing Zarr store metadata` | One input does not point at a Zarr store root. | Check the URI and rerun with the store directory, not a parent directory or array path. |
| `shape`, `dtype`, `chunks`, or `dimension_names` mismatch | The stores use different array schemas. | Compare the ingestion configuration and plugin schema before copying or promoting either store. |
| `attrs differ` | User-visible array attrs differ after Firecube-managed attrs were removed. | Inspect the array metadata and decide which store owns the intended metadata. |
| `values differ` | Array payloads differ. | Re-run the source verification or re-ingest the affected product before promotion. |
| `firecube_static_written` mismatch | A static array has different write-once marker state. | Treat this as a resume-safety difference. Recreate or repair the affected store instead of ignoring the marker. |

## Operational Notes

The check is strict equivalence: no tolerances, no repair, no JSON output. Use
it where a mismatch must stop the workflow, not as a diff tool.

## See Also

- [Run Parallel Zarr Writes](parallel-zarr-writes.md)
- [Inspect And Manage The Resolved Index](firecube-index.md)
