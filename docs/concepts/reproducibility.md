# Reproducibility

What Firecube guarantees when the same ingestion runs twice, and how to answer
the two questions that follow from it: do these stores contain the same values,
and should they also be byte-identical.

## Question 1: Identical Values

This tier always holds. Given the same input catalog, the same plugin at a
pinned version, and the same schema at preallocation time, two ingestions
produce value-identical and schema-identical stores, including NaN and NaT
positions, across machines, worker counts, and slot-range splits: each item's
slot position is resolved from the declared index, not from arrival or
completion order.

Verify it between any two stores of the same product:

```bash
firecube zarr compare "$PRODUCT_URI" "$RERUN_URI" \
  --storage-type local \
  --storage-driver fsspec
echo "compare exit: $?"
```

Expected output when the stores are equivalent:

```text
compare exit: 0
```

A store that differs prints one line per mismatched array and exits `1`, so
the command can gate a promotion or migration step directly. See
[Compare Zarr Stores](../operations/zarr-compare.md) for the full contract.

## Question 2: Identical Bytes

Byte-level identity needs more than Question 1: a pinned environment with the
same zarr-python version, the same codec pipeline, and the same
`zarr_write_empty_chunks` setting.

Outside those preconditions, chunk files can differ in count and content while every value is identical; toggling `zarr_write_empty_chunks` alone changed a measured product's chunk-file count by roughly 21 percent with no value change.

TL,DR: Compare values, not bytes, unless you control all three preconditions.

## Next Steps

- **[Compare Zarr Stores](../operations/zarr-compare.md)**: the verification
  command's full contract.
- **[Run Parallel Zarr Writes](../operations/parallel-zarr-writes.md)**: plan
  and resume slot-range ingestion with `zarr slots`.
- **[Control-Plane Spec](../reference/control-plane-spec.md)**: the record
  formats behind these answers.
