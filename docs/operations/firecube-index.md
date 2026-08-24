# Inspect And Manage The Resolved Index

The `firecube zarr index` commands let you inspect, verify, and rebuild the
resolved-index control-plane record that Firecube writes to
`.firecube/index/current.json` after the first successful ingestion run.

The record stores the `identity_hash` of the resolved `IndexSpec`. Subsequent
runs read it back and refuse to write if the declared spec produces a different
hash, protecting the cube from silent schema drift.

## Commands

### `firecube zarr index show`

Print the current resolved-index record for a product.

```bash
firecube zarr index show \
  --target file:///data/products/my_product.zarr \
  --product-name my_product
```

Output includes the schema version, the timestamp the record was written, the
run that wrote it, the identity hash, and a table of groups with their axis
kind and size.

Add `--json` to receive the raw record as JSON, suitable for scripting:

```bash
firecube zarr index show \
  --target file:///data/products/my_product.zarr \
  --product-name my_product \
  --json
```

**Required flags:**

| Flag | Description |
|---|---|
| `--target` | Product Zarr URI (`file://` or `s3://`). |
| `--product-name` | Logical product name. |

**Optional flags:**

| Flag | Description |
|---|---|
| `--json` | Emit the raw record as JSON instead of the human-readable summary. |
| `--derived` | For `regular_time` groups, compute and print the derived coordinate values (timestamps) from the stored epoch, cadence, and size. Emits a note to stderr for `irregular_time` and `integer` groups. Does not write any files. |

Add `--derived` to see the full list of timestamps for a `RegularTimeAxis` group:

```bash
firecube zarr index show \
  --target file:///data/products/my_product.zarr \
  --product-name my_product \
  --derived
```

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record found and printed. |
| `1` | Storage or manifest error. |
| `3` | No index record found. |

### `firecube zarr index verify`

Check that the current resolved-index record can be read and is not a legacy
format that requires migration.

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
  --product-name my_product
```

Pass `--plugin` to name the plugin that owns the product. The command uses the
plugin name to check for legacy slot-index records that predate the current
`IndexSpec` format.

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
  --product-name my_product \
  --plugin my_plugin
```

**Required flags:**

| Flag | Description |
|---|---|
| `--target` | Product Zarr URI (`file://` or `s3://`). |
| `--product-name` | Logical product name. |

**Optional flags:**

| Flag | Description |
|---|---|
| `--plugin` | Plugin name for legacy-record detection. |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record verified. |
| `1` | Record is a legacy format or cannot be read. |
| `3` | No index record found. |

### `firecube zarr index rebuild`

Rebuild the resolved-index record from a plugin's `index_spec()` declaration.
Use this command when the record is missing, corrupted, or was written by an
older Firecube version that used a different format.

```bash
firecube zarr index rebuild \
  --target file:///data/products/my_product.zarr \
  --plugin my_plugin \
  --product-name my_product
```

The command loads the named plugin, calls `index_spec()`, resolves the spec,
and writes the resulting record to `.firecube/index/current.json`. If a record
already exists with the same identity hash, the command reports "unchanged" and
exits cleanly. If the existing record has a different hash, the command exits
with code `1` and prints the conflict.

**Required flags:**

| Flag | Description |
|---|---|
| `--target` | Product Zarr URI (`file://` or `s3://`). |
| `--plugin` | Plugin name used to resolve the index spec. |
| `--product-name` | Logical product name. |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record rebuilt or unchanged. |
| `1` | Plugin failed to resolve the spec, or the existing record conflicts. |

## When To Use Each Command

| Situation | Command |
|---|---|
| Check what index a product was written with | `show` |
| Confirm a product is ready for a new ingest run | `verify` |
| Recover a missing or corrupted index record | `rebuild` |
| Migrate a cube that has only a legacy slot-index record | `rebuild` |

## Migration From v0.1.4.post1

Cubes written by Firecube v0.1.4.post1 and earlier used a legacy slot-index
format. Run `firecube zarr index verify` to detect the legacy format, then
`firecube zarr index rebuild` to write the current record:

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
  --product-name my_product \
  --plugin my_plugin

firecube zarr index rebuild \
  --target file:///data/products/my_product.zarr \
  --plugin my_plugin \
  --product-name my_product
```

After rebuilding, run `firecube zarr index show` to confirm the record is present
and the identity hash matches the plugin's current `index_spec()` declaration.

## See Also

- **[Index Specification Reference](../reference/parallelism.md)** - `IndexSpec`, `IntegerAxis`, `IrregularTimeAxis`, `RegularTimeAxis`, and `ResolvedIndexRecord` types
- **[Implement DirectZarrIngestor](../guides/plugins/direct-zarr.md)** - declare an `IndexSpec` in a plugin
- **[IrregularTimeAxis Plugin Guide](../guides/plugins/irregular-axis.md)** - declare and use `IrregularTimeAxis` with `AUTO`
- **[ChunkManager Operations](chunk-manager/index.md)** - inspect and recover the broader control plane
