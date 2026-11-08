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
  --target file:///data/products/my_product.zarr
```

Output includes the schema version, the timestamp the record was written, the
run that wrote it, the identity hash, and a table of groups with their axis
kind and size.

Add `-f json` to receive the raw record as JSON, suitable for scripting:

```bash
firecube zarr index show \
  --target file:///data/products/my_product.zarr \
  -f json
```

The command reads the product label from the store. If the store records no
label, the command exits 2 and names `firecube migrate adopt`.

**Required flags:**

| Flag | Description |
|---|---|
| `-t/--target` | Product Zarr URI (`file://` or `s3://`). |

**Optional flags:**

| Flag | Description |
|---|---|
| `-f/--format` | `table` (default) prints the human-readable summary. `json` prints the raw record. |
| `--derived` | For `regular_time` groups, compute and print the derived coordinate values (timestamps) from the stored epoch, cadence, and size. Emits a note to stderr for `irregular_time` and `integer` groups. Does not write any files. |

Add `--derived` to see the full list of timestamps for a `RegularTimeAxis` group:

```bash
firecube zarr index show \
  --target file:///data/products/my_product.zarr \
  --derived
```

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record found and printed. |
| `1` | No index record found, or a storage or manifest error. |
| `2` | The store records no product label. |

### `firecube zarr index verify`

Check that the current resolved-index record can be read and is not a legacy
format that requires migration.

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr
```

Pass `--plugin` to name the plugin that owns the product. The command uses the
plugin name to check for legacy slot-index records that predate the current
`IndexSpec` format.

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
  --plugin my_plugin
```

If the plugin needs an option to declare its index, pass the option with
`--option KEY=VALUE`. The command configures the plugin before it reads the
index spec. Without the option, a plugin such as `mtg_fci_l1c` refuses:

```text
Error: Plugin 'mtg_fci_l1c' failed to resolve index_spec: product_type is required. Pass --option product_type=FDHSI or --option product_type=HRFI.
```

Pass each option once:

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
  --plugin mtg_fci_l1c \
  --option product_type=FDHSI \
  --option time_epoch=2024-01-01 \
  --option time_slots=3
```

Expected output resembles:

```text
VERIFIED resolved index 7998667a5eba8d09
```

**Required flags:**

| Flag | Description |
|---|---|
| `-t/--target` | Product Zarr URI (`file://` or `s3://`). |

**Optional flags:**

| Flag | Description |
|---|---|
| `--plugin` | Plugin name for legacy-record detection. |
| `--option KEY=VALUE` | Plugin option. Repeat the flag for each option. Requires `--plugin`. |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record verified. |
| `1` | No index record found, the record is a legacy format or cannot be read, or the plugin cannot resolve its spec. |
| `2` | The command received `--option` without `--plugin`, or the store records no product label. |

### `firecube zarr index rebuild`

Rebuild the resolved-index record from a plugin's `index_spec()` declaration.
Use this command when the record is missing, corrupted, or was written by an
older Firecube version that used a different format. If the root attrs of the
product still hold the resolved-index hash, the command refuses. See
[Recover From Attrs Drift](#recover-from-attrs-drift).

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

If the plugin needs an option to declare its index, pass it with
`--option KEY=VALUE`. Repeat the flag for each option:

```bash
firecube zarr index rebuild \
  --target file:///data/products/my_product.zarr \
  --plugin mtg_fci_l1c \
  --product-name my_product \
  --option product_type=FDHSI \
  --option time_epoch=2024-01-01 \
  --option time_slots=3
```

Expected output when the record already matches:

```text
Index record unchanged: identity_hash=7998667a5eba8d09
```

Add `--dry-run` to see the outcome without a write. For a matching record the
output resembles:

```text
Dry run: would keep the index record, which matches (identity_hash=81b87699c4d6c5e1) and record one rebuild run; nothing is written
```

**Required flags:**

| Flag | Description |
|---|---|
| `-t/--target` | Product Zarr URI (`file://` or `s3://`). |
| `--plugin` | Plugin name used to resolve the index spec. |
| `--product-name` | Logical product name. The store refuses a name that differs from its recorded label. |

**Optional flags:**

| Flag | Description |
|---|---|
| `--option KEY=VALUE` | Plugin option. Repeat the flag for each option. |
| `--dry-run` | Report what the command would do and write nothing. |

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Record rebuilt or unchanged. |
| `1` | Plugin failed to resolve the spec, or the existing record conflicts. |
| `2` | `--product-name` differs from the label that the store records. |

## When To Use Each Command

| Situation | Command |
|---|---|
| Check what index a product was written with | `show` |
| Confirm a product is ready for a new ingest run | `verify` |
| Recover a missing or corrupted index record, with no root attrs hash | `rebuild` |
| Recover a state that `verify` reports as attrs drift | [Recover From Attrs Drift](#recover-from-attrs-drift) |
| Migrate a cube that has only a legacy slot-index record | `rebuild` |

## Recover From Attrs Drift

`verify` exits with code 1 and reports attrs drift when the root attrs of the
product disagree with the record. `rebuild` refuses both states below. Each
state has its own recovery.

1. If the root attr holds the hash and `.firecube/index/current.json` is
   absent, restore the file from a backup. Or remove the root attrs
   `firecube_resolved_index_identity_hash` and `firecube_resolved_index`,
   then run `firecube zarr index rebuild` with the plugin that wrote the data.
2. If the record exists and the root attr
   `firecube_resolved_index_identity_hash` differs from it, remove that attr.
   Then run `firecube zarr index rebuild`. The command writes the attr again
   from the record.
3. Run `firecube zarr index verify` to confirm the result.

If `firecube zarr slots` warns that the plugin resolved index differs from the
record, use the plugin that wrote the product, or a new target.
`rebuild` does not change the record in that case.

## Migration From v0.1.4.post1

Cubes written by Firecube v0.1.4.post1 and earlier used a legacy slot-index
format. Run `firecube zarr index verify` to detect the legacy format, then
`firecube zarr index rebuild` to write the current record:

```bash
firecube zarr index verify \
  --target file:///data/products/my_product.zarr \
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
- **[Declare The Schema And Index](../guides/plugins/direct-zarr.md)** - declare an `IndexSpec` in a plugin
- **[ChunkManager Operations](chunk-manager/index.md)** - inspect and recover the broader control plane
