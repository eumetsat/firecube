# Compact Cache

The compact cache is a derived read model for the `.firecube/` control plane.
It makes inspection and cleanup commands faster. The WAL under `.firecube/runs/`
is the authoritative state.

Use this page to check the compact cache of one product and to rebuild it.

## What The Cache Holds

The cache lives in the `.firecube/compact/` directory of the product:

| File | Content |
|---|---|
| `compact-<generation>.jsonl` | The chunk records, one JSON object per line. |
| `compact-<generation>.meta.json` | The generation, the record count and the creation time. |
| `compact.lock` | The lock that exists while a local rebuild runs. A remote rebuild takes no lock. |
| `LATEST.json` | The pointer to the current generation. |

Each rebuild writes a new generation and moves the pointer to it. A rebuild does
not delete the earlier generations.

## Check The Cache Status

Pass the full URI through `--product-name` to bind the command directly to the
product without a storage configuration.

```bash
firecube chunks compact status --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output when no cache exists:

```text
No compact cache found for MY_PRODUCT.zarr
```

Expected output after a rebuild resembles:

```text
Product:    MY_PRODUCT.zarr
Age:        3m
Cutoff:     2026-06-03T15:54:27.100797+00:00
Generation: 1780502101660654725
Records:    0
```

The status command does not currently report the record count. The JSON output
of the rebuild carries it.

If automation must branch on `exists`, use JSON:

```bash
firecube chunks compact status \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --format json
```

Expected output after a rebuild resembles:

```json
{
  "exists": true,
  "completed_before": "2026-06-03T15:54:27.100797+00:00",
  "age_seconds": 180.0,
  "age_human": "3m",
  "generation": "1780502101660654725",
  "records": 0
}
```

## Rebuild The Cache

Preview the rebuild:

```bash
firecube chunks compact rebuild \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --dry-run
```

Expected output:

```text
[dry-run] Would rebuild compact cache for product 'MY_PRODUCT.zarr'
```

Rebuild the cache:

```bash
firecube chunks compact rebuild --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output:

```text
Rebuilt compact cache for MY_PRODUCT.zarr: generation=1780502101660654725 records=4
```

If you need the path of the new generation, use JSON:

```bash
firecube chunks compact rebuild \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --format json
```

Expected output resembles:

```json
{
  "product": "MY_PRODUCT.zarr",
  "generation": "1780502101660654725",
  "records": 4,
  "snapshot_path": "file:///data/products/MY_PRODUCT.zarr/.firecube/compact/compact-1780502101660654725.jsonl",
  "locked": false,
  "remote": false
}
```

The key `snapshot_path` keeps its name. Its value names the file in `compact/`.

## Upgrade From A Store With The Old Layout

Firecube 0.1.8 and earlier kept the cache in `.firecube/snapshots/` and the
pointer in `.firecube/LATEST.json`. Firecube ignores both locations.

If a store has only the old layout, every read replays the WAL. The result is
correct but slower. Run `firecube chunks compact rebuild` once to create the
new cache.

Firecube does not delete the old files. After the rebuild succeeds, delete
`.firecube/snapshots/` and `.firecube/LATEST.json` to free the space.

## Renamed Command

The command group was `firecube chunks snapshots`. The old name stays as a hidden
alias for one release. It accepts the same `rebuild` and `status` commands and
options. Change your scripts to `firecube chunks compact` now.

The metric names, the summary keys and the keys of `--format json` output keep
their names. They still use the word `snapshot`.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `No compact cache found` | The store has no cache, or only the old layout. | Run `firecube chunks compact rebuild`. |
| `Compact cache rebuild skipped ... another rebuild is already in progress` | The file `compact.lock` exists. | Wait for the other rebuild. If no rebuild runs, delete `.firecube/compact/compact.lock`. |
| Rebuild fails | The event log is corrupt or missing. | See [Post-Crash Recovery](post-crash-recovery.md). |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — check records after rebuild
- **[Delete And Reingest](delete.md)** — clean up after inspection
