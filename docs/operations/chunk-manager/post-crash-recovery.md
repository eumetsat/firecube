# Post-Crash Recovery

Use this page when a cluster crash, pod eviction, or SIGKILL left multiple
ingestion runs stuck and multiple write claims blocking a product. The bulk
sweep flags let you clear all stale state in one pass instead of abandoning
runs and clearing claims one at a time.

!!! warning "Live pods are never touched"
    `--all-stale` only clears entries that are **currently stale** (age >=
    the configured staleness threshold). Any run or claim whose heartbeat is
    still fresh is left untouched. Confirm all writers for the product are
    stopped before running the sweep.

```bash
PRODUCT_NAME="MY_PRODUCT"
```

## Prerequisites

- Firecube CLI installed and on `PATH` (or prefix commands with `uv run`).
- All ingestion processes for the product are stopped. Verify with your
  scheduler or `ps`/`kubectl` before proceeding.
- Storage credentials configured if the product is on S3.

## Step 1: Detect Stuck State

List runs that are still in a non-terminal state (`started`):

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_NAME" \
  --status started
```

Expected output when runs are stuck:

```text
Run ID                Status   State   Parts Events
----------------------------------------------------
run-20260710-abc123   started  active  4     4
run-20260710-def456   started  active  2     2
```

List all active write claims for the product:

```bash
firecube chunks claims list --product-name "$PRODUCT_NAME"
```

Expected output when claims are blocking:

```text
Product       State   Owner                        Domain
---------------------------------------------------------------------------
product.zarr  active  run-20260710-abc123:F001      product.zarr:zarr_region:F001
product.zarr  active  run-20260710-abc123:F002      product.zarr:zarr_region:F002
product.zarr  active  run-20260710-def456:F003      product.zarr:zarr_region:F003
```

If both lists are empty, the product is already clean. Skip to
[Resume Ingestion](#resume-ingestion).

## Step 2: Preview the Claim Sweep

Run a dry-run to see which claims `--all-stale` would clear:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_NAME" \
  --all-stale \
  --dry-run
```

Expected output:

```text
[dry-run] Would clear claim product.zarr:zarr_region:F001
[dry-run] Would clear claim product.zarr:zarr_region:F002
[dry-run] Would clear claim product.zarr:zarr_region:F003
```

If the dry-run output matches the claims you saw in Step 1, proceed to Step 4.
If some claims are missing from the dry-run output, those claims are not yet
stale. Wait for the staleness threshold to pass, or use `--domain` with
`--force` for individual claims after confirming the writer is gone.

## Step 3: Preview the Run Sweep

Run a dry-run to see which runs `--all-stale` would abandon:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_NAME" \
  --all-stale \
  --reason "cluster-crash" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon run 'run-20260710-abc123' for MY_PRODUCT (reason: cluster-crash)
[dry-run] Would abandon run 'run-20260710-def456' for MY_PRODUCT (reason: cluster-crash)
```

## Step 4: Commit the Sweep

Clear all stale claims first, then abandon all stale runs. Order matters:
clearing claims before abandoning runs avoids a window where a run is
abandoned but its claims still block the next ingest.

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_NAME" \
  --all-stale \
  --yes-i-really-mean-it

firecube chunks runs abandon \
  --product-name "$PRODUCT_NAME" \
  --all-stale \
  --reason "cluster-crash" \
  --yes-i-really-mean-it
```

Verify the product is clean:

```bash
firecube chunks runs list --product-name "$PRODUCT_NAME" --status started
firecube chunks claims list --product-name "$PRODUCT_NAME"
```

Both commands should return empty tables.

## Step 5: Rebuild the Snapshot

After a bulk sweep, rebuild the chunk snapshot to restore fast query
performance:

```bash
firecube chunks snapshots rebuild --product-name "$PRODUCT_NAME"
```

Expected output:

```text
Rebuilt snapshot for MY_PRODUCT: N chunks indexed.
```

Preview first if you want to confirm what the rebuild will touch:

```bash
firecube chunks snapshots rebuild --product-name "$PRODUCT_NAME" --dry-run
```

## Flag Reference

| Flag | Command | Required | Purpose |
|---|---|---|---|
| `--all-stale` | `claims clear` | Yes (or `--domain`) | Clear every stale claim for the product in one pass. |
| `--all-stale` | `runs abandon` | Yes (or `--run-id`) | Abandon every stale non-terminal run in one pass. |
| `--dry-run` | both | No | Preview changes without applying them. |
| `--yes-i-really-mean-it` | both | Yes (non-TTY) | Confirm destructive operation. Required in CI and scripts. |
| `--reason` | `runs abandon` | Yes | Operator note recorded with the abandoned run. |

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| Dry-run shows fewer claims than `claims list` | Some claims are not yet stale. | Wait for the threshold, or use `--domain` + `--force` per claim after confirming the writer is gone. |
| Dry-run shows fewer runs than `runs list --status started` | Some runs are not yet stale. | Wait, or use `--run-id` per run. |
| `claims list` still shows entries after the sweep | A new writer started between steps. | Stop the writer and repeat the sweep. |
| Snapshot rebuild fails | The event log is corrupt or missing. | See [Snapshots](snapshots.md). |

## Resume Ingestion

Once the product is clean, rerun ingestion with resume enabled:

```bash
firecube ingest <plugin> \
  --source /data/source \
  --target "file:///data/products/$PRODUCT_NAME.zarr" \
  --product-name "$PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option resume_existing=true
```

## Next Steps

- **[Recover Runs And Claims](recover.md)** — single-run and single-claim recovery
- **[Inspect ChunkManager State](inspect.md)** — confirm state before and after recovery
- **[Snapshots](snapshots.md)** — rebuild snapshots
