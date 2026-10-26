# Recover Runs And Claims

Use this page when ingestion failed, a pod was killed, or a writer left a
blocking claim behind.

## Set The Product

Set the full product URI and the logical product name used during ingestion:

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

Pass the full URI through `--product-name` for every ChunkManager command on
this page. This binds the command directly to the product without a storage
configuration.

## Inspect Runs

Start by listing runs:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
```

A stuck run usually appears as `started`:

```text
Run ID                               Status     State    Parts Events
--------------------------------------------------------------------------------
docs-started-run                     started    active   1     1
```

Confirm the original process is no longer active before abandoning the run.

If the run is already `failed`, go to
[Recover From A Failed Batch](#recover-from-a-failed-batch).

## Abandon A Stuck Run

Preview the operation:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_URI" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon run 'docs-started-run' for product 'MY_PRODUCT.zarr' (reason: process is no longer active)
```

Abandon the run in a non-interactive shell:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_URI" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --yes-i-really-mean-it
```

Expected output:

```text
Abandoned run docs-started-run for MY_PRODUCT.zarr
Released claim MY_PRODUCT:zarr_region:F001
Released claim MY_PRODUCT:zarr_region:F002
```

The abandon also releases the write claims that only this run owns. Each
`Released claim` line names the domain of one released claim.

The abandon keeps a claim and prints a `Left claim` line in two cases. More
than one run can own the claim, or another writer replaced the claim during
the release:

```text
Left claim MY_PRODUCT:zarr_region:F001
```

Do not clear a left claim blindly. List the claims to see the current owner.
Confirm that its writer is stopped, then clear the claim. See
[Clear A Blocking Claim](#clear-a-blocking-claim).

If the run is already terminal, the command keeps its status. It still
releases the claims that the run owns.

Verify:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
```

Expected output:

```text
Run ID                               Status     State    Parts Events
--------------------------------------------------------------------------------
docs-started-run                     abandoned  active   2     2
```

### If The Claims Cannot Be Listed

If Firecube cannot list the claims of the product, it still records the
abandon. The command then exits 1 and releases no claim:

```text
Error: The write claims of MY_PRODUCT.zarr could not be listed: [Errno 13] Permission denied: '/data/products/MY_PRODUCT.zarr/.firecube/claims'.
Run docs-started-run is recorded abandoned; run the abandon again to release its claims:
  firecube chunks runs abandon --product-name file:///data/products/MY_PRODUCT.zarr --run-id docs-started-run --reason "<reason>"
```

Fix the cause of the listing error. Then run the same abandon command again:

```text
Run docs-started-run already terminal with status=abandoned
Released claim MY_PRODUCT:zarr_region:F001
Released claim MY_PRODUCT:zarr_region:F002
```

## Inspect Claims

List claims before clearing anything:

```bash
firecube chunks claims list --product-name "$PRODUCT_URI"
```

Expected output when a claim exists:

```text
Product                  State    Owner                    Domain
--------------------------------------------------------------------------------------------
MY_PRODUCT.zarr          active   docs-started-run:F001    MY_PRODUCT:zarr_region:F001
```

The `Domain` value is the exact value to pass to `--domain`.

## Clear A Blocking Claim

Use `chunks claims clear` for a claim that no single run owns. The abandon
leaves such a claim in place and prints it as `Left claim`. A successor can
also own a left claim now. Verify that the current owner is stopped before
you clear it.

Clear a stale claim in non-interactive context:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_URI" \
  --domain MY_PRODUCT:zarr_region:F001 \
  --yes-i-really-mean-it
```

Use `--force` only after verifying no writer is active and the claim must be
cleared even though it does not look stale:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_URI" \
  --domain MY_PRODUCT:zarr_region:F001 \
  --force \
  --yes-i-really-mean-it
```

Expected output:

```text
Cleared claim for MY_PRODUCT:zarr_region:F001
```

Verify:

```bash
firecube chunks claims list --product-name "$PRODUCT_URI"
```

Expected output:

```text
No claims found.
```

## Resume Ingestion

After you abandon the stuck run and clear every claim that remains, rerun
ingestion with `--option resume_existing=true`. This keeps the batches that
already succeeded:

```bash
firecube ingest <plugin> \
  --input-data /data/source \
  --target "$PRODUCT_URI" \
  --product-name "$PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option resume_existing=true
```

## Recover From A Failed Batch

A run recorded as `failed` has already stopped and does not need to be
abandoned. Read the ingestion error and fix its cause before retrying.

For Zarr append ingestion with `--write-mode direct`, rerun the command in
[Resume Ingestion](#resume-ingestion), keeping the original plugin settings and
supplying all inputs needed to finish the run. With
`--option resume_existing=true`, successfully written timestamps are kept,
slots marked as failed are refilled, and new timestamps are appended.

Use `--option force_reingest=true` instead when previously written timestamps
also need replacement. Supply the corrected inputs for those timestamps;
Firecube overwrites them in place.

If a batch failed with `--write-mode staged`, rerun the original staged
command with `--option force_reingest=true` and all required inputs. The failed
run did not publish its staged data, so every batch must be written again.

After the retry, verify the new run:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
```

The new run should show `complete`. The earlier run remains `failed` in the
history.

Validate each affected Zarr group, replacing `data` with its group path:

```bash
firecube zarr validate \
  --product "$PRODUCT_URI" \
  --group data
```

Check that the JSON report has `is_valid: true` and the command exits 0. If it
exits 1, resolve the reported validation issues before using the product.

Check that `arrays_checked` lists the arrays of the group. A report that
checked no array is not valid: the command exits 1 and `info_notes` holds this
note:

```text
no array of data was checked: no time dimension found; pass --time-dim to name it
```

If you see the note, pass the time dimension of the plugin:

```bash
firecube zarr validate \
  --product "$PRODUCT_URI" \
  --group data \
  --time-dim time
```

Without `--time-dim`, the command finds the time dimension from the
`firecube_timestamp_state` array. A DirectZarr cube has no such array, so the
command then uses the marker of its time coordinate.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `started` run blocks resume | Firecube cannot prove the old process is dead. | Verify the process is gone, then use `chunks runs abandon`. It also releases the claims that only this run owns. |
| `chunks runs abandon` exits 1 with "could not be listed" | Firecube recorded the abandon but could not list the claims. | Fix the listing error, then run the same abandon again. |
| `Left claim` line after an abandon | More than one run can own the claim, or another writer replaced it. | Inspect the current owner with `chunks claims list`. Verify that writer is stopped, then use `chunks claims clear`. |
| Claim remains after crash | No abandon covers the claim. | Verify no writer is active, then use `chunks claims clear`. |
| Claim does not look stale | The heartbeat timestamp is still recent. | Use `--force` only if the writer is gone. |
| Run ends with `status=failed` and "later batch(es) were not attempted" | Append ingestion stopped before processing all inputs. | Fix the reported error, then [recover the failed batch](#recover-from-a-failed-batch). |
| Rerun refused: "spans from a failed run exist" | The retry needs an explicit choice about keeping earlier work. | Follow [Recover From A Failed Batch](#recover-from-a-failed-batch) for the original write mode. |
| Append refuses an insertion | An incoming timestamp is absent from the target but earlier than its latest timestamp. | Rebuild a new target from all required inputs in chronological order. Resume, force-reingest, and span deletion cannot enable insertion into the existing time coordinate. |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — confirm current runs and claims
- **[Delete And Reingest](delete.md)** — remove a written span or range during recovery
