# Post-Crash Recovery

Use this page when a cluster crash, pod eviction, or SIGKILL left multiple
ingestion runs stuck and multiple write claims blocking a product. The bulk
sweep flags let you clear all stale state in one pass instead of abandoning
runs and clearing claims one at a time.

!!! warning "Stop all writers before the sweep"
    Stop every writer for the product before you run the sweep. The sweep
    abandons only runs that are **currently stale**. A run is stale when its
    age is at least the staleness threshold. The committed run sweep also
    releases claims, but only claims that are stale. A claim is stale 120
    seconds after its last heartbeat. The sweep releases a stale claim when its
    owner is a run that the sweep abandons or a run that is already terminal.
    The sweep leaves a fresh claim in place, because a live process can still
    renew it. The dry run previews the claims that the sweep releases and
    leaves.

```bash
PRODUCT_NAME="MY_PRODUCT"
```

Pass the full URI through `--product-name` for every ChunkManager command on
this page. This binds the command directly to the product without a storage
configuration.

## Prerequisites

- Firecube CLI installed and on `PATH` (or prefix commands with `uv run`).
- All ingestion processes for the product are stopped. Verify with your
  scheduler or `ps`/`kubectl` before proceeding.
- Storage credentials configured if the product is on S3.

## Step 1: Detect Stuck State

List runs that are still in a non-terminal state (`started`):

```bash
firecube chunks runs list \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --status started
```

Expected output when runs are stuck:

```text
Run ID                               Status     State    Parts Events
--------------------------------------------------------------------------------
run-20260710-abc123                  started    stale    1     1
run-20260710-def456                  started    stale    1     1
```

List all active write claims for the product:

```bash
firecube chunks claims list --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output when claims are blocking:

```text
Product                  State    Owner                    Domain
--------------------------------------------------------------------------------------------
MY_PRODUCT.zarr          stale    run-20260710-abc123:F001 MY_PRODUCT.zarr:zarr_region:F001
MY_PRODUCT.zarr          stale    run-20260710-abc123:F002 MY_PRODUCT.zarr:zarr_region:F002
MY_PRODUCT.zarr          stale    run-20260710-def456:F003 MY_PRODUCT.zarr:zarr_region:F003
```

If both lists are empty, the product is already clean. Skip to
[Resume Ingestion](#resume-ingestion).

## Step 2: Preview the Run Sweep

Run a dry-run to see which runs `--all-stale` would abandon and which claims
it would release:

```bash
firecube chunks runs abandon \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --all-stale \
  --reason "cluster-crash" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon stale runs for MY_PRODUCT.zarr:
  - run-20260710-abc123
  - run-20260710-def456
  Would release claim MY_PRODUCT.zarr:zarr_region:F001
  Would release claim MY_PRODUCT.zarr:zarr_region:F002
  Would release claim MY_PRODUCT.zarr:zarr_region:F003
```

A `Would release claim` line names a stale claim that the committed sweep
releases. A `Would leave claim` line names a claim that the committed sweep
keeps. The sweep keeps a claim that is fresh or whose owner is ambiguous. The
dry run judges each claim by its age at the time of the listing:

```text
  Would leave claim MY_PRODUCT.zarr:zarr_region:F002
```

If the output matches the runs you saw in Step 1, proceed to Step 3. If a run
is missing, it is not yet stale. Wait for the staleness threshold to pass, or
use `--run-id` for that run after you verify its process is gone. If a claim
shows `Would leave claim`, see [Claims the Sweep Leaves](#claims-the-sweep-leaves).

## Step 3: Preview the Claim Sweep

Run a dry-run to see which claims `--all-stale` would clear:

```bash
firecube chunks claims clear \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --all-stale \
  --dry-run
```

Expected output:

```text
Previewed stale claims:
  - MY_PRODUCT.zarr:zarr_region:F001
  - MY_PRODUCT.zarr:zarr_region:F002
  - MY_PRODUCT.zarr:zarr_region:F003
Dry run only; no claims cleared.
Skipped fresh claims: 0
Skipped missing claims: 0
```

If the output matches the claims you saw in Step 1, proceed to Step 4. If a
claim is missing, it is not yet stale. Wait for the staleness threshold to
pass, or use `--domain` with `--force` after you verify the writer is gone.

## Step 4: Commit the Sweep

Abandon the stale runs first, then clear the stale claims that remain. The
run sweep releases a claim only if the claim is stale. The owner of the claim
must be one run that the sweep abandons or one run that is already terminal.
The sweep keeps any claim whose owner changed since it listed the claim.

```bash
firecube chunks runs abandon \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --all-stale \
  --reason "cluster-crash" \
  --yes-i-really-mean-it
```

Expected output:

```text
Abandoned stale runs for MY_PRODUCT.zarr:
  Abandoned:
    - run-20260710-abc123
    - run-20260710-def456
  Released claim MY_PRODUCT.zarr:zarr_region:F001
  Released claim MY_PRODUCT.zarr:zarr_region:F002
  Released claim MY_PRODUCT.zarr:zarr_region:F003
```

A `Left claim` line marks a claim that the sweep kept. Do not clear it
blindly. See [Claims the Sweep Leaves](#claims-the-sweep-leaves).

Clear the remaining stale claims:

```bash
firecube chunks claims clear \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --all-stale \
  --yes-i-really-mean-it
```

Expected output when the run sweep released every claim:

```text
No stale claims found
```

If Firecube cannot list the claims, the run sweep abandons no run. It prints
the listing error and exits 1. Fix the cause and run the sweep again.

Verify the product is clean:

```bash
firecube chunks runs list \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --status started
firecube chunks claims list --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output:

```text
No runs found for MY_PRODUCT.zarr.
No claims found.
```

## Step 5: Rebuild the Compact Cache

After a bulk sweep, rebuild the compact cache to restore fast query
performance:

```bash
firecube chunks compact rebuild --product-name "file:///data/products/MY_PRODUCT.zarr"
```

Expected output:

```text
Rebuilt compact cache for MY_PRODUCT.zarr: generation=<n> records=<n>
```

Preview first if you want to confirm what the rebuild will touch:

```bash
firecube chunks compact rebuild --product-name "file:///data/products/MY_PRODUCT.zarr" --dry-run
```

Expected output:

```text
[dry-run] Would rebuild compact cache for product 'MY_PRODUCT.zarr'
```

## Claims the Sweep Leaves

The sweep prints `Left claim` for a claim that it kept. The dry run prints
`Would leave claim` for the same claim.

```text
  Left claim MY_PRODUCT.zarr:zarr_region:F002
```

The sweep keeps a claim for one of these reasons:

- The claim is fresh. Its last heartbeat is less than 120 seconds old.
- More than one recorded run can own the claim.
- Another writer replaced the claim during the release.

A fresh claim has two possible causes. A process can still run and renew the
claim, by default every 30 seconds. Or the process stopped less than 120
seconds ago.

If the process stopped, wait until the claim is stale. Then run the sweep
again. The claim then has a terminal owner, and the sweep releases it.

If a process still renews the claim, stop the process first. Then release the
claim with one of these commands.

Release every claim of one run, fresh or stale:

```bash
firecube chunks runs abandon \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --run-id <run id> \
  --reason "<text>" \
  --yes-i-really-mean-it
```

Release one claim:

```bash
firecube chunks claims clear \
  --product-name "file:///data/products/MY_PRODUCT.zarr" \
  --domain <domain> \
  --force \
  --yes-i-really-mean-it
```

## Flag Reference

| Flag | Command | Required | Purpose |
|---|---|---|---|
| `--all-stale` | `claims clear` | Yes (or `--domain`) | Clear every stale claim for the product in one pass. |
| `--all-stale` | `runs abandon` | Yes (or `--run-id`) | Abandon every stale non-terminal run in one pass. Release the stale claims that one terminal run alone owns. Keep fresh claims. With `--dry-run`, list the claims that the sweep would release or leave. |
| `--dry-run` | both | No | Preview changes without applying them. |
| `--yes-i-really-mean-it` | both | To mutate | Commit the bulk sweep. Without it, the command automatically uses preview mode. |
| `--reason` | `runs abandon` | Yes | Operator note recorded with the abandoned run. |

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| Dry-run shows fewer claims than `claims list` | Some claims are not yet stale. | Wait for the threshold, or use `--domain` + `--force` per claim after confirming the writer is gone. |
| `Left claim` line in the run sweep output | The claim is fresh, more than one run can own it, or another writer replaced it. | See [Claims the Sweep Leaves](#claims-the-sweep-leaves). Stop its writer before you release the claim. |
| Dry-run shows fewer runs than `runs list --status started` | Some runs are not yet stale. | Wait, or use `--run-id` per run. |
| `claims list` still shows entries after the sweep | A new writer started between steps. | Stop the writer and repeat the sweep. |
| `runs abandon --all-stale` exits 1 with "could not be listed" | Firecube could not list the claims, so it abandoned no run. | Fix the listing error and run the sweep again. |
| Compact cache rebuild fails | The event log is corrupt or missing. | See [Compact Cache](compact.md). |

## Resume Ingestion

Once the product is clean, rerun ingestion with resume enabled:

```bash
firecube ingest <plugin> \
  --input-data file:///data/source \
  --target "file:///data/products/MY_PRODUCT.zarr" \
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
- **[Compact Cache](compact.md)** — rebuild the compact cache
