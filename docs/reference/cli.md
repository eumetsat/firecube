# CLI Reference

This page is generated from the live Click command tree. Use
`firecube <command> --help` for the same information in your terminal.

::: mkdocs-click
    :module: firecube.cli.main
    :command: cli
    :prog_name: firecube
    :depth: 1
    :style: table

---

## Bulk Sweep: `--all-stale`

Both `firecube chunks claims clear` and `firecube chunks runs abandon` support
`--all-stale` for sweeping every stale entry for a product in one command. Use
this after a cluster crash or pod eviction leaves multiple stuck runs or claims
behind.

### Safety gate: `--yes-i-really-mean-it`

Both commands require explicit confirmation before mutating anything. In an
interactive terminal you get a prompt. In a non-TTY context (CI, scripts,
cron) the command exits with an error unless you pass
`--yes-i-really-mean-it`. This prevents accidental bulk mutations from
copy-pasted commands or automation that runs without human review.

`--dry-run` always works without the confirmation flag. Use it first to see
exactly what would change.

---

### `chunks claims clear --all-stale`

Clears every stale write-coordination claim for a product. A claim is
considered stale when the writer that created it is no longer active.

`--all-stale` and `--domain` are mutually exclusive. Pass one or the other,
not both.

**Step 1: preview the sweep**

```bash
firecube chunks claims clear \
  --product-name MY_PRODUCT \
  --all-stale \
  --dry-run
```

Expected output:

```text
[dry-run] Would clear 3 stale claim(s) for MY_PRODUCT
```

**Step 2: commit the sweep**

```bash
firecube chunks claims clear \
  --product-name MY_PRODUCT \
  --all-stale \
  --yes-i-really-mean-it
```

Expected output:

```text
Cleared 3 stale claim(s) for MY_PRODUCT
```

**Verify:**

```bash
firecube chunks claims list --product-name MY_PRODUCT
```

Expected output when all claims are gone:

```text
No claims found.
```

---

### `chunks runs abandon --all-stale`

Marks every non-terminal run for a product as abandoned. A run stuck in
`started` state blocks future ingestion for that product.

`--all-stale` and `--run-id` are mutually exclusive. Pass one or the other,
not both.

`--reason` is required even in bulk mode. It records why the runs were
abandoned and appears in the run history.

**Step 1: preview the sweep**

```bash
firecube chunks runs abandon \
  --product-name MY_PRODUCT \
  --all-stale \
  --reason "cluster-crash" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon 2 stale run(s) for MY_PRODUCT (reason: cluster-crash)
```

**Step 2: commit the sweep**

```bash
firecube chunks runs abandon \
  --product-name MY_PRODUCT \
  --all-stale \
  --reason "cluster-crash" \
  --yes-i-really-mean-it
```

Expected output:

```text
Abandoned 2 stale run(s) for MY_PRODUCT
```

**Verify:**

```bash
firecube chunks runs list --product-name MY_PRODUCT --status started
```

Expected output when no stuck runs remain:

```text
No runs found.
```

---

### Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `--all-stale and --domain are mutually exclusive` | Both flags passed to `claims clear` | Remove `--domain` when using `--all-stale` |
| `--all-stale and --run-id are mutually exclusive` | Both flags passed to `runs abandon` | Remove `--run-id` when using `--all-stale` |
| `--reason is required` | `runs abandon --all-stale` called without `--reason` | Add `--reason "<description>"` |
| Command exits without mutating in non-TTY | `--yes-i-really-mean-it` not passed | Add `--yes-i-really-mean-it` or run `--dry-run` first |
| No stale entries found | All claims/runs are already terminal or active | Run `firecube chunks claims list` or `firecube chunks runs list` to inspect current state |
