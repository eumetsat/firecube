# Route Writes To Multiple Groups

## Goal

Route one batch's items to more than one output group in the same Zarr
store.

## Declare The Groups

[`get_batch_groups`](../../reference/hooks.md#firecube.ingestor.api.BaseIngestor.get_batch_groups)
is a `BaseIngestor` hook. Override it to name more than the single default
group:

```python
# Stable, sorted list -> these become the zarr group paths in the store
def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
    return ["quality", "sst"]
```

Firecube calls the per-group write hook once for each name returned here,
passing the full batch item list every time, so the plugin selects what
belongs to the group it was called for. The list must be stable and sorted
so the group set stays consistent across runs; each name becomes its own
group path in the Zarr store.

## Route Writes Per Template

Declaring the groups is the same everywhere; writing to them is
template-specific:

- **`GenericZarrIngestor`**: branch on `group` inside `build_dataset` — see
  [Implement GenericZarrIngestor](generic-zarr.md).
- **`DirectZarrIngestor`**: declare a matching `ZarrGroupSpec` per group in
  `zarr_schema`, then tag each `WriteIntent` with the `group` it targets —
  see [Implement DirectZarrIngestor](direct-zarr.md).

## Verify

Confirm the override returns the declared groups, without needing a
template or a full ingest run:

```bash
uv run python -c "
from firecube_my_plugin.ingestor import MyPlugin
print(MyPlugin.get_batch_groups(None, [], None))
"
```

Expected output:

```text
['quality', 'sst']
```

This confirms the override itself. Confirming that Firecube actually writes
each group to the store needs the chosen template's own `Verify` section,
once its writes are wired up.

## Next Steps

- **[Implement GenericZarrIngestor](generic-zarr.md)** — the template that branches on `group` in `build_dataset`
- **[Implement DirectZarrIngestor](direct-zarr.md)** — the template that tags each `WriteIntent` with `group`
- **[Plugin Templates](../../reference/templates.md)** — look up `zarr_schema` and `WriteIntent` fields
