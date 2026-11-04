# Metadata Migration

This reference covers the types that a plugin imports from
`firecube.ingestor.api` to declare a metadata migration step. A step reads a
`StoreView` and returns a `MetadataPlan`. To write a step, follow
[Write A Migration Step](../guides/plugins/write-a-migration-step.md).

{{ render_api_summary("firecube.ingestor.api", [
    "MigrationStep",
    "StoreView",
    "AttributePatch",
    "MetadataPlan",
    "ReleaseRecord",
    "NodeView",
    "ABSENT",
]) }}

## Step Contract

::: firecube.ingestor.api.MigrationStep

## Plan Types

::: firecube.ingestor.api.AttributePatch

::: firecube.ingestor.api.MetadataPlan

## Store View

::: firecube.ingestor.api.StoreView

::: firecube.ingestor.api.NodeView

::: firecube.ingestor.api.ABSENT

::: firecube.ingestor.api.ReleaseRecord

## See Also

- [Write A Migration Step](../guides/plugins/write-a-migration-step.md)
- [Migrate Metadata](../operations/maintenance/migrate.md): run a step
- [Control-Plane Spec](control-plane-spec.md): the journal that a migration run
  writes
