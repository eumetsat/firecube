# Metadata Migration

This reference covers the types that a plugin imports from
`firecube.sdk` to declare a metadata migration step. A step reads a
`StoreView` and returns a `MetadataPlan`. To write a step, follow
[Write A Migration Step](../guides/plugins/write-a-migration-step.md).

{{ render_api_summary("firecube.sdk", [
    "MigrationStep",
    "StoreView",
    "AttributePatch",
    "MetadataPlan",
    "ReleaseRecord",
    "NodeView",
    "ABSENT",
]) }}

## Step Contract

::: firecube.sdk.MigrationStep

## Plan Types

::: firecube.sdk.AttributePatch

::: firecube.sdk.MetadataPlan

## Store View

::: firecube.sdk.StoreView

::: firecube.sdk.NodeView

::: firecube.sdk.ABSENT

::: firecube.sdk.ReleaseRecord

## See Also

- [Write A Migration Step](../guides/plugins/write-a-migration-step.md)
- [Migrate Metadata](../operations/maintenance/migrate.md): run a step
- [Control-Plane Spec](control-plane-spec.md): the journal that a migration run
  writes
