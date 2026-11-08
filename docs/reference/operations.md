# Operations

The module `firecube.operations` holds the Python functions that do what the `firecube` commands do on a finished product. The operations are:

- Archive and restore a product.
- Take a metadata snapshot.
- Adopt a store and migrate its metadata.
- Create, list, read and verify releases.
- Rebuild and verify the index record.
- Preallocate arrays and plan slots.
- Build multi-resolution levels.
- Consolidate time coordinates.

Use this module in scripts, pipelines and tools that manage products from Python. A plugin does not need it, because a plugin imports from `firecube.sdk`.

Open a product with `open_product`, used as a context manager. It gives a `Product`. Pass the `Product` as the first argument of an operation.

The module does not re-export exceptions. Most are in `firecube.core.errors`. Some are in the runtime module that raises them, for example `SlotWindowError` in `firecube.ingestor.runtime.preallocate`. `Product.session` and `Product.manager` are internal. Do not use them.

## Opening a Product

::: firecube.operations.open_product

::: firecube.operations.Product

::: firecube.operations.StorageDriverConfig

::: firecube.operations.StorageUri

::: firecube.operations.ChunkManager
    options:
        members: false

### Resolving Settings

::: firecube.operations.resolve_product_name

::: firecube.operations.resolve_storage_config

::: firecube.operations.resolve_slot_window

## Archive

::: firecube.operations.create_archive

::: firecube.operations.restore_archive

::: firecube.operations.read_restore_source

::: firecube.operations.restore_label

::: firecube.operations.ArchiveCreatePlan

::: firecube.operations.ArchiveCreateResult

::: firecube.operations.ArchiveRestorePlan

::: firecube.operations.ArchiveRestoreResult

::: firecube.operations.ArchiveRestoreSource

::: firecube.operations.ChainBackup

## Migrate

::: firecube.operations.migrate_product

::: firecube.operations.adopt_product

::: firecube.operations.list_migrations

::: firecube.operations.load_migration_steps

::: firecube.operations.MigrationResult

::: firecube.operations.MigrationRunInfo

::: firecube.operations.AdoptionResult

::: firecube.operations.AttributeVerdict

::: firecube.operations.InstalledStep

::: firecube.operations.ReleaseCreator

## Release

::: firecube.operations.create_release_for_product

::: firecube.operations.list_releases_for_product

::: firecube.operations.read_release_for_product

::: firecube.operations.verify_release_for_product

::: firecube.operations.ReleasePlan

::: firecube.operations.ReleaseCreateResult

::: firecube.operations.ReleaseListing

::: firecube.operations.ReleaseCheck

::: firecube.operations.ReleaseVerification

::: firecube.operations.RetentionVerification

## Snapshot

::: firecube.operations.take_snapshot_for_product

::: firecube.operations.SnapshotPlan

::: firecube.operations.SnapshotPreview

::: firecube.operations.SnapshotRecord

::: firecube.operations.SnapshotConsistency

## Zarr

### Index

::: firecube.operations.rebuild_index_record

::: firecube.operations.verify_index_record

::: firecube.operations.IndexRebuildResult

::: firecube.operations.IndexVerification

### Preallocate and Slots

::: firecube.operations.preallocate_product

::: firecube.operations.plan_slots

::: firecube.operations.PreallocateHost

::: firecube.operations.PreallocateDryRunGroup

::: firecube.operations.GroupSlotPlan

### Consolidate

::: firecube.operations.consolidate_time_coords

::: firecube.operations.ConsolidationSummary

### Multires

::: firecube.operations.ZarrMultiresBuilder

::: firecube.operations.MultiresConfig

## Sealing Markers

The sealing markers record who owns the values of a time-coordinate array.
`firecube zarr preallocate` stamps `ATTR_PREALLOCATED` on grid-valued coordinates
and `ATTR_COORD_MANAGED` on the observed-values coordinates it materializes.

::: firecube.operations.ATTR_PREALLOCATED

::: firecube.operations.ATTR_COORD_MANAGED

## See Also

- [Plugin SDK](plugin-sdk.md)
- [Core Utilities](core-utilities.md)
- [CLI Reference](cli.md)
- [Exceptions](exceptions.md)
- [Storage Drivers](storage-drivers.md)
