# Exceptions

This reference covers the error types exported from `firecube.ingestor.api`.
Ingestor-layer errors inherit from `IngestorError`; `ManifestError`,
`SchemaDriftError`, and `StorageError` are shared error types re-exported
through the same facade.

`MissingIrregularCoordinateError`, `DuplicateIrregularCoordinateError`, and
`NoDiscoveredItemsError` are raised during `IrregularTimeAxis(values=AUTO)`
discovery.

{{ render_api_summary("firecube.ingestor.api", [
    "IngestorError",
    "ConfigurationError",
    "MissingIrregularCoordinateError",
    "DuplicateIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "IndexedWriteCompilationError",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "ManifestError",
    "StorageError",
    "ResumeConflictError",
    "RangeOverlapError",
    "WriteIntentRangeError",
]) }}

## Ingestor Errors

::: firecube.ingestor.api.IngestorError

::: firecube.ingestor.api.ConfigurationError

::: firecube.ingestor.api.SchemaSizeMismatchError

::: firecube.ingestor.api.ResumeConflictError

::: firecube.ingestor.api.RangeOverlapError

::: firecube.ingestor.api.WriteIntentRangeError

## Irregular Axis Discovery Errors

These errors are raised when `IrregularTimeAxis(values=AUTO)` discovery fails.
All three inherit from `ConfigurationError`.

::: firecube.ingestor.api.MissingIrregularCoordinateError

::: firecube.ingestor.api.DuplicateIrregularCoordinateError

::: firecube.ingestor.api.NoDiscoveredItemsError

## Shared Errors

::: firecube.ingestor.api.ManifestError

::: firecube.ingestor.api.SchemaDriftError

::: firecube.ingestor.api.StorageError

## IndexedWrite Compilation Errors

`IndexedWriteCompilationError` is raised when the engine cannot map an
`IndexedWrite.coordinate` to a slot index at compile time.

::: firecube.ingestor.api.IndexedWriteCompilationError

## Core API Re-Exports

The core facade re-exports the error types used by plugin code and the engine.

::: firecube.core.api.MissingIrregularCoordinateError

::: firecube.core.api.DuplicateIrregularCoordinateError

::: firecube.core.api.NoDiscoveredItemsError

::: firecube.core.api.IndexedWriteCompilationError

## See Also

- [Templates](templates.md)
- [Index Specification](parallelism.md)
- [IrregularTimeAxis Plugin Guide](../guides/plugins/irregular-axis.md)
- [Implement DirectZarrIngestor](../guides/plugins/direct-zarr.md)
