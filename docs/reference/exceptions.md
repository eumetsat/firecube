# Exceptions

This reference covers the error types exported from `firecube.ingestor.api`.
Ingestor-layer errors inherit from `IngestorError`; `ManifestError`,
`SchemaDriftError`, and `StorageError` are shared error types re-exported
through the same facade.

{{ render_api_summary("firecube.ingestor.api", [
    "IngestorError",
    "ConfigurationError",
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

## Shared Errors

::: firecube.ingestor.api.ManifestError

::: firecube.ingestor.api.SchemaDriftError

::: firecube.ingestor.api.StorageError

## See Also

- [Templates](templates.md)
- [Slot-Range Parallelism](parallelism.md)
