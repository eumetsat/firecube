# WAL v0.1.5 fixture

`events_sample.jsonl` is a synthetic WAL event log in schema version `v2`
representing a complete `precip_daily` ingestion run (30 daily files, 3 batches
of 10, all committed, run completed).

## Provenance

Generated from a local `firecube ingest precip_daily` run against synthetic
precipitation data produced by `tests/fixtures/gen_synthetic_precip_daily.py`.
The `output_path` fields have been replaced with the portable placeholder
`file:///workspace/precip_daily/ts-noshard.zarr` to avoid embedding
operator-local paths.

## Schema

Each line is a JSON object conforming to the firecube WAL `v2` schema.
Event types present: `run_started`, `span_committed` (×3), `run_completed`.
