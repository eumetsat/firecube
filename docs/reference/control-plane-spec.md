# Control-Plane Specification

This page specifies the on-disk format of the `.firecube/` control plane that
Firecube maintains next to every product it writes. It is a format
specification, not a guide: a reader with no Firecube installation can parse
`.firecube/` from this page alone. The key words MUST, MUST NOT, and MAY are
normative.

Two format versions apply, and every JSON document carries its own
`schema_version` field:

| Surface | Version |
|---|---|
| Control-plane envelope: `schema.json`, run records, WAL events, snapshots, `LATEST.json` | `v2` |
| Index records: `index/current.json`, `slot_index/current.json` | `v1` |

## Layout

The control plane lives inside the product root, in a directory named
`.firecube`. All paths below are relative to that directory.

```text
.firecube/
    schema.json                  Control-plane format descriptor, written once
    LATEST.json                  Pointer to the newest snapshot generation
    runs/<run_id>/run.json       Run record, one directory per run
    runs/<run_id>/events-NNNNN.jsonl   WAL segments for that run, NNNNN from 00000
    claims/<digest>.json         Write claims, one file per claimed write domain
    snapshots/snapshot-<generation>.jsonl        Projected chunk records
    snapshots/snapshot-<generation>.meta.json    Snapshot metadata
    index/current.json           Resolved-index record
    slot_index/current.json      Slot-index model record
```

Only the directories a product has used exist. A serial ingest without a
declared index has no `index/` directory; a product never snapshotted has no
`snapshots/` directory or `LATEST.json`.

## Atomicity

Every control-plane JSON document is published atomically: a reader that can
see a file MUST be able to parse it as complete JSON. Writers publish through
a temporary object followed by an atomic rename (local filesystems) or a
conditional put (object stores). Creation-only writes (`schema.json`, claim
files, `index/current.json`, `slot_index/current.json`) fail instead of
overwriting when the file already exists; `run.json` is replaced atomically in
place because peer processes read it while the run is live.

## `schema.json`

Written once, when the first run initializes the control plane. Never
rewritten.

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `layout` | string | `"chunkmanager-v2"` |
| `created_at` | number | Unix timestamp (seconds) of first initialization |

## Run Records

Each run owns the directory `runs/<run_id>/`. Its `run.json` is the run's
liveness and resume record, heartbeat-refreshed while the run is active.

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `product` | string | Logical product name |
| `run_id` | string | Run identifier, equals the directory name |
| `status` | string | Run status; terminal states are recorded by WAL events |
| `parts` | integer | Number of sealed WAL segments |
| `events` | integer | Total WAL events written so far |
| `started_at` | number | Unix timestamp (seconds) |
| `updated_at` | number | Unix timestamp of the last heartbeat |
| `run_uri` | string | Absolute URI of the run directory |
| `run_stale_threshold_s` | integer | Seconds after which a silent run counts as stale |
| `error` | string | Present only on failure |
| `completed_at` | number | Present only after completion |
| `slot_range` | array | Present only for slot-range parallel runs: `[start, end)` |
| `slot_group` | string | Present only for slot-range parallel runs |

A run whose `updated_at` is older than `run_stale_threshold_s` MAY be treated
as abandoned by recovery tooling. See
[Recover a Product](../operations/chunk-manager/recover.md).

### WAL events

The write-ahead log is the authoritative, append-only record of what happened
to the product. Events are JSON objects, one per line, in segment files
`events-00000.jsonl`, `events-00001.jsonl`, and so on under the run directory.
Segments are immutable once sealed. Every event carries this envelope:

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `event_id` | string | Unique event identifier |
| `event_type` | string | One of the event types below |
| `product` | string | Logical product name |
| `run_id` | string | Emitting run |
| `timestamp` | number | Unix timestamp (seconds) |
| `record` | object | Event-type-specific payload |
| `meta` | object | Optional; omitted when empty |

Event types:

| `event_type` | Emitted when |
|---|---|
| `run_started` | A run begins |
| `run_started_with_replacement` | A run begins and will replace existing chunks |
| `run_completed` | A run ends successfully |
| `run_failed` | A run ends with an error |
| `run_abandoned` | A stale run is marked abandoned |
| `span_committed` | A write span commits |
| `span_failed` | A write span fails |
| `span_noop` | A write span is skipped as already satisfied |
| `record_replaced` | A chunk record is replaced |
| `record_upsert` | A chunk record is inserted or updated |
| `replacement_committed` | A replacement transaction commits |
| `maintenance_started` | A maintenance operation begins |
| `maintenance_completed` | A maintenance operation ends successfully |
| `maintenance_failed` | A maintenance operation fails |
| `schema_verification` | A store schema check is recorded |
| `index_ensured` | A resolved index is written, matched, rebuilt, or refused |
| `slot_index_model_recorded` | A slot-index model record is first written |
| `slot_index_model_verified` | An existing slot-index model record is verified |
| `consolidated_time_coord` | A time-coordinate consolidation rewrites an array |

The `index_ensured` payload records `run_id`, `product`, `identity_hash`, the
sorted `axis_kinds` and `groups` of the declaration, an ISO 8601 `timestamp`,
and an `outcome` that is one of `created`, `matched_existing`,
`conflict_refused`, or `rebuilt`.

## Claims

A claim grants one owner exclusive write access to a write domain, identified
by the string `product:category:name`. The claim file name is the SHA-256 hex
digest of that identifier, so claim lookup never requires listing:
`claims/<sha256(product:category:name)>.json`.

| Key | Type | Meaning |
|---|---|---|
| `product` | string | Logical product name |
| `domain` | string | The `product:category:name` identifier |
| `owner_id` | string | Claim owner |
| `claim_path` | string | Absolute path of this claim file |
| `acquired_at` | number | Unix timestamp (seconds) |
| `last_heartbeat_at` | number | Refreshed by the owner while held |
| `heartbeat_interval_s` | number | Owner's refresh cadence |
| `stale_threshold_s` | number | Staleness cutoff, default 120 |

Semantics:

- Acquisition is the atomic creation of the claim file. If the file already
  exists, acquisition MUST fail with a claim conflict; it MUST NOT take over
  the claim, even a stale one.
- Release deletes the file.
- A claim is stale when `now - last_heartbeat_at > stale_threshold_s`. A
  crashed owner leaves a stale claim behind; operators clear stale claims with
  the recovery tooling, which refuses to clear fresh claims unless forced.

See [Write Safety](../concepts/orchestration/write-safety.md) for how claims
coordinate concurrent writers.

## Snapshots and `LATEST.json`

A snapshot is a projection of the WAL into one chunk record per line, for
readers that do not want to replay every run's events. Rebuilding a snapshot
writes `snapshots/snapshot-<generation>.jsonl` (records sorted by key), a
sibling `snapshot-<generation>.meta.json`, and then updates `LATEST.json` to
point at the new generation. `<generation>` is a nanosecond timestamp, so
generations sort chronologically.

`snapshot-<generation>.meta.json` and `LATEST.json` both carry
`schema_version` (`"v2"`), `generation`, `completed_before` (Unix timestamp:
runs completed before this instant are folded in), and `product`;
`LATEST.json` adds `snapshot_path` and `snapshot_meta_path`, and the meta file
adds `created_at` and `records` (line count). Snapshots are derived data: they
MAY be deleted and rebuilt from the WAL at any time.

## Resolved Index: `index/current.json`

The resolved-index record is the durable form of an engine-resolved
[`IndexSpec`](parallelism.md#firecube.ingestor.api.IndexSpec). It is written
once, after the first successful resolution, and verified against the plugin
declaration on every later run. Inspect it with `firecube zarr index show`;
regenerate it with `firecube zarr index rebuild`. The rendered API reference
for the record type is
[`ResolvedIndexRecord`](parallelism.md#firecube.ingestor.api.ResolvedIndexRecord).

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v1"` |
| `recorded_at` | string | ISO 8601 UTC timestamp |
| `recorded_by_run_id` | string | Run that wrote the record |
| `identity_hash` | string | 64-character lowercase hex SHA-256, see below |
| `index` | object | The resolved-index payload |
| `items` | array | Present only for discovered axes: content-addressed item manifest, sorted by coordinate value |

### Identity hash

The identity hash content-addresses the resolved index. Canonical bytes are
the UTF-8 encoding of the payload serialized as JSON with sorted keys,
separators `(",", ":")`, and `ensure_ascii=false`.

- Without items: `identity_hash = sha256(canonical_bytes(index))`.
- With items: the hash covers `{"index": index, "items": items}` with items
  sorted by their per-item `identity_hash`, so the value is invariant under
  input order and changes whenever an item is added or removed.

The same hash is mirrored into the Zarr root attributes
`firecube_resolved_index` (the payload) and
`firecube_resolved_index_identity_hash`, so a store detached from its
control plane still carries its identity.

### Precedence rules

On every run, the engine compares three things: the control-plane record
(`index/current.json`), the mirrored attrs hash on the store, and the index
resolved from the current plugin declaration. The rules are exhaustive and
evaluated in this order:

| Row | Record | Attrs hash | Relation | Outcome |
|---|---|---|---|---|
| 1 | absent | absent | fresh store | write record and attrs, outcome `created` |
| 6 | present | any | record `schema_version` is not `v1` | refuse: manifest error |
| 2 | present | present | both match the declaration | outcome `matched_existing` |
| 3 | present | absent | record matches the declaration | re-mirror attrs, outcome `created` |
| 4 | absent | present | attrs without a record | refuse: manifest error |
| 5 | present | any | record disagrees with the declaration | refuse: resolved-index conflict |
| 7 | present | present | record matches, attrs disagree | refuse: manifest error |

Refusals surface as documented control-plane errors, never as silent
overwrites. Row 5 is the guard against pointing a changed declaration at an
existing store; `firecube zarr index rebuild` is the explicit way through.

## Slot Index: `slot_index/current.json`

Products using slot-index models carry a parallel record with the same
envelope discipline: `schema_version` (`"v1"`), `recorded_at`,
`recorded_by_run_id`, `identity_hash`, and `model`. The model's canonical
bytes cover `name`, `epoch`, `time_unit`, and the per-group axes (cadence and
mode) with sorted keys; the identity hash is the SHA-256 of those bytes and is
mirrored to store attributes the same way as the resolved index.

## Reserved Zarr array attributes

Firecube stamps a small set of reserved attribute names on the arrays it
manages, including the write-once static-array marker
`firecube_static_written`. The authoritative list and the guard helper are in
[Core Utilities](core-utilities.md#reserved-array-attributes). Plugins MUST
NOT write these names.

## Co-location

`.firecube/` and the readable data store are one product. When copying or
moving a product manually, the control plane MUST move with the data for
inspection, resume, recovery, and cleanup to keep working. Firecube's archive
and restore commands preserve it automatically.

## Versioning

This specification is versioned by the `schema_version` fields above, and
each documented file names its version explicitly. The compatibility rule:

- Within a version, readers MUST ignore keys they do not recognize; keys MAY
  be added without a version change, and documented keys keep their meaning.
- A change that would break a conforming reader (removing a key, changing a
  type or the canonical-bytes rule) increments the `schema_version` of the
  affected document.
- Firecube refuses to act on an `index/current.json` whose `schema_version`
  it does not support (precedence row 6) rather than guessing.

## See Also

- [Inspect a Product](../operations/chunk-manager/inspect.md): read these
  records with the CLI instead of by hand
- [Recover a Product](../operations/chunk-manager/recover.md): stale runs and
  claims
- [Resolved Index Operations](../operations/firecube-index.md): `show`,
  `verify`, and `rebuild`
- [Storage Concepts](../concepts/storage.md): staged and direct writes, and
  where the product root lives
