# Sentinel-3 FRP Plugin

You have downloaded Sentinel-3 FRP ZIP files and want tabular output. This
tutorial builds a small `GenericParquetIngestor` plugin that reads CSV members
from those ZIP files and writes Parquet. The staging step also includes a tiny
local ZIP fixture so you can run the full tutorial before using downloaded
products.

## What You Will Do

1. Scaffold a Parquet plugin.
2. Implement ZIP -> CSV parsing in `build_dataset(...)`.
3. Run ingestion on a local FRP-shaped ZIP fixture, or on Sentinel-3 FRP ZIP
   files downloaded from EUMETSAT Data Store.
4. Verify the Parquet output.

## Prerequisites

- Firecube installed (`uv sync`)
- `uv` available
- Optional: Sentinel-3 SLSTR FRP products downloaded from the EUMETSAT Data
  Store (`EO:EUM:DAT:0417`). The tutorial commands below create a small local
  ZIP fixture if you do not have downloaded products yet.

If this is your first Firecube plugin, do [Weather CSV Plugin](weather-csv.md)
first. It is smaller and uses local generated data.

## Step 1: Scaffold the Plugin

```bash
uv run firecube plugins create sentinel3-frp \
  --template parquet \
  --target-dir plugins_dev \
  --non-interactive
```

Expected output:

```text
✨ Created plugin project: plugins_dev/firecube-sentinel3-frp

To install for development:
  cd plugins_dev/firecube-sentinel3-frp
  uv sync
```

## Step 2: Implement Plugin Logic

Replace
`plugins_dev/firecube-sentinel3-frp/src/firecube_sentinel3_frp/ingestor.py`
with:

```python
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import ClassVar

import pandas as pd

from firecube.ingestor.api import (
    GenericParquetIngestor,
    PipelineBatch,
    PluginContext,
    register_ingestor,
)


@register_ingestor("sentinel3_frp")
class Sentinel3FrpIngestor(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "sentinel3_frp"
    name = "sentinel3_frp"

    def build_dataset(
        self,
        group: str,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> pd.DataFrame | None:
        _ = (group, ctx)
        frames: list[pd.DataFrame] = []

        for zip_path in batch.items:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_members = [
                    m
                    for m in zf.namelist()
                    if m.lower().endswith(".csv") and "frp" in m.lower()
                ]
                for member in csv_members:
                    with zf.open(member) as handle:
                        df = pd.read_csv(handle)
                    if df.empty:
                        continue
                    df["source_zip"] = Path(zip_path).name
                    df["source_csv"] = member
                    frames.append(df)

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)
```

Source discovery uses the built-in default behavior. To customize file
discovery, see [Weather CSV: Source Discovery](source-discovery.md).

This implementation returns a `pandas.DataFrame`. The Parquet scaffold leaves
`pandas` commented in `plugins_dev/firecube-sentinel3-frp/pyproject.toml`; add
or uncomment `"pandas"` there before syncing the plugin project outside the
Firecube development environment.

## Step 3: Stage Input Files

Create a small local ZIP fixture with a CSV member whose name contains `frp`:

```bash
mkdir -p data/sentinel3_frp
uv run python - <<'PY'
from pathlib import Path
import zipfile

root = Path("data/sentinel3_frp")
root.mkdir(parents=True, exist_ok=True)

csv_text = """time,latitude,longitude,frp_mw,confidence
2024-07-01T00:00:00Z,45.10,12.20,17.5,nominal
2024-07-01T00:01:00Z,45.12,12.24,21.8,nominal
"""

with zipfile.ZipFile(root / "S3A_SL_2_FRP_sample.zip", "w") as zf:
    zf.writestr("measurement/frp_sample.csv", csv_text)
PY
find data/sentinel3_frp -type f -name "*.zip"
```

Expected output:

```text
data/sentinel3_frp/S3A_SL_2_FRP_sample.zip
```

To use downloaded Sentinel-3 products instead, copy them into
`data/sentinel3_frp` before running ingestion. Nested folders are fine.

## Step 4: Install the Plugin

```bash
uv run firecube plugins install --editable plugins_dev/firecube-sentinel3-frp
uv run firecube plugins describe sentinel3_frp
```

Expected output from `plugins describe`:

```text
Name:        sentinel3_frp
Version:     0.1.0 (firecube-sentinel3-frp)
Module:      firecube_sentinel3_frp.ingestor
Product:     sentinel3_frp

Options Sections:
  [ENGINE]
      pipeline_parallel [boolean] (default: False)
      ...
```

## Step 5: Run Ingestion

```bash
mkdir -p output
PRODUCT_URI="file://$PWD/output/sentinel3_frp"

uv run firecube ingest sentinel3_frp \
  --input-data data/sentinel3_frp \
  --target "$PRODUCT_URI" \
  --product-name sentinel3_frp \
  --storage-type local \
  --storage-driver fsspec \
  --output-format parquet \
  --write-mode direct \
  --option 'include_patterns=["*.zip"]'
```

Expected output includes the number of ZIP files Firecube found and the Parquet
run summary:

```text
"message":"Found <N> files"
...
"plugin": "sentinel3_frp"
...
"files_processed": <N>
...
"product": "sentinel3_frp"
```

## Step 6: Verify Output

```bash
uv run python - <<'PY'
from pathlib import Path
import pandas as pd

root = Path("output/sentinel3_frp")
parts = sorted(root.rglob("*.parquet"))
print(f"Parquet part files: {len(parts)}")
for p in parts[:5]:
    print(" -", p)

if not parts:
    raise SystemExit("No parquet files were produced")

df = pd.read_parquet(root)
print(df.head())
print("rows=", len(df))
PY
```

Expected output:

```text
Parquet part files: <number greater than 0>
 - output/sentinel3_frp/part-sentinel3_frp_batch_0000.parquet
...
rows= <number greater than 0>
```

## Troubleshooting

- No files discovered: check `--input-data` and
  `--option 'include_patterns=["*.zip"]'`
- Unexpected parsing errors: inspect ZIP members and verify CSV schema
- Debug mode: `--option pipeline_parallel=false`

## Next Steps

- **[Parquet](../concepts/output-formats/parquet.md)** — understand Parquet output behavior
- **[GenericParquetIngestor](../concepts/plugins/generic-parquet.md)** — read the plugin base-class details
- **[Weather CSV: Source Discovery](source-discovery.md)** — customize built-in source discovery
- **[Run Ingestion](../quickstart/ingestion.md)** — run local and S3 examples
