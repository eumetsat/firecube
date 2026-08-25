# Installation

Firecube requires Python 3.12 or later. This quickstart uses `uv` to create an
isolated environment and install the released package from PyPI.

## Create The Environment

Create a working directory and a Python 3.12 environment:

```bash
mkdir firecube-quickstart
cd firecube-quickstart
uv venv --python 3.12
source .venv/bin/activate
```

Keep this shell open for the rest of the quickstart. If you open a new shell,
return to `firecube-quickstart/` and run `source .venv/bin/activate` again.

## Install Firecube

Install Firecube from PyPI:

```bash
uv pip install firecube
```

The next step creates and installs a local plugin in this environment so the
Firecube CLI can discover it.

## Verify The Installation

```bash
firecube --version
```

Expected output:

```text
Firecube {{ firecube_version() }}
```

To change Firecube itself, use the source setup in
[Contributing To Firecube](../contributing/firecube-contributors.md) instead.

## Next Steps

Continue to **[Install the Example Plugin](plugins.md)**. Firecube provides
the ingestion engine, but it does not know how to read every dataset by
itself. The next page installs the dataset-specific Python package used by the
rest of this quickstart.
