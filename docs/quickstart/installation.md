# Installation

Firecube requires Python 3.12 or later. This quickstart uses `uv` to create an
isolated environment and install the released package from PyPI.

## Create The Environment

Create a working directory and a Python 3.12 environment:

```bash
mkdir firecube-quickstart
cd firecube-quickstart
uv venv --python 3.12
```

Keep the remaining quickstart commands in this directory so `uv run` uses the
same environment.

## Install Firecube

Install Firecube from PyPI:

```bash
uv pip install firecube
```

The next step installs plugins into this environment so the Firecube CLI can
discover them.

## Verify The Installation

```bash
uv run firecube --version
```

Expected output:

```text
Firecube {{ firecube_version() }}
```

To change Firecube itself, use the source setup in
[Contributing To Firecube](../contributing/firecube-contributors.md) instead.

## Next Steps

- **[Install a Plugin](plugins.md)** — install an existing product plugin
- **[Build a Plugin](../guides/plugins/index.md)** — create a plugin for a new product
