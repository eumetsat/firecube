# Installation

Firecube requires **Python 3.12+**. We recommend using `uv` for lightning-fast dependency management and isolated environments.

### 1. Clone the Repository

Clone the core Firecube ingestor repository:

```bash
git clone https://github.com/eumetsat/firecube.git
cd firecube
```

### 2. Setup Environment

Use `uv` to install dependencies and prepare the virtual environment:

```bash
uv venv --python 3.12
uv sync
```

### 3. Verify CLI

Confirm the installation by checking the version:

```bash
uv run firecube --version
```

Output:
```text
Firecube {{ firecube_version() }}
```

## Next Steps

- **[Plugin Setup](plugins.md)** — install or create a plugin
