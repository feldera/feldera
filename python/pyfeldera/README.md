# pyfeldera — Self-Contained Feldera Python Package

A pip-installable package that bundles **everything** needed to run Feldera:
pipeline-manager binary, SQL compiler JAR, Java 21 JRE, Rust 1.93.1 toolchain,
mold linker, and a precompiled Rust dependency cache.  No host compilers required.

## Prerequisites (host machine)

- Python 3.10+
- GCC / build-essential (for Rust linker)
- `libssl`, `libsasl2`, `zlib` (runtime shared libraries)

## Quick start

```bash
pip install pyfeldera-0.1.0-py3-none-any.whl
python -m pyfeldera --bind-address=127.0.0.1
# → Feldera Web UI at http://127.0.0.1:8080
```

## Python API

```python
from pyfeldera import FelderaServer

server = FelderaServer(bind_address="127.0.0.1", port=8080)
server.start()
server.wait_for_healthy()

from feldera import FelderaClient
client = FelderaClient("http://127.0.0.1:8080")
# ... create pipelines, run SQL, etc. ...

server.stop()
```

## Building

```bash
# 1. Extract binaries + toolchain from the official Docker image
.scripts/run.sh extract

# 2. Copy Rust crate sources from the repo
.scripts/run.sh collect

# 3. Build the wheel (~2 GB)
.scripts/run.sh build-wheel

# 4. Build the Fabric-simulation Docker image
.scripts/run.sh build-image

# 5. Run basic tests
.scripts/run.sh test

# 6. Run dbt integration tests
.scripts/run.sh dbt-test

# Or run everything:
.scripts/run.sh all
```

## Uploading the wheel to Azure Blob Storage

The wheel can be uploaded to an Azure Storage account so that Fabric notebooks
(or any Python environment) can `pip install` it directly from a URL.

```bash
export STORAGE_ACCOUNT="<your-storage-account>"
export STORAGE_KEY="<your-storage-account-key>"
export CONTAINER="public"
export BLOB_NAME="whls/pyfeldera-0.1.0-py3-none-any.whl"
export WHL_PATH="dist/pyfeldera-0.1.0-py3-none-any.whl"

curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

cd /workspaces/feldera/python/pyfeldera
az storage blob upload \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$STORAGE_KEY" \
  --container-name "$CONTAINER" \
  --name "$BLOB_NAME" \
  --file "$WHL_PATH" \
  --overwrite

# The wheel is now installable from:
#   pip install https://<account>.blob.core.windows.net/<container>/<blob_name>
```

## Running in Microsoft Fabric

See [FABRIC_QUICKSTART.md](FABRIC_QUICKSTART.md) for step-by-step notebook cells.
