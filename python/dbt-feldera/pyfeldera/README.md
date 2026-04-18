# pyfeldera — Self-Contained Feldera Python Package

A pip-installable package that bundles the Feldera pipeline-manager binary,
SQL compiler JAR, and Rust runtime crate sources. Similar to how PySpark
ships pre-built JARs, pyfeldera lets you run a full Feldera instance from
Python with minimal host prerequisites.

## Prerequisites

- Python 3.10+
- Java 21 (JRE)
- Rust toolchain (1.93.1+)
- System packages: `libssl-dev`, `pkg-config`, `libsasl2-dev`, `zlib1g-dev`,
  `build-essential`, `libclang-dev`

## Quick start

```bash
pip install pyfeldera-0.1.0-py3-none-linux_x86_64.whl
python -m pyfeldera --bind-address=127.0.0.1
```

## Python API

```python
from pyfeldera import FelderaServer

server = FelderaServer(bind_address="127.0.0.1", port=8080)
server.start()
server.wait_for_healthy()

# Use the Feldera Python SDK or dbt-feldera to interact
from feldera import FelderaClient
client = FelderaClient("http://127.0.0.1:8080")

server.stop()
```

## Building

```bash
.scripts/run.sh build-wheel   # extract binaries + build wheel
.scripts/run.sh build-image   # build the customer Docker image
.scripts/run.sh test           # run basic Docker tests
```
