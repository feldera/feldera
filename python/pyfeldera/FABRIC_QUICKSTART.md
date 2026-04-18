# Running Feldera in a Microsoft Fabric Notebook

## Prerequisites
- A Fabric Lakehouse with a Python notebook
- The notebook runtime must have network access to download the wheel

## Cell 1 — Install pyfeldera (one-time, ~2 min)

```python
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/pyfeldera-0.1.0-py3-none-any.whl
%pip uninstall -y pathlib
```

> **Note:** The `pathlib` uninstall removes a legacy PyPI package that
> shadows Python's built-in `pathlib` module and breaks imports on Python 3.12+.

## Cell 2 — Start Feldera in the background

```python
import subprocess, time, os, threading

from pyfeldera.server import FelderaServer
server = FelderaServer(bind_address="127.0.0.1", port=8080)

def _run():
    server.start_blocking()

t = threading.Thread(target=_run, daemon=True)
t.start()
server.wait_for_healthy(timeout=180)
print("Feldera is running!")
```

## Cell 3 — Verify the REST API

```python
import requests

base = "http://127.0.0.1:8080"

r = requests.get(f"{base}/healthz")
print(f"Health: {r.status_code} — {r.json()}")

r = requests.get(f"{base}/v0/config")
cfg = r.json()
print(f"Version: {cfg['version']}")
print(f"Edition: {cfg['edition']}")

r = requests.get(f"{base}/v0/pipelines")
print(f"Pipelines: {r.json()}")
```

## Cell 4 — Use the Feldera Python SDK

```python
from feldera import FelderaClient

client = FelderaClient("http://127.0.0.1:8080")
cfg = client.get_config()
print(f"Connected to Feldera {cfg.version} ({cfg.edition})")
```

## Cell 5 — Browse the Feldera Web UI (via Privy proxy)

The Feldera Web UI is a client-side SPA that needs a browser JS engine.
Privy proxies browser traffic from your laptop through Azure Relay to
Feldera running on Fabric.

### On Fabric

```bash
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/privy-0.0.1-py3-none-any.whl
```

In the Fabric notebook cell where you start Privy, use `proxy_target`:

```python
from privy import RelayServer
RelayServer(
    namespace="...", path="...", keyrule="...", key="...",
    proxy_target="http://127.0.0.1:8080",
).serve_forever()
```

### On your laptop — start the local proxy

```python
import os
from privy import ProxyClientServer

proxy = ProxyClientServer(
    namespace="...",  # same relay credentials
    path="...",
    keyrule="...",
    key="...",
    local_port=3000,
)
proxy.serve_forever()
# Open http://localhost:3000 in your browser → full Feldera UI!
```

## Notes

- The first run deploys ~6 GB of bundled data to `~/.pyfeldera/` — this takes ~40s.
  Subsequent runs reuse the deployed data and start instantly.
- The wheel bundles: pipeline-manager binary, SQL compiler JAR, Java 21 JRE,
  Rust 1.93.1 toolchain, mold linker, and a precompiled Rust dependency cache.
- Fabric's Java 11 is NOT used — the bundled Java 21 is used for the SQL compiler.
- Pipeline compilation uses the bundled Rust toolchain (no `rustc` needed on the host).
- The Feldera server runs on `127.0.0.1:8080` — accessible only from within the notebook.
