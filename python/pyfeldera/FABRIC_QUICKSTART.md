# Feldera on Microsoft Fabric

## Cell 1 — Install (restart kernel after first run)

```python
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/pyfeldera-0.1.0-py3-none-any.whl
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/privy-0.0.1-py3-none-any.whl
%pip uninstall -y pathlib
```

## Cell 2 — Start Feldera + Privy

```python
import threading, requests
from pyfeldera.server import FelderaServer
from feldera import FelderaClient
from privy import RelayServer

# ── Config ──
RELAY_NS   = "..."
RELAY_PATH = "demo"
RELAY_RULE = "demo-listen-send"
RELAY_KEY  = "..."

# ── Start Feldera ──
server = FelderaServer(bind_address="127.0.0.1", port=8080)
threading.Thread(target=server.start_blocking, daemon=True).start()
server.wait_for_healthy(timeout=180)

# ── Verify ──
client = FelderaClient("http://127.0.0.1:8080")
cfg = client.get_config()
print(f" Feldera {cfg.version} ({cfg.edition})")
print(f" Pipelines: {requests.get('http://127.0.0.1:8080/v0/pipelines').json()}")

# ── Start Privy proxy (bridges your laptop browser → Feldera UI) ──
print(" Starting Privy Azure relay proxy...")
RelayServer(namespace=RELAY_NS, path=RELAY_PATH, keyrule=RELAY_RULE, key=RELAY_KEY, proxy_target="http://127.0.0.1:8080").serve_forever()
```

## On your laptop — open the Web UI

```bash
pip install https://rakirahman.blob.core.windows.net/public/whls/privy-0.0.1-py3-none-any.whl
```

Open http://localhost:3000 in your browser:

```python
from privy import ProxyClientServer
ProxyClientServer(namespace="...", path="demo", keyrule="demo-listen-send", key="...", local_port=3000).serve_forever()
```

## Notes

- First run deploys ~6 GB (~40s). Subsequent starts are instant.
- Ships everything: pipeline-manager, SQL compiler, Java 21, Rust 1.93.1, mold, precompiled cache.
- No Rust, no Java, no Docker needed — just Python + GCC on the host.
