# Feldera on Microsoft Fabric

Run a full Feldera streaming SQL engine inside a Fabric notebook — no Docker, no Rust, no Java install required. Everything ships in a single `pip install`.

## Architecture

![Feldera + Fabric demo](https://rakirahman.blob.core.windows.net/public/images/Misc/feldera-fabric.png)

```mermaid
graph TB
    subgraph "Devcontainer (this machine)"
        RALPH["🔁 Ralph Loop<br/><i>scripts/ralph.sh</i>"]
        COPILOT["Copilot Agent<br/><i>skill.md instructions</i>"]
        FAB["fab CLI<br/><i>start / cancel notebook</i>"]
        BUILD["Build Pipeline<br/><i>pyfeldera/.scripts/run.sh</i>"]
        BLOB[("Azure Blob<br/><i>rakirahman/public/whls/</i>")]
        PROXY["Privy ProxyClient<br/><i>localhost:3000</i>"]
        DBT["dbt CLI + PyTest<br/><i>--target fabric</i>"]
    end

    subgraph "Azure"
        RELAY["Azure Relay<br/><i>Hybrid Connection</i>"]
        FABRIC_API["Fabric REST API<br/><i>job start/cancel/status</i>"]
    end

    subgraph "Fabric VM (Azure Linux 3.0)"
        NB["📓 Notebook<br/><i>Cell 0: configure<br/>Cell 1: pip install whl<br/>Cell 2: start Feldera + Privy</i>"]

        subgraph "/mnt/.pyfeldera (deployed from whl)"
            PM["pipeline-manager<br/><i>Feldera server :8080</i>"]
            PG["Embedded PostgreSQL<br/><i>state store</i>"]
            SQL_JAR["SQL Compiler<br/><i>JAR + Java 21</i>"]
            RUST_TC["Rust 1.93.1 + mold<br/><i>pipeline compilation</i>"]
            CLANG["libclang + LLVM<br/><i>bundled for bindgen</i>"]
            CACHE["Precompile Cache<br/><i>~4.9 GB warm cargo</i>"]
        end

        PRIVY_SRV["Privy RelayServer<br/><i>proxy_target → :8080</i>"]
        LH[("/lakehouse/default/Tables/<br/><i>Delta Lake output</i>")]
    end

    RALPH -- "re-invokes until<br/>Succeeded signal" --> COPILOT
    COPILOT -- "fab job start/cancel" --> FAB
    FAB -- "REST API" --> FABRIC_API
    FABRIC_API -- "runs notebook" --> NB

    COPILOT -- "fix code → rebuild<br/>→ upload .whl" --> BUILD
    BUILD -- "az storage blob upload" --> BLOB
    NB -- "pip install .whl" --> BLOB

    NB -- "starts" --> PM
    NB -- "starts" --> PRIVY_SRV
    PM -- "compiles SQL → Rust" --> SQL_JAR
    PM -- "cargo build" --> RUST_TC
    RUST_TC -- "bindgen FFI" --> CLANG
    RUST_TC -- "reuses deps" --> CACHE
    PM -- "embedded" --> PG
    PM -- "writes Delta" --> LH

    PRIVY_SRV -- "WebSocket" --> RELAY
    PROXY -- "WebSocket" --> RELAY
    DBT -- "HTTP :3000" --> PROXY
    PROXY -. "tunneled via relay" .-> PM

    COPILOT -- "runs dbt tests<br/>--target fabric" --> DBT

    style RALPH fill:#f9a825,stroke:#f57f17,color:#000
    style PM fill:#1565c0,stroke:#0d47a1,color:#fff
    style RELAY fill:#7b1fa2,stroke:#4a148c,color:#fff
    style BLOB fill:#ef6c00,stroke:#e65100,color:#fff
    style LH fill:#2e7d32,stroke:#1b5e20,color:#fff
    style NB fill:#00838f,stroke:#006064,color:#fff
```

## Prerequisites

| Component             | Where         | Notes                               |
| --------------------- | ------------- | ----------------------------------- |
| Fabric notebook       | Fabric portal | Any Spark-attached notebook         |
| Azure Relay namespace | Azure portal  | For Privy browser proxy (optional)  |
| Fabric CLI (`fab`)    | Your laptop   | To start/stop the notebook remotely |

## Part 1 — Fabric notebook

### Cell 0 — Configure infra

```bash
%%configure
{
 "vCores": 2
}
```

### Cell 1 — Install

```python
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/pyfeldera-0.1.0-py3-none-any.whl
%pip install --force-reinstall https://rakirahman.blob.core.windows.net/public/whls/privy-0.0.1-py3-none-any.whl
%pip uninstall -y pathlib
```

> **Note:** Fabric ships a `pathlib` PyPI package that shadows the Python 3.12 stdlib. The uninstall is required.

### Cell 2 — Start Feldera + Privy proxy

```python
import threading, shutil, glob, requests
from pyfeldera.server import FelderaServer
from feldera import FelderaClient
from privy import RelayServer

# ── Config ──
RELAY_NS   = "..."
RELAY_PATH = "demo"
RELAY_RULE = "demo-listen-send"
RELAY_KEY  = "..."

# ── Clean lakehouse ──
for p in glob.glob("/lakehouse/default/Tables/*") + glob.glob("/lakehouse/default/Files/*"):
    shutil.rmtree(p, ignore_errors=True)

# ── Start Feldera (auto-deploys to /mnt if available for disk space) ──
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

## Part 2 — This devcontainer

### Install tooling

```bash
sudo apt install -y pipx
sudo pipx ensurepath
sudo pipx install ms-fabric-cli
source ~/.bashrc

sudo mkdir -p ~/.config/fab
sudo chown -R $(id -u):$(id -g) ~/.config/fab

fab --version

# First-time auth
fab config set encryption_fallback_enabled true
fab auth login

# Privy — proxy Feldera UI to your browser
pip install https://rakirahman.blob.core.windows.net/public/whls/privy-0.0.1-py3-none-any.whl
```

### Fill up env file

Fill up `/workspaces/feldera/.vite/privy/.env`:

```text
STORAGE_KEY=...
PRIVY_RELAY_NAMESPACE=mdrrahman-dev-relay
PRIVY_RELAY_PATH=demo
PRIVY_RELAY_KEYRULE=demo-listen-send
PRIVY_RELAY_KEY=...
```

### Start the notebook remotely

```bash
# Navigate to your workspace
fab cd "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.workspace"

# List items (should show Feldera.Notebook)
fab ls "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace"

# Start the notebook (async — returns a job ID)
fab job start "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook"

# List all runs (find your job ID + status)
fab job run-list "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook"

# Check a specific run
fab job run-status "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook" --id <JOB_ID>
```

### Browse the Feldera Web UI

Once the notebook is running and Privy is serving, open http://localhost:3000:

```python
from privy import ProxyClientServer

ProxyClientServer(
    namespace="...",           # same Azure Relay namespace
    path="demo",
    keyrule="demo-listen-send",
    key="...",                 # same SAS key
    local_port=3000,
).serve_forever()
```

### Cancel the notebook

```bash
# Find the job ID from the run list
fab job run-list "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook"

# Cancel it
fab job run-cancel "SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook" --id <JOB_ID>
```

### Run dbt against Fabric-hosted Feldera

With the Privy proxy running locally on port 3000:

```bash
cd python/dbt-feldera
.scripts/run.sh fabric-test
```

## Notes

- Fabric VMs have ~59 GB disk. The wheel + deploy uses ~20 GB. Cell 2 cleans duplicates to free ~9 GB.
- The `pathlib` PyPI package **must** be uninstalled — it breaks Python 3.12 stdlib imports.
- Privy uses Azure Relay hybrid connections — no inbound ports or public IPs needed.
- The Fabric CLI (`fab`) authenticates via your Microsoft Entra ID (device code flow).
