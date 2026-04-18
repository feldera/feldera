---
name: ralph-feldera-fabric
description: "Run pyfeldera dbt regression tests against Feldera on Microsoft Fabric. Iteratively fix failures, rebuild the wheel, upload, restart the notebook, and re-test until green."
user-invocable: true
---

# Feldera-on-Fabric dbt Regression Loop

Iterative development loop to get dbt tests passing against a real Feldera instance running on Microsoft Fabric via the self-contained `pyfeldera` wheel.

---

## CRITICAL: Rules

**NEVER `git add` or `git commit`**, ALL changes will be reviewed, committed and pushed by a human.

**When tests fail, they represent REAL issues** — missing bundled libs, broken env vars, incompatible paths, etc. The failures are not noise — something needs a code fix in `python/pyfeldera/`.

**You MUST follow this order:**

1. **Cancel & Start** — cancel stale notebook runs, start fresh (Step 1)
2. **Wait & Proxy** — wait for Feldera healthy via Privy proxy (Step 2)
3. **Test** — run dbt Fabric tests (Step 3)
4. **Fix & Rebuild** — if tests fail, fix source, rebuild wheel, upload (Step 4)
5. **Iterate** — go back to Step 1 (Step 5)
6. **Signal completion** — only after ALL tests are green (Step 6)

**NEVER emit `{ "status": "Succeeded" }` if any test is failing.**

---

## Environment

| Component              | Path                                                                  |
| ---------------------- | --------------------------------------------------------------------- |
| pyfeldera package      | `/workspaces/feldera/python/pyfeldera`                                |
| dbt-feldera adapter    | `/workspaces/feldera/python/dbt-feldera`                              |
| Privy relay            | `/workspaces/feldera/.vite/privy`                                     |
| Privy .env (secrets)   | `/workspaces/feldera/.vite/privy/.env`                                |
| pyfeldera build script | `/workspaces/feldera/python/pyfeldera/.scripts/run.sh`                |
| dbt test script        | `/workspaces/feldera/python/dbt-feldera/.scripts/run.sh`              |
| Fabric workspace       | `SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace` |
| Fabric notebook        | `Feldera.Notebook`                                                    |

### Secrets (from `.vite/privy/.env`)

```
STORAGE_KEY           — Azure blob storage key (for wheel upload)
PRIVY_RELAY_NAMESPACE — Azure Relay namespace
PRIVY_RELAY_PATH      — Hybrid connection name
PRIVY_RELAY_KEYRULE   — SAS rule name
PRIVY_RELAY_KEY       — SAS key
```

---

## Step 1: Cancel Stale Runs & Start Fresh Notebook

```bash
set -a; source /workspaces/feldera/.vite/privy/.env; set +a

WORKSPACE="SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace"
NOTEBOOK="Feldera.Notebook"
ITEM="${WORKSPACE}/${NOTEBOOK}"

# Cancel any in-progress runs
fab job run-list "${ITEM}" 2>/dev/null | grep -i "InProgress" | awk '{print $1}' | while read id; do
    echo "Cancelling ${id}..."
    fab job run-cancel "${ITEM}" --id "${id}" 2>/dev/null
done

# Start a fresh run
fab job start "${ITEM}"
```

Capture the job ID from the output. Then poll until `InProgress`:

```bash
fab job run-status "${ITEM}" --id <JOB_ID>
```

Wait until status transitions from `NotStarted` → `InProgress`. This means the notebook cells are executing (installing the wheel, starting Feldera, starting Privy relay).

---

## Step 2: Wait for Feldera Healthy via Privy Proxy

Start the local Privy proxy client (if not already running):

```bash
cd /workspaces/feldera/python/dbt-feldera && source .venv/bin/activate 2>/dev/null || true
set -a; source /workspaces/feldera/.vite/privy/.env; set +a

# Check if proxy already running
if ! curl -sf --connect-timeout 3 http://localhost:3000/ >/dev/null 2>&1; then
    nohup python3 -c "
import os
from privy import ProxyClientServer
ProxyClientServer(
    namespace=os.environ['PRIVY_RELAY_NAMESPACE'],
    path=os.environ['PRIVY_RELAY_PATH'],
    keyrule=os.environ['PRIVY_RELAY_KEYRULE'],
    key=os.environ['PRIVY_RELAY_KEY'],
    local_port=3000,
).serve_forever()
" > /tmp/privy-proxy.log 2>&1 &
    sleep 3
fi
```

Poll until Feldera is healthy (may take 2-5 minutes for wheel install + deploy):

```bash
for i in $(seq 1 60); do
    if curl -sf --connect-timeout 5 http://localhost:3000/healthz 2>/dev/null; then
        echo "Feldera healthy via proxy!"
        break
    fi
    echo "Waiting... ($i/60)"
    sleep 10
done
```

**Do NOT proceed to Step 3 until Feldera is healthy.**

If Feldera never becomes healthy after 10 minutes, use Privy RelayClient to check logs:

```python
import os
from privy import RelayClient
c = RelayClient(
    namespace=os.environ["PRIVY_RELAY_NAMESPACE"],
    path=os.environ["PRIVY_RELAY_PATH"],
    keyrule=os.environ["PRIVY_RELAY_KEYRULE"],
    key=os.environ["PRIVY_RELAY_KEY"],
)
r = c.run_bash("cat ~/.pyfeldera/pipeline-manager.log 2>/dev/null | tail -50")
print(r.stdout)
```

---

## Step 3: Run dbt Fabric Tests

```bash
cd /workspaces/feldera/python/dbt-feldera && source .venv/bin/activate
export FELDERA_SKIP_DOCKER=1 FELDERA_URL=http://localhost:3000
uv run pytest integration_tests/test_dbt_feldera.py::TestDbtFelderaFabric -vv --timeout=600 2>&1 | tee /tmp/fabric-test.log
```

This runs:

- `test_fabric_dbt_debug` — connection verification
- `test_fabric_dbt_seed` — data loading via HTTP ingress
- `test_fabric_dbt_build` — model deployment (seeds + views, no Kafka/Delta connectors)
- `test_fabric_dbt_test` — data integrity checks

**If ALL 4 tests pass → go to Step 6 (completion signal).**

**If any test fails → go to Step 4.**

---

## Step 4: Fix, Rebuild, Upload

### 4a. Diagnose the failure

Read the test output carefully. Common failure patterns:

| Error                        | Root Cause                         | Fix Location                                      |
| ---------------------------- | ---------------------------------- | ------------------------------------------------- |
| `Unable to find libclang`    | Missing bundled lib                | `run.sh` extraction + `server.py` wrapper         |
| `stddef.h not found`         | Missing clang resource headers     | `run.sh` extraction (clang/18/include)            |
| `Unable to find libsasl2`    | Missing dev headers/pkg-config     | `run.sh` sysroot extraction + `server.py` wrapper |
| `No space left on device`    | Disk full (59 GB limit)            | Clean site-packages duplicates in notebook Cell 2 |
| `rustup could not choose`    | Cargo wrapper calling wrong binary | `server.py` `_ensure_cargo_wrapper()`             |
| `Pipeline failed to compile` | Cache invalidation (wrong paths)   | Check symlink at `/home/ubuntu/feldera`           |
| `Connection refused`         | Feldera not running / Privy down   | Restart notebook (Step 1)                         |

### 4b. Fix the source code

Edit files in:

- `/workspaces/feldera/python/pyfeldera/pyfeldera/server.py` — server lifecycle, env vars, cargo wrapper
- `/workspaces/feldera/python/pyfeldera/.scripts/run.sh` — extraction from Docker image
- `/workspaces/feldera/python/pyfeldera/tests/docker/Dockerfile.customer` — Docker test image

### 4c. Rebuild wheel

```bash
cd /workspaces/feldera/python/pyfeldera

# If extraction changed:
.scripts/run.sh extract

# If crate sources changed:
.scripts/run.sh collect

# Always:
.scripts/run.sh build-wheel
```

### 4d. Upload wheel to Azure blob

```bash
set -a; source /workspaces/feldera/.vite/privy/.env; set +a

az storage blob upload \
    --account-name rakirahman \
    --account-key "${STORAGE_KEY}" \
    --container-name public \
    --name "whls/pyfeldera-0.1.0-py3-none-any.whl" \
    --file "/workspaces/feldera/python/pyfeldera/dist/pyfeldera-0.1.0-py3-none-any.whl" \
    --overwrite --no-progress
```

### 4e. Verify local Docker tests still pass

```bash
cd /workspaces/feldera/python/pyfeldera
.scripts/run.sh build-image
.scripts/run.sh dbt-test
```

---

## Step 5: Iterate

Go back to **Step 1** — cancel the old notebook, start fresh (it will reinstall the updated wheel), wait for healthy, and re-run tests.

**Key:** Each iteration should fix at least one failure. If the same error persists after a fix, re-read the error carefully — you may have missed a path, env var, or symlink.

---

## Step 6: Completion Signal

**CRITICAL: You MUST emit exactly one of these JSON objects as the absolute last line of your output.**

If ALL 4 Fabric dbt tests passed:

```
{ "status": "Succeeded" }
```

If you were unable to fix all failures after exhausting your approaches:

```
{ "status": "Failed" }
```

**The Ralph loop script parses your final output for this signal.** If neither is found, the loop assumes the task is incomplete and will re-invoke you for another iteration. Always emit a status.
