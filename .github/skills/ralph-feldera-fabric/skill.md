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

**Delta Lake on OneLake is MANDATORY.** The `fabric` target MUST write Delta output to `/lakehouse/default/Tables/<table_name>`. Do NOT disable Delta connectors for the `fabric` target — that is unacceptable. 

The Fabric Lakehouse FUSE mount at `/lakehouse` does not support atomic renames (`rename()` syscall fails), which Delta Lake's default commit protocol requires. 

You MUST find a way to make this work — options include:

- Configuring `delta-rs` to use a non-rename commit strategy (e.g. `put-if-absent` / direct write)  
- Setting environment variables or Feldera config that changes the Delta writer behavior
- Patching the Feldera delta connector config (e.g. `allow_unsafe_rename`, `UNSAFE_RENAME_OK`, storage options)
- Other engines have made Delta lake work with ADLSG2 on OneLake, explore those if required
- Any other creative solution — this is a POC, hacks are welcome

**Do NOT set fabric Delta connectors to `[]`. That defeats the purpose.**

**You MUST follow this order:**

1. **Cancel & Start** — cancel stale notebook runs, start fresh (Step 1)
2. **Wait & Proxy** — wait for Feldera healthy via Privy proxy (Step 2)
3. **Test** — run dbt Fabric tests (Step 3)
4. **Fix & Rebuild** — if tests fail, fix source, rebuild wheel, upload (Step 4)
5. **Breadcrumbs** — update this skill file inline with lessons learned (Step 5)
6. **Iterate** — go back to Step 1 (Step 6)
7. **Signal completion** — only after ALL tests are green AND Delta output lands in `/lakehouse/default/Tables/` (Step 7)

**NEVER emit `{ "status": "Succeeded" }` if any test is failing or Delta output is disabled.**

You are encouraged to parallelize operations like polling APIs via sub-agents and /fleet.
Keep your main context window clean so you're able to orchestrate the root-cause-analysis.

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

If Feldera never becomes healthy after 10 minutes, diagnose using these methods:

### Notebook-level diagnostics (via `fab` CLI)

Check if the notebook job itself failed:

```bash
ITEM="SQL Telemetry & Intelligence - Insights - Dev - mdrrahman.Workspace/Feldera.Notebook"

# Check job status + failure reason
fab job run-list "${ITEM}" --output_format json 2>&1 | grep -v '^!'
```

Look at `failureReason` — if it says `JobInstanceStatusFailed`, the notebook crashed during execution.

Get per-cell execution state to see which cell is stuck or failed:

```bash
fab get "${ITEM}" \
    -q "definition.parts[0].payload.cells[].{cell_type:cell_type,cellStatus:metadata.cellStatus,source:source[0]}" \
    --output_format json -f 2>&1 | grep -v '^!'
```

Each cell's `cellStatus` JSON contains `normalized_state` (`finished`, `running`, `error`), `execution_start_time`, and `execution_finish_time`. Parse it to find which cell is problematic:

- Cell 0 (markdown) — no execution
- Cell 1 (pip install) — should finish in ~3 min. If stuck, the wheel URL may be unreachable.
- Cell 2 (start Feldera + Privy) — should transition to `running` and stay there. If it errors, the `FelderaServer.start()` or deploy failed.

### Privy diagnostics (via RelayClient)

If Privy relay IS reachable but Feldera isn't healthy, use the RelayClient to inspect:

```python
import os
from privy import RelayClient
c = RelayClient(
    namespace=os.environ["PRIVY_RELAY_NAMESPACE"],
    path=os.environ["PRIVY_RELAY_PATH"],
    keyrule=os.environ["PRIVY_RELAY_KEYRULE"],
    key=os.environ["PRIVY_RELAY_KEY"],
)

# Pipeline-manager log
r = c.run_bash("cat ~/.pyfeldera/pipeline-manager.log 2>/dev/null | tail -50")
print(r.stdout)

# Check if process is running
r = c.run_bash("ps aux | grep pipeline-manager | grep -v grep || echo 'NOT RUNNING'")
print(r.stdout)

# Check deploy status
r = c.run_bash("ls -la ~/.pyfeldera/.deployed 2>/dev/null && echo DEPLOYED || echo NOT_DEPLOYED")
print(r.stdout)

# Check disk
r = c.run_bash("df -h / | tail -1")
print(r.stdout)
```

If Privy relay is NOT reachable, the notebook Cell 2 likely hasn't reached the `RelayServer.serve_forever()` line yet — wait longer or check the job status via `fab`.

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

** YOU MUST NOT SKIP THIS, YOU MUST RESILIENTLY KEEP TRYING TO FIGUE OUT THE ROOT CAUSE **

Read the test output in `/tmp/fabric-test.log` carefully. The dbt output contains the Feldera error in the assertion message.

### Disk space — using /mnt to avoid "No space left on device"

Fabric VMs have two filesystems:

| Mount        | Total    | Purpose                                     |
| ------------ | -------- | ------------------------------------------- |
| `/` (root)   | ~58.5 GB | OS + pip packages (~36 GB used at baseline) |
| `/mnt`       | ~58.5 GB | Scratch storage (~20 GB free)               |
| `/lakehouse` | ~69 GB   | Lakehouse FUSE mount (~69 GB free)          |

The pyfeldera wheel installs to root (`~/jupyter-env/.../site-packages/`, ~6.7 GB), and deploys to root (`~/.pyfeldera/`, ~11 GB). That's ~18 GB on a filesystem with only ~20 GB free — leaving almost nothing for Rust compilation artifacts.

**Fix:** Use `deploy_dir="/mnt/.pyfeldera"` when creating the `FelderaServer`. This puts the ~11 GB deploy (toolchain, cache, binaries) on `/mnt` instead of root:

```python
server = FelderaServer(bind_address="127.0.0.1", port=8080, deploy_dir="/mnt/.pyfeldera")
```

The deploy step also auto-cleans the site-packages source data after copying, freeing ~6.7 GB on root.

If you need even more space, the pip cache can be cleaned:

```python
# Via Privy
r = c.run_bash("rm -rf ~/.cache/pip/ && df -h / /mnt | tail -2")
print(r.stdout)
```

**When updating the notebook Cell 2**, ensure `deploy_dir="/mnt/.pyfeldera"` is set. The cargo wrapper, pkg-config paths, and log file all adapt automatically to the deploy dir.

**Remote diagnostics via Privy:** When the test output alone isn't enough, use the Privy RelayClient to interrogate the Fabric VM directly.

> It's possible privy is down on the Fabric side, in that case use the fab CLI method to get the cell ouptut below.

Set up the client:

```python
import os
set_a = lambda: None  # source .env before running
from privy import RelayClient
c = RelayClient(
    namespace=os.environ["PRIVY_RELAY_NAMESPACE"],
    path=os.environ["PRIVY_RELAY_PATH"],
    keyrule=os.environ["PRIVY_RELAY_KEYRULE"],
    key=os.environ["PRIVY_RELAY_KEY"],
)
```

Then use these diagnostic commands (note: adjust paths if using `/mnt/.pyfeldera`):

```python
# Pipeline-manager log (most useful — shows Rust compilation errors, startup failures)
DEPLOY="/mnt/.pyfeldera"  # or ~/.pyfeldera if using default
r = c.run_bash(f"cat {DEPLOY}/pipeline-manager.log 2>/dev/null | tail -100")
print(r.stdout)

# Disk space on BOTH filesystems
r = c.run_bash("df -h / /mnt /lakehouse 2>/dev/null | tail -3")
print(r.stdout)

# Disk breakdown
r = c.run_bash(f"du -sh {DEPLOY}/*/ 2>/dev/null")
print(r.stdout)

# Check if the cargo wrapper is correct
r = c.run_bash(f"cat {DEPLOY}/toolchain/cargo-wrapper/cargo")
print(r.stdout)

# Check if /home/ubuntu/feldera symlink exists (needed for precompile cache)
r = c.run_bash("ls -la /home/ubuntu/feldera 2>/dev/null || echo 'NO SYMLINK'")
print(r.stdout)

# Check what libs are installed on the host
r = c.run_bash("find /usr/lib -name 'libsasl2*' -o -name 'libclang*' -o -name 'libssl*' 2>/dev/null")
print(r.stdout)

# Check pkg-config paths in the deployed sysroot
r = c.run_bash(f"cat {DEPLOY}/toolchain/sysroot/lib/pkgconfig/libsasl2.pc 2>/dev/null || echo 'NO SYSROOT'")
print(r.stdout)

# Check deployed toolchain structure
r = c.run_bash(f"ls {DEPLOY}/toolchain/")
print(r.stdout)

# Check if libclang and its deps resolve
r = c.run_bash(f"LD_LIBRARY_PATH={DEPLOY}/toolchain/libclang ldd {DEPLOY}/toolchain/libclang/libclang.so 2>&1 | grep 'not found' || echo 'All deps OK'")
print(r.stdout)

# Free disk by cleaning site-packages duplicates and pip cache
r = c.run_bash("""
rm -rf ~/.cache/pip/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/cache/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/toolchain/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/bin/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/build/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/crates/
rm -rf ~/jupyter-env/python3.12/lib/python3.12/site-packages/pyfeldera/data/sql-to-dbsp-compiler/
df -h / /mnt | tail -2
""")
print(r.stdout)
```

Common failure patterns:

| Error                        | Root Cause                            | Fix Location                                                                        |
| ---------------------------- | ------------------------------------- | ----------------------------------------------------------------------------------- |
| `Unable to find libclang`    | Missing bundled lib                   | `run.sh` extraction + `server.py` wrapper                                           |
| `stddef.h not found`         | Missing clang resource headers        | `run.sh` extraction (clang/18/include)                                              |
| `Unable to find libsasl2`    | Missing dev headers/pkg-config        | `run.sh` sysroot extraction + `server.py` wrapper                                   |
| `No space left on device`    | Disk full on root                     | Use `deploy_dir="/mnt/.pyfeldera"`, clean pip cache, clean site-packages duplicates |
| `rustup could not choose`    | Cargo wrapper calling wrong binary    | `server.py` `_ensure_cargo_wrapper()`                                               |
| `Pipeline failed to compile` | Cache invalidation (wrong paths)      | Check CARGO_HOME, RUSTFLAGS, dbsp-override-path consistency                         |
| `Connection refused`         | Feldera not running / Privy down      | Restart notebook (Step 1)                                                           |
| `signal.signal()` ValueError | Called from non-main thread           | `server.py` `start_blocking()` try/except                                           |
| `__tunable_is_initialized`   | LD_LIBRARY_PATH leaking to PostgreSQL | Use `ld.so --library-path` not `LD_LIBRARY_PATH`                                    |
| `rename` / atomic rename err | Lakehouse FUSE doesn't support rename | Fix Delta writer config — do NOT disable connectors (see CRITICAL rules above)      |

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

### 4e. (Optional) Verify local Docker tests still pass

If you changed `server.py` or `run.sh`, run the local Docker regression to make sure you didn't break the existing flow:

```bash
cd /workspaces/feldera/python/pyfeldera
.scripts/run.sh build-image
.scripts/run.sh dbt-test
```

This runs all 8 dbt integration tests (including Kafka/Delta) against the Docker customer image. It takes ~12 min.

---

## Step 5: Summarize your lessons learned and leave Breadcrumbs for Next Ralph

**Before emitting a completion signal, improve THIS skill file** (`/workspaces/feldera/.github/skills/ralph-feldera-fabric/skill.md`).

Update the instructions **inline throughout the document** — not as an appendix. For example:
- If you discovered a new failure pattern, add a row to the failure table in Step 4a
- If a timeout was too short, update the value in the relevant Step
- If a diagnostic command was missing or wrong, fix it in place
- If you found a better sequence of operations, reorder the Steps
- If a path or env var changed, update every reference to it

The goal is that the next Ralph reads a **better, more accurate** version of this skill — not a changelog at the bottom.

---

## Step 6: Iterate

Go back to **Step 1** — cancel the old notebook, start fresh (it will reinstall the updated wheel), wait for healthy, and re-run tests.

**Key:** Each iteration should fix at least one failure. If the same error persists after a fix, re-read the error carefully — you may have missed a path, env var, or symlink.

---

## Step 7: Completion Signal

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
