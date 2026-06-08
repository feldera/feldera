#!/bin/bash
# Bring up a self-contained Feldera RBAC playground for inspection:
#   - the dummy OIDC issuer (browser login with a role picker)
#   - the pipeline-manager with auth enabled and owner@example.com as owner
#   - four provisioned users, one per role (read / write / admin / owner)
#
# Everything lives under an isolated demo directory, so it never touches your
# real ~/.feldera data. Stop with Ctrl-C.
#
# Usage:  scripts/rbac_up.sh [--rebuild] [--keep-db]
#   --rebuild   cargo build the manager first (otherwise uses target/debug)
#   --keep-db   keep the demo database between runs (default: wipe for a clean slate)
set -euo pipefail
cd "$(dirname "$0")/.."

DEMO="${FELDERA_RBAC_DEMO_DIR:-/tmp/feldera-rbac-demo}"
BIN="target/debug/pipeline-manager"
KEEP_DB=0
REBUILD=0
for a in "$@"; do
  case "$a" in
    --keep-db) KEEP_DB=1 ;;
    --rebuild) REBUILD=1 ;;
    *) echo "unknown arg: $a"; exit 1 ;;
  esac
done

if [ "$REBUILD" = 1 ] || [ ! -x "$BIN" ]; then
  echo "== building pipeline-manager (debug, with web console) =="
  PATH="$HOME/.bun/bin:$PATH" cargo build -p pipeline-manager --features feldera-enterprise
fi

[ "$KEEP_DB" = 1 ] || rm -rf "$DEMO/pg"
mkdir -p "$DEMO/pg" "$DEMO/runner" "$DEMO/compiler"

OIDC_PID=""
MGR_PID=""
cleanup() { echo; echo "== shutting down =="; kill "$MGR_PID" "$OIDC_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "== starting dummy OIDC issuer (:9876) =="
uv run scripts/dummy_oidc.py >"$DEMO/oidc.log" 2>&1 &
OIDC_PID=$!
for _ in $(seq 1 30); do
  curl -sf http://localhost:9876/.well-known/openid-configuration >/dev/null 2>&1 && break
  sleep 0.5
done

echo "== starting pipeline-manager (:8080, auth on, owner=owner@example.com) =="
AUTH_PROVIDER=generic-oidc \
FELDERA_AUTH_CLIENT_ID=feldera \
FELDERA_AUTH_ISSUER=http://localhost:9876 \
FELDERA_AUTH_AUDIENCE=feldera-api \
FELDERA_OWNERS=owner@example.com \
FELDERA_UNSTABLE_FEATURES='runtime_version,testing' \
  "$BIN" \
    --pg-embed-working-directory="$DEMO/pg" \
    --runner-working-directory="$DEMO/runner" \
    --compiler-working-directory="$DEMO/compiler" \
    >"$DEMO/manager.log" 2>&1 &
MGR_PID=$!

echo -n "   waiting for the API"
up=0
for _ in $(seq 1 180); do
  if curl -sf http://localhost:8080/healthz >/dev/null 2>&1; then up=1; break; fi
  kill -0 "$MGR_PID" 2>/dev/null || { echo " -- manager exited:"; tail -20 "$DEMO/manager.log"; exit 1; }
  echo -n "."; sleep 1
done
echo
[ "$up" = 1 ] || { echo "manager did not come up; see $DEMO/manager.log"; exit 1; }

echo "== provisioning one user per role =="
uv run scripts/rbac_demo.py --manager http://localhost:8080 --oidc http://localhost:9876 || true

cat <<EOF

========================================================================
 Open the web console:   http://localhost:8080
   It redirects to the dummy login where you PICK A ROLE
   (reader / writer / admin / owner). The Admin UI lives under the
   profile menu -> Admin (visible for admin and owner).
 Tokens for CLI/curl are printed above (READ / WRITE / ADMIN / OWNER).
 Logs: $DEMO/manager.log  and  $DEMO/oidc.log
 Press Ctrl-C to stop everything.
========================================================================
EOF

wait
