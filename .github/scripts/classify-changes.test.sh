#!/usr/bin/env bash
# Tests for classify-changes.sh. Each case feeds a `git diff --name-status`
# listing through --files and checks the outputs. Run from anywhere.
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/classify-changes.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failures=0

# run <name> <enforce> <expected outputs, space separated key=value> <<< diff
run() {
  local name=$1 enforce=$2 expected=$3 diff
  diff=$(cat)
  printf '%s\n' "$diff" > "$tmp/diff"
  : > "$tmp/out"
  if ! GITHUB_OUTPUT="$tmp/out" GITHUB_STEP_SUMMARY=/dev/null \
      "$script" --enforce "$enforce" --files "$tmp/diff"; then
    echo "FAIL ${name}: classifier exited non-zero" >&2
    failures=$((failures + 1))
    return
  fi
  local kv
  for kv in $expected; do
    if ! grep -qx "$kv" "$tmp/out"; then
      echo "FAIL ${name}: expected ${kv}, got:" >&2
      sed 's/^/    /' "$tmp/out" >&2
      failures=$((failures + 1))
      return
    fi
  done
  echo "ok   ${name}"
}

run "docs only" true "docs=true web_console=false all=false enforced=true" <<'EOF'
M	docs.feldera.com/docs/sql/functions.md
A	docs/design/new-thing.md
M	README.md
D	docs.feldera.com/docs/old-page.md
M	python/docs/conf.py
EOF

run "frontend only" true "docs=false web_console=true all=false" <<'EOF'
M	js-packages/web-console/src/lib/components/Foo.svelte
M	js-packages/profiler-lib/src/index.ts
M	bun.lock
M	package.json
EOF

run "frontend and docs" true "docs=true web_console=true all=false" <<'EOF'
M	js-packages/web-console/src/routes/+page.svelte
M	docs.feldera.com/docs/ui.md
EOF

run "rust change" true "all=true" <<'EOF'
M	crates/dbsp/src/lib.rs
EOF

run "frontend plus rust" true "web_console=true all=true" <<'EOF'
M	js-packages/web-console/src/app.ts
M	crates/pipeline-manager/src/main.rs
EOF

run "deleted markdown outside docs" true "all=true" <<'EOF'
D	crates/dbsp/README.md
EOF

run "openapi.json" true "all=true" <<'EOF'
M	openapi.json
EOF

run "workflow change" true "all=true" <<'EOF'
M	.github/workflows/ci.yml
EOF

run "nested js-packages-like path elsewhere" true "all=true" <<'EOF'
M	crates/pipeline-manager/js-packages/x.ts
EOF

run "empty diff" true "all=true" <<'EOF'
EOF

run "observe mode keeps buckets but forces all" false "docs=true web_console=false all=true enforced=false" <<'EOF'
M	docs.feldera.com/docs/intro.md
EOF

# Not a merge group: --base/--head mode with another event name.
: > "$tmp/out"
if GITHUB_OUTPUT="$tmp/out" GITHUB_STEP_SUMMARY=/dev/null GITHUB_EVENT_NAME=schedule \
    "$script" --enforce true --base HEAD --head HEAD && grep -qx "all=true" "$tmp/out"; then
  echo "ok   scheduled run forces all"
else
  echo "FAIL scheduled run forces all" >&2
  failures=$((failures + 1))
fi

if [ $failures -gt 0 ]; then
  echo "${failures} failure(s)" >&2
  exit 1
fi
echo "all classify-changes tests passed"
