#!/usr/bin/env bash

set -ex

TESTS=(
    "aggregate_tests/main.py"
    "aggregate_tests2/main.py"
    "aggregate_tests3/main.py"
    "aggregate_tests4/main.py"
    "aggregate_tests5/main.py"
    "aggregate_tests6/main.py"
    "arithmetic_tests/main.py"
    "asof_tests/main.py"
    "complex_type_tests/main.py"
    "illarg_tests/main.py"
    "negative_tests/main.py"
    "orderby_tests/main.py"
    "variant_tests/main.py"
)

function run_one() {
    uv run --locked $PYTHONPATH/tests/runtime_aggtest/$1
}

if [ "${RUNTIME_AGGTEST_JOBS:-1}" -le 1 ]; then
  for t in "${TESTS[@]}"; do run_one "$t"; done
else
  echo "Running tests in parallel: ${RUNTIME_AGGTEST_JOBS} jobs"
  printf '%s\n' "${TESTS[@]}" | xargs -P "${RUNTIME_AGGTEST_JOBS}" -I{} bash -e -c 'echo "Running: {}"; uv run --locked "$PYTHONPATH/tests/runtime_aggtest/{}"'
fi