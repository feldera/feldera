#!/usr/bin/env bash

set -ex

function runtest() {
    uv run --locked $PYTHONPATH/tests/runtime_aggtest/$1
}

runtest aggregate_tests/main.py
runtest aggregate_tests2/main.py
runtest aggregate_tests3/main.py
runtest aggregate_tests4/main.py
runtest arithmetic_tests/main.py
runtest complex_type_tests/main.py
runtest orderby_tests/main.py
runtest variant_tests/main.py
runtest illarg_tests/main.py
runtest negative_tests/main.py
