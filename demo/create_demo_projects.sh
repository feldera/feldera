#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${THIS_DIR}/../"
SERVER_DIR="${ROOT}/crates/pipeline_manager/"


set -e

# We want non-matching wildcards to return an empty list
# instead of a non-expanded wildcard.
shopt -s nullglob

project_prefix=${THIS_DIR}/project_

# for each demo directory
for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}
    printf "Creating demo '${project_name}'... "
    python3 "${project_dir}/run.py" compile create
done
