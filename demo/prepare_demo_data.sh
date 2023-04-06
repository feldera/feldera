#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

set -e

# We want non-matching wildcards to return an empty list
# instead of a non-expanded wildcard.
shopt -s nullglob

project_prefix=${THIS_DIR}/project_

for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}

    # TimeSeriesEnrich data is the same as we loaded in SimpleSelect
    if [ "$project_name" != "demo01-TimeSeriesEnrich" ]
    then
        python3  "${project_dir}/run.py" prepare
    fi
done
