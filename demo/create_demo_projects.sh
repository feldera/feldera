#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${THIS_DIR}/../"
SERVER_DIR="${ROOT}/crates/pipeline_manager/"

export REDPANDA_BROKERS=localhost:19092

set -e

# We want non-matching wildcards to return an empty list
# instead of a non-expanded wildcard.
shopt -s nullglob

project_prefix=${THIS_DIR}/project_

# for each demo directory
for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}

    printf "Creating project '${project_name}'... "
    project_id=$("${SERVER_DIR}/scripts/new_project.sh" "${project_name}" < "${project_dir}/project.sql" | jq '.project_id')
    printf "OK(project_id=$project_id)\n"

    for config_file in "${project_dir}/"*.yaml
    do
        config_name=$(basename "${config_file}" .yaml)
        printf "  Adding project config '${config_name}'... "
        config_id=$("${SERVER_DIR}/scripts/new_config.sh" "${project_id}" "${config_name}"  < "${config_file}" | jq '.config_id')
        printf "OK(config_id=$config_id)\n"
    done
done

for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}

    if test -f "${project_dir}/prepare.sh"; then
        echo "  Running custom project setup script"
        "${project_dir}"/prepare.sh
    fi
done
